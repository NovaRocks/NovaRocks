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

//! The serial owner of the frozen attempt's protocol components.

mod abort;
mod candidates;
mod diagnostic_drive;
mod round_tail;
mod stop;
mod subscriptions;
mod turn;
use round_tail::RoundTail;
use subscriptions::{SubscriptionStep, Subscriptions};
pub use turn::{AttemptStep, InputSelection};

use crate::{
    authorization::gate::{AuthorizationGate, AuthorizationStep, ObserverStep, QaReply},
    capacity::StepContext,
    convergence::RemoteConvergence,
    diagnostics::controller::DiagnosticCollector,
    dispatch::{BatchDispatch, BatchDispatchPass},
    effect::{AttemptEffect, EffectBody, OperationIntent},
    error::TaskExecutionError,
    execution::{QueryTaskExecution, UnsentOwner, ack_phase::NativeAckPhase},
    failure::AttemptFailure,
    graph::TaskGraph,
    ids::OperationIds,
    input::{
        AttemptInputId, AttemptSourceCut, AttemptSourceId, ExecutorFact, ObservationBatch,
        ObservationEvent, ObservationFact, OperationObservation, PublicationApplied,
        ReceivedObservation,
    },
    lease::RequestedLeaseDurations,
    observation::controller::ObservationDisposition,
    publication::{
        ProtocolPublication,
        protocol_cursor::{
            ProtocolPublicationCursor, PublicationApplyDisposition, PublicationProgress,
        },
    },
    root_control::{RootControl, SealSettlement},
    start::{AttemptSources, AttemptStart, FrozenAttemptPlan},
    terminal::TerminalController,
};
use novarocks_coordination_contract::{
    AttemptActionId, AttemptActivationIdentity, AttemptRequestToken, AttemptRootFailure,
    AttemptRootProjection, LogicalToAttempt, LogicalToAttemptBody, MonotonicInstant,
};
use novarocks_execution_contract::{ParticipationRef, TaskOperationId};
use std::collections::BTreeMap;

#[derive(Debug)]
pub struct Core {
    activation: AttemptActivationIdentity,
    sources: AttemptSources,
    turn: turn::Turn,
    maximum_send_members: usize,
    observation_source: AttemptSourceId,
    execution: QueryTaskExecution,
    acknowledgements: NativeAckPhase,
    subscriptions: Subscriptions,
    root: RootControl,
    publication: ProtocolPublicationCursor,
    terminal: TerminalController,
    convergence: RemoteConvergence,
    diagnostics: diagnostic_drive::DiagnosticDrive,
    authorizations: AuthorizationGate,
    dispatch: BatchDispatchPass,
    round_failure: Option<AttemptFailure>,
    round_tail: RoundTail,
    stop: stop::StopWithdrawals,
    aborts: abort::Aborts,
    candidates: candidates::Candidates,
    establish_material: BTreeMap<ParticipationRef, crate::start::FrozenEstablish>,
    retry: crate::config::TaskUpdateRetryPolicy,
    pumps: Box<[(crate::start::PayloadRef, crate::effect::PumpKind)]>,
    writers: std::collections::BTreeSet<novarocks_execution_contract::TaskIdentity>,
    summary_sources: novarocks_result_contract::PerKind<
        std::collections::BTreeSet<novarocks_execution_contract::TaskIdentity>,
    >,
}

#[derive(Debug)]
pub(crate) enum AckSettlementProgress {
    Yield,
    Waiting,
    Request(AttemptEffect),
    Consumed { retire: Option<TaskOperationId> },
    Complete(Result<(), AttemptFailure>),
}

/// Preserve the business projection selected at the same serial cut as the
/// seal. A later frame in this batch may advance diagnostics or convergence.
#[derive(Debug)]
pub(crate) struct SealedRootCut {
    pub(crate) root_before_seal: Option<AttemptRootProjection>,
    pub(crate) settlement: SealSettlement,
}

#[derive(Debug)]
struct ObservationPass {
    items: std::vec::IntoIter<ReceivedObservation>,
    pending_quiesce: Option<ParticipationRef>,
    sealed: Option<SealedRootCut>,
    first_error: Option<TaskExecutionError>,
}

#[derive(Debug)]
pub(crate) struct RootControlApplication {
    pub(crate) seal: Option<SealedRootCut>,
    pub(crate) terminal_control: Option<(AttemptRequestToken, Result<(), TaskExecutionError>)>,
}

impl Core {
    pub(crate) fn new(start: AttemptStart) -> Result<Self, AttemptFailure> {
        Self::from_start(start).map_err(|error| AttemptFailure::TaskProtocol(Box::new(error)))
    }

    fn from_start(start: AttemptStart) -> Result<Self, TaskExecutionError> {
        let AttemptStart {
            activation,
            plan,
            ids,
            initial_now,
            sources,
            observe_dev_root,
        } = start;
        let FrozenAttemptPlan {
            graph,
            authorization_limits,
            writers,
            summary_sources,
            compatibility,
            admission_epochs,
            preparation_positions,
            deployment_window,
            establish_material,
            coordination,
            transport,
            retry,
            pumps,
            initial_lease,
            steady_lease,
        } = plan;
        let AttemptSources {
            actor,
            publication: publication_source,
            qa,
            process,
            native,
            observation,
            mailbox,
        } = sources;
        let distinct = [actor, publication_source, qa, process, native, observation];
        let graph = TaskGraph::from_frozen(graph)?;
        if distinct
            .iter()
            .enumerate()
            .any(|(index, source)| distinct[..index].contains(source))
            || authorization_limits.activation != activation.identity
            || deployment_window.get() != coordination.dispatch.create_permits()
            || !establish_material.keys().eq(graph.participations())
        {
            return Err(schedule(
                "attempt registration differs from the frozen protocol plan",
            ));
        }
        let mut split_sources = pumps.iter().filter_map(|(source, kind)| {
            matches!(kind, crate::effect::PumpKind::SplitAssignment).then_some(*source)
        });
        let split_source = split_sources.next();
        if split_sources.next().is_some() {
            return Err(schedule("more than one original split profile Source"));
        }
        let mut dynamic_sources = pumps.iter().filter_map(|(source, kind)| {
            matches!(kind, crate::effect::PumpKind::DynamicFilters).then_some(*source)
        });
        let dynamic_source = dynamic_sources.next();
        if dynamic_sources.next().is_some()
            || dynamic_source.is_some() && dynamic_source == split_source
        {
            return Err(schedule(
                "dynamic filter feedback differs from its original Source",
            ));
        }
        let authorizations = AuthorizationGate::new(
            activation.identity,
            authorization_limits,
            graph.participations().copied(),
        )?;
        let root = RootControl::new(activation.identity, &graph)?;
        let publication = ProtocolPublicationCursor::new(&graph);
        let convergence = RemoteConvergence::new(&graph);
        let diagnostics = DiagnosticCollector::new(
            activation.identity,
            graph.root_identity(),
            activation.policy.retain_diagnostics,
            graph.tasks().map(|task| task.identity()),
        );
        let subscriptions = Subscriptions::new(
            graph.participations().copied(),
            ids,
            coordination.status_subscription_error_budget,
        )?;
        let preparing_positions = preparation_positions
            .into_iter()
            .map(|(process, positions)| (process, positions.get()))
            .collect();
        let mut execution = QueryTaskExecution::new_with_split_profile_source(
            graph,
            coordination.dispatch,
            transport,
            compatibility,
            &admission_epochs,
            &preparing_positions,
            initial_now,
            OperationIds::new(ids),
            RequestedLeaseDurations::new(initial_lease, steady_lease),
            split_source,
        )?;
        if !activation.inherited_admissions.is_empty() {
            execution.adopt_replacement_admissions(&activation.inherited_admissions)?;
        }
        execution.activate_covered_observation();
        execution.bind_diagnostic_collector(diagnostics);
        execution.bind_split_pump(split_source);
        execution.bind_dynamic_feedback(dynamic_source);
        let acknowledgements = NativeAckPhase::new(native, mailbox)
            .map_err(|_| schedule("invalid Native acknowledgement mailbox geometry"))?;
        Ok(Self {
            activation: activation.identity,
            sources,
            turn: turn::Turn::new(observe_dev_root),
            maximum_send_members: transport.max_batch_items,
            observation_source: observation,
            execution,
            acknowledgements,
            subscriptions,
            root,
            publication,
            terminal: TerminalController::new(),
            convergence,
            diagnostics: diagnostic_drive::DiagnosticDrive::default(),
            authorizations,
            dispatch: BatchDispatchPass::new(),
            round_failure: None,
            round_tail: RoundTail::default(),
            stop: stop::StopWithdrawals::default(),
            aborts: abort::Aborts::new(authorization_limits.abort_per_participation),
            candidates: candidates::Candidates::default(),
            establish_material,
            retry,
            pumps,
            writers,
            summary_sources,
        })
    }

    /// Called after pending split input and already requested close are advanced.
    pub(crate) fn begin_native_prefix(
        &mut self,
        now: MonotonicInstant,
        cut: AttemptSourceCut,
    ) -> Result<(), TaskExecutionError> {
        if self.authorizations.waiting_for_reply()
            || self.publication.has_pending_apply()
            || self.subscriptions.assembling()
            || !self.round_tail.is_idle()
        {
            return Err(schedule(
                "Native acknowledgement crossed an outstanding actual reply",
            ));
        }
        self.execution.set_now(now)?;
        self.acknowledgements
            .begin(cut)
            .map_err(|_| schedule("invalid Native acknowledgement cut"))
    }

    pub(crate) fn apply_native(
        &mut self,
        now: MonotonicInstant,
        source: AttemptSourceId,
        input: AttemptInputId,
        fact: &ExecutorFact,
        observe_abort: impl FnOnce(
            &mut QueryTaskExecution,
            &OperationObservation,
        ) -> Result<(), TaskExecutionError>,
    ) -> Result<Option<EffectBody>, TaskExecutionError> {
        self.execution.set_now(now)?;
        if self.acknowledgements.selected_source() == Some(source)
            && let ExecutorFact::Operation(observation) = fact
            && observation.reply.kind()
                == novarocks_execution_contract::OperationKind::AcquireParticipationAdmissionTicket
        {
            self.authorizations.note_admission_physical_result(
                observation.reply.operation_id(),
                observation.generation,
            );
        }
        let participation = match fact {
            ExecutorFact::Operation(observation)
                if observation.reply.kind()
                    == novarocks_execution_contract::OperationKind::UpdateParticipation =>
            {
                self.execution
                    .dispatched_participation(observation.reply.operation_id())
                    .or_else(|| self.execution.prior_lifecycle_participation(observation))
            }
            _ => None,
        };
        let split_reply = match fact {
            ExecutorFact::Operation(observation)
                if observation.reply.kind()
                    == novarocks_execution_contract::OperationKind::UpdateTask
                    && self.execution.dispatched_reply_needs_observer(observation) =>
            {
                Some(&observation.reply)
            }
            _ => None,
        };
        let action = self
            .acknowledgements
            .apply(source, input, fact, &mut self.execution, observe_abort)
            .map_err(|_| schedule("invalid fact in the Native acknowledgement prefix"))?;
        if let Some(reply) = split_reply {
            self.execution.note_split_reply(reply);
        }
        if let Some(participation) = participation
            && self.execution.needs_covered_subscription(participation)
        {
            self.subscriptions.activate(now, participation)?;
        }
        Ok(action)
    }

    /// Called at the original dispatch point. All members are priced before
    /// reservation; the queued batch keeps its grants if authorization fails.
    pub(crate) fn advance_dispatch(
        &mut self,
        effect: AttemptActionId,
        context: &mut dyn StepContext,
    ) -> Result<BatchDispatch, TaskExecutionError> {
        if self.acknowledgements.run_round_tail() != Ok(true)
            || !self.permits_residual_protocol()
            || self.subscriptions.assembling()
        {
            return Err(schedule("dispatch crossed an unfinished round phase"));
        }
        let progress =
            self.execution
                .advance_send_batch(&mut self.dispatch, effect, context, |intent| {
                    if self.stop.business_closed() && !intent.requires_control_progress() {
                        return Err(schedule("ordinary Native send crossed the original stop"));
                    }
                    self.authorizations.validate_send(intent)
                })?;
        if let BatchDispatch::Ready(AttemptEffect {
            body: EffectBody::SendBatch { batch, .. },
            ..
        }) = &progress
        {
            for (intent, grant) in batch.operations.iter().zip(&batch.queued_inputs) {
                self.authorizations.note_sent(intent, grant.generation());
                self.execution.note_split_released(intent.operation_id());
            }
        }
        Ok(progress)
    }

    /// The adapter charged the full bounded primary withdrawal before
    /// calling. Keep folding the original members after the first error.
    pub(crate) fn apply_transport_withdrawal(
        &mut self,
        now: MonotonicInstant,
        fact: &ExecutorFact,
        mut settle_abort: impl FnMut(TaskOperationId) -> Result<(), TaskExecutionError>,
    ) -> Result<(), TaskExecutionError> {
        self.execution.set_now(now)?;
        let (effect, outcome) = match *fact {
            ExecutorFact::Withdrawn { effect, outcome } => (effect, outcome),
            ExecutorFact::QueuedInputWithdrawn {
                operation,
                generation,
            } => {
                if !self
                    .execution
                    .close_lifecycle_admission(operation, generation)
                {
                    self.execution.close_create_admission(operation, generation);
                }
                return Ok(());
            }
            _ => return Err(schedule("transport withdrawal received an unrelated fact")),
        };
        let Some(members) = self
            .execution
            .begin_send_withdrawal_result(effect, outcome)?
        else {
            return Ok(());
        };
        let mut first_error = None;
        for member in members.iter().copied() {
            if member.shape
                == novarocks_execution_contract::OperationShape::AcquireParticipationAdmissionTicket
            {
                self.authorizations
                    .note_admission_physical_result(member.operation, member.generation);
            }
            let result =
                self.execution
                    .settle_unsent_member(member)
                    .and_then(|owner| match owner {
                        UnsentOwner::ActorAbort => settle_abort(member.operation),
                        UnsentOwner::Establish(participation) => {
                            self.authorizations.note_establish_withdrawn(
                                member.operation,
                                participation,
                                member.generation,
                            )
                        }
                        UnsentOwner::Local => Ok(()),
                    });
            if let Err(error) = result {
                first_error.get_or_insert(error);
            }
        }
        first_error.map_or(Ok(()), Err)
    }

    fn permits_residual_protocol(&self) -> bool {
        self.publication.permits_protocol_turn()
            || (self.stop.business_closed() && self.publication.failed())
    }

    fn verify_common_observation_phase(
        &self,
        source: AttemptSourceId,
    ) -> Result<(), TaskExecutionError> {
        if source != self.observation_source {
            return Err(schedule("ordered observation used a foreign Source"));
        }
        if self.acknowledgements.run_round_tail() != Ok(true)
            || !self.permits_residual_protocol()
            || self.subscriptions.assembling()
            || !self.round_tail.is_idle()
        {
            return Err(schedule(
                "ordered observation crossed an unfinished round phase",
            ));
        }
        Ok(())
    }

    /// Keep the bounded observation claim at its original position. Charge
    /// each frame and derived membership row on the turn that applies it.
    fn begin_observations(
        &mut self,
        now: MonotonicInstant,
        source: AttemptSourceId,
        batch: ObservationBatch,
    ) -> Result<ObservationPass, TaskExecutionError> {
        self.verify_common_observation_phase(source)?;
        self.execution.set_now(now)?;
        Ok(ObservationPass {
            items: batch.into_items().into_vec().into_iter(),
            pending_quiesce: None,
            sealed: None,
            first_error: None,
        })
    }

    fn advance_observations(
        &mut self,
        now: MonotonicInstant,
        pass: &mut ObservationPass,
        context: &mut dyn StepContext,
    ) -> Option<Option<SealedRootCut>> {
        loop {
            let result = if let Some(participation) = pass.pending_quiesce {
                let completed = self.execution.advance_quiesce(context)?;
                pass.pending_quiesce = None;
                completed.and_then(|()| self.finish_observation(now, participation, true))
            } else {
                if pass.items.len() == 0 {
                    break;
                }
                if !context.charge(1) {
                    return None;
                }
                let observation = pass.items.next().expect("original observation frame");
                let participation = observation.participation;
                match self.apply_observation_item(now, observation) {
                    Ok(()) if self.execution.has_pending_quiesce() => {
                        pass.pending_quiesce = Some(participation);
                        continue;
                    }
                    result => result,
                }
            };
            if let Err(error) = result {
                pass.first_error.get_or_insert(error);
            }
            if pass.first_error.is_none()
                && let Some(seal) = self.evaluate_pending_seal()
            {
                pass.sealed = Some(seal);
            }
        }
        if let Some(error) = pass.first_error.take() {
            self.acknowledgements
                .fail_round_tail(error)
                .expect("an observation batch was admitted at the round tail");
        }
        Some(pass.sealed.take())
    }

    fn apply_observation_item(
        &mut self,
        now: MonotonicInstant,
        observation: ReceivedObservation,
    ) -> Result<(), TaskExecutionError> {
        let participation = observation.participation;
        let convergence_changed = matches!(
            &observation.event,
            ObservationEvent::Fact {
                fact: ObservationFact::TaskConvergence(_)
                    | ObservationFact::ParticipationConvergence(_),
                ..
            }
        );
        if let ObservationDisposition::ApplyEntity(accepted) =
            self.subscriptions.observe(now, observation)?
        {
            let report = self.execution.apply_covered_observation(accepted)?;
            if !self.execution.has_pending_quiesce() {
                self.finish_observation(
                    now,
                    participation,
                    report.accepted != 0 || convergence_changed,
                )?;
            }
        }
        Ok(())
    }

    fn finish_observation(
        &mut self,
        now: MonotonicInstant,
        participation: ParticipationRef,
        changed: bool,
    ) -> Result<(), TaskExecutionError> {
        if self.execution.needs_covered_subscription(participation) {
            if changed {
                self.subscriptions.applied(participation);
            }
        } else if self
            .execution
            .owner(participation)
            .is_some_and(crate::participation_owner::ParticipationOwner::establish_acknowledged)
        {
            self.subscriptions.stop(now, participation)?;
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn apply_observations(
        &mut self,
        now: MonotonicInstant,
        source: AttemptSourceId,
        batch: ObservationBatch,
    ) -> Result<Option<SealedRootCut>, TaskExecutionError> {
        struct Unlimited;
        impl StepContext for Unlimited {
            fn charge(&mut self, _: usize) -> bool {
                true
            }
            fn reserve_queue_stage(
                &mut self,
                _: crate::config::DispatchLane,
                _: bool,
                _: novarocks_types::BackendProcessId,
                _: usize,
                _: usize,
            ) -> crate::capacity::QueueStage {
                panic!("common observation cannot reserve transport")
            }
        }
        let mut pass = self.begin_observations(now, source, batch)?;
        Ok(self
            .advance_observations(now, &mut pass, &mut Unlimited)
            .expect("unlimited observation fixture budget"))
    }

    /// Read confirmation has its own prepaid source, independent of the
    /// ordered frame and root-control intake.
    pub(crate) fn apply_subscription_input(
        &mut self,
        now: MonotonicInstant,
        fact: &ExecutorFact,
    ) -> Result<(), TaskExecutionError> {
        self.execution.set_now(now)?;
        self.subscriptions.apply_independent(now, fact)
    }

    pub(crate) fn next_subscription_deadline(&self) -> Option<MonotonicInstant> {
        let now = self.execution.monotonic_now();
        if self.execution.has_subscription_reconciliation() {
            return Some(now);
        }
        self.subscriptions.next_deadline(now)
    }

    pub(crate) fn advance_subscription(
        &mut self,
        now: MonotonicInstant,
        effect: AttemptActionId,
        token: AttemptRequestToken,
        context: &mut dyn StepContext,
    ) -> Result<SubscriptionStep, TaskExecutionError> {
        if self.acknowledgements.run_round_tail() != Ok(true) || !self.permits_residual_protocol() {
            return Err(schedule(
                "subscription drive crossed an unfinished round phase",
            ));
        }
        self.execution.set_now(now)?;
        while !self.subscriptions.assembling() && self.execution.has_subscription_reconciliation() {
            if !context.charge(1) {
                return Ok(SubscriptionStep::Yield);
            }
            let participation = self
                .execution
                .pop_subscription_reconciliation()
                .expect("original reconciliation row");
            if self.execution.needs_covered_subscription(participation) {
                self.subscriptions.reconcile(now, participation)?;
            }
        }
        self.subscriptions
            .advance(&self.execution, now, effect, token, context)
    }

    /// A host may acknowledge the original root-control claim only after
    /// this first decision, including when the same seal remains pending.
    pub(crate) fn apply_root_control(
        &mut self,
        now: MonotonicInstant,
        source: AttemptSourceId,
        input: LogicalToAttempt<AttemptFailure>,
    ) -> Result<RootControlApplication, TaskExecutionError> {
        self.verify_common_observation_phase(source)?;
        if input.activation != self.activation {
            return Err(schedule("root control used a foreign activation"));
        }
        self.execution.set_now(now)?;
        let terminal_control = match input.body {
            LogicalToAttemptBody::SealRequest { token } => {
                self.root.request(token)?;
                None
            }
            LogicalToAttemptBody::TerminalControlDemand { token, root } => {
                Some((token, self.execution.require_result_terminal_control(root)))
            }
            _ => {
                return Err(schedule(
                    "ordered root-control Source received an unrelated port message",
                ));
            }
        };
        Ok(RootControlApplication {
            seal: self.evaluate_pending_seal(),
            terminal_control,
        })
    }

    /// Called at the old round's publication point after the Root effects.
    pub(crate) fn select_protocol_publication(&mut self) -> Result<bool, TaskExecutionError> {
        if self.acknowledgements.run_round_tail() != Ok(true) || self.subscriptions.assembling() {
            return Err(schedule(
                "protocol publication crossed an unfinished round phase",
            ));
        }
        self.publication
            .select(&mut self.execution, self.root.sealed())
    }

    pub(crate) fn advance_protocol_publication(
        &mut self,
        context: &mut dyn StepContext,
    ) -> Result<PublicationProgress, TaskExecutionError> {
        self.publication.advance(&self.execution, context)
    }

    /// Take only once the host can accept the publication and its result.
    pub(crate) fn take_protocol_publication(
        &mut self,
        effect: AttemptActionId,
    ) -> Option<ProtocolPublication> {
        self.publication.take_ready(effect)
    }

    pub(crate) fn protocol_publication_applied(
        &mut self,
        effect: AttemptActionId,
        result: Result<PublicationApplied, AttemptFailure>,
    ) -> PublicationApplyDisposition {
        let disposition = match result {
            Ok(PublicationApplied::ProtocolBatch { published }) => {
                self.publication.applied(effect, Ok(published))
            }
            Err(failure) => self.publication.applied(effect, Err(failure)),
            Ok(_) => PublicationApplyDisposition::Stale,
        };
        if let PublicationApplyDisposition::Failed(failure) = &disposition {
            self.round_failure.get_or_insert_with(|| failure.clone());
        }
        disposition
    }

    /// The caller has run the old round tail or reached its early-error path.
    /// A selected publication still awaits its actual result, even if its
    /// effect has already completed handoff and the original job has exited.
    pub(crate) fn begin_ack_settlement(
        &mut self,
        round_failure: Option<AttemptFailure>,
    ) -> Result<(), TaskExecutionError> {
        if self.publication.has_pending_apply()
            || self.authorizations.waiting_for_reply()
            || self.subscriptions.assembling()
        {
            return Err(schedule(
                "QA settlement crossed an outstanding actual reply",
            ));
        }
        self.acknowledgements
            .begin_settlement(Ok(()))
            .map_err(|_| schedule("QA settlement crossed the original round tail"))?;
        if self.round_failure.is_none() {
            self.round_failure = round_failure;
        }
        Ok(())
    }

    /// Advance one charged journal row or one selected Establish guard. No
    /// call is issued before the adapter can commit the outbound effect.
    pub(crate) fn advance_ack_settlement(
        &mut self,
        token: AttemptRequestToken,
        action: AttemptActionId,
        context: &mut dyn StepContext,
    ) -> Result<AckSettlementProgress, TaskExecutionError> {
        let debt = self
            .acknowledgements
            .current()
            .map_err(|_| schedule("QA settlement has no original acknowledgement cut"))?;
        if self.authorizations.waiting_for_reply() {
            return Ok(AckSettlementProgress::Waiting);
        }
        let Some(debt) = debt else {
            let result = self
                .acknowledgements
                .finish()
                .map_err(|_| schedule("QA settlement still has an observed acknowledgement"))?;
            let failure = result
                .err()
                .map(|error| AttemptFailure::TaskProtocol(Box::new(error)))
                .or(self.round_failure.take())
                .or(self.authorizations.take_observer_failure());
            return Ok(AckSettlementProgress::Complete(failure.map_or(Ok(()), Err)));
        };
        if !context.charge(1) {
            return Ok(AckSettlementProgress::Yield);
        }
        match self
            .authorizations
            .settle_observation(debt, token, action)?
        {
            ObserverStep::Request(effect) => Ok(AckSettlementProgress::Request(effect)),
            ObserverStep::Consumed { retire } => {
                self.complete_observer_row()?;
                Ok(AckSettlementProgress::Consumed { retire })
            }
            ObserverStep::Waiting => Ok(AckSettlementProgress::Waiting),
        }
    }

    /// One candidate at the original authorization point after observer
    /// settlement. The candidate driver retains the immutable intent.
    pub(crate) fn authorize_candidate(
        &mut self,
        intent: &OperationIntent,
        token: AttemptRequestToken,
        action: AttemptActionId,
    ) -> Result<AuthorizationStep, TaskExecutionError> {
        if self.stop.business_closed() {
            return Ok(AuthorizationStep::Retired);
        }
        if !self.acknowledgements.is_idle() || self.publication.has_pending_apply() {
            return Ok(AuthorizationStep::Waiting);
        }
        self.authorizations.authorize(intent, token, action)
    }

    pub(crate) fn apply_qa_reply(
        &mut self,
        input: LogicalToAttempt<AttemptFailure>,
    ) -> Result<QaReply, TaskExecutionError> {
        if input.activation != self.activation {
            return Ok(QaReply::Stale);
        }
        let reply = self.authorizations.replied(input.body)?;
        if matches!(reply, QaReply::ObserverComplete { .. }) {
            self.complete_observer_row()?;
        }
        Ok(reply)
    }

    fn complete_observer_row(&mut self) -> Result<(), TaskExecutionError> {
        self.acknowledgements
            .settled_current(Ok(()))
            .map_err(|_| schedule("QA completed without a selected original acknowledgement"))
    }

    fn evaluate_pending_seal(&mut self) -> Option<SealedRootCut> {
        if self.stop.business_closed() {
            return self
                .root
                .refuse(crate::root_control::RootSealRefusal::AttemptCleanup)
                .map(|settlement| SealedRootCut {
                    root_before_seal: None,
                    settlement,
                });
        }
        let settlement = self.root.evaluate(self.execution.root_seal_facts(false))?;
        let root_before_seal = if settlement.result.is_ok() {
            let projection = self.root_projection();
            self.execution.begin_normal_drain();
            projection
        } else {
            None
        };
        Some(SealedRootCut {
            root_before_seal,
            settlement,
        })
    }

    fn root_projection(&self) -> Option<AttemptRootProjection> {
        let root = self.execution.graph().root_identity();
        let task = self.execution.task(root.task_id())?;
        let status = task.status()?;
        if !task.installed() && !status.is_terminal() {
            return None;
        }
        let failure = match self.execution.selected_task_failure() {
            Some(selected) if selected.detail().is_derived() => AttemptRootFailure::DerivedPending,
            Some(selected) => AttemptRootFailure::Authoritative(selected.clone()),
            None => AttemptRootFailure::None,
        };
        Some(AttemptRootProjection {
            status: status.clone(),
            failure,
        })
    }
}

fn schedule(detail: &str) -> TaskExecutionError {
    TaskExecutionError::Schedule(detail.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        capacity::QueueStage,
        config::{CoordinationBudgets, DispatchLane, FrozenTransportLimits},
        ids::IdSeed,
        input::{ObservationOpening, ReceivedObservation},
        root_control::RootSealRefusal,
        start::{FrozenEstablish, PayloadRef},
    };
    use novarocks_coordination_contract::{
        AttemptActivation, AttemptAuthorizationLimits, AttemptCompletionPolicy,
        AttemptLifecyclePolicy,
    };
    use novarocks_execution_contract::{
        AbortCause, AdmissionEpochCapability, TaskIdentity, TaskOutputFacts, TaskState, TaskStatus,
        TaskStatusVersion, TerminationDetail,
    };
    use novarocks_types::{BackendProcessId, NativeCompatibilityId};
    use std::{
        num::{NonZeroU64, NonZeroUsize},
        time::Duration,
    };

    fn id(value: u64) -> NonZeroU64 {
        NonZeroU64::new(value).unwrap()
    }

    fn start() -> AttemptStart {
        let (graph, identity) = crate::test_support::frozen_graph_with_strict_input(false);
        let mut admission_epochs = BTreeMap::new();
        let mut preparation_positions = BTreeMap::new();
        let mut establish_material = BTreeMap::new();
        let coordination = CoordinationBudgets::DEFAULT;
        let deployment_window = NonZeroUsize::new(coordination.dispatch.create_permits()).unwrap();
        for (index, task) in graph.tasks.values().enumerate() {
            let backend = task.participation.backend_process_id();
            admission_epochs.insert(
                backend,
                AdmissionEpochCapability::try_from_bytes([0x61; 16]).unwrap(),
            );
            preparation_positions.insert(backend, deployment_window);
            establish_material.insert(
                task.participation,
                FrozenEstablish {
                    material: PayloadRef(id(index as u64 + 20)),
                    queued_bytes: 256,
                },
            );
        }
        AttemptStart {
            activation: AttemptActivation {
                identity,
                policy: AttemptLifecyclePolicy {
                    completion: AttemptCompletionPolicy::AcceptedRootSuccessSeal,
                    retain_diagnostics: true,
                },
                inherited_admissions: Box::new([]),
            },
            plan: FrozenAttemptPlan {
                graph,
                authorization_limits: AttemptAuthorizationLimits {
                    activation: identity,
                    establish_per_participation: NonZeroUsize::new(3).unwrap(),
                    abort_per_participation: NonZeroUsize::new(3).unwrap(),
                },
                writers: Default::default(),
                summary_sources: Default::default(),
                compatibility: NativeCompatibilityId::new([0x41; 32]),
                admission_epochs,
                preparation_positions,
                deployment_window,
                establish_material,
                coordination,
                transport: FrozenTransportLimits {
                    max_batch_items: 4,
                    max_batch_encoded_bytes: 4096,
                    max_descriptor_encoded_bytes: 4096,
                    max_query_backend_queued_operations: 1024,
                    max_query_backend_queued_bytes: 1_048_576,
                    max_backend_queued_operations: 4096,
                    max_backend_queued_bytes: 4_194_304,
                    max_tasks_per_participation: 1024,
                    max_active_tasks_per_backend: 1024,
                    frontend_queue_residence: Duration::from_secs(5),
                },
                retry: Default::default(),
                pumps: Box::new([]),
                initial_lease: Duration::from_secs(30),
                steady_lease: Duration::from_secs(5),
            },
            ids: IdSeed::new(1, 100),
            initial_now: MonotonicInstant::from_origin(Duration::from_secs(2)),
            observe_dev_root: false,
            sources: AttemptSources {
                actor: AttemptSourceId(id(3)),
                publication: AttemptSourceId(id(4)),
                qa: AttemptSourceId(id(5)),
                process: AttemptSourceId(id(6)),
                native: AttemptSourceId(id(7)),
                observation: AttemptSourceId(id(8)),
                mailbox: [4; 4],
            },
        }
    }

    fn round() -> Core {
        round_with_start(start())
    }

    fn round_with_start(start: AttemptStart) -> Core {
        let source = start.sources.native;
        let now = start.initial_now;
        let mut core = Core::new(start).unwrap();
        assert_eq!(core.execution.monotonic_now(), now);
        core.begin_native_prefix(now, AttemptSourceCut { source, tail: None })
            .unwrap();
        core
    }

    fn seal(core: &mut Core) -> Result<RootControlApplication, TaskExecutionError> {
        core.apply_root_control(
            core.execution.monotonic_now(),
            core.observation_source,
            LogicalToAttempt {
                activation: core.activation,
                body: LogicalToAttemptBody::SealRequest {
                    token: AttemptRequestToken::new(id(90)),
                },
            },
        )
    }

    fn frame(
        core: &mut Core,
        task: TaskIdentity,
        state: TaskState,
        termination: Option<TerminationDetail>,
    ) -> ReceivedObservation {
        let participation = core
            .execution
            .graph()
            .task(task.task_id())
            .unwrap()
            .participation();
        let now = core.execution.monotonic_now();
        core.subscriptions.activate(now, participation).unwrap();
        let SubscriptionStep::Effect(AttemptEffect {
            body: EffectBody::OpenSubscription(request),
            ..
        }) = core
            .advance_subscription(
                now,
                AttemptActionId::new(id(51)),
                AttemptRequestToken::new(id(52)),
                &mut Units(10),
            )
            .unwrap()
        else {
            panic!("expected the exact original subscription opening");
        };
        let generation = request.generation;
        ReceivedObservation {
            participation,
            generation,
            received_at: now,
            opening: ObservationOpening {
                started_at: now,
                opened_at: Some(now),
            },
            event: ObservationEvent::Fact {
                source_revision: None,
                fact: ObservationFact::Status(
                    TaskStatus::try_new(
                        task,
                        TaskStatusVersion::new(1).unwrap(),
                        state,
                        termination,
                        TaskOutputFacts::new(state == TaskState::Finished),
                    )
                    .unwrap(),
                ),
            },
        }
    }

    #[test]
    fn common_source_reconsiders_pending_seal_before_the_next_frame() {
        for failure_first in [false, true] {
            let mut core = round();
            assert!(seal(&mut core).unwrap().seal.is_none());
            let root = core.execution.graph().root_identity();
            let producer = core
                .execution
                .graph()
                .tasks()
                .find(|task| task.identity() != root)
                .unwrap()
                .identity();
            let finished = frame(&mut core, root, TaskState::Finished, None);
            let aborted = frame(
                &mut core,
                producer,
                TaskState::Aborted,
                Some(TerminationDetail::Aborted(AbortCause::QueryFailed)),
            );
            let frames = if failure_first {
                vec![aborted, finished]
            } else {
                vec![finished, aborted]
            };
            let cut = core
                .apply_observations(
                    core.execution.monotonic_now(),
                    core.observation_source,
                    ObservationBatch::new(frames).unwrap(),
                )
                .unwrap()
                .unwrap();

            assert_eq!(
                core.execution.failure_cause(),
                Some(&TerminationDetail::Aborted(AbortCause::QueryFailed))
            );
            if failure_first {
                assert_eq!(
                    cut.settlement.result,
                    Err(RootSealRefusal::TerminalWithoutSuccess)
                );
                assert!(cut.root_before_seal.is_none());
                assert!(!core.root.sealed());
            } else {
                let projection = cut.root_before_seal.unwrap();
                assert_eq!(projection.status.identity(), root);
                assert_eq!(projection.status.state(), TaskState::Finished);
                assert_eq!(projection.failure, AttemptRootFailure::None);
                assert!(cut.settlement.result.unwrap().is_empty());
                assert!(core.root.sealed());
                assert!(!core.execution.normal_drain_started());
            }
        }
    }

    #[test]
    fn actual_qa_replies_settle_all_original_debts_after_publication() {
        use crate::{
            effect::EffectClass,
            execution::ack_phase::{admission_intent, dispatch_admission},
            failure::{AttemptFailureSite, HostFailureKind, HostFailureRef, HostProjectedFailure},
        };
        use novarocks_coordination_contract::{
            AttemptAuthorization, AttemptAuthorizationDecision, AttemptAuthorizationFailure,
            AttemptAuthorizationRef, AttemptFailureClass, AttemptToLogicalBody,
        };
        use novarocks_execution_contract::OperationOutcome;
        let start = start();
        let native = start.sources.native;
        let now = start.initial_now;
        let mut core = Core::new(start).unwrap();
        let participants: Vec<_> = core.execution.graph().participations().copied().collect();
        let activation = core.activation;
        let token = |value| AttemptRequestToken::new(id(value));
        let action = |value| AttemptActionId::new(id(value));
        let issue = |value| AttemptAuthorizationRef::new(id(value));
        let reply = |value, decision| LogicalToAttempt {
            activation,
            body: LogicalToAttemptBody::AuthorizationReply {
                token: token(value),
                decision,
            },
        };
        let mut observations = Vec::new();
        for (index, participation) in participants.into_iter().take(2).enumerate() {
            let n = index as u64 + 1;
            let request = admission_intent(&mut core.execution, participation);
            let AuthorizationStep::Request(effect) = core
                .authorize_candidate(
                    &OperationIntent::AcquireParticipationAdmissionTicket(request),
                    token(n),
                    action(n),
                )
                .unwrap()
            else {
                panic!("expected one original admission authorization");
            };
            assert!(matches!(effect.body, EffectBody::Port(port)
                if matches!(port.body, AttemptToLogicalBody::AuthorizationRequest {
                    token: actual, request: AttemptAuthorization::BeginAdmission(request),
                } if actual == token(n) && request.participation() == participation)));
            assert!(matches!(
                core.apply_qa_reply(reply(n, Ok(AttemptAuthorizationDecision::AdmissionStarted(issue(n))))).unwrap(),
                QaReply::Authorized(operation) if operation == request.envelope().operation_id()
            ));
            observations.push(dispatch_admission(
                &mut core.execution,
                request,
                n,
                OperationOutcome::Accepted,
            ));
        }
        let tail = AttemptInputId(id(2));
        core.begin_native_prefix(
            now,
            AttemptSourceCut {
                source: native,
                tail: Some(tail),
            },
        )
        .unwrap();
        for (index, observation) in observations.iter().enumerate() {
            core.apply_native(
                now,
                native,
                AttemptInputId(id(index as u64 + 1)),
                &ExecutorFact::Operation(observation.clone()),
                |_, _| panic!("not an Abort receipt"),
            )
            .unwrap();
        }
        assert!(core.select_protocol_publication().unwrap());
        assert!(core.begin_ack_settlement(None).is_err());
        assert_eq!(
            core.advance_protocol_publication(&mut NoTraversal).unwrap(),
            PublicationProgress::Ready
        );
        let publication = action(40);
        assert!(core.take_protocol_publication(publication).is_some());
        assert!(core.begin_ack_settlement(None).is_err());
        core.protocol_publication_applied(
            publication,
            Ok(PublicationApplied::ProtocolBatch { published: true }),
        );
        core.begin_ack_settlement(None).unwrap();

        struct Charge(bool);
        impl StepContext for Charge {
            fn charge(&mut self, units: usize) -> bool {
                assert_eq!(units, 1);
                self.0
            }
            fn reserve_queue_stage(
                &mut self,
                _: DispatchLane,
                _: bool,
                _: BackendProcessId,
                _: usize,
                _: usize,
            ) -> QueueStage {
                panic!("QA settlement has no Native queue reservation")
            }
        }

        let original_failure = AttemptFailure::Host(HostProjectedFailure {
            original: HostFailureRef(id(55)),
            kind: HostFailureKind::LogicalActor,
            class: AttemptFailureClass::ExecutionFailure,
            site: AttemptFailureSite::Attempt,
        });
        assert!(matches!(
            core.advance_ack_settlement(token(10), action(10), &mut Charge(false))
                .unwrap(),
            AckSettlementProgress::Yield
        ));
        for (index, observation) in observations.iter().enumerate() {
            let n = index as u64 + 10;
            let expected_issue = issue(index as u64 + 1);
            let operation = observation.reply.operation_id();
            let AckSettlementProgress::Request(effect) = core
                .advance_ack_settlement(token(n), action(n), &mut Charge(true))
                .unwrap()
            else {
                panic!("expected original selected admission settlement");
            };
            assert_eq!(effect.class, EffectClass::Settlement);
            assert!(matches!(effect.body, EffectBody::Port(port)
                if matches!(port.body, AttemptToLogicalBody::AuthorizationRequest {
                    token: actual,
                    request: AttemptAuthorization::SettleAdmission {
                        issue, operation_id, outcome: Some(OperationOutcome::Accepted), ..
                    },
                } if actual == token(n) && issue == expected_issue && operation_id == operation)));
            assert!(matches!(
                core.apply_qa_reply(reply(
                    n + 100,
                    Ok(AttemptAuthorizationDecision::AdmissionGranted)
                ))
                .unwrap(),
                QaReply::Stale
            ));
            assert!(matches!(
                core.advance_ack_settlement(token(n + 100), action(n + 100), &mut NoTraversal)
                    .unwrap(),
                AckSettlementProgress::Waiting
            ));
            let decision = if index == 0 {
                Err(AttemptAuthorizationFailure::Rejected(
                    original_failure.clone(),
                ))
            } else {
                Ok(AttemptAuthorizationDecision::AdmissionGranted)
            };
            assert!(matches!(core.apply_qa_reply(reply(n, decision)).unwrap(),
                QaReply::ObserverComplete { retire: Some(retired) } if retired == operation));
        }
        assert!(matches!(
            core.advance_ack_settlement(token(12), action(12), &mut NoTraversal).unwrap(),
            AckSettlementProgress::Complete(Err(failure)) if failure == original_failure
        ));
    }

    #[test]
    fn only_same_send_withdrawal_releases_the_current_establish_generation() {
        use crate::{
            capacity::ReservationToken,
            execution::ack_phase::{admission_intent, dispatch_admission, queue_establish},
            input::{QueuedInputGrantRef, SendGeneration, WithdrawalOutcome},
        };
        use novarocks_coordination_contract::{
            AttemptAuthorizationDecision, AttemptAuthorizationRef,
        };
        use novarocks_execution_contract::OperationOutcome;
        let start = start();
        let native = start.sources.native;
        let now = start.initial_now;
        let mut core = Core::new(start).unwrap();
        let participant = *core.execution.graph().participations().next().unwrap();
        let request = admission_intent(&mut core.execution, participant);
        let admission =
            dispatch_admission(&mut core.execution, request, 1, OperationOutcome::Accepted);
        core.execution.apply_dispatched_reply(&admission).1.unwrap();
        let action = |value| AttemptActionId::new(id(value));
        let token = AttemptRequestToken::new(id(10));
        let grant = QueuedInputGrantRef::new(id(2), SendGeneration::new(action(2)));
        let intent = queue_establish(&mut core.execution, participant, grant);
        assert!(matches!(
            core.authorize_candidate(&intent, token, action(10))
                .unwrap(),
            AuthorizationStep::Request(AttemptEffect {
                body: EffectBody::EstablishAuthorization { .. },
                ..
            })
        ));
        let reply = LogicalToAttempt {
            activation: core.activation,
            body: LogicalToAttemptBody::AuthorizationReply {
                token,
                decision: Ok(AttemptAuthorizationDecision::EstablishAuthorized(
                    AttemptAuthorizationRef::new(id(15)),
                )),
            },
        };
        assert!(matches!(
            core.apply_qa_reply(reply).unwrap(),
            QaReply::Authorized(_)
        ));
        core.begin_native_prefix(
            now,
            AttemptSourceCut {
                source: native,
                tail: None,
            },
        )
        .unwrap();

        struct SendTurn(Option<QueueStage>);
        impl StepContext for SendTurn {
            fn charge(&mut self, _: usize) -> bool {
                true
            }
            fn reserve_queue_stage(
                &mut self,
                _: DispatchLane,
                _: bool,
                _: BackendProcessId,
                records: usize,
                _: usize,
            ) -> QueueStage {
                assert_eq!(records, 1);
                self.0.take().expect("one original reservation")
            }
        }

        // Process capacity refusal leaves both the queued grant and the unused
        // current guard in place for the next real capacity wake.
        assert!(matches!(
            core.advance_dispatch(action(12), &mut SendTurn(Some(QueueStage::ProcessFull)))
                .unwrap(),
            BatchDispatch::Blocked
        ));
        core.authorizations.validate_send(&intent).unwrap();
        let BatchDispatch::Ready(effect) = core
            .advance_dispatch(
                action(13),
                &mut SendTurn(Some(QueueStage::Granted(ReservationToken(id(33))))),
            )
            .unwrap()
        else {
            panic!("expected original grant and current Establish guard");
        };
        assert!(matches!(effect.body, EffectBody::SendBatch { batch, .. }
            if batch.queued_inputs.as_ref() == [grant]));
        assert!(core.authorizations.validate_send(&intent).is_err());
        let mut apply = |effect, outcome| {
            core.apply_transport_withdrawal(
                now,
                &ExecutorFact::Withdrawn { effect, outcome },
                |_| panic!("Establish is not Abort"),
            )
        };
        apply(action(12), WithdrawalOutcome::DefinitelyUnsent).unwrap();
        apply(action(13), WithdrawalOutcome::TooLate).unwrap();
        assert!(core.authorizations.validate_send(&intent).is_err());
        core.apply_transport_withdrawal(
            now,
            &ExecutorFact::Withdrawn {
                effect: action(13),
                outcome: WithdrawalOutcome::DefinitelyUnsent,
            },
            |_| panic!("Establish is not Abort"),
        )
        .unwrap();
        assert_eq!(core.execution.dispatcher().in_flight_operations(), 0);
        core.authorizations.validate_send(&intent).unwrap();
    }

    #[test]
    fn subscription_uses_same_snapshot_and_waits_for_exact_actual_exit() {
        use crate::input::{
            ReadPauseCoverage, ReadStateSnapshot, ReceptionCoverage, SubscriptionPhaseSnapshot,
        };
        let mut core = round();
        let opened = core.execution.monotonic_now();
        let at = |seconds| opened.saturating_add(Duration::from_secs(seconds));
        let participation = *core.execution.graph().participations().next().unwrap();
        let effect = |n| AttemptActionId::new(id(n));
        let token = |n| AttemptRequestToken::new(id(n));
        core.subscriptions.activate(opened, participation).unwrap();

        assert!(matches!(
            core.advance_subscription(opened, effect(10), token(10), &mut Units(1))
                .unwrap(),
            SubscriptionStep::Yield
        ));
        assert!(seal(&mut core).is_err());
        assert_eq!(core.next_subscription_deadline(), Some(opened));
        let SubscriptionStep::Effect(AttemptEffect {
            body: EffectBody::OpenSubscription(request),
            ..
        }) = core
            .advance_subscription(opened, effect(10), token(10), &mut Units(1))
            .unwrap()
        else {
            panic!("expected original open after the charged cursor resumes");
        };
        assert_eq!(request.required_identities.len(), 1);
        assert_eq!(
            request.required_identities[0],
            request.status_cursors[0].identity()
        );
        let generation = request.generation;
        core.apply_subscription_input(
            opened,
            &ExecutorFact::SubscriptionProgress(SubscriptionPhaseSnapshot {
                participation,
                generation,
                opening_started_at: opened,
                opened_at: Some(opened),
            }),
        )
        .unwrap();
        assert_eq!(core.next_subscription_deadline(), Some(at(5)));

        for received in [at(2), at(3)] {
            let bookmark = ReceivedObservation {
                participation,
                generation,
                received_at: received,
                opening: ObservationOpening {
                    started_at: opened,
                    opened_at: Some(opened),
                },
                event: ObservationEvent::Fact {
                    source_revision: None,
                    fact: ObservationFact::Bookmark {
                        generation: generation.get(),
                        sequence: 1,
                        covered_prefix: 0,
                        source_cut: 0,
                    },
                },
            };
            core.apply_observations(
                received,
                core.observation_source,
                ObservationBatch::new(vec![bookmark]).unwrap(),
            )
            .unwrap();
            assert_eq!(core.next_subscription_deadline(), Some(at(7)));
        }

        let first = core
            .advance_subscription(at(7), effect(11), token(11), &mut Units(1))
            .unwrap();
        assert!(matches!(first, SubscriptionStep::Effect(AttemptEffect {
            body: EffectBody::ConfirmReadState { requested_at, token: request, .. }, ..
        }) if requested_at == at(7) && request == token(11)));
        assert_eq!(core.next_subscription_deadline(), None);
        let snapshot = |n, sampled_at| {
            ExecutorFact::ReadState(ReadStateSnapshot {
                request: token(n),
                participation,
                generation,
                snapshot_at: sampled_at,
                opening_started_at: Some(opened),
                opened_at: Some(opened),
                had_liveness: true,
                last_liveness_at: at(2),
                pauses: ReadPauseCoverage {
                    from: at(2),
                    through: sampled_at,
                    paused: Duration::from_secs(1),
                },
                reading_paused: false,
                resume_revision: 1,
                closed: false,
                received_through_snapshot: ReceptionCoverage::Complete,
            })
        };
        core.apply_subscription_input(at(9), &snapshot(11, at(7)))
            .unwrap();
        assert_eq!(core.next_subscription_deadline(), Some(at(8)));
        assert!(matches!(
            core.advance_subscription(at(9), effect(12), token(12), &mut Units(1))
                .unwrap(),
            SubscriptionStep::Effect(AttemptEffect {
                body: EffectBody::ConfirmReadState { .. },
                ..
            })
        ));
        core.apply_subscription_input(at(9), &snapshot(12, at(9)))
            .unwrap();
        assert!(matches!(
            core.advance_subscription(at(9), effect(13), token(13), &mut Units(1)).unwrap(),
            SubscriptionStep::Effect(AttemptEffect { body: EffectBody::CloseSubscription { generation: closing, .. }, .. }) if closing == generation
        ));
        assert_eq!(core.next_subscription_deadline(), None);
        let exited = |effect| ExecutorFact::SubscriptionExited {
            effect,
            participation,
            generation,
        };
        core.apply_subscription_input(at(15), &exited(effect(13)))
            .unwrap();
        assert_eq!(core.next_subscription_deadline(), None);
        core.apply_subscription_input(at(15), &exited(effect(10)))
            .unwrap();
        assert!(
            core.next_subscription_deadline()
                .is_some_and(|deadline| deadline <= at(15))
        );
        assert!(matches!(
            core.advance_subscription(at(15), effect(14), token(14), &mut Units(2)).unwrap(),
            SubscriptionStep::Effect(AttemptEffect { body: EffectBody::OpenSubscription(request), .. })
                if request.generation.get() == generation.get() + 1
        ));
    }

    #[test]
    fn round_tail_waits_for_publication_then_releases_expired_grant_before_qa() {
        use super::round_tail::RoundTailStep;
        use crate::{
            execution::ack_phase::{admission_intent, dispatch_admission, queue_establish},
            failure::{AttemptFailureSite, HostFailureKind, HostFailureRef, HostProjectedFailure},
            input::{QueuedInputGrantRef, SendGeneration},
        };
        use novarocks_coordination_contract::{
            AttemptAuthorization, AttemptAuthorizationDecision, AttemptAuthorizationFailure,
            AttemptAuthorizationRef, AttemptFailureClass, AttemptToLogicalBody,
        };
        use novarocks_execution_contract::OperationOutcome;

        let start = start();
        let native = start.sources.native;
        let now = start.initial_now;
        let expiry = now.saturating_add(Duration::from_secs(5));
        let mut core = Core::new(start).unwrap();
        let participation = *core.execution.graph().participations().next().unwrap();
        let action = |n| AttemptActionId::new(id(n));
        let token = |n| AttemptRequestToken::new(id(n));
        let activation = core.activation;
        let qa_reply = |n, decision| LogicalToAttempt {
            activation,
            body: LogicalToAttemptBody::AuthorizationReply {
                token: token(n),
                decision,
            },
        };
        let request = admission_intent(&mut core.execution, participation);
        core.authorize_candidate(
            &OperationIntent::AcquireParticipationAdmissionTicket(request),
            token(1),
            action(1),
        )
        .unwrap();
        let issue = AttemptAuthorizationRef::new(id(9));
        core.apply_qa_reply(qa_reply(
            1,
            Ok(AttemptAuthorizationDecision::AdmissionStarted(issue)),
        ))
        .unwrap();
        let original =
            dispatch_admission(&mut core.execution, request, 1, OperationOutcome::Accepted);
        let tail = AttemptInputId(id(2));
        core.begin_native_prefix(
            now,
            AttemptSourceCut {
                source: native,
                tail: Some(tail),
            },
        )
        .unwrap();
        assert_eq!(core.next_round_deadline(), None);
        assert!(core.begin_round_tail(true).is_err());
        core.apply_native(
            now,
            native,
            tail,
            &ExecutorFact::Operation(original.clone()),
            |_, _| panic!("no Abort in the original prefix"),
        )
        .unwrap();

        let grant = QueuedInputGrantRef::new(id(5), SendGeneration::new(action(5)));
        let expiring = queue_establish(&mut core.execution, participation, grant);
        assert_eq!(core.next_round_deadline(), Some(expiry));
        core.begin_round_tail(true).unwrap();
        assert!(matches!(
            core.advance_round_tail(expiry, action(10), token(10), &mut Units(0))
                .unwrap(),
            RoundTailStep::Yield
        ));
        assert!(seal(&mut core).is_err());
        let RoundTailStep::Effect(AttemptEffect {
            body: EffectBody::Publish(crate::effect::Publication::ProtocolBatch(_)),
            id: publication,
            ..
        }) = core
            .advance_round_tail(expiry, action(10), token(10), &mut Units(1))
            .unwrap()
        else {
            panic!("original protocol publisher runs before the expired queue cut");
        };
        assert_eq!(core.next_round_deadline(), None);
        assert_eq!(
            core.protocol_publication_applied(publication, Ok(PublicationApplied::RootStatus)),
            PublicationApplyDisposition::Stale
        );
        assert!(matches!(
            core.advance_round_tail(expiry, action(11), token(11), &mut NoTraversal)
                .unwrap(),
            RoundTailStep::AwaitPublication
        ));
        core.protocol_publication_applied(
            publication,
            Ok(PublicationApplied::ProtocolBatch { published: true }),
        );
        assert_eq!(core.next_round_deadline(), Some(expiry));
        assert!(matches!(
            core.advance_round_tail(expiry, action(11), token(11), &mut Units(1))
                .unwrap(),
            RoundTailStep::Yield
        ));
        assert_eq!(core.execution.dispatcher().queued_items(), 1);

        let later = expiry.saturating_add(Duration::from_secs(3));
        assert!(matches!(
            core.advance_round_tail(later, action(12), token(12), &mut Units(16)).unwrap(),
            RoundTailStep::Effect(AttemptEffect { body: EffectBody::ReleaseQueuedInput { grant: returned }, .. })
                if returned == grant
        ));
        let RoundTailStep::Effect(AttemptEffect {
            body: EffectBody::Port(port),
            ..
        }) = core
            .advance_round_tail(later, action(13), token(13), &mut Units(4))
            .unwrap()
        else {
            panic!("expected exact Admission QA settlement after expiry");
        };
        assert!(
            matches!(port.body, AttemptToLogicalBody::AuthorizationRequest {
            token: request_token,
            request: AttemptAuthorization::SettleAdmission { issue: original_issue, operation_id, .. },
        } if request_token == token(13) && original_issue == issue && operation_id == original.reply.operation_id())
        );
        assert_eq!(core.next_round_deadline(), None);
        assert!(matches!(
            core.advance_round_tail(later, action(14), token(14), &mut NoTraversal)
                .unwrap(),
            RoundTailStep::AwaitQa
        ));

        let later_error = AttemptFailure::Host(HostProjectedFailure {
            original: HostFailureRef(id(40)),
            kind: HostFailureKind::LogicalActor,
            class: AttemptFailureClass::ExecutionFailure,
            site: AttemptFailureSite::Attempt,
        });
        core.apply_qa_reply(qa_reply(
            13,
            Err(AttemptAuthorizationFailure::Rejected(later_error)),
        ))
        .unwrap();
        assert!(matches!(
            core.advance_round_tail(later, action(14), token(14), &mut NoTraversal).unwrap(),
            RoundTailStep::Complete(Err(AttemptFailure::TaskProtocol(error)))
                if matches!(*error, TaskExecutionError::QueueResidenceExpired { operation_id, waited, .. }
                    if operation_id == expiring.operation_id() && waited == Duration::from_secs(5))
        ));
        assert_eq!(core.next_round_deadline(), None);
    }

    #[test]
    fn unified_step_keeps_frozen_native_prefix_and_observation_claim_order() {
        use crate::{
            AttemptCore,
            input::{AttemptInput, AttemptInputClaim},
        };
        let start = start();
        let now = start.initial_now;
        let sources = start.sources;
        let activation = start.activation.identity;
        let mut core = <Core as AttemptCore>::new(start).unwrap();
        let input_id = |n| AttemptInputId(id(n));
        let cut = |source, tail| Some(AttemptSourceCut { source, tail });
        let claim = |source, n, native_tail, selected_cut| AttemptInputClaim {
            source,
            input: input_id(n),
            native_cut: cut(sources.native, native_tail),
            selected_cut,
        };
        let step =
            |core: &mut Core, claim, input| core.handle(now, claim, input, &mut Units(32)).unwrap();
        let bootstrap = step(
            &mut core,
            claim(sources.actor, 1, None, None),
            AttemptInput::Bootstrap,
        );
        assert_eq!(bootstrap.selection, InputSelection::Prefer(sources.native));

        // Timer is allowed to win the actor's own FIFO but cannot widen the
        // original two-member Native cut, or drive a later phase early.
        let tail = input_id(12);
        let late = input_id(100);
        let first = step(
            &mut core,
            claim(sources.actor, 2, Some(tail), None),
            AttemptInput::Timer,
        );
        assert_eq!(first.selection, InputSelection::Only(sources.native));
        assert_eq!(core.next_deadline(), None);
        let started = || {
            AttemptInput::Executor(ExecutorFact::SendStarted {
                effect: AttemptActionId::new(id(50)),
                started_at: now,
            })
        };
        assert_eq!(
            step(
                &mut core,
                claim(sources.native, 49, Some(late), None),
                started()
            )
            .selection,
            InputSelection::Only(sources.native)
        );
        assert_eq!(
            step(
                &mut core,
                claim(sources.native, 12, Some(late), None),
                started()
            )
            .selection,
            InputSelection::Only(sources.observation)
        );
        let control = input_id(18);
        let selected = cut(sources.observation, Some(control));
        let wait = step(
            &mut core,
            claim(sources.actor, 3, Some(late), selected),
            AttemptInput::Timer,
        );
        assert_eq!(wait.selection, InputSelection::Only(sources.observation));
        assert!(wait.effect.is_none());
        assert_eq!(core.next_deadline(), None);

        let seal = AttemptInput::Port(LogicalToAttempt {
            activation,
            body: LogicalToAttemptBody::SealRequest {
                token: AttemptRequestToken::new(id(19)),
            },
        });
        let processed = step(
            &mut core,
            claim(sources.observation, 18, Some(late), selected),
            seal,
        );
        assert_eq!(processed.root_control_processed, Some(control));
        assert!(processed.effect.is_none());
        assert!(core.root.pending());
        assert_eq!(processed.selection, InputSelection::Only(sources.actor));
        assert_eq!(core.next_deadline(), Some(now));

        step(
            &mut core,
            claim(sources.actor, 4, Some(late), None),
            AttemptInput::Timer,
        );
        let published = step(
            &mut core,
            claim(sources.actor, 5, Some(late), None),
            AttemptInput::Timer,
        );
        let Some(AttemptEffect {
            id: publication,
            body: EffectBody::Publish(crate::effect::Publication::ProtocolBatch(_)),
            ..
        }) = published.effect
        else {
            panic!("original round must publish protocol facts");
        };
        assert_eq!(
            published.selection,
            InputSelection::Only(sources.publication)
        );
        assert_eq!(core.next_deadline(), None);
        let actual = |kind| {
            AttemptInput::Executor(ExecutorFact::PublicationApplied {
                effect: publication,
                result: Ok(kind),
            })
        };
        let wrong = step(
            &mut core,
            claim(sources.publication, 6, Some(late), None),
            actual(PublicationApplied::RootStatus),
        );
        assert_eq!(wrong.selection, InputSelection::Only(sources.publication));
        assert!(wrong.round_result.is_none());
        let completed = step(
            &mut core,
            claim(sources.publication, 7, Some(late), None),
            actual(PublicationApplied::ProtocolBatch { published: true }),
        );
        assert_eq!(completed.selection, InputSelection::Prefer(sources.native));
        assert_eq!(completed.round_result, Some(Ok(())));
        assert_eq!(core.next_deadline(), None);
        // The later paid Native input can only open a new prefix now.
        let next = step(
            &mut core,
            claim(sources.native, 100, Some(late), None),
            started(),
        );
        assert_eq!(next.selection, InputSelection::Only(sources.observation));
    }

    #[test]
    fn terminal_control_recovery_timer_waits_for_the_selected_observation_cut() {
        use crate::{
            AttemptCore,
            effect::Publication,
            input::{AttemptInput, AttemptInputClaim},
        };

        for authoritative_evidence_at_deadline in [false, true] {
            let start = start();
            let now = start.initial_now;
            let deadline = now.saturating_add(Duration::from_secs(30));
            let sources = start.sources;
            let activation = start.activation.identity;
            let mut core = Core::new(start).unwrap();
            let root = core.execution.graph().root_identity();
            let step = |core: &mut Core, now, source, n, observation_tail, input| {
                let claim = AttemptInputClaim {
                    source,
                    input: AttemptInputId(id(n)),
                    native_cut: Some(AttemptSourceCut {
                        source: sources.native,
                        tail: None,
                    }),
                    selected_cut: Some(AttemptSourceCut {
                        source: sources.observation,
                        tail: observation_tail,
                    }),
                };
                core.handle(now, claim, input, &mut Units(32)).unwrap()
            };
            step(
                &mut core,
                now,
                sources.actor,
                1,
                None,
                AttemptInput::Bootstrap,
            );
            let demanded = step(
                &mut core,
                now,
                sources.observation,
                2,
                None,
                AttemptInput::Port(LogicalToAttempt {
                    activation,
                    body: LogicalToAttemptBody::TerminalControlDemand {
                        token: AttemptRequestToken::new(id(40)),
                        root,
                    },
                }),
            );
            assert_eq!(demanded.root_control_processed, Some(AttemptInputId(id(2))));
            step(&mut core, now, sources.actor, 3, None, AttemptInput::Timer);
            let publication = step(&mut core, now, sources.actor, 4, None, AttemptInput::Timer);
            let Some(AttemptEffect {
                id: effect,
                body: EffectBody::Publish(Publication::ProtocolBatch(_)),
                ..
            }) = publication.effect
            else {
                panic!("original round must publish its initial protocol facts");
            };
            let completed = step(
                &mut core,
                now,
                sources.publication,
                5,
                None,
                AttemptInput::Executor(ExecutorFact::PublicationApplied {
                    effect,
                    result: Ok(PublicationApplied::ProtocolBatch { published: true }),
                }),
            );
            assert_eq!(completed.round_result, Some(Ok(())));
            assert_eq!(core.next_deadline(), Some(deadline));

            let timer = step(
                &mut core,
                deadline,
                sources.actor,
                5,
                None,
                AttemptInput::Timer,
            );
            assert_eq!(timer.selection, InputSelection::Only(sources.observation));
            assert!(timer.round_result.is_none());

            if authoritative_evidence_at_deadline {
                let selected = Some(AttemptInputId(id(7)));
                let waiting = step(
                    &mut core,
                    deadline,
                    sources.actor,
                    6,
                    selected,
                    AttemptInput::Timer,
                );
                assert_eq!(waiting.selection, InputSelection::Only(sources.observation));
                assert_eq!(core.next_deadline(), None);
                let observation = frame(
                    &mut core,
                    root,
                    TaskState::Aborted,
                    Some(TerminationDetail::Aborted(AbortCause::QueryFailed)),
                );
                step(
                    &mut core,
                    deadline,
                    sources.observation,
                    7,
                    selected,
                    AttemptInput::Executor(ExecutorFact::Observations(
                        ObservationBatch::new(vec![observation]).unwrap(),
                    )),
                );
                let published = step(
                    &mut core,
                    deadline,
                    sources.actor,
                    8,
                    None,
                    AttemptInput::Timer,
                );
                assert!(matches!(
                    published.effect,
                    Some(AttemptEffect { body: EffectBody::Publish(Publication::RootStatus(projection)), .. })
                    if matches!(projection.failure, AttemptRootFailure::Authoritative(_))
                ));
            } else {
                step(
                    &mut core,
                    deadline,
                    sources.actor,
                    6,
                    None,
                    AttemptInput::Timer,
                );
                let failed = step(
                    &mut core,
                    deadline,
                    sources.actor,
                    7,
                    None,
                    AttemptInput::Timer,
                );
                assert!(matches!(
                    failed.round_result,
                    Some(Err(AttemptFailure::TaskProtocol(error)))
                    if matches!(*error, TaskExecutionError::Schedule(ref detail)
                        if detail.contains("did not arrive within its recovery budget"))
                ));
            }
        }
    }

    #[test]
    fn diagnostic_snapshot_keeps_early_data_and_excludes_late_data_until_actual_exit() {
        use super::round_tail::RoundTailStep;
        use crate::{
            AttemptCore,
            effect::Publication,
            input::{
                AttemptInput, AttemptInputClaim, DiagnosticRead, FinalTaskInfoProjection,
                FinalTaskInfoRef,
            },
            publication::{DiagnosticClosure, DiagnosticMissing, DiagnosticTaskObservation},
        };
        use novarocks_coordination_contract::{
            AttemptComputationCut, AttemptConvergenceMode, AttemptToLogicalBody,
        };
        use novarocks_execution_contract::root_result::RootResultEnd;

        for at_deadline in [false, true] {
            let mut core = round();
            let sources = core.sources;
            let now = core.execution.monotonic_now();
            let deadline = now.saturating_add(Duration::from_secs(1));
            let root = core.execution.graph().root_identity();
            let ready = AttemptComputationCut {
                activation: core.activation,
                root,
                cut: now,
                diagnostic_deadline: deadline,
                convergence_deadline: now.saturating_add(Duration::from_secs(10)),
                end: RootResultEnd {
                    sequence: id(1),
                    output_rows: 1,
                },
            };
            let finished = frame(&mut core, root, TaskState::Finished, None);
            let ObservationEvent::Fact {
                fact: ObservationFact::Status(status),
                ..
            } = &finished.event
            else {
                panic!("original terminal");
            };
            let final_status = status.clone();
            core.apply_observations(
                now,
                sources.observation,
                ObservationBatch::new(vec![finished]).unwrap(),
            )
            .unwrap();
            core.execution
                .with_diagnostics(|collector, _| collector.begin(ready))
                .unwrap();
            core.begin_round_tail(true).unwrap();
            let advance = |core: &mut Core, n| {
                core.advance_round_tail(
                    now,
                    AttemptActionId::new(id(n)),
                    AttemptRequestToken::new(id(n)),
                    &mut Units(32),
                )
                .unwrap()
            };
            let RoundTailStep::Effect(AttemptEffect {
                id: publication,
                body: EffectBody::Publish(Publication::ProtocolBatch(_)),
                ..
            }) = advance(&mut core, 101)
            else {
                panic!("expected original protocol publication")
            };
            core.protocol_publication_applied(
                publication,
                Ok(PublicationApplied::ProtocolBatch { published: true }),
            );
            let mut sequence = 102;
            let (read_effect, request) = loop {
                match advance(&mut core, sequence) {
                    RoundTailStep::Effect(AttemptEffect {
                        id,
                        body:
                            EffectBody::CollectDiagnostics {
                                request,
                                task,
                                deadline: exact,
                            },
                        ..
                    }) => {
                        assert_eq!((task, exact), (root, deadline));
                        break (id, request);
                    }
                    RoundTailStep::Effect(AttemptEffect {
                        body:
                            EffectBody::ReleaseCreationPayload { .. }
                            | EffectBody::CloseSubscription { .. }
                            | EffectBody::AdjustReadWindow(_)
                            | EffectBody::OpenSubscription(_),
                        ..
                    }) => {}
                    other => panic!("unexpected original tail progress: {other:?}"),
                }
                sequence += 1;
                assert!(sequence < 110);
            };
            assert!(matches!(
                advance(&mut core, sequence + 1),
                RoundTailStep::Complete(Ok(()))
            ));

            let handle = |core: &mut Core, at, source, n, input| {
                core.handle(
                    at,
                    AttemptInputClaim {
                        source,
                        input: AttemptInputId(id(n)),
                        native_cut: Some(AttemptSourceCut {
                            source: sources.native,
                            tail: None,
                        }),
                        selected_cut: Some(AttemptSourceCut {
                            source: sources.observation,
                            tail: None,
                        }),
                    },
                    input,
                    &mut Units(32),
                )
                .unwrap()
            };
            handle(&mut core, now, sources.actor, 120, AttemptInput::Bootstrap);
            assert_eq!(core.next_deadline(), Some(deadline));
            core.convergence
                .begin(AttemptConvergenceMode::Normal, ready.convergence_deadline);
            let local_first = handle(
                &mut core,
                now,
                sources.actor,
                121,
                AttemptInput::Executor(ExecutorFact::LocalConvergenceExited),
            );
            assert!(local_first.effect.is_none());

            let reference = FinalTaskInfoRef::new(request, root);
            let observed = handle(
                &mut core,
                if at_deadline { deadline } else { now },
                sources.actor,
                122,
                AttemptInput::Executor(ExecutorFact::DiagnosticsFinished {
                    request,
                    task: root,
                    result: DiagnosticRead::Available(FinalTaskInfoProjection {
                        reference,
                        final_status,
                    }),
                }),
            );
            if at_deadline {
                assert!(matches!(observed.effect,
                Some(AttemptEffect { body: EffectBody::DisposeDiagnosticInfo {
                    request: exact, task, information }, ..
                }) if exact == request && task == root && information == reference));
            } else {
                assert!(observed.effect.is_none());
            }
            assert!(observed.diagnostic_failure.is_none());
            assert!(!core.diagnostic_local_ready());

            let applied = |effect, kind| {
                AttemptInput::Executor(ExecutorFact::PublicationApplied {
                    effect,
                    result: Ok(kind),
                })
            };
            let mut n = 123;
            let snapshot = loop {
                let step = handle(&mut core, deadline, sources.actor, n, AttemptInput::Timer);
                n += 1;
                assert!(n < 143, "bounded diagnostic snapshot assembly");
                match step.effect {
                    Some(AttemptEffect {
                        id,
                        body: EffectBody::Publish(Publication::Diagnostics(cut)),
                        ..
                    }) => {
                        assert_eq!(cut.closure, DiagnosticClosure::ObservationDeadline);
                        assert_eq!(cut.sources.selected_at, deadline);
                        let observation = if at_deadline {
                            DiagnosticTaskObservation::Missing(DiagnosticMissing::ReadPending)
                        } else {
                            DiagnosticTaskObservation::Available(reference)
                        };
                        assert_eq!(cut.tasks[0], (root, observation));
                        assert_eq!(step.selection, InputSelection::Only(sources.publication));
                        break id;
                    }
                    Some(AttemptEffect {
                        id,
                        body: EffectBody::Publish(Publication::RootStatus(_)),
                        ..
                    }) => {
                        handle(
                            &mut core,
                            deadline,
                            sources.publication,
                            n,
                            applied(id, PublicationApplied::RootStatus),
                        );
                        n += 1;
                    }
                    None => {}
                    other => panic!("unexpected effect during diagnostic snapshot: {other:?}"),
                }
            };
            assert_eq!(core.next_deadline(), None);
            let wrong = handle(
                &mut core,
                deadline,
                sources.publication,
                n,
                applied(snapshot, PublicationApplied::FinalTaskInfo),
            );
            assert_eq!(wrong.selection, InputSelection::Only(sources.publication));
            assert!(!core.diagnostic_local_ready());
            handle(
                &mut core,
                deadline,
                sources.publication,
                n + 1,
                applied(snapshot, PublicationApplied::Diagnostics),
            );
            assert!(!core.diagnostic_local_ready());
            let done = handle(
                &mut core,
                deadline,
                sources.actor,
                n + 2,
                AttemptInput::Timer,
            );
            assert_eq!(done.round_result, Some(Ok(())));
            handle(
                &mut core,
                deadline,
                sources.actor,
                n + 3,
                AttemptInput::Executor(ExecutorFact::ActualExit {
                    effect: read_effect,
                }),
            );
            assert!(core.diagnostic_local_ready());
            let mut reported_local = false;
            for step_id in n + 4..n + 15 {
                let step = handle(
                    &mut core,
                    deadline,
                    sources.actor,
                    step_id,
                    AttemptInput::Timer,
                );
                if matches!(step.effect, Some(AttemptEffect { body: EffectBody::Port(port), .. })
                    if matches!(port.body, AttemptToLogicalBody::ConvergenceProgress { local_exited: true, .. }))
                {
                    reported_local = true;
                    break;
                }
            }
            assert!(
                reported_local,
                "local convergence reports only after original diagnostic exit"
            );
        }
    }

    #[test]
    fn dynamic_feedback_waits_for_protocol_publication_and_does_not_park_the_round() {
        use super::round_tail::RoundTailStep;
        use crate::{effect::PumpKind, input::PublicationApplied};
        use novarocks_execution_contract::{DomainVersion, DynamicFilterAdvertisement};

        let mut core = round();
        let now = core.execution.monotonic_now();
        let root = core.execution.graph().root_identity();
        let pump = PayloadRef(id(901));
        core.execution.bind_dynamic_feedback(Some(pump));
        core.candidates.enabled = true;
        let mut advertised = frame(&mut core, root, TaskState::Running, None);
        let ObservationEvent::Fact {
            fact: ObservationFact::Status(status),
            ..
        } = &mut advertised.event
        else {
            panic!("original task status");
        };
        *status = status
            .clone()
            .with_dynamic_filters(DynamicFilterAdvertisement::new(DomainVersion::FIRST, 1));
        core.apply_observations(
            now,
            core.observation_source,
            ObservationBatch::new(vec![advertised]).unwrap(),
        )
        .unwrap();
        core.begin_round_tail(true).unwrap();

        let advance = |core: &mut Core, n| {
            core.advance_round_tail(
                now,
                AttemptActionId::new(id(n)),
                AttemptRequestToken::new(id(n)),
                &mut Units(32),
            )
            .unwrap()
        };
        let RoundTailStep::Effect(AttemptEffect {
            id: publication,
            body: EffectBody::Publish(crate::effect::Publication::ProtocolBatch(_)),
            ..
        }) = advance(&mut core, 51)
        else {
            panic!("publication precedes feedback")
        };
        assert!(matches!(
            advance(&mut core, 52),
            RoundTailStep::AwaitPublication
        ));
        core.protocol_publication_applied(
            publication,
            Ok(PublicationApplied::ProtocolBatch { published: true }),
        );
        let mut n = 53;
        loop {
            match advance(&mut core, n) {
                RoundTailStep::Effect(AttemptEffect {
                    body:
                        EffectBody::StartPump {
                            pump: exact,
                            kind: PumpKind::DynamicFilters,
                        },
                    ..
                }) => {
                    assert_eq!(exact, pump);
                    break;
                }
                RoundTailStep::Effect(AttemptEffect {
                    body:
                        EffectBody::OpenSubscription(_)
                        | EffectBody::AdjustReadWindow(_)
                        | EffectBody::CloseSubscription { .. }
                        | EffectBody::ReleaseCreationPayload { .. },
                    ..
                }) => {}
                other => {
                    panic!(
                        "expected cleanup or subscription progress before the feedback pump: {other:?}"
                    )
                }
            }
            n += 1;
            assert!(n < 60, "bounded cleanup and subscription progress");
        }
        assert!(matches!(advance(&mut core, n + 1),
            RoundTailStep::Effect(AttemptEffect {
                body: EffectBody::ReadDynamicFilters { request, task, acknowledged: None, advertised, .. }, ..
            }) if request == AttemptRequestToken::new(id(n + 1)) && task == root && advertised == DomainVersion::FIRST));
        assert!(matches!(
            advance(&mut core, n + 2),
            RoundTailStep::Complete(Ok(()))
        ));
    }

    #[test]
    fn first_split_pump_failure_keeps_its_typed_cause_and_upgrades_later_convergence() {
        use crate::{
            AttemptCore,
            effect::PumpKind,
            failure::{AttemptFailureSite, HostFailureKind, HostFailureRef, HostProjectedFailure},
            input::{AttemptInput, AttemptInputClaim},
        };
        use novarocks_coordination_contract::{AttemptConvergenceMode, AttemptFailureClass};

        let mut start = start();
        let pump = PayloadRef(id(701));
        start.plan.pumps = Box::new([(pump, PumpKind::SplitAssignment)]);
        let sources = start.sources;
        let now = start.initial_now;
        let activation = start.activation.identity;
        let mut core = Core::new(start).unwrap();
        let handle = |core: &mut Core, input, n| {
            core.handle(
                now,
                AttemptInputClaim {
                    source: sources.actor,
                    input: AttemptInputId(id(n)),
                    native_cut: Some(AttemptSourceCut {
                        source: sources.native,
                        tail: None,
                    }),
                    selected_cut: None,
                },
                input,
                &mut Units(32),
            )
            .unwrap()
        };
        handle(&mut core, AttemptInput::Bootstrap, 1);
        assert!(matches!(core.execution.split_pump_lifecycle(true),
            Some(EffectBody::StartPump { pump: original, .. }) if original == pump));
        let failure = AttemptFailure::Host(HostProjectedFailure {
            original: HostFailureRef(id(702)),
            kind: HostFailureKind::SplitAssignment,
            class: AttemptFailureClass::ExecutionFailure,
            site: AttemptFailureSite::Attempt,
        });
        let failed = handle(
            &mut core,
            AttemptInput::Executor(ExecutorFact::PumpFailed {
                pump,
                failure: failure.clone(),
            }),
            2,
        );
        assert!(failed.stop_producing);
        let deadline = now.saturating_add(Duration::from_secs(2));
        handle(
            &mut core,
            AttemptInput::Port(LogicalToAttempt {
                activation,
                body: LogicalToAttemptBody::BeginConvergence {
                    mode: AttemptConvergenceMode::Normal,
                    remote_deadline: deadline,
                    computation_ready: None,
                    retain_diagnostics: false,
                },
            }),
            3,
        );
        assert_eq!(
            core.convergence.mode(),
            Some(AttemptConvergenceMode::Cleanup)
        );
        assert_eq!(core.convergence.next_deadline(), Some(deadline));
        let settled = handle(&mut core, AttemptInput::Timer, 4);
        assert_eq!(settled.round_result, Some(Err(failure)));
    }

    #[test]
    fn quiesce_continuation_keeps_the_native_suffix_and_common_frames_behind_its_original_cut() {
        use crate::{
            AttemptCore,
            execution::ack_phase::emit_quiesce,
            input::{AttemptInput, AttemptInputClaim},
        };
        use novarocks_execution_contract::{ParticipationState, QuiesceParticipationReceipt};
        use std::sync::Arc;

        let start = start();
        let now = start.initial_now;
        let later = now.saturating_add(Duration::from_secs(1));
        let sources = start.sources;
        let mut core = Core::new(start).unwrap();
        let root = core.execution.graph().root_identity();
        let participation = core
            .execution
            .graph()
            .task(root.task_id())
            .unwrap()
            .participation();
        let receipt = Arc::new(QuiesceParticipationReceipt::new(
            participation,
            1,
            vec![root],
            ParticipationState::Quiescing,
        ));
        let ack = emit_quiesce(&mut core.execution, Arc::clone(&receipt));
        let native_tail = AttemptInputId(id(4));
        let claim = |source, value| AttemptInputClaim {
            source,
            input: AttemptInputId(id(value)),
            native_cut: Some(AttemptSourceCut {
                source: sources.native,
                tail: Some(native_tail),
            }),
            selected_cut: None,
        };

        core.handle(
            now,
            claim(sources.actor, 1),
            AttemptInput::Bootstrap,
            &mut Units(1),
        )
        .unwrap();
        core.handle(
            now,
            claim(sources.actor, 2),
            AttemptInput::Timer,
            &mut Units(1),
        )
        .unwrap();
        let primary = core
            .handle(
                now,
                claim(sources.native, 3),
                AttemptInput::Executor(ExecutorFact::Operation(ack)),
                &mut Units(1),
            )
            .unwrap();
        assert_eq!(primary.selection, InputSelection::Only(sources.actor));
        assert!(core.execution.has_pending_quiesce());
        assert!(
            !core
                .execution
                .task(root.task_id())
                .unwrap()
                .create_ownership_proven()
        );
        let suffix = || {
            AttemptInput::Executor(ExecutorFact::SendStarted {
                effect: AttemptActionId::new(id(500)),
                started_at: later,
            })
        };
        assert!(
            core.handle(later, claim(sources.native, 4), suffix(), &mut Units(1))
                .is_err()
        );

        let mut selection = primary.selection;
        for n in 10..32 {
            if selection == InputSelection::Only(sources.native) {
                break;
            }
            selection = core
                .handle(
                    later,
                    claim(sources.actor, n),
                    AttemptInput::Timer,
                    &mut Units(2),
                )
                .unwrap()
                .selection;
        }
        assert_eq!(selection, InputSelection::Only(sources.native));
        assert_eq!(core.execution.monotonic_now(), now);
        assert!(
            core.execution
                .task(root.task_id())
                .unwrap()
                .create_ownership_proven()
        );
        assert!(!core.execution.has_pending_quiesce());
        let after_suffix = core
            .handle(later, claim(sources.native, 4), suffix(), &mut Units(1))
            .unwrap();
        assert_eq!(
            after_suffix.selection,
            InputSelection::Only(sources.observation)
        );

        let trailing = frame(&mut core, root, TaskState::Running, None);
        let mut quiesce = trailing.clone();
        quiesce.event = ObservationEvent::Fact {
            source_revision: None,
            fact: ObservationFact::Quiesce(receipt),
        };
        let batch = ObservationBatch::new(vec![quiesce, trailing]).unwrap();
        let first = core
            .handle(
                later,
                claim(sources.observation, 40),
                AttemptInput::Executor(ExecutorFact::Observations(batch)),
                &mut Units(2),
            )
            .unwrap();
        assert!(core.execution.has_pending_quiesce());
        assert_eq!(first.selection, InputSelection::Only(sources.actor));
        assert!(
            core.execution
                .task(root.task_id())
                .unwrap()
                .status()
                .is_none()
        );
        let seal = AttemptInput::Port(LogicalToAttempt {
            activation: core.activation,
            body: LogicalToAttemptBody::SealRequest {
                token: AttemptRequestToken::new(id(700)),
            },
        });
        assert!(
            core.handle(later, claim(sources.observation, 41), seal, &mut Units(1))
                .is_err()
        );

        let mut timers = 0;
        for n in 42..64 {
            if !core.execution.has_pending_quiesce() {
                break;
            }
            core.handle(
                later,
                claim(sources.actor, n),
                AttemptInput::Timer,
                &mut Units(2),
            )
            .unwrap();
            timers += 1;
        }
        assert!(timers > 1);
        assert!(!core.execution.has_pending_quiesce());
        assert!(
            core.execution
                .task(root.task_id())
                .unwrap()
                .status()
                .is_none()
        );
        core.handle(
            later,
            claim(sources.actor, 64),
            AttemptInput::Timer,
            &mut Units(2),
        )
        .unwrap();
        assert_eq!(
            core.execution.task(root.task_id()).unwrap().task_state(),
            TaskState::Running
        );
    }

    #[test]
    fn unified_step_emits_seal_only_after_matching_root_publication() {
        use crate::{
            AttemptCore,
            effect::Publication,
            input::{AttemptInput, AttemptInputClaim},
        };
        use novarocks_coordination_contract::AttemptToLogicalBody;

        let mut attempt = start();
        attempt.observe_dev_root = true;
        let mut core = round_with_start(attempt);
        let root = core.execution.graph().root_identity();
        let now = core.execution.monotonic_now();
        let root_finished = frame(&mut core, root, TaskState::Finished, None);
        let strict: Vec<_> = core
            .execution
            .graph()
            .tasks()
            .map(|task| task.identity())
            .filter(|&task| task != root)
            .collect();
        let strict_finished: Vec<_> = strict
            .into_iter()
            .map(|task| frame(&mut core, task, TaskState::Finished, None))
            .collect();
        core.apply_observations(
            now,
            core.observation_source,
            ObservationBatch::new(vec![root_finished]).unwrap(),
        )
        .unwrap();
        core.begin_ack_settlement(None).unwrap();
        assert!(matches!(
            core.advance_ack_settlement(
                AttemptRequestToken::new(id(60)),
                AttemptActionId::new(id(60)),
                &mut NoTraversal
            )
            .unwrap(),
            AckSettlementProgress::Complete(Ok(()))
        ));

        let sources = core.sources;
        let activation = core.activation;
        let step = |core: &mut Core, source, n, input| {
            let claim = AttemptInputClaim {
                source,
                input: AttemptInputId(id(n)),
                native_cut: Some(AttemptSourceCut {
                    source: sources.native,
                    tail: None,
                }),
                selected_cut: None,
            };
            core.handle(now, claim, input, &mut Units(128)).unwrap()
        };
        let observe_dev_root = |core: &mut Core| {
            let observed = step(core, sources.actor, 201, AttemptInput::Timer);
            let Some(AttemptEffect {
                id: effect,
                body: EffectBody::Publish(Publication::DevRootObservation { status, .. }),
                ..
            }) = observed.effect
            else {
                panic!("expected original development root observer effect");
            };
            assert_eq!(status.identity(), root);
            assert_eq!(
                observed.selection,
                InputSelection::Only(sources.publication)
            );
            assert_eq!(core.next_deadline(), None);
            for (index, kind) in [
                PublicationApplied::RootStatus,
                PublicationApplied::DevRootObservation,
            ]
            .into_iter()
            .enumerate()
            {
                let acknowledged = step(
                    core,
                    sources.publication,
                    202 + index as u64,
                    AttemptInput::Executor(ExecutorFact::PublicationApplied {
                        effect,
                        result: Ok(kind),
                    }),
                );
                let next = if index == 0 {
                    sources.publication
                } else {
                    sources.actor
                };
                assert_eq!(acknowledged.selection, InputSelection::Only(next));
            }
        };
        step(&mut core, sources.actor, 1, AttemptInput::Bootstrap);
        let seal_token = AttemptRequestToken::new(id(22));
        let processed = step(
            &mut core,
            sources.observation,
            2,
            AttemptInput::Port(LogicalToAttempt {
                activation,
                body: LogicalToAttemptBody::SealRequest { token: seal_token },
            }),
        );
        assert_eq!(
            processed.root_control_processed,
            Some(AttemptInputId(id(2)))
        );
        assert!(processed.effect.is_none());
        observe_dev_root(&mut core);
        let root_work = step(&mut core, sources.actor, 3, AttemptInput::Timer);
        let Some(AttemptEffect {
            id: root_effect,
            body: EffectBody::Publish(crate::effect::Publication::RootStatus(projection)),
            ..
        }) = root_work.effect
        else {
            panic!("expected original Root publisher effect");
        };
        assert_eq!(projection.status.identity(), root);
        assert_eq!(
            root_work.selection,
            InputSelection::Only(sources.publication)
        );

        let publication = |kind| {
            AttemptInput::Executor(ExecutorFact::PublicationApplied {
                effect: root_effect,
                result: Ok(kind),
            })
        };
        let wrong = step(
            &mut core,
            sources.publication,
            4,
            publication(PublicationApplied::ProtocolBatch { published: true }),
        );
        assert!(wrong.effect.is_none());
        assert_eq!(wrong.selection, InputSelection::Only(sources.publication));
        let applied = step(
            &mut core,
            sources.publication,
            5,
            publication(PublicationApplied::RootStatus),
        );
        assert_eq!(applied.selection, InputSelection::Only(sources.actor));
        let settled = step(&mut core, sources.actor, 6, AttemptInput::Timer);
        let Some(AttemptEffect {
            id: seal_effect,
            body: EffectBody::Port(port),
            ..
        }) = settled.effect
        else {
            panic!("expected original seal delivery");
        };
        assert!(
            matches!(port.body, AttemptToLogicalBody::SealResult { token, result: Ok(_) } if token == seal_token)
        );
        assert_eq!(settled.selection, InputSelection::Only(sources.publication));
        assert_eq!(core.next_deadline(), None);

        let seal_ack = |result| {
            AttemptInput::Executor(ExecutorFact::PublicationApplied {
                effect: seal_effect,
                result: Ok(result),
            })
        };
        let wrong = step(
            &mut core,
            sources.publication,
            7,
            seal_ack(PublicationApplied::RootStatus),
        );
        assert_eq!(wrong.selection, InputSelection::Only(sources.publication));
        assert!(wrong.effect.is_none());
        assert!(core.terminal.terminal().is_none());
        step(
            &mut core,
            sources.publication,
            8,
            seal_ack(PublicationApplied::SealResult),
        );
        let published = step(&mut core, sources.actor, 9, AttemptInput::Timer);
        let first = published.effect.unwrap().id;
        let applied = |effect| {
            AttemptInput::Executor(ExecutorFact::PublicationApplied {
                effect,
                result: Ok(PublicationApplied::ProtocolBatch { published: true }),
            })
        };
        let cleanup = step(&mut core, sources.publication, 10, applied(first));
        assert!(matches!(cleanup.effect, Some(AttemptEffect {
            body: EffectBody::ReleaseCreationPayload { task, .. }, ..
        }) if task == core.execution.graph().root_identity()));
        let done = step(&mut core, sources.actor, 101, AttemptInput::Timer);
        assert_eq!(done.round_result, Some(Ok(())));
        assert!(done.effect.is_none());
        assert!(!core.execution.normal_drain_started());
        assert!(core.terminal.terminal().is_none());

        step(
            &mut core,
            sources.observation,
            11,
            AttemptInput::Executor(ExecutorFact::Observations(
                ObservationBatch::new(strict_finished).unwrap(),
            )),
        );
        observe_dev_root(&mut core);
        step(&mut core, sources.actor, 12, AttemptInput::Timer);
        let published = step(&mut core, sources.actor, 13, AttemptInput::Timer);
        let second = published.effect.unwrap().id;
        let mut completed = step(&mut core, sources.publication, 14, applied(second));
        assert!(core.execution.normal_drain_started());
        for n in 15..22 {
            match &completed.effect {
                Some(AttemptEffect {
                    body: EffectBody::ReleaseCreationPayload { .. },
                    ..
                })
                | None => {}
                Some(_) => break,
            }
            completed = step(&mut core, sources.actor, n, AttemptInput::Timer);
        }
        assert!(
            matches!(completed.effect, Some(AttemptEffect { body: EffectBody::Port(port), .. })
            if matches!(port.body, AttemptToLogicalBody::AttemptTerminal(novarocks_coordination_contract::AttemptTerminal::Completed)))
        );
    }

    #[test]
    fn stop_keeps_native_prefix_and_withdraws_only_unstarted_business() {
        use crate::{
            AttemptCore,
            execution::ack_phase::{admission_intent, dispatch_admission},
            input::{AttemptInput, AttemptInputClaim, WithdrawalOutcome},
        };
        use novarocks_coordination_contract::AttemptStopReason;
        use novarocks_execution_contract::OperationOutcome;

        let start = start();
        let sources = start.sources;
        let activation = start.activation.identity;
        let now = start.initial_now;
        let mut core = Core::new(start).unwrap();
        let participants: Vec<_> = core
            .execution
            .graph()
            .participations()
            .take(2)
            .copied()
            .collect();
        for (index, &part) in participants.iter().enumerate() {
            let request = admission_intent(&mut core.execution, part);
            dispatch_admission(
                &mut core.execution,
                request,
                index as u64 + 1,
                OperationOutcome::Accepted,
            );
        }
        let started = AttemptActionId::new(id(101));
        let unstarted = AttemptActionId::new(id(102));
        let cut = |source, tail| Some(AttemptSourceCut { source, tail });
        let step = |core: &mut Core, source, n, input, native_tail, selected_cut| {
            core.handle(
                now,
                AttemptInputClaim {
                    source,
                    input: AttemptInputId(id(n)),
                    native_cut: cut(sources.native, native_tail),
                    selected_cut,
                },
                input,
                &mut Units(32),
            )
            .unwrap()
        };
        step(
            &mut core,
            sources.actor,
            1,
            AttemptInput::Bootstrap,
            None,
            None,
        );
        let tail = AttemptInputId(id(7));
        step(
            &mut core,
            sources.actor,
            2,
            AttemptInput::Timer,
            Some(tail),
            None,
        );
        let stop = AttemptInput::Port(LogicalToAttempt {
            activation,
            body: LogicalToAttemptBody::Stop {
                reason: AttemptStopReason::LogicalExecutionFailed,
                failure: AttemptFailure::TaskProtocol(Box::new(schedule("original stop"))),
            },
        });
        let closed = step(&mut core, sources.actor, 3, stop.clone(), Some(tail), None);
        assert!(closed.stop_producing);
        assert_eq!(closed.selection, InputSelection::Only(sources.native));
        assert!(closed.effect.is_none());
        assert!(!step(&mut core, sources.actor, 4, stop, Some(tail), None).stop_producing);
        assert!(core.terminal.requires_logical_failure_stand_down());
        step(
            &mut core,
            sources.native,
            7,
            AttemptInput::Executor(ExecutorFact::SendStarted {
                effect: started,
                started_at: now,
            }),
            Some(tail),
            None,
        );
        step(
            &mut core,
            sources.actor,
            8,
            AttemptInput::Timer,
            None,
            cut(sources.observation, None),
        );
        step(&mut core, sources.actor, 9, AttemptInput::Timer, None, None);
        let publish = step(
            &mut core,
            sources.actor,
            10,
            AttemptInput::Timer,
            None,
            None,
        );
        let publication = publish.effect.unwrap().id;
        let returned = step(
            &mut core,
            sources.publication,
            11,
            AttemptInput::Executor(ExecutorFact::PublicationApplied {
                effect: publication,
                result: Ok(PublicationApplied::ProtocolBatch { published: true }),
            }),
            None,
            None,
        );
        assert!(matches!(returned.effect, Some(AttemptEffect {
            body: EffectBody::Withdraw { effect }, class: crate::effect::EffectClass::Cleanup, ..
        }) if effect == unstarted));
        assert_eq!(core.execution.dispatcher().in_flight_operations(), 2);
        let mut finished = step(
            &mut core,
            sources.actor,
            12,
            AttemptInput::Timer,
            None,
            None,
        );
        let mut stand_down = None;
        for n in 121..128 {
            match &finished.effect {
                Some(AttemptEffect {
                    body: EffectBody::ReleaseCreationPayload { .. },
                    ..
                }) => {}
                Some(AttemptEffect {
                    body:
                        EffectBody::AdmitQueuedInput {
                            operation: OperationIntent::AbortParticipation(request),
                            ..
                        },
                    class: crate::effect::EffectClass::Settlement,
                    ..
                }) => {
                    assert!(stand_down.replace(request.participation()).is_none());
                    assert!(finished.abort_settlement.is_none());
                }
                _ => break,
            }
            finished = step(&mut core, sources.actor, n, AttemptInput::Timer, None, None);
        }
        assert_eq!(stand_down, Some(participants[0]));
        assert!(
            core.execution
                .participation_convergence_receipt(participants[0])
                .is_none()
        );
        assert_eq!(finished.round_result, Some(Ok(())));
        assert!(
            matches!(finished.effect, Some(AttemptEffect { body: EffectBody::Port(port), .. })
            if matches!(port.body, novarocks_coordination_contract::AttemptToLogicalBody::AttemptTerminal(
                novarocks_coordination_contract::AttemptTerminal::Failed(AttemptFailure::TaskProtocol(ref error))
            ) if **error == schedule("original stop")))
        );

        step(
            &mut core,
            sources.actor,
            13,
            AttemptInput::Executor(ExecutorFact::Withdrawn {
                effect: unstarted,
                outcome: WithdrawalOutcome::DefinitelyUnsent,
            }),
            None,
            None,
        );
        assert_eq!(core.execution.dispatcher().in_flight_operations(), 1);
    }

    #[test]
    fn convergence_waits_for_exact_process_read_and_reports_local_exit_separately() {
        use crate::{
            AttemptCore,
            execution::ack_phase::{admission_intent, dispatch_admission},
            input::{AttemptInput, AttemptInputClaim, MembershipObservation},
        };
        use novarocks_coordination_contract::{AttemptConvergenceMode, AttemptToLogicalBody};
        use novarocks_execution_contract::OperationOutcome;

        let start = start();
        let now = start.initial_now;
        let sources = start.sources;
        let activation = start.activation.identity;
        let mut core = Core::new(start).unwrap();
        let participation = *core.execution.graph().participations().next().unwrap();
        let admission = admission_intent(&mut core.execution, participation);
        let original = dispatch_admission(
            &mut core.execution,
            admission,
            1,
            OperationOutcome::Accepted,
        );
        core.execution.apply_dispatched_reply(&original).1.unwrap();
        let step = |core: &mut Core, source, n, input| {
            core.handle(
                now,
                AttemptInputClaim {
                    source,
                    input: AttemptInputId(id(n)),
                    native_cut: Some(AttemptSourceCut {
                        source: sources.native,
                        tail: None,
                    }),
                    selected_cut: None,
                },
                input,
                &mut Units(16),
            )
            .unwrap()
        };
        step(&mut core, sources.actor, 1, AttemptInput::Bootstrap);
        let begun = step(
            &mut core,
            sources.actor,
            2,
            AttemptInput::Port(LogicalToAttempt {
                activation,
                body: LogicalToAttemptBody::BeginConvergence {
                    mode: AttemptConvergenceMode::Normal,
                    remote_deadline: now.saturating_add(Duration::from_secs(3)),
                    computation_ready: None,
                    retain_diagnostics: false,
                },
            }),
        );
        assert!(!begun.stop_producing);
        let read = step(&mut core, sources.actor, 3, AttemptInput::Timer);
        let Some(AttemptEffect {
            body:
                EffectBody::ObserveProcess {
                    request,
                    participation: original,
                },
            ..
        }) = read.effect
        else {
            panic!("expected original exact process read");
        };
        assert_eq!(original, participation);
        assert_eq!(read.selection, InputSelection::Only(sources.process));
        assert_eq!(core.next_deadline(), None);
        let sample = |token| {
            AttemptInput::Executor(ExecutorFact::ProcessSnapshot {
                request: token,
                participation,
                sampled_at: now,
                observation: MembershipObservation::ExactReplacement(crate::test_support::backend(
                    99,
                )),
            })
        };
        let stale = step(
            &mut core,
            sources.process,
            4,
            sample(AttemptRequestToken::new(id(99))),
        );
        assert!(stale.effect.is_none());
        assert_eq!(stale.selection, InputSelection::Only(sources.process));
        assert_eq!(core.next_deadline(), None);
        step(&mut core, sources.process, 5, sample(request));
        let remote = step(&mut core, sources.actor, 6, AttemptInput::Timer);
        assert!(
            matches!(remote.effect, Some(AttemptEffect { body: EffectBody::Port(port), .. })
            if matches!(port.body, AttemptToLogicalBody::ConvergenceProgress { local_exited: false, closure: Some(_) }))
        );
        let local = step(
            &mut core,
            sources.actor,
            7,
            AttemptInput::Executor(ExecutorFact::LocalConvergenceExited),
        );
        assert!(
            matches!(local.effect, Some(AttemptEffect { body: EffectBody::Port(port), .. })
            if matches!(port.body, AttemptToLogicalBody::ConvergenceProgress { local_exited: true, closure: None }))
        );
        let done = step(&mut core, sources.actor, 8, AttemptInput::Timer);
        assert!(done.effect.is_none());
        assert_eq!(core.next_deadline(), None);
    }

    #[test]
    fn original_drive_replays_admission_before_establish_and_create() {
        use crate::{
            AttemptCore,
            capacity::ReservationToken,
            effect::{OperationIntent, Publication},
            input::{
                AckPayload, AttemptInput, AttemptInputClaim, LocalDispatchResult,
                OperationObservation, OperationReply, OperationReplyResult, QueuedInputGrantRef,
            },
        };
        use novarocks_coordination_contract::{
            AttemptAuthorization, AttemptAuthorizationDecision, AttemptAuthorizationRef,
            AttemptToLogicalBody,
        };
        use novarocks_execution_contract::{
            AdmissionTicketId, OperationKind, OperationOutcome,
            ParticipationAdmissionTicketReceipt, operation::OperationResult,
        };
        use std::cell::Cell;

        struct Turn;
        impl StepContext for Turn {
            fn charge(&mut self, _: usize) -> bool {
                true
            }

            fn reserve_queue_stage(
                &mut self,
                _: DispatchLane,
                control: bool,
                _: BackendProcessId,
                records: usize,
                _: usize,
            ) -> QueueStage {
                assert!(!control);
                assert_eq!(records, 1);
                QueueStage::Granted(ReservationToken(id(39)))
            }
        }

        let mut start = start();
        let root = start.plan.graph.root;
        start.plan.graph.tasks.retain(|id, _| *id == root.task_id());
        start
            .plan
            .graph
            .stages
            .retain(|id, _| *id == root.stage_id());
        start.plan.graph.edges.clear();
        let participation = start.plan.graph.tasks[&root.task_id()].participation;
        start
            .plan
            .establish_material
            .retain(|key, _| *key == participation);
        start
            .plan
            .admission_epochs
            .retain(|key, _| *key == root.backend_process_id());
        start
            .plan
            .preparation_positions
            .retain(|key, _| *key == root.backend_process_id());
        let sources = start.sources;
        let now = start.initial_now;
        let activation = start.activation.identity;
        let mut core = Core::new(start).unwrap();
        let sequence = Cell::new(0);
        let step = |core: &mut Core, source, input| {
            sequence.set(sequence.get() + 1);
            let input_id = AttemptInputId(id(sequence.get()));
            core.handle(
                now,
                AttemptInputClaim {
                    source,
                    input: input_id,
                    native_cut: Some(AttemptSourceCut {
                        source: sources.native,
                        tail: (source == sources.native).then_some(input_id),
                    }),
                    selected_cut: Some(AttemptSourceCut {
                        source: sources.observation,
                        tail: None,
                    }),
                },
                input,
                &mut Turn,
            )
            .unwrap()
        };
        let drive = |core: &mut Core| {
            let mut next = None;
            for _ in 0..16 {
                let current = next.take().unwrap_or_else(|| {
                    assert_eq!(core.next_deadline(), Some(now));
                    step(core, sources.actor, AttemptInput::Timer)
                });
                match current.effect {
                    Some(AttemptEffect {
                        id: effect,
                        body: EffectBody::Publish(Publication::ProtocolBatch(_)),
                        ..
                    }) => {
                        next = Some(step(
                            core,
                            sources.publication,
                            AttemptInput::Executor(ExecutorFact::PublicationApplied {
                                effect,
                                result: Ok(PublicationApplied::ProtocolBatch { published: true }),
                            }),
                        ));
                    }
                    Some(effect) => return effect,
                    None => {}
                }
            }
            panic!("the original candidate produced no effect");
        };
        let idle = |core: &mut Core| {
            for _ in 0..16 {
                let turn = step(core, sources.actor, AttemptInput::Timer);
                assert!(turn.effect.is_none(), "{turn:?}");
                if turn.selection == InputSelection::Prefer(sources.native) {
                    return;
                }
            }
            panic!("the original round did not return to its Native input");
        };
        let qa_reply = |core: &mut Core, token, decision| {
            step(
                core,
                sources.qa,
                AttemptInput::Port(LogicalToAttempt {
                    activation,
                    body: LogicalToAttemptBody::AuthorizationReply {
                        token,
                        decision: Ok(decision),
                    },
                }),
            )
        };
        let accept_grant = |core: &mut Core, intent: &OperationIntent, grant| {
            step(
                core,
                sources.actor,
                AttemptInput::Executor(ExecutorFact::QueuedInputGranted {
                    operation: intent.operation_id(),
                    grant,
                    queued_bytes: intent.queued_bytes(),
                }),
            )
        };

        step(&mut core, sources.actor, AttemptInput::Bootstrap);
        assert_eq!(core.next_deadline(), None);
        step(&mut core, sources.actor, AttemptInput::StartProtocol);
        let EffectBody::Port(port) = drive(&mut core).body else {
            panic!("the original participant did not request QA admission");
        };
        let AttemptToLogicalBody::AuthorizationRequest {
            token,
            request: AttemptAuthorization::BeginAdmission(request),
        } = port.body
        else {
            panic!("expected the original admission request");
        };
        assert_eq!(request.participation(), participation);
        let issue = AttemptAuthorizationRef::new(id(70));
        let Some(AttemptEffect {
            body:
                EffectBody::AdmitQueuedInput {
                    operation,
                    generation,
                    ..
                },
            ..
        }) = qa_reply(
            &mut core,
            token,
            AttemptAuthorizationDecision::AdmissionStarted(issue),
        )
        .effect
        else {
            panic!("expected a fresh WC generation after actual QA permission");
        };
        let first = QueuedInputGrantRef::new(id(71), generation);
        idle(&mut core);
        assert!(accept_grant(&mut core, &operation, first).effect.is_none());
        assert!(matches!(drive(&mut core).body,
            EffectBody::SendBatch { batch, .. } if batch.queued_inputs.as_ref() == [first]));
        idle(&mut core);

        let operation_id = request.envelope().operation_id();
        let observation = |result| {
            AttemptInput::Executor(ExecutorFact::Operation(OperationObservation {
                generation,
                reply: OperationReply {
                    operation_id,
                    kind: OperationKind::AcquireParticipationAdmissionTicket,
                    result,
                },
            }))
        };
        let unknown = || {
            observation(OperationReplyResult::Local {
                result: LocalDispatchResult::TransportUnknown,
                failure: None,
                protocol_failure: None,
                detail: None,
            })
        };
        step(&mut core, sources.native, unknown());
        let AttemptEffect {
            id: replay_admission,
            body:
                EffectBody::AdmitQueuedInput {
                    operation: replay,
                    generation: replay_generation,
                    ..
                },
            ..
        } = drive(&mut core)
        else {
            panic!("the replay did not request its own WC generation");
        };
        assert_eq!(replay.operation_id(), operation_id);
        assert!(replay_generation > generation);
        idle(&mut core);

        assert!(step(&mut core, sources.native, unknown()).effect.is_none());
        idle(&mut core);
        let ticket = ParticipationAdmissionTicketReceipt::new(
            AdmissionTicketId::try_from_bytes([0x71; 16]).unwrap(),
            participation,
            request.valid_for(),
        );
        assert!(
            matches!(step(&mut core, sources.native, observation(OperationReplyResult::Worker(
            OperationResult::Verdict {
                outcome: OperationOutcome::Accepted,
                detail: None,
                acknowledgement: Some(AckPayload::AdmissionTicket(ticket)),
            },
        ))).effect, Some(AttemptEffect {
            body: EffectBody::Withdraw { effect }, ..
        }) if effect == replay_admission)
        );
        let EffectBody::Port(port) = drive(&mut core).body else {
            panic!("late receipt did not settle the original QA issue");
        };
        let AttemptToLogicalBody::AuthorizationRequest {
            token,
            request:
                AttemptAuthorization::SettleAdmission {
                    issue: original,
                    operation_id: actual,
                    ..
                },
        } = port.body
        else {
            panic!("expected settlement of the original Admission operation");
        };
        assert_eq!(actual, operation_id);
        assert_eq!(original, issue);
        assert!(
            qa_reply(
                &mut core,
                token,
                AttemptAuthorizationDecision::AdmissionGranted
            )
            .effect
            .is_none()
        );

        let EffectBody::EstablishAuthorization { operation, request } = drive(&mut core).body
        else {
            panic!("the original participant did not request Establish permission");
        };
        let AttemptToLogicalBody::AuthorizationRequest { token, .. } = request.body else {
            panic!("expected the exact Establish QA request");
        };
        let Some(AttemptEffect {
            body:
                EffectBody::AdmitQueuedInput {
                    operation: admitted,
                    generation,
                    ..
                },
            ..
        }) = qa_reply(
            &mut core,
            token,
            AttemptAuthorizationDecision::EstablishAuthorized(AttemptAuthorizationRef::new(id(72))),
        )
        .effect
        else {
            panic!("expected separate WC admission for Establish");
        };
        assert_eq!(admitted.operation_id(), operation.operation_id());
        let establish = QueuedInputGrantRef::new(id(73), generation);
        idle(&mut core);
        assert!(
            accept_grant(&mut core, &operation, establish)
                .effect
                .is_none()
        );
        let AttemptEffect {
            id: send,
            body: EffectBody::SendBatch { batch, .. },
            ..
        } = drive(&mut core)
        else {
            panic!("expected exact Establish dispatch");
        };
        assert_eq!(batch.queued_inputs.as_ref(), [establish]);
        idle(&mut core);

        let late_grant = QueuedInputGrantRef::new(id(74), replay_generation);
        assert!(matches!(
            accept_grant(&mut core, &replay, late_grant).effect,
            Some(AttemptEffect {
                body: EffectBody::ReleaseQueuedInput { grant }, ..
            }) if grant == late_grant
        ));
        idle(&mut core);
        step(
            &mut core,
            sources.native,
            AttemptInput::Executor(ExecutorFact::EstablishRpcStarted {
                effect: send,
                operation: operation.operation_id(),
                participation,
                started_at: now,
            }),
        );
        assert!(matches!(drive(&mut core).body,
            EffectBody::AdmitQueuedInput { operation: create, .. } if create.kind() == OperationKind::CreateTask));
    }

    #[test]
    fn actor_abort_gets_exact_control_grant_and_owner_failed_never_closes_participation() {
        use crate::{
            AttemptCore,
            capacity::ReservationToken,
            input::{
                AttemptInput, AttemptInputClaim, OperationReply, OperationReplyResult,
                QueuedInputGrantRef,
            },
        };
        use novarocks_coordination_contract::{AbortIssueSettlement, AttemptToLogicalBody};
        use novarocks_execution_contract::{
            AbortCause, AbortParticipation, OperationKind, OperationOutcome,
            operation::OperationResult,
        };
        struct Turn;
        impl StepContext for Turn {
            fn charge(&mut self, _: usize) -> bool {
                true
            }
            fn reserve_queue_stage(
                &mut self,
                lane: DispatchLane,
                control: bool,
                _: BackendProcessId,
                records: usize,
                _: usize,
            ) -> QueueStage {
                assert_eq!(lane, DispatchLane::Lifecycle);
                assert!(control);
                assert_eq!(records, 1);
                QueueStage::Granted(ReservationToken(id(39)))
            }
        }

        let start = start();
        let sources = start.sources;
        let now = start.initial_now;
        let activation = start.activation.identity;
        let mut core = Core::new(start).unwrap();
        let participation = *core.execution.graph().participations().next().unwrap();
        let operation = core.execution.issue_operation_id();
        let request = AbortParticipation::new(operation, participation, AbortCause::QueryFailed);
        let actor_action = AttemptActionId::new(id(65));
        let cut = |source, tail| Some(AttemptSourceCut { source, tail });
        let step = |core: &mut Core, source, n, input, native_tail, selected_cut| {
            core.handle(
                now,
                AttemptInputClaim {
                    source,
                    input: AttemptInputId(id(n)),
                    native_cut: cut(sources.native, native_tail),
                    selected_cut,
                },
                input,
                &mut Turn,
            )
            .unwrap()
        };
        step(
            &mut core,
            sources.actor,
            1,
            AttemptInput::Bootstrap,
            None,
            None,
        );
        let accepted = step(
            &mut core,
            sources.actor,
            2,
            AttemptInput::Port(LogicalToAttempt {
                activation,
                body: LogicalToAttemptBody::Abort {
                    action: actor_action,
                    request,
                },
            }),
            None,
            None,
        );
        assert!(accepted.effect.is_none());
        assert_eq!(core.next_deadline(), Some(now));
        step(&mut core, sources.actor, 3, AttemptInput::Timer, None, None);
        step(
            &mut core,
            sources.actor,
            4,
            AttemptInput::Timer,
            None,
            cut(sources.observation, None),
        );
        step(&mut core, sources.actor, 5, AttemptInput::Timer, None, None);
        let published = step(&mut core, sources.actor, 6, AttemptInput::Timer, None, None);
        let publication = published.effect.unwrap().id;
        let admitted = step(
            &mut core,
            sources.publication,
            7,
            AttemptInput::Executor(ExecutorFact::PublicationApplied {
                effect: publication,
                result: Ok(PublicationApplied::ProtocolBatch { published: true }),
            }),
            None,
            None,
        );
        let Some(AttemptEffect {
            id: admission,
            class: crate::effect::EffectClass::Settlement,
            body:
                EffectBody::AdmitQueuedInput {
                    operation: intent,
                    generation,
                    lane: DispatchLane::Lifecycle,
                    ..
                },
        }) = admitted.effect
        else {
            panic!("expected exact Abort WC admission");
        };
        assert_eq!(intent.operation_id(), operation);
        assert_eq!(admission, generation.admission_effect());
        assert_eq!(core.execution.dispatcher().queued_items(), 0);
        assert_eq!(
            step(&mut core, sources.actor, 8, AttemptInput::Timer, None, None).round_result,
            Some(Ok(()))
        );

        let grant = QueuedInputGrantRef::new(id(70), generation);
        step(
            &mut core,
            sources.actor,
            9,
            AttemptInput::Executor(ExecutorFact::QueuedInputGranted {
                operation,
                grant,
                queued_bytes: intent.queued_bytes(),
            }),
            None,
            None,
        );
        step(
            &mut core,
            sources.actor,
            10,
            AttemptInput::Timer,
            None,
            cut(sources.observation, None),
        );
        step(
            &mut core,
            sources.actor,
            11,
            AttemptInput::Timer,
            None,
            None,
        );
        let sent = step(
            &mut core,
            sources.actor,
            12,
            AttemptInput::Timer,
            None,
            None,
        );
        assert!(matches!(sent.effect, Some(AttemptEffect {
            body: EffectBody::SendBatch { batch, .. }, class: crate::effect::EffectClass::Settlement, ..
        }) if batch.queued_inputs.as_ref() == [grant]));
        step(
            &mut core,
            sources.actor,
            13,
            AttemptInput::Timer,
            None,
            None,
        );

        let failed = step(
            &mut core,
            sources.native,
            14,
            AttemptInput::Executor(ExecutorFact::Operation(OperationObservation {
                generation,
                reply: OperationReply {
                    operation_id: operation,
                    kind: OperationKind::AbortParticipation,
                    result: OperationReplyResult::Worker(OperationResult::Verdict {
                        outcome: OperationOutcome::OwnerFailed,
                        detail: None,
                        acknowledgement: None,
                    }),
                },
            })),
            Some(AttemptInputId(id(14))),
            None,
        );
        assert!(failed.stop_producing);
        assert!(
            matches!(failed.abort_settlement, Some(AttemptEffect { body: EffectBody::Port(port), .. })
            if matches!(port.body, AttemptToLogicalBody::AbortSettlement {
                abort_action, generation: settled, receipt_generation: None, operation_id, settlement: AbortIssueSettlement::FailedClosed, ..
            } if abort_action == actor_action && settled == admission && operation_id == operation))
        );
        assert_eq!(core.execution.dispatcher().in_flight_operations(), 0);
        assert!(!core.execution.owner(participation).unwrap().is_released());
        assert!(
            core.execution
                .participation_convergence_receipt(participation)
                .is_none()
        );
        let round = step(
            &mut core,
            sources.actor,
            15,
            AttemptInput::Timer,
            None,
            None,
        );
        assert!(
            matches!(round.round_result, Some(Err(AttemptFailure::TaskProtocol(error)))
            if matches!(*error, TaskExecutionError::OperationFailed { operation_id, .. } if operation_id == operation))
        );
    }

    #[test]
    fn stop_waits_for_unused_establish_guard_before_finishing_gate_close() {
        use crate::{
            AttemptCore,
            execution::ack_phase::{admission_intent, dispatch_admission, queue_establish},
            input::{AttemptInput, AttemptInputClaim, QueuedInputGrantRef, SendGeneration},
            terminal::GatePreparation,
        };
        use novarocks_coordination_contract::{
            AttemptAuthorizationDecision, AttemptAuthorizationRef, AttemptIssueSettlement,
            AttemptStopReason, AttemptToLogicalBody,
        };
        use novarocks_execution_contract::OperationOutcome;

        let start = start();
        let sources = start.sources;
        let now = start.initial_now;
        let activation = start.activation.identity;
        let mut core = Core::new(start).unwrap();
        let participation = *core.execution.graph().participations().next().unwrap();
        let request = admission_intent(&mut core.execution, participation);
        let received =
            dispatch_admission(&mut core.execution, request, 1, OperationOutcome::Accepted);
        core.execution.apply_dispatched_reply(&received).1.unwrap();
        let grant =
            QueuedInputGrantRef::new(id(3), SendGeneration::new(AttemptActionId::new(id(3))));
        let intent = queue_establish(&mut core.execution, participation, grant);
        let request = AttemptRequestToken::new(id(60));
        let guard = AttemptAuthorizationRef::new(id(61));
        core.authorize_candidate(&intent, request, AttemptActionId::new(id(60)))
            .unwrap();
        core.apply_qa_reply(LogicalToAttempt {
            activation,
            body: LogicalToAttemptBody::AuthorizationReply {
                token: request,
                decision: Ok(AttemptAuthorizationDecision::EstablishAuthorized(guard)),
            },
        })
        .unwrap();

        let step = |core: &mut Core, source, n, input, selected_cut| {
            core.handle(
                now,
                AttemptInputClaim {
                    source,
                    input: AttemptInputId(id(n)),
                    native_cut: Some(AttemptSourceCut {
                        source: sources.native,
                        tail: None,
                    }),
                    selected_cut,
                },
                input,
                &mut Units(32),
            )
            .unwrap()
        };
        step(&mut core, sources.actor, 1, AttemptInput::Bootstrap, None);
        let stopped = step(
            &mut core,
            sources.actor,
            2,
            AttemptInput::Port(LogicalToAttempt {
                activation,
                body: LogicalToAttemptBody::Stop {
                    reason: AttemptStopReason::Client,
                    failure: AttemptFailure::TaskProtocol(Box::new(schedule(
                        "original client stop",
                    ))),
                },
            }),
            None,
        );
        assert!(stopped.stop_producing);
        let closing = step(&mut core, sources.actor, 3, AttemptInput::Timer, None);
        assert_eq!(closing.selection, InputSelection::Only(sources.qa));
        let Some(AttemptEffect {
            body: EffectBody::Port(port),
            ..
        }) = closing.effect
        else {
            panic!("expected unused Establish settlement");
        };
        let AttemptToLogicalBody::EstablishSettlement {
            token,
            authorization,
            settlement,
            ..
        } = port.body
        else {
            panic!("expected exact guard settlement");
        };
        assert_eq!(authorization, guard);
        assert_eq!(settlement, AttemptIssueSettlement::DefinitelyUnsent);
        assert_eq!(core.next_deadline(), None);
        let actual = |authorization| {
            AttemptInput::Port(LogicalToAttempt {
                activation,
                body: LogicalToAttemptBody::EstablishSettlementReply {
                    token,
                    authorization,
                    result: Ok(()),
                },
            })
        };
        let stale = step(
            &mut core,
            sources.qa,
            4,
            actual(AttemptAuthorizationRef::new(id(62))),
            None,
        );
        assert_eq!(stale.selection, InputSelection::Only(sources.qa));
        assert_eq!(core.authorizations.preparation(), GatePreparation::Applying);
        step(&mut core, sources.qa, 5, actual(guard), None);
        assert_eq!(
            core.authorizations.preparation(),
            GatePreparation::Applied { ready: true }
        );

        // The unused authorization result is not return of the original WC grant.
        assert_eq!(core.execution.dispatcher().queued_items(), 1);
        let stopped = step(&mut core, sources.actor, 6, AttemptInput::Timer, None);
        assert!(
            matches!(stopped.effect, Some(AttemptEffect { body: EffectBody::Port(port), .. })
            if matches!(port.body, AttemptToLogicalBody::AttemptTerminal(novarocks_coordination_contract::AttemptTerminal::Failed(_))))
        );
        step(&mut core, sources.actor, 7, AttemptInput::Timer, None);
        step(
            &mut core,
            sources.actor,
            8,
            AttemptInput::Timer,
            Some(AttemptSourceCut {
                source: sources.observation,
                tail: None,
            }),
        );
        step(&mut core, sources.actor, 9, AttemptInput::Timer, None);
        let published = step(&mut core, sources.actor, 10, AttemptInput::Timer, None);
        let mut release = step(
            &mut core,
            sources.publication,
            11,
            AttemptInput::Executor(ExecutorFact::PublicationApplied {
                effect: published.effect.unwrap().id,
                result: Ok(PublicationApplied::ProtocolBatch { published: true }),
            }),
            None,
        );
        for n in 12..17 {
            if !matches!(
                release.effect,
                Some(AttemptEffect {
                    body: EffectBody::ReleaseCreationPayload { .. },
                    ..
                })
            ) {
                break;
            }
            release = step(&mut core, sources.actor, n, AttemptInput::Timer, None);
        }
        assert!(
            matches!(release.effect, Some(AttemptEffect { body: EffectBody::ReleaseQueuedInput { grant: returned }, .. }) if returned == grant)
        );
    }

    #[test]
    fn terminal_replacement_waits_for_the_original_process_token() {
        use crate::{
            AttemptCore,
            input::{AttemptInput, AttemptInputClaim, MembershipObservation},
        };
        use novarocks_coordination_contract::{AttemptTerminal, AttemptToLogicalBody};

        let start = start();
        let sources = start.sources;
        let now = start.initial_now;
        let mut core = Core::new(start).unwrap();
        let participation = *core.execution.graph().participations().next().unwrap();
        let original = participation.backend_process_id();
        let replacement = crate::test_support::backend(80);
        core.terminal
            .record_execution_failure(
                TaskExecutionError::PreReadyEstablishTransportUnknown { backend: original },
                &core.execution,
            )
            .unwrap();
        let step = |core: &mut Core, source, n, input, selected_cut| {
            core.handle(
                now,
                AttemptInputClaim {
                    source,
                    input: AttemptInputId(id(n)),
                    native_cut: Some(AttemptSourceCut {
                        source: sources.native,
                        tail: None,
                    }),
                    selected_cut,
                },
                input,
                &mut Units(64),
            )
            .unwrap()
        };
        step(&mut core, sources.actor, 1, AttemptInput::Bootstrap, None);
        step(&mut core, sources.actor, 2, AttemptInput::Timer, None);
        step(
            &mut core,
            sources.actor,
            3,
            AttemptInput::Timer,
            Some(AttemptSourceCut {
                source: sources.observation,
                tail: None,
            }),
        );
        step(&mut core, sources.actor, 4, AttemptInput::Timer, None);
        let published = step(&mut core, sources.actor, 5, AttemptInput::Timer, None);
        let read = step(
            &mut core,
            sources.publication,
            6,
            AttemptInput::Executor(ExecutorFact::PublicationApplied {
                effect: published.effect.unwrap().id,
                result: Ok(PublicationApplied::ProtocolBatch { published: true }),
            }),
            None,
        );
        let Some(AttemptEffect {
            body:
                EffectBody::ObserveProcess {
                    request,
                    participation: exact,
                },
            ..
        }) = read.effect
        else {
            panic!("expected original pre-ready process sample");
        };
        assert_eq!(exact, participation);
        assert_eq!(read.selection, InputSelection::Only(sources.process));
        assert_eq!(core.next_deadline(), None);
        let sample = |token| {
            AttemptInput::Executor(ExecutorFact::ProcessSnapshot {
                request: token,
                participation,
                sampled_at: now,
                observation: MembershipObservation::ExactReplacement(replacement),
            })
        };
        let stale = step(
            &mut core,
            sources.process,
            7,
            sample(AttemptRequestToken::new(id(70))),
            None,
        );
        assert!(stale.effect.is_none());
        assert_eq!(stale.selection, InputSelection::Only(sources.process));
        step(&mut core, sources.process, 8, sample(request), None);
        step(&mut core, sources.actor, 9, AttemptInput::Timer, None);
        let terminal = step(&mut core, sources.actor, 10, AttemptInput::Timer, None);
        assert!(
            matches!(terminal.effect, Some(AttemptEffect { body: EffectBody::Port(port), .. })
            if matches!(port.body, AttemptToLogicalBody::AttemptTerminal(AttemptTerminal::Failed(AttemptFailure::BackendReplaced {
                original: observed_original, replacement: observed_replacement,
            })) if observed_original == original && observed_replacement == replacement))
        );
    }

    struct Units(usize);
    impl StepContext for Units {
        fn charge(&mut self, units: usize) -> bool {
            if let Some(left) = self.0.checked_sub(units) {
                self.0 = left;
                true
            } else {
                false
            }
        }

        fn reserve_queue_stage(
            &mut self,
            _: DispatchLane,
            _: bool,
            _: BackendProcessId,
            _: usize,
            _: usize,
        ) -> QueueStage {
            panic!("subscription drive has no Native queue reservation")
        }
    }

    struct NoTraversal;
    impl StepContext for NoTraversal {
        fn charge(&mut self, _: usize) -> bool {
            panic!("the initial empty cut has no dirty Task rows");
        }
        fn reserve_queue_stage(
            &mut self,
            _: DispatchLane,
            _: bool,
            _: BackendProcessId,
            _: usize,
            _: usize,
        ) -> QueueStage {
            panic!("publication has no Native queue reservation");
        }
    }

    #[test]
    fn only_the_actual_protocol_publication_result_releases_the_common_source() {
        let mut core = round();
        assert!(core.select_protocol_publication().unwrap());
        assert!(seal(&mut core).is_err());
        assert_eq!(
            core.advance_protocol_publication(&mut NoTraversal).unwrap(),
            PublicationProgress::Ready
        );
        let effect = AttemptActionId::new(id(40));
        assert!(core.take_protocol_publication(effect).is_some());
        assert!(seal(&mut core).is_err());
        assert_eq!(
            core.protocol_publication_applied(effect, Ok(PublicationApplied::RootStatus)),
            PublicationApplyDisposition::Stale
        );
        assert_eq!(
            core.protocol_publication_applied(
                AttemptActionId::new(id(41)),
                Ok(PublicationApplied::ProtocolBatch { published: true })
            ),
            PublicationApplyDisposition::Stale
        );
        assert!(seal(&mut core).is_err());
        assert_eq!(
            core.protocol_publication_applied(
                effect,
                Ok(PublicationApplied::ProtocolBatch { published: true })
            ),
            PublicationApplyDisposition::Applied { published: true }
        );
        assert!(seal(&mut core).unwrap().seal.is_none());
        assert!(core.root.pending());
    }
}
