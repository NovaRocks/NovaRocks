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

//! Normal retirement consumes the original Task inventory. The Retire effect
//! cannot wait for its own causal header: its actual physical cleanup and
//! publication finish first, and the still-paid Task owner then drains that
//! header and the remaining independent sources and reports.

use super::*;
use novarocks_control_runtime::exit_custody::ExitCustodyPhase;
use novarocks_control_runtime::transaction::RuntimeLedger;
use novarocks_control_runtime_core::{completion::Consumption, identity::ConsumerId};
use std::mem::ManuallyDrop;

impl TaskHostRecord {
    pub(crate) fn take_production_retire_job(
        &self,
        ticket: &EffectTicket<WorkerAction>,
        host: Arc<dyn crate::TaskExecutionHost>,
        ledger: &RuntimeLedger,
    ) -> Result<Option<super::super::participation_host::task_io::TaskIoHostJob>, TaskHostError>
    {
        if ticket.actor != self.actor
            || !matches!(&ticket.effect,
                WorkerAction::Participation(ParticipationEffect::RetireTask { task, .. }
                    | ParticipationEffect::FinalizeTaskExecution { task, .. }) if *task == self.task)
        {
            return Err(TaskHostError::WrongEffect);
        }
        self.resources
            .causal
            .runtime()
            .validate_effect_ticket(ticket)
            .map_err(TaskHostError::ActorTurn)?;
        // RetireTask is the original Core's positive final authority: a
        // healthy Task may never have needed Stop. Revoke only that original
        // unexposed capability; a queued/started capture still owes real exit.
        let stop = self.host_call(TaskHostCallKind::Stop);
        if !stop.actual_call_exited() {
            match stop.retire_unused(ledger) {
                Ok(_) => {}
                Err(TaskHostCallError::AlreadyIssued | TaskHostCallError::AlreadyCaptured) => {
                    return Ok(None);
                }
                Err(error) => return Err(TaskHostError::HostCall(error)),
            }
        }
        if !self
            .execution_borrowers_exited()
            .map_err(TaskHostError::HostCall)?
        {
            return Ok(None);
        }
        let owner = match &self.resources.production {
            Some(owner) => Some(owner.clone()),
            None => {
                // This actual Core retirement and the original never-created
                // preparation inventory prove that no Native execution port
                // was exposed. The existing RetireBorrowing role must still
                // run its real port-close/observer notifications on a host.
                if !matches!(
                    &ticket.effect,
                    WorkerAction::Participation(ParticipationEffect::RetireTask { .. })
                ) || self.resources.preparation.is_some()
                    || self.resources.pending_preparation_adoption.is_some()
                    || self.resources.completion.is_some()
                    || self.resources.completion_action.is_some()
                {
                    return Err(TaskHostError::WrongEffect);
                }
                None
            }
        };
        // A genuine strict-input cancellation withdraws rather than claims
        // consumption, even if another input already drained. Its original
        // conclusion remains Canceled while its receiver uses abort cleanup.
        let strict_canceled = self.resources.port.current().state()
            == novarocks_execution_contract::TaskState::Canceled
            && owner.as_ref().is_some_and(|owner| {
                owner.state.lock().expect("original retirement descriptor")
                    .descriptor.as_ref().is_some_and(|descriptor| {
                        descriptor.topology().inbound().iter().any(|input| {
                            input.delivery() == novarocks_execution_contract::ExchangeDeliveryRequirement::MustDrain
                        })
                    })
            });
        let exited = TaskExecutionBorrowersExited { owner: self };
        let call = self.host_call(TaskHostCallKind::RetireBorrowing).clone();
        let job = match call.take_retire_effect_job(ticket, &exited) {
            Ok(job) => job,
            // A preparation refusal can finish this once-only call before
            // its original reports and Work header retire. RetireTask still
            // owns that inventory; wait for it instead of issuing the call
            // again or treating physical exit as report consumption.
            Err(TaskHostCallError::AlreadyIssued) => return Ok(None),
            Err(error) => return Err(TaskHostError::HostCall(error)),
        };
        Ok(Some(
            super::super::participation_host::task_io::TaskIoHostJob::RetireExecution {
                job,
                call,
                owner,
                host,
                ingress: match &ticket.effect {
                    WorkerAction::Participation(ParticipationEffect::FinalizeTaskExecution {
                        normally_stopped: true,
                        ..
                    }) if !strict_canceled => crate::TaskIngressRetirement::PreserveNormalClose,
                    _ => crate::TaskIngressRetirement::Remove,
                },
                pending: self.pending.clone(),
            },
        ))
    }
    pub(crate) fn production_retired(&self) -> bool {
        match &self.resources.production {
            Some(owner) => {
                owner.is_retired()
                    && self
                        .host_call(TaskHostCallKind::RetireBorrowing)
                        .actual_call_exited()
            }
            None => {
                self.resources.preparation.is_none()
                    && self.resources.pending_preparation_adoption.is_none()
                    && self.resources.completion.is_none()
                    && self.resources.completion_action.is_none()
                    && self
                        .resources
                        .host_calls
                        .iter()
                        .all(|call| call.is_retired())
            }
        }
    }
    /// Consume each actual report at most once. The CompletionExited author's
    /// saved real Core receipt authorizes the fixed completion reports; an
    /// execution terminal or a missing causal header never does.
    pub(crate) fn advance_normal_retirement(
        &mut self,
        ledger: &RuntimeLedger,
    ) -> Result<bool, TaskHostError> {
        let mut progress = false;
        self.observe_completion_action()?;
        if !self.completion_core_observed
            && let Some(action) = &self.resources.completion_action
        {
            self.completion_core_observed = action.phase() == ExitCustodyPhase::Retired
                || action.report().consumption(ConsumerId::new(1).unwrap())?
                    == Consumption::Consumed;
        }
        if let Some(roles) = &self.resources.completion
            && !roles.is_retired()
            && roles.execution_calls_exited()
        {
            for kind in [
                TaskLifecycleInputKind::Finisher,
                TaskLifecycleInputKind::WallTime,
                TaskLifecycleInputKind::TerminalPublisher,
                TaskLifecycleInputKind::Settlement,
            ] {
                let retired = if kind == TaskLifecycleInputKind::Settlement {
                    roles.settlement_phase() == crate::settlement::SettlementPhase::Retired
                } else {
                    roles.phase(kind) == ExitCustodyPhase::Retired
                };
                if retired {
                    continue;
                }
                let report = roles.report(kind);
                for id in [2, 1] {
                    if id == 1 && !self.completion_core_observed {
                        continue;
                    }
                    let consumer = ConsumerId::new(id).unwrap();
                    if report.consumption(consumer)? == Consumption::Pending {
                        report.consume(consumer)?;
                        progress = true;
                    }
                }
            }
            // A newly retired settlement child is real progress even
            // while its finisher awaits the next original header reaping.
            let before = (
                roles.phase(TaskLifecycleInputKind::Finisher),
                roles.phase(TaskLifecycleInputKind::WallTime),
                roles.phase(TaskLifecycleInputKind::TerminalPublisher),
                roles.settlement_phase(),
            );
            roles.try_retire();
            let after = (
                roles.phase(TaskLifecycleInputKind::Finisher),
                roles.phase(TaskLifecycleInputKind::WallTime),
                roles.phase(TaskLifecycleInputKind::TerminalPublisher),
                roles.settlement_phase(),
            );
            progress |= before != after;
        }
        if let Some(statistics) = &mut self.resources.statistics
            && !statistics.is_retired()
        {
            let turn = statistics.advance().map_err(TaskHostError::Statistics)?;
            progress |= turn.submitted || turn.consumed || turn.retired;
        }
        for call in &self.resources.host_calls {
            if !call.is_retired() && call.actual_call_exited() {
                progress |= call
                    .advance_retirement(ledger)
                    .map_err(TaskHostError::HostCall)?;
            }
        }
        self.observe_lifecycle_inputs()?;
        if !self.production_retired()
            || (self.resources.preparation.is_some()
                && self.resources.preparation_remainder.is_none())
        {
            // Preparation's original body/whole reports still need their
            // lifecycle inputs. Only the actual report retirement and bundle
            // handoff prove that no preparation author remains to be issued.
            return Ok(progress);
        }
        // FinalizeTaskExecution can reach this boundary before RetireTask.
        // Fence new RPC cuts before closing their same original source; a
        // later stable read must wait on its original R for the real seal.
        self.close_request_admission();
        let ingress = self.resources.port.ingress();
        ingress.seal_source_admission();
        // A buffered open summary remains a real input obligation. It must
        // pass through the same reusable paid input before final Source4 can
        // be detached; a closed producer gate is not Core consumption.
        if let Some(open) = &mut self.resources.open_summary
            && !open.is_closed()
        {
            // A later original Task tail can wake this owner after its
            // reusable summary input has already closed on actual exit.
            // That closed input is never a new delivery opportunity.
            if !open.is_idle().map_err(TaskHostError::OpenSummary)? {
                return Ok(progress);
            }
            if open
                .deliver_next(self.resources.causal.protocol_generation())
                .map_err(TaskHostError::OpenSummary)?
                .is_some()
            {
                return Ok(true);
            }
        }
        if !ingress.open_summary_drained() {
            return Ok(progress);
        }
        // Both fixed pairs share one real header retirement per turn.
        let mut source_reap_budget = 1;
        for (index, route) in [
            crate::task_proposal_ingress::PairRoute::Open,
            crate::task_proposal_ingress::PairRoute::ConsumingOpen,
        ]
        .into_iter()
        .enumerate()
        {
            if self.normal_source_detached[index] {
                continue;
            }
            progress |= self.resources.causal.observe_closed_source_pair(route)?;
            for position in ingress
                .pair_inventory(route)
                .map_err(|error| TaskHostError::Causal(error.into()))?
            {
                if position.custody.phase() != ExitCustodyPhase::Retired {
                    return Ok(progress);
                }
            }
            for position in ingress
                .pair_inventory(route)
                .map_err(|error| TaskHostError::Causal(error.into()))?
            {
                if source_reap_budget != 0 && !position.custody.reusable() {
                    let reaped = position.custody.financial_scope().reap_ready(1)?;
                    source_reap_budget -= reaped;
                    progress |= reaped != 0;
                }
            }
            let [a, b] = ingress
                .prepare_pair_reap(route)
                .map_err(|error| TaskHostError::Causal(error.into()))?
                .map(|probe| probe.observe_reaped());
            let (Ok(a), Ok(b)) = (a, b) else {
                return Ok(progress);
            };
            let detached = ingress
                .release_physically_closed_producer_pair(route, [a, b])
                .map_err(|(error, _)| TaskHostError::Causal(error.into()))?;
            drop(detached);
            self.normal_source_detached[index] = true;
            progress = true;
        }
        // A consumed RPC may still own its process-funded delivery tail.
        // Close and physically retire the original producer pairs first;
        // remaining causal roles still block actual Task lifetime exit.
        if !self.resources.causal.is_empty() || !self.host_retired_pairs.is_empty() {
            return Ok(progress);
        }
        if !self.normal_lifetime_ended {
            if !self.retirement_projections_exited()? {
                return Ok(progress);
            }
            // All foreign execution captures have actually been dropped by
            // the physical Retire job. All remaining owner fields are neutral
            // metadata, and no source/header may still derive from this Work.
            if self
                .resources
                .host_calls
                .iter()
                .any(|call| !call.is_retired())
                || self
                    .resources
                    .completion
                    .as_ref()
                    .is_some_and(|roles| !roles.is_retired())
                || self
                    .resources
                    .statistics
                    .as_ref()
                    .is_some_and(|statistics| !statistics.is_retired())
                || self
                    .resources
                    .dynamic_filters
                    .as_ref()
                    .is_some_and(|source| !source.is_retired())
                || self
                    .resources
                    .lifecycle_inputs
                    .values()
                    .any(|input| input.phase() != ExitCustodyPhase::Retired)
            {
                return Ok(progress);
            }
            self.resources.lifetime.body_exited()?;
            self.resources.lifetime.captures_exited()?;
            self.normal_lifetime_ended = true;
            progress = true;
        }
        if self.resources.lifetime.actual_exit_ready()?
            && let Some(open) = &mut self.resources.open_summary
            && !open.is_closed()
        {
            open.close_after_exit(&self.resources.lifetime)
                .map_err(TaskHostError::OpenSummary)?;
            progress = true;
        }
        Ok(progress)
    }
    pub(crate) fn normal_retirement_ready(&self) -> bool {
        self.resources.seal_read_observation().is_ok()
    }
}
impl ProductionPreparation {
    /// The original Task lifetime retains this whole custody until this real
    /// host call and the last captured runnable/refusal destructor return.
    pub(crate) fn retire_execution_on_host(
        &self,
        host: &dyn crate::TaskExecutionHost,
        ingress: crate::TaskIngressRetirement,
    ) -> ExecutionRetirementOutcome {
        let descriptor = {
            let mut state = self.state.lock().expect("original execution retirement");
            if state.retired {
                return ExecutionRetirementOutcome::Completed;
            }
            if state.retirement_started {
                return match &state.retirement_failure {
                    Some(ExecutionRetirementFailure::Rejected(original)) => {
                        let _original_source = std::ptr::from_ref(&**original);
                        ExecutionRetirementOutcome::Rejected
                    }
                    Some(ExecutionRetirementFailure::Panicked(original)) => {
                        let _original_panic = std::ptr::from_ref(&**original);
                        ExecutionRetirementOutcome::Unresolved
                    }
                    None => ExecutionRetirementOutcome::Unresolved,
                };
            }
            state.retirement_started = true;
            state.descriptor.clone()
        };
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            if let Some(descriptor) = &descriptor {
                host.retire_receiver_checked(descriptor, ingress)?;
            }
            let (runnable, refusal, descriptor) = {
                let mut state = self
                    .state
                    .lock()
                    .expect("original physical execution captures");
                (
                    state.runnable.take(),
                    state.refusal.take(),
                    state.descriptor.take(),
                )
            };
            if let Some(refusal) = refusal {
                assert!(
                    refusal.dispose(),
                    "original preparation refusal did not exit"
                );
            }
            drop(runnable.map(ManuallyDrop::into_inner));
            drop(descriptor);
            Ok::<(), crate::HostRejection>(())
        }));
        let mut state = self
            .state
            .lock()
            .expect("actual execution retirement result");
        let (outcome, fact) = match result {
            Ok(Ok(())) => {
                state.retired = true;
                return ExecutionRetirementOutcome::Completed;
            }
            Ok(Err(rejection)) => {
                let fact = novarocks_execution_contract::failure::FailureProjection::failure_fact(
                    &rejection,
                );
                state.retirement_failure = Some(ExecutionRetirementFailure::Rejected(
                    ManuallyDrop::new(Box::new(rejection)),
                ));
                (ExecutionRetirementOutcome::Rejected, fact)
            }
            Err(payload) => {
                state.retirement_failure = Some(ExecutionRetirementFailure::Panicked(
                    ManuallyDrop::new(payload),
                ));
                (
                    ExecutionRetirementOutcome::Unresolved,
                    novarocks_execution_contract::failure::FailureFact::new(
                        novarocks_execution_contract::failure::FailureClass::Invariant,
                        crate::failure_reason::WorkerFailureReason::CompletionWorkUnresolved
                            .reason(),
                    ),
                )
            }
        };
        drop(state);
        if let (Some(health), Some((task, _, _))) = (&self.health, &self.root_binding) {
            health.report_unresolved(
                novarocks_execution::runtime::execution_health::ExecutionExitOrigin::Task {
                    identity: *task,
                    phase: novarocks_execution_contract::TaskFailurePhase::Execution,
                },
                fact,
            );
        }
        outcome
    }
}
/// The actual removed Task owner remains in the Participation's finite Task slot
/// until its physical payload destruction and original payment release finish.
/// Unexpected foreign Drop retains the same payment and panic inventory.
pub(crate) struct TaskDisposal {
    disposal_custody: super::super::task_disposal::TaskDisposalCustody,
    retirement: novarocks_control_runtime::obligation::WorkRetirementObserver,
    state: std::sync::Mutex<TaskDisposalState>,
}
struct TaskDisposalState {
    observation: Option<Box<ManuallyDrop<(crate::TaskExecutionPorts, TaskIdentity)>>>,
    origin: super::super::task_disposal::TaskDisposalOrigin,
    started: bool,
    payload: Option<Box<ManuallyDrop<Box<dyn std::any::Any + Send>>>>,
    payment: Option<(
        TaskLifetime,
        ReservationObligation,
        Option<ReservationObligation>,
        Option<ReservationObligation>,
    )>,
    panic: Option<ManuallyDrop<Box<dyn std::any::Any + Send>>>,
    // Neutral identity copied only from the same original RetireTask effect.
    // This holds no Work and never substitutes for actual disposal completion.
    accepted_custody: Option<(PayloadId, Generation)>,
    retirement_input: Option<novarocks_control_runtime_core::identity::InputId>,
}
impl TaskDisposal {
    pub(crate) fn from_original(
        resources: TaskHostResources,
        ports: Option<crate::TaskExecutionPorts>,
        authorization: super::super::task_disposal::TaskDisposalAuthorization,
    ) -> (Self, super::super::task_disposal::TaskDisposalJob) {
        assert!(
            resources.seal_read_observation().is_ok(),
            "original Task inventory is still live"
        );
        let origin = authorization.origin();
        let job = match resources.disposal.take_job(authorization) {
            Ok(job) => job,
            Err(original) => {
                // Trusted transfer from one just-validated original Role. Do
                // not unwind opaque resources on a metadata thread.
                std::mem::forget((original, resources, ports));
                std::process::abort();
            }
        };
        let retirement = resources.lifetime.retirement_observer();
        let TaskHostResources {
            active,
            preparation_remainder,
            accepted_completion_storage,
            lifetime,
            port,
            host_calls,
            disposal,
            loader,
            loading_events,
            loading_return,
            pending_loading,
            preparation,
            pending_preparation_adoption,
            source_partials,
            completion,
            completion_action,
            lifecycle_inputs,
            statistics,
            dynamic_filters,
            open_summary,
            causal,
            production,
        } = resources;
        let task = lifetime.observation().identity();
        (
            Self {
                disposal_custody: disposal,
                retirement,
                state: std::sync::Mutex::new(TaskDisposalState {
                    observation: ports.map(|ports| Box::new(ManuallyDrop::new((ports, task)))),
                    origin,
                    started: false,
                    payload: Some(Box::new(ManuallyDrop::new(Box::new((
                        port,
                        host_calls,
                        loader,
                        loading_events,
                        loading_return,
                        pending_loading,
                        preparation,
                        pending_preparation_adoption,
                        source_partials,
                        completion,
                        completion_action,
                        lifecycle_inputs,
                        statistics,
                        dynamic_filters,
                        open_summary,
                        causal,
                        production,
                    ))))),
                    payment: Some((
                        lifetime,
                        active,
                        preparation_remainder,
                        accepted_completion_storage,
                    )),
                    panic: None,
                    accepted_custody: None,
                    retirement_input: None,
                }),
            },
            job,
        )
    }
    pub(crate) fn key(
        &self,
    ) -> (
        novarocks_control_runtime_core::identity::ScopeId,
        novarocks_control_runtime_core::identity::WorkId,
    ) {
        self.disposal_custody.key()
    }
    pub(crate) fn complete(&self) -> bool {
        self.disposal_custody.is_retired()
            && self.retirement.is_retired()
            && self
                .state
                .lock()
                .expect("original Task disposal payment")
                .payment
                .is_none()
    }
    /// Preserve this original effect's custody identity before handing its
    /// physical disposal job off. A rejected creation retains its separate
    /// same-identity retry protocol rather than becoming an accepted Task.
    pub(crate) fn bind_custody_retirement(&self, payload: PayloadId, close_epoch: Generation) {
        let mut state = self.state.lock().expect("original Task custody identity");
        match state.origin {
            super::super::task_disposal::TaskDisposalOrigin::AcceptedTerminal { .. } => {
                assert!(
                    state
                        .accepted_custody
                        .replace((payload, close_epoch))
                        .is_none(),
                    "one original accepted Task custody identity"
                );
            }
            super::super::task_disposal::TaskDisposalOrigin::RejectedCreation {
                payload: original,
                ..
            } => assert_eq!(payload, original, "same rejected creation payload"),
            super::super::task_disposal::TaskDisposalOrigin::OwnerFailed => {
                panic!("failed-owner disposal has no normal Task custody reply");
            }
        }
    }
    /// Reading this neutral identity requires the same disposal role, Task
    /// lifetime header and every original payment to have actually retired.
    pub(crate) fn accepted_custody(&self) -> Option<(PayloadId, Generation)> {
        assert!(self.complete(), "original Task disposal is not complete");
        let state = self.state.lock().expect("original accepted Task custody");
        match state.origin {
            super::super::task_disposal::TaskDisposalOrigin::AcceptedTerminal { .. } => Some(
                state
                    .accepted_custody
                    .expect("original RetireTask custody identity"),
            ),
            _ => None,
        }
    }
    pub(crate) fn rejected_payload(&self) -> Option<PayloadId> {
        match self
            .state
            .lock()
            .expect("actual Task disposal origin")
            .origin
        {
            super::super::task_disposal::TaskDisposalOrigin::RejectedCreation {
                payload, ..
            } => Some(payload),
            _ => None,
        }
    }
    pub(crate) fn retirement_input(
        &self,
    ) -> Option<novarocks_control_runtime_core::identity::InputId> {
        self.state
            .lock()
            .expect("original disposal receipt")
            .retirement_input
    }
    pub(crate) fn bind_retirement_input(
        &self,
        input: novarocks_control_runtime_core::identity::InputId,
    ) {
        assert!(
            self.state
                .lock()
                .expect("original disposal receipt")
                .retirement_input
                .replace(input)
                .is_none(),
            "one original disposal receipt"
        );
    }
    pub(crate) fn take_failed_job(
        &self,
        stage: &super::super::failure::ScopePublicationsDrained<'_>,
    ) -> Result<
        super::super::task_disposal::TaskDisposalJob,
        super::super::task_disposal::TaskDisposalError,
    > {
        let authorization = self.disposal_custody.authorize_failure(stage)?;
        self.disposal_custody
            .take_job(authorization)
            .map_err(|(error, _authorization)| error)
    }
    /// Only TaskDisposalJob calls this on a physical worker after its original
    /// Cleanup authority really started. A panic never retries partial Drop.
    pub(crate) fn dispose_on_host(
        &self,
        origin: super::super::task_disposal::TaskDisposalOrigin,
    ) -> bool {
        let (payload, observation) = {
            let mut state = self.state.lock().expect("actual Task physical payload");
            if state.started || state.panic.is_some() {
                return false;
            }
            state.started = true;
            state.origin = origin;
            (state.payload.take(), state.observation.take())
        };
        if let Some(mut payload) = payload {
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                // SAFETY: this stable original frame is destroyed once. A
                // partial unwind is retained in place and never moved/retried.
                unsafe { ManuallyDrop::drop(payload.as_mut()) };
            }));
            if let Err(panic) = result {
                let mut state = self.state.lock().expect("original Task destruction panic");
                state.payload = Some(payload);
                state.observation = observation;
                state.panic = Some(ManuallyDrop::new(panic));
                return false;
            }
            drop(payload);
        }
        if let Some(mut observation) = observation {
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let (ports, task) = &**observation;
                if let super::super::task_disposal::TaskDisposalOrigin::AcceptedTerminal { state } =
                    origin
                {
                    ports.observe(crate::TaskProtocolEvent::task_terminal_retained(
                        *task, state,
                    ));
                }
                // SAFETY: original stable observer captures, once and only on
                // this physical body. Failure retains its partially dropped frame.
                unsafe { ManuallyDrop::drop(observation.as_mut()) };
            }));
            if let Err(panic) = result {
                let mut state = self.state.lock().expect("original Task observer panic");
                state.observation = Some(observation);
                state.panic = Some(ManuallyDrop::new(panic));
                return false;
            }
            drop(observation);
        }
        true
    }
    /// The original A position stays occupied through physical exit AND the
    /// disposal role's actual report/header/payment tail. A later Core receipt
    /// retains only neutral disposal metadata and grants no execution capacity.
    pub(crate) fn advance_retirement(&self, ledger: &RuntimeLedger) -> Result<bool, TaskHostError> {
        let progress = self
            .disposal_custody
            .advance_retirement(ledger)
            .map_err(TaskHostError::Disposal)?;
        if !self.disposal_custody.is_retired() || !self.retirement.is_retired() {
            return Ok(progress);
        }
        let payment = {
            let mut state = self.state.lock().expect("actual Task final payment exit");
            let Some((_, active, preparation, source)) = state.payment.as_mut() else {
                return Ok(progress);
            };
            for capacity in [preparation, source].into_iter().flatten() {
                if !capacity.is_released() {
                    capacity.release_quota(&capacity.resources()?.quota)?;
                    capacity.finish()?;
                }
            }
            // All sibling roles and their original storage have truly retired.
            // Return the remaining quota at this actual exit gate, then destroy
            // only unused owned positions from the original last Task bundle.
            active.release_quota(&active.resources()?.quota)?;
            active.finish()?;
            state.payment.take()
        };
        drop(payment);
        Ok(true)
    }
}

#[cfg(test)]
impl TaskHostRecord {
    pub(crate) fn print_neutral_retirement_inventory(&self) {
        eprintln!(
            "Task retirement task={:?} request_open={} sources_detached={:?} lifetime_ended={} lifetime_ready={:?} completion_core={} partials={} pending_preparation={} accepted_remainder={}",
            self.task,
            self.request_admission_open,
            self.normal_source_detached,
            self.normal_lifetime_ended,
            self.resources.lifetime.actual_exit_ready(),
            self.completion_core_observed,
            self.resources.source_partials.len(),
            self.resources.pending_preparation_adoption.is_some(),
            self.resources.preparation_remainder.is_some()
        );
        if let Some(owner) = &self.resources.production {
            let state = owner
                .state
                .lock()
                .expect("original Task retirement diagnostic");
            eprintln!(
                "Task production retired={} activated={} runnable={} refusal={} panic={} root={} root_author_published={} root_handoff={} root_author={}",
                state.retired,
                state.activated,
                state.runnable.is_some(),
                state.refusal.is_some(),
                state.retirement_failure.is_some(),
                state.root.is_some(),
                state.root_ownership_reported,
                state.root_handoff.is_some(),
                state.root_idle_author.is_some()
            );
        }
        for call in &self.resources.host_calls {
            eprintln!(
                "Task original host call kind={:?} phase={:?} actual_exit={} retired={} panic={}",
                call.kind(),
                call.phase(),
                call.actual_call_exited(),
                call.is_retired(),
                call.has_panic()
            );
        }
        if let Some(statistics) = &self.resources.statistics {
            eprintln!(
                "Task statistics phase={:?} retired={}",
                statistics.phase(),
                statistics.is_retired()
            );
        }
        if let Some(open) = &self.resources.open_summary {
            eprintln!(
                "Task open summary idle={:?} closed={} source_drained={}",
                open.is_idle(),
                open.is_closed(),
                self.resources.port.ingress().open_summary_drained()
            );
        }
        for (index, route) in [
            crate::task_proposal_ingress::PairRoute::Open,
            crate::task_proposal_ingress::PairRoute::ConsumingOpen,
        ]
        .into_iter()
        .enumerate()
        {
            if self.normal_source_detached[index] {
                continue;
            }
            match self.resources.port.ingress().pair_inventory(route) {
                Ok(positions) => {
                    for position in positions {
                        eprintln!(
                            "Task final source route={} address={:?} source={:?} position_phase={:?} role_phase={:?} actual_exit={:?} reaped={}",
                            index,
                            position.key.address(),
                            position.source,
                            position.phase,
                            position.custody.phase(),
                            position.custody.actual_exit_ready(),
                            position.custody.reusable()
                        );
                    }
                }
                Err(error) => eprintln!(
                    "Task final source route={} inventory_error={error:?}",
                    index
                ),
            }
        }
    }
}
