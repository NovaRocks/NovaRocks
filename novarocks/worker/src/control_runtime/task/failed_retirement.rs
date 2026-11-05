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

//! Failure takeover of the elected Task's original inventory. Cursors below
//! schedule metadata visits; only the original holders sign physical exit.

use super::*;
use crate::control_runtime::{
    actors::WorkerMessage, causal::CausalSourceRetirement, failure::ScopePublicationsDrained,
};
use crate::task_port::TaskProjection;
use crate::task_proposal_ingress::{CausalEventInput, PairRoute, SummaryInput};
use novarocks_control_runtime::exit_custody::ExitCustodyPhase;
use novarocks_control_runtime::transaction::RuntimeLedger;
use novarocks_control_runtime_core::{
    completion::Consumption,
    identity::{ConsumerId, ScopeId, WorkId},
};
use novarocks_worker_protocol::proposal_summary::{CutError, CutId};
use std::mem::ManuallyDrop;

const CLASSES: usize = 9;
/// A bounded no-progress sweep, not an exit ledger. A real holder return
/// wakes its original Task and starts another sweep of these same owners.
pub(super) struct FailedRetirementCursor {
    class: usize,
    lifecycle: Option<TaskLifecycleInputKind>,
    cut: Option<CutId>,
    probes_left: usize,
}
impl Default for FailedRetirementCursor {
    fn default() -> Self {
        Self {
            class: 0,
            lifecycle: None,
            cut: None,
            probes_left: CLASSES,
        }
    }
}
#[must_use = "The original failure custodian must retain every returned owner"]
#[derive(Default)]
pub(crate) struct FailedTaskStep {
    pub progressed: bool,
    pub runnable: bool,
    pub action: Option<FailedTaskAction>,
    /// Large neutral bodies must be disposed outside the metadata turn by
    /// the same original failure Work, not dropped while returning this step.
    pub neutral: Option<FailedTaskNeutral>,
    pub root: Option<RootParticipationRetention>,
    pub detached_projection: Option<Arc<TaskProjection>>,
    pub ready_for_disposal: bool,
}
#[must_use = "Dispose the exact abandoned neutral body on its original failure owner"]
pub(crate) enum FailedTaskNeutral {
    Queued {
        _original: CausalEventInput,
    },
    Rejected {
        _original_prefix: Option<WorkerMessage>,
        _original_event: WorkerMessage,
    },
    Open {
        _original: SummaryInput,
    },
    OpenRejected {
        _original: WorkerMessage,
    },
    Loading {
        _original: [Option<novarocks_worker_protocol::participation::ParticipationInput>; 2],
    },
}
/// Each variant contains an existing unique physical permission. The queue
/// key is scheduling metadata; it does not grant another Work or capture.
#[must_use = "Physical queue refusal must return this same original action"]
pub(crate) struct FailedTaskAction {
    key: (ScopeId, WorkId),
    pending: Arc<dyn TaskPendingNotice>,
    kind: Physical,
}
enum Physical {
    Stop(TaskHostCallJob),
    Retire {
        job: TaskHostCallJob,
        owner: Option<Arc<ProductionPreparation>>,
        host: Arc<dyn crate::TaskExecutionHost>,
    },
    Settlement(crate::settlement::FailedSettlementDispatch),
    Filters(super::super::dynamic_filters::TaskDynamicFilterHostJob),
    Loader(LoadingHostJob),
    UnstartedPreparation(Box<TaskPreparationJob>),
    ActivateStopped {
        preparation: PreparationCustody,
        owner: Arc<ProductionPreparation>,
    },
    LoaderInput(super::super::loading::InputDisposalJob),
    Withdraw(InputAuthor),
    Rollback {
        preparation: PreparationCustody,
        owner: Arc<ProductionPreparation>,
        host: Arc<dyn crate::TaskExecutionHost>,
    },
    Source {
        key: (ScopeId, WorkId),
        source:
            Box<ManuallyDrop<novarocks_execution_contract::RetainedTaskSource<ExecutionFailure>>>,
    },
}
impl FailedTaskAction {
    pub(crate) fn queue_key(&self) -> (ScopeId, WorkId) {
        self.key
    }
    pub(crate) fn run(self) {
        let Self { kind, pending, .. } = self;
        match kind {
            Physical::Stop(job) => job.run(),
            Physical::Retire { job, owner, host } => job.run_failure_retirement(owner, host),
            Physical::Settlement(job) => {
                if let Err(error) = job.run() {
                    tracing::error!(?error, "Original failed settlement dispatch refused");
                }
            }
            Physical::Filters(job) => job.run(),
            Physical::Loader(job) => job.run_on_host(),
            Physical::UnstartedPreparation(job) => {
                job.dispose_failed_on_host();
                // The inner body notice can race the outer child's final
                // body_exited write. This synchronous tail has now returned;
                // observe that last original child transition as well.
                pending.pending();
            }
            Physical::ActivateStopped { preparation, owner } => {
                // A pending provider/adoption owns its real readiness wake.
                // Returning this physical invocation is not adoption progress.
                if owner.activate(&preparation) {
                    pending.pending();
                }
            }
            Physical::LoaderInput(job) => {
                job.run();
            }
            Physical::Withdraw(author) => {
                if let Err(error) = author.withdraw() {
                    tracing::error!(?error, "Original unused lifecycle author exit refused");
                }
            }
            Physical::Rollback {
                preparation,
                owner,
                host,
            } => owner.rollback_on_host(&preparation, host.as_ref(), pending.as_ref()),
            Physical::Source { mut source, .. } => {
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    // SAFETY: one destruction attempt at the same stable
                    // address. A partial unwind never retries the carrier.
                    unsafe {
                        ManuallyDrop::drop(source.as_mut());
                    }
                }));
                if let Err(panic) = result {
                    std::mem::forget((source, pending));
                    std::panic::resume_unwind(panic);
                }
                drop(source);
                // This synchronous carrier disposal actually returned. Its
                // last alias may still retain the original source role.
                pending.pending();
            }
        }
        // All other variants notify through their original owner at the
        // actual return/exit boundary. In particular Rollback(false) and a
        // Deferred settlement park on that owner's real readiness source.
    }
}

impl TaskHostRecord {
    fn failed_action(&self, kind: Physical) -> FailedTaskAction {
        // FairWorkQueue retains membership while a body is running. Use
        // each original role's key so distinct Task roles cannot collide
        // merely because they share this lifetime parent.
        let key = match &kind {
            Physical::Stop(job) | Physical::Retire { job, .. } => job.queue_key(),
            Physical::Settlement(job) => job.queue_key(),
            Physical::Filters(job) => job.queue_key(),
            Physical::Loader(job) => job.queue_key(),
            Physical::UnstartedPreparation(job) => job.queue_key(),
            Physical::ActivateStopped { preparation, .. }
            | Physical::Rollback { preparation, .. } => preparation.key(),
            Physical::LoaderInput(job) => job.queue_key(),
            Physical::Withdraw(author) => {
                let work = author.custody().work_id();
                (work.scope, work.work)
            }
            Physical::Source { key, .. } => *key,
        };
        FailedTaskAction {
            key,
            pending: self.pending.clone(),
            kind,
        }
    }
    fn failed_source_key(
        ingress: &TaskProposalIngress,
        route: PairRoute,
        ordinal: usize,
    ) -> Result<(ScopeId, WorkId), TaskHostError> {
        let owner = ingress
            .pair_inventory(route)
            .map_err(|error| TaskHostError::Causal(error.into()))?
            .into_iter()
            .nth(ordinal)
            .ok_or(TaskHostError::Causal(CausalError::IngressGeometry))?;
        Ok(owner.custody.queue_key())
    }
    fn advance_source_cleanups(
        positions: impl IntoIterator<Item = super::super::source_positions::SourcePositionObservation>,
    ) -> Result<bool, TaskHostError> {
        let mut progressed = false;
        for position in positions {
            progressed |= position
                .custody
                .advance_cleanup()
                .map_err(|error| match error {
                    super::super::source_custody::SourceCleanupError::Exit(error) => {
                        TaskHostError::Causal(CausalError::ExitCustody(error))
                    }
                    super::super::source_custody::SourceCleanupError::ActorTurn(error) => {
                        TaskHostError::ActorTurn(error)
                    }
                })?;
        }
        Ok(progressed)
    }
    /// Visit at most budget original owner classes, and return at most one
    /// physical action. Awaiting a foreign call or alias does not self-wake.
    pub(crate) fn advance_failed_retirement(
        &mut self,
        stage: &ScopePublicationsDrained<'_>,
        ledger: &RuntimeLedger,
        execution: &Arc<dyn crate::TaskExecutionHost>,
        budget: usize,
    ) -> Result<FailedTaskStep, TaskHostError> {
        stage
            .validate_scope(self.resources.causal.runtime(), self.actor, &self.scope)
            .map_err(TaskHostError::ActorTurn)?;
        let mut step = FailedTaskStep::default();
        if budget == 0 {
            return Ok(step);
        }
        self.close_request_admission();
        self.resources.port.ingress().fail();
        self.resources.port.ingress().seal_source_admission();
        // A caller invokes this method only from a genuine owner wake. This
        // finite sweep also catches another class after one late holder exits.
        if self.failed_retirement.probes_left == 0 {
            self.failed_retirement.probes_left = CLASSES;
        }
        for _ in 0..budget.min(CLASSES) {
            let class = self.failed_retirement.class;
            self.failed_retirement.class = (class + 1) % CLASSES;
            self.failed_retirement.probes_left =
                self.failed_retirement.probes_left.saturating_sub(1);
            match class {
                0 => self.failed_calls(stage, ledger, execution, &mut step)?,
                1 => self.failed_loading(stage, &mut step)?,
                2 => self.failed_preparation(stage, execution, &mut step)?,
                3 => self.failed_author(stage, &mut step)?,
                4 => self.failed_completion(stage, &mut step)?,
                5 => self.failed_repeated_sources(stage, &mut step)?,
                6 => self.failed_cut(stage, &mut step)?,
                7 => self.failed_open(stage, &mut step)?,
                8 => self.failed_final_inventory(stage, ledger, &mut step)?,
                _ => unreachable!("fixed original owner classes"),
            }
            // Moving an original physical action is not a new readiness
            // observation. In particular a quick Pending rollback must not
            // restart this sweep forever before its child can actually exit.
            if step.progressed && step.action.is_none() {
                self.failed_retirement.probes_left = CLASSES;
            }
            if step.action.is_some()
                || step.neutral.is_some()
                || step.root.is_some()
                || step.detached_projection.is_some()
                || step.ready_for_disposal
            {
                break;
            }
        }
        step.runnable |= self.failed_retirement.probes_left != 0;
        Ok(step)
    }
    fn failed_calls(
        &mut self,
        stage: &ScopePublicationsDrained<'_>,
        ledger: &RuntimeLedger,
        execution: &Arc<dyn crate::TaskExecutionHost>,
        step: &mut FailedTaskStep,
    ) -> Result<(), TaskHostError> {
        for call in &self.resources.host_calls {
            if !call.is_retired() && call.actual_call_exited() {
                step.progressed |= call
                    .advance_retirement(ledger)
                    .map_err(TaskHostError::HostCall)?;
            }
        }
        let stop = self.host_call(TaskHostCallKind::Stop);
        if !stop.actual_call_exited() {
            match stop.take_failure_job(stage) {
                Ok(job) => {
                    step.action = Some(self.failed_action(Physical::Stop(job)));
                    step.progressed = true;
                }
                Err(TaskHostCallError::AlreadyIssued | TaskHostCallError::Withdrawn) => {}
                Err(error) => return Err(TaskHostError::HostCall(error)),
            }
            return Ok(());
        }
        let retire = self.host_call(TaskHostCallKind::RetireBorrowing);
        if !retire.actual_call_exited() {
            match self.take_failed_retire_borrowing_job(stage) {
                Ok(Some(job)) => {
                    step.action = Some(self.failed_action(Physical::Retire {
                        job,
                        owner: self.resources.production.clone(),
                        host: execution.clone(),
                    }));
                    step.progressed = true;
                }
                Ok(None) | Err(TaskHostCallError::AlreadyIssued | TaskHostCallError::Withdrawn) => {
                }
                Err(error) => return Err(TaskHostError::HostCall(error)),
            }
        }
        Ok(())
    }
    fn failed_loading(
        &mut self,
        stage: &ScopePublicationsDrained<'_>,
        step: &mut FailedTaskStep,
    ) -> Result<(), TaskHostError> {
        if let Some(loader) = &self.resources.loader {
            loader.cancel();
            // Actual input-role exit, not loader return or a missing carrier,
            // settles an adopted old DisposeLoadedInput effect independently
            // of its runtime input's later header retirement.
            step.progressed |= loader
                .advance_input_cleanup()
                .map_err(|error| match error {
                    super::super::loading::InputCleanupError::Exit(error) => {
                        TaskHostError::Loading(LoadingEventError::Role(error))
                    }
                    super::super::loading::InputCleanupError::ActorTurn(error) => {
                        TaskHostError::ActorTurn(error)
                    }
                })?;
        }
        if let Some(job) = self.resources.pending_loading.take() {
            step.action = Some(self.failed_action(Physical::Loader(job)));
            step.progressed = true;
            return Ok(());
        }
        if let Some(returned) = self.take_loading_return()? {
            if let Err((error, original)) = self.observe_loading_job_return(returned) {
                // A shared origin invariant cannot destroy the original
                // return receipt or its captures on this metadata stack.
                std::mem::forget(original);
                return Err(error);
            }
            step.progressed = true;
        }
        if let Some(loader) = &self.resources.loader
            && let Some(job) = loader.take_input_disposal()
        {
            step.action = Some(self.failed_action(Physical::LoaderInput(job)));
            step.progressed = true;
            return Ok(());
        }
        if let Some(events) = &self.resources.loading_events {
            // This owner saves exact abandoned Core receipts before any
            // original loader/event header can retire.
            let turn = events
                .advance_failed(stage, &mut self.resources.causal, self.core_generation)
                .map_err(TaskHostError::Loading)?;
            step.progressed |= turn.progressed;
            if let Some(receipt) = turn.author_return {
                if let Err((error, original)) = self.observe_loading_author_return(receipt) {
                    std::mem::forget(original);
                    return Err(error);
                }
                step.progressed = true;
            }
            if turn.rejected.iter().any(Option::is_some) {
                step.neutral = Some(FailedTaskNeutral::Loading {
                    _original: turn.rejected,
                });
            }
        }
        Ok(())
    }
    fn failed_preparation(
        &mut self,
        stage: &ScopePublicationsDrained<'_>,
        execution: &Arc<dyn crate::TaskExecutionHost>,
        step: &mut FailedTaskStep,
    ) -> Result<(), TaskHostError> {
        if let Some(job) = self.resources.pending_preparation_adoption.take() {
            step.action = Some(self.failed_action(Physical::UnstartedPreparation(Box::new(job))));
            step.progressed = true;
            return Ok(());
        }
        if let Some(owner) = &self.resources.production {
            if let Some(author) = owner
                .state
                .lock()
                .expect("original failed Root author")
                .root_idle_author
                .take()
            {
                step.action = Some(self.failed_action(Physical::Withdraw(author)));
                step.progressed = true;
                return Ok(());
            }
            if let Some(root) = owner
                .state
                .lock()
                .expect("original failed Root token")
                .root_handoff
                .take()
            {
                step.root = Some(root);
                step.progressed = true;
                return Ok(());
            }
        }
        // An already submitted dormant runtime can own terminal/finisher
        // capabilities before its Installed input reaches the failed Core.
        // Stop actually returned first. Drain this same stopped runtime using
        // its original adoption cell; never invent Installed or run the
        // mutually exclusive never-adopted rollback.
        if self.host_call(TaskHostCallKind::Stop).actual_call_exited()
            && self.resources.port.is_stopped()
            && let (Some(preparation), Some(owner)) =
                (&self.resources.preparation, &self.resources.production)
            && preparation.body_and_captures_exited()
        {
            let mut state = owner
                .state
                .lock()
                .expect("original failed stopped activation");
            if state.runnable.is_some() && !state.activated && !state.activation_queued {
                state.activation_queued = true;
                step.action = Some(self.failed_action(Physical::ActivateStopped {
                    preparation: preparation.clone(),
                    owner: owner.clone(),
                }));
                step.progressed = true;
                return Ok(());
            }
        }
        if let Some(job) = self.take_preparation_rollback_job(execution.clone()) {
            let super::super::participation_host::task_io::TaskIoHostJob::RollbackPreparation {
                preparation,
                owner,
                host,
                ..
            } = job
            else {
                unreachable!("same original rollback issuer");
            };
            step.action = Some(self.failed_action(Physical::Rollback {
                preparation,
                owner,
                host,
            }));
            step.progressed = true;
            return Ok(());
        }
        let Some(preparation) = self.resources.preparation.as_ref() else {
            return Ok(());
        };
        let whole = preparation.publish_whole_exit()?;
        for (report, retired, exited) in [
            (
                preparation.body_report(),
                preparation.body_report_retired(),
                preparation.body_and_captures_exited(),
            ),
            (
                preparation.exit_report(),
                preparation.exit_report_retired(),
                whole,
            ),
        ] {
            if retired {
                continue;
            }
            step.progressed |= abandon_report(stage, self, report)?;
            let host = ConsumerId::new(2).unwrap();
            if exited && report.consumption(host)? == Consumption::Pending {
                report.consume(host)?;
                step.progressed = true;
            }
        }
        if whole && self.resources.preparation_remainder.is_none() {
            step.progressed |= self.collect_preparation_remainder()?;
        }
        Ok(())
    }
    fn failed_author(
        &mut self,
        stage: &ScopePublicationsDrained<'_>,
        step: &mut FailedTaskStep,
    ) -> Result<(), TaskHostError> {
        use std::ops::Bound::{Excluded, Unbounded};
        let next = self
            .resources
            .lifecycle_inputs
            .range((
                self.failed_retirement.lifecycle.map_or(Unbounded, Excluded),
                Unbounded,
            ))
            .next()
            .or_else(|| self.resources.lifecycle_inputs.first_key_value())
            .map(|(&kind, input)| (kind, input.clone()));
        if let Some((kind, input)) = next {
            self.failed_retirement.lifecycle = Some(kind);
            step.runnable |= self
                .resources
                .lifecycle_inputs
                .range((Excluded(kind), Unbounded))
                .next()
                .is_some();
            let before = input.phase();
            if before != ExitCustodyPhase::Retired {
                let core = input.report().consumption(ConsumerId::new(1).unwrap())?;
                input
                    .observe_failed_consumers(
                        stage,
                        &mut self.resources.causal,
                        self.core_generation,
                    )
                    .map_err(TaskHostError::CompletionAction)?;
                step.progressed |= before != input.phase()
                    || (input.phase() != ExitCustodyPhase::Retired
                        && core != input.report().consumption(ConsumerId::new(1).unwrap())?);
            }
        }
        if let Some(action) = &self.resources.completion_action {
            let before = action.phase();
            if before != ExitCustodyPhase::Retired {
                let core = action.report().consumption(ConsumerId::new(1).unwrap())?;
                action
                    .observe_failed_consumers(
                        stage,
                        &mut self.resources.causal,
                        self.core_generation,
                    )
                    .map_err(TaskHostError::CompletionAction)?;
                step.progressed |= before != action.phase()
                    || (action.phase() != ExitCustodyPhase::Retired
                        && core != action.report().consumption(ConsumerId::new(1).unwrap())?);
            }
        }
        Ok(())
    }
    fn failed_completion(
        &self,
        stage: &ScopePublicationsDrained<'_>,
        step: &mut FailedTaskStep,
    ) -> Result<(), TaskHostError> {
        let Some(roles) = &self.resources.completion else {
            return Ok(());
        };
        let before = roles.settlement_phase();
        roles.observe_unused_settlement();
        step.progressed |= before != roles.settlement_phase();
        if let Some(job) = roles
            .take_failed_settlement_dispatch(
                stage,
                self.resources.causal.runtime(),
                self.actor,
                self.resources.port.clone(),
            )
            .map_err(TaskHostError::FailureSettlement)?
        {
            step.action = Some(self.failed_action(Physical::Settlement(job)));
            step.progressed = true;
            return Ok(());
        }
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
            step.progressed |= abandon_report(stage, self, report)?;
            if roles.execution_calls_exited() {
                let host = ConsumerId::new(2).unwrap();
                if report.consumption(host)? == Consumption::Pending {
                    report.consume(host)?;
                    step.progressed = true;
                }
            }
        }
        let before = (
            roles.phase(TaskLifecycleInputKind::Finisher),
            roles.phase(TaskLifecycleInputKind::WallTime),
            roles.phase(TaskLifecycleInputKind::TerminalPublisher),
            roles.settlement_phase(),
        );
        roles.try_retire();
        step.progressed |= before
            != (
                roles.phase(TaskLifecycleInputKind::Finisher),
                roles.phase(TaskLifecycleInputKind::WallTime),
                roles.phase(TaskLifecycleInputKind::TerminalPublisher),
                roles.settlement_phase(),
            );
        Ok(())
    }
    fn failed_repeated_sources(
        &mut self,
        stage: &ScopePublicationsDrained<'_>,
        step: &mut FailedTaskStep,
    ) -> Result<(), TaskHostError> {
        if let Some(statistics) = &mut self.resources.statistics
            && !statistics.is_retired()
        {
            let turn = statistics
                .advance_failed(stage, self.core_generation, 1)
                .map_err(TaskHostError::Statistics)?;
            step.progressed |= turn.progressed;
        }
        if let Some(filters) = &mut self.resources.dynamic_filters
            && !filters.is_retired()
        {
            let turn = filters
                .advance_failed(stage, 1)
                .map_err(TaskHostError::DynamicFilters)?;
            step.progressed |= turn.progressed;
            if let Some(job) = turn.job {
                step.action = Some(self.failed_action(Physical::Filters(job)));
                step.progressed = true;
            }
        }
        Ok(())
    }
    fn failed_cut(
        &mut self,
        stage: &ScopePublicationsDrained<'_>,
        step: &mut FailedTaskStep,
    ) -> Result<(), TaskHostError> {
        // A queued value never transferred to the runtime is still the same
        // original failed-owner body. Save its abandoned receipt first.
        if let Some(original) = self.resources.causal.take_failed_queued(stage)? {
            step.neutral = Some(FailedTaskNeutral::Queued {
                _original: original,
            });
            step.progressed = true;
            return Ok(());
        }
        if let Some((_, retired)) = self.host_retired_pairs.first_key_value() {
            let positions = self
                .resources
                .port
                .ingress()
                .owned_pair_inventory(&retired.pair)
                .map_err(|error| TaskHostError::Causal(error.into()))?;
            let cleanup_progress = Self::advance_source_cleanups(positions)?;
            step.progressed |= cleanup_progress;
            if cleanup_progress {
                self.pending_notice().pending();
            }
            // The failed-owner helper drains only the original returned pair;
            // it never enters normal Core observation.
            let retirement = self.advance_retired_host_pair()?;
            step.progressed |= retirement.progressed;
            step.runnable |= retirement.runnable;
            if retirement.progressed {
                self.pending_notice().pending();
            }
            return Ok(());
        }
        let next = self
            .resources
            .causal
            .cut_inventory(self.failed_retirement.cut, 1)
            .into_iter()
            .next()
            .or_else(|| {
                // A remaining last cut can need another bounded header
                // retirement after real progress renewed this sweep. Wrap
                // within this same class visit rather than spending its last
                // probe only clearing the cursor and then parking.
                self.resources
                    .causal
                    .cut_inventory(None, 1)
                    .into_iter()
                    .next()
            });
        let Some(cut) = next else {
            self.failed_retirement.cut = None;
            return Ok(());
        };
        self.failed_retirement.cut = Some(cut);
        step.runnable |= !self.resources.causal.cut_inventory(Some(cut), 1).is_empty();
        if self.resources.causal.rpc_cut(cut)? {
            return Ok(());
        }
        if let Some((original_cut, original_work)) = self.production_settlement_cut
            && original_cut == cut
            && self.resources.completion.as_ref().is_some_and(|roles| {
                roles.settlement_work() == original_work && roles.settlement_call_exited()
            })
        {
            self.resources
                .port
                .ingress()
                .observe_cut_exit(cut, true, false)
                .map_err(|error| TaskHostError::Causal(error.into()))?;
        }
        let turn = self
            .resources
            .causal
            .advance_failed_original_cut(stage, cut, 1)?;
        step.progressed |= turn.progressed;
        if let Some((prefix, event)) = turn.rejected {
            step.neutral = Some(FailedTaskNeutral::Rejected {
                _original_prefix: prefix,
                _original_event: event,
            });
            return Ok(());
        }
        // Prepaid future authors are real Pending holders. Their pair still
        // contains unsealed funding until this original event is delivered or
        // actually withdrawn. Waiting for input retirement here would instead
        // recreate the old DisposeSource effect/source-header dependency cycle.
        if !self.resources.causal.failed_source_pair_ready(stage, cut)? {
            return Ok(());
        }
        for ordinal in 0..2 {
            let key = Self::failed_source_key(
                self.resources.port.ingress(),
                PairRoute::Cut(cut),
                ordinal,
            )?;
            if let Some(source) =
                self.resources
                    .causal
                    .take_failed_source(stage, PairRoute::Cut(cut), ordinal)?
            {
                step.action = Some(self.failed_action(Physical::Source {
                    key,
                    source: Box::new(ManuallyDrop::new(source)),
                }));
                step.progressed = true;
                return Ok(());
            }
        }
        let positions = self
            .resources
            .port
            .ingress()
            .pair_inventory(PairRoute::Cut(cut))
            .map_err(|error| TaskHostError::Causal(error.into()))?;
        step.progressed |= Self::advance_source_cleanups(positions)?;
        // The old input can still own a cleanup effect whose resource
        // owner is waiting for this Source's actual report/header retirement.
        // Advance those independent original roles first; only final cut/input
        // detach waits for the old batch to complete.
        let sources = self.resources.causal.advance_failed_source_pair(
            stage,
            cut,
            &self.scope,
            self.core_generation,
            1,
        )?;
        step.progressed |= sources.progressed;
        if !turn.inputs_retired {
            return Ok(());
        }
        match self
            .resources
            .causal
            .retire_after_exit(cut, CausalSourceRetirement::Unclaimed)
        {
            Ok(pair) => {
                self.retain_returned_host_pair(cut, pair);
                step.progressed = true;
            }
            Err(refusal) => {
                if !waiting_cut(refusal.error) {
                    return Err(TaskHostError::Causal(refusal.error));
                }
                if let Some(reaped) = sources.reaped {
                    match self
                        .resources
                        .causal
                        .retire_after_exit(cut, CausalSourceRetirement::Reaped(reaped))
                    {
                        Ok(pair) => {
                            self.retain_returned_host_pair(cut, pair);
                            step.progressed = true;
                        }
                        Err(refusal) if waiting_cut(refusal.error) => {}
                        Err(refusal) => return Err(TaskHostError::Causal(refusal.error)),
                    }
                }
            }
        }
        Ok(())
    }
    fn failed_open(
        &mut self,
        stage: &ScopePublicationsDrained<'_>,
        step: &mut FailedTaskStep,
    ) -> Result<(), TaskHostError> {
        if let Some(open) = &mut self.resources.open_summary {
            if open.is_closed() {
                return Ok(());
            }
            if open.is_idle().map_err(TaskHostError::OpenSummary)?
                && let Some(summary) = open
                    .take_failed_queued(stage)
                    .map_err(TaskHostError::OpenSummary)?
            {
                step.neutral = Some(FailedTaskNeutral::Open { _original: summary });
                step.progressed = true;
                return Ok(());
            }
            let turn = open
                .advance_failed(stage, 1)
                .map_err(TaskHostError::OpenSummary)?;
            step.progressed |= turn.progressed;
            if let Some(rejected) = turn.rejected {
                step.neutral = Some(FailedTaskNeutral::OpenRejected {
                    _original: rejected,
                });
                return Ok(());
            }
            // Clear only the original partial input. Its sources keep their
            // saved exact receipts in the original pair and retire below in
            // final inventory, after all actual holders and aliases exit.
            if turn.input_retired
                && turn
                    .consumption
                    .is_some_and(|receipt| receipt != Consumption::Pending)
            {
                open.finish_failed_delivery(stage)
                    .map_err(TaskHostError::OpenSummary)?;
                step.progressed = true;
            }
        }
        Ok(())
    }
    fn failed_final_inventory(
        &mut self,
        stage: &ScopePublicationsDrained<'_>,
        ledger: &RuntimeLedger,
        step: &mut FailedTaskStep,
    ) -> Result<(), TaskHostError> {
        // Reaping a different ready sibling is a positive bounded transition,
        // not evidence that any particular observer or source has retired.
        step.progressed |= ledger.reap_scope_ready(self.scope.id(), 1)? != 0;
        if let Some(partial) = self
            .resources
            .source_partials
            .iter_mut()
            .find(|p| !p.is_empty())
        {
            let complete = partial
                .try_retire(ledger)
                .map_err(TaskHostError::SourceSlots)?;
            step.progressed |= complete;
            // This partial owns only never-exposed original slots. Each
            // call exits/retires them and performs one bounded header reap;
            // it has no foreign borrower to await. Keep the finite sweep
            // until its actual fee holders are gone, even if another ready
            // sibling won this particular reaper step.
            step.runnable |= !complete;
            return Ok(());
        }
        let production_exited = match &self.resources.production {
            Some(owner) => owner.is_retired(),
            None => {
                self.resources.pending_preparation_adoption.is_none()
                    && self.resources.preparation.as_ref().is_none_or(|p| {
                        p.host_borrowers_exited() && self.resources.preparation_remainder.is_some()
                    })
            }
        } && self
            .host_call(TaskHostCallKind::RetireBorrowing)
            .actual_call_exited();
        if !production_exited {
            return Ok(());
        }
        if self.resources.port.publication().borrow().has_rich_source() {
            step.detached_projection = Some(Arc::new(
                self.resources
                    .port
                    .prepare_source_retirement()
                    .map_err(TaskHostError::FailureProjection)?,
            ));
            return Ok(());
        }
        if !self.resources.causal.is_empty()
            || !self.host_retired_pairs.is_empty()
            || !self.resources.port.ingress().open_summary_drained()
        {
            return Ok(());
        }
        let ingress = self.resources.port.ingress().clone();
        for (index, route) in [PairRoute::Open, PairRoute::ConsumingOpen]
            .into_iter()
            .enumerate()
        {
            if self.normal_source_detached[index] {
                continue;
            }
            for ordinal in 0..2 {
                let key = Self::failed_source_key(&ingress, route, ordinal)?;
                if let Some(source) = self
                    .resources
                    .causal
                    .take_failed_source(stage, route, ordinal)?
                {
                    step.action = Some(self.failed_action(Physical::Source {
                        key,
                        source: Box::new(ManuallyDrop::new(source)),
                    }));
                    step.progressed = true;
                    return Ok(());
                }
            }
            let positions = ingress
                .pair_inventory(route)
                .map_err(|error| TaskHostError::Causal(error.into()))?;
            step.progressed |= Self::advance_source_cleanups(positions)?;
            for holder in ingress
                .take_unused_pair(route)
                .map_err(|error| TaskHostError::Causal(error.into()))?
                .into_iter()
                .flatten()
            {
                holder.exit();
                step.progressed = true;
            }
            for position in ingress
                .pair_inventory(route)
                .map_err(|error| TaskHostError::Causal(error.into()))?
            {
                // A completed partial input can leave its accepted source in
                // this original pair. Preserve that input's exact receipt;
                // source/header exit remains independent of failed teardown.
                if position.source.is_some() {
                    step.progressed |= position
                        .custody
                        .observe_failed_consumers(
                            stage,
                            self.resources.causal.runtime(),
                            self.actor,
                            self.core_generation,
                            position.custody.core_consumption(),
                        )
                        .map_err(|error| TaskHostError::Causal(CausalError::ExitCustody(error)))?;
                }
                let before = position.custody.phase();
                if !position
                    .custody
                    .try_retire()
                    .map_err(|error| TaskHostError::Causal(CausalError::ExitCustody(error)))?
                {
                    return Ok(());
                }
                step.progressed |= before != position.custody.phase();
            }
            // A Source's actual process payer owns its header, including
            // unused fixed-pair positions. Spend one real reap per turn and
            // do not let an already reusable first position hide the second.
            for position in ingress
                .pair_inventory(route)
                .map_err(|error| TaskHostError::Causal(error.into()))?
                .into_iter()
                .filter(|position| !position.custody.reusable())
                .take(1)
            {
                step.progressed |= position.custody.financial_scope().reap_ready(1)? != 0;
            }
            let [a, b] = ingress
                .prepare_pair_reap(route)
                .map_err(|error| TaskHostError::Causal(error.into()))?
                .map(|probe| probe.observe_reaped());
            let (Ok(a), Ok(b)) = (a, b) else {
                return Ok(());
            };
            let detached = ingress
                .release_closed_route_pair(route, [a, b])
                .map_err(|(error, _)| TaskHostError::Causal(error.into()))?;
            drop(detached);
            self.normal_source_detached[index] = true;
            step.progressed = true;
            return Ok(());
        }
        if !self.normal_lifetime_ended {
            if !self.retirement_projections_exited()? {
                return Ok(());
            }
            if self.resources.host_calls.iter().any(|c| !c.is_retired())
                || self
                    .resources
                    .completion
                    .as_ref()
                    .is_some_and(|c| !c.is_retired())
                || self
                    .resources
                    .completion_action
                    .as_ref()
                    .is_some_and(|c| c.phase() != ExitCustodyPhase::Retired)
                || self
                    .resources
                    .lifecycle_inputs
                    .values()
                    .any(|c| c.phase() != ExitCustodyPhase::Retired)
                || self
                    .resources
                    .statistics
                    .as_ref()
                    .is_some_and(|s| !s.is_retired())
                || self
                    .resources
                    .dynamic_filters
                    .as_ref()
                    .is_some_and(|s| !s.is_retired())
                || self
                    .resources
                    .loader
                    .as_ref()
                    .is_some_and(|l| !l.is_retired())
                || self
                    .resources
                    .loading_events
                    .as_ref()
                    .is_some_and(|e| !e.is_retired())
                || self.resources.pending_loading.is_some()
                || self.resources.pending_preparation_adoption.is_some()
                || (self.resources.preparation.is_some()
                    && self.resources.preparation_remainder.is_none())
            {
                return Ok(());
            }
            self.resources.lifetime.body_exited()?;
            self.resources.lifetime.captures_exited()?;
            self.normal_lifetime_ended = true;
            step.progressed = true;
        }
        if self.resources.lifetime.actual_exit_ready()?
            && let Some(open) = &mut self.resources.open_summary
            && !open.is_closed()
        {
            open.close_failed_after_exit(stage, &self.resources.lifetime)
                .map_err(TaskHostError::OpenSummary)?;
            step.progressed = true;
        }
        step.ready_for_disposal = self.resources.seal_read_observation().is_ok();
        Ok(())
    }
}
fn waiting_cut(error: CausalError) -> bool {
    matches!(
        error,
        CausalError::ActorTurn(ActorTurnError::StandingInputBusy)
            | CausalError::Cut(CutError::ExitUnobserved | CutError::NotReady | CutError::Consuming)
            | CausalError::Source(
                super::super::source_positions::SourcePositionError::NotAvailable
                    | super::super::source_positions::SourcePositionError::SourceStillHeld
            )
    )
}
fn abandon_report(
    stage: &ScopePublicationsDrained<'_>,
    task: &TaskHostRecord,
    report: &ReportSlot,
) -> Result<bool, TaskHostError> {
    stage
        .validate_scope(task.resources.causal.runtime(), task.actor, &task.scope)
        .map_err(TaskHostError::ActorTurn)?;
    let core = ConsumerId::new(1).unwrap();
    if report.consumption(core)? != Consumption::Pending {
        return Ok(false);
    }
    report.abandon_failed_owner(core, &task.scope, task.core_generation)?;
    Ok(true)
}
