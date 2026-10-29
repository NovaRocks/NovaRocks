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
use novarocks_control_runtime::exit_role::ExitRolePhase;
use novarocks_control_runtime::transaction::RuntimeLedger;
use novarocks_control_runtime_core::{completion::Consumption, identity::ConsumerId};
use std::mem::ManuallyDrop;

impl TaskHostRecord {
    pub(crate) fn take_production_retire_job(
        &self,
        ticket: &EffectTicket<WorkerAction>,
        host: Arc<dyn crate::TaskExecutionHost>,
        ledger: &RuntimeLedger,
    ) -> Result<Option<super::super::context_host::task_driver::TaskDriverHostJob>, TaskHostError>
    {
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
        let Some(owner) = self.resources.production.as_ref() else {
            return Ok(None);
        };
        let exited = TaskExecutionBorrowersExited { owner: self };
        let call = self.host_call(TaskHostCallKind::RetireBorrowing).clone();
        let job = call
            .take_retire_effect_job(ticket, &exited)
            .map_err(TaskHostError::HostCall)?;
        Ok(Some(
            super::super::context_host::task_driver::TaskDriverHostJob::RetireExecution {
                job,
                call,
                owner: owner.clone(),
                host,
                pending: self.pending.clone(),
            },
        ))
    }
    pub(crate) fn production_retired(&self) -> bool {
        self.resources
            .production
            .as_ref()
            .is_some_and(|owner| owner.is_retired())
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
        if !self.completion_core_observed {
            if let Some(action) = &self.resources.completion_action {
                self.completion_core_observed = action.phase() == ExitRolePhase::Retired
                    || action.report().consumption(ConsumerId::new(1).unwrap())?
                        == Consumption::Consumed;
            }
        }
        if let Some(roles) = &self.resources.completion {
            if !roles.is_retired() && roles.execution_calls_exited() {
                for kind in [
                    TaskLifecycleRole::Finisher,
                    TaskLifecycleRole::WallTime,
                    TaskLifecycleRole::TerminalPublisher,
                    TaskLifecycleRole::Settlement,
                ] {
                    let retired = if kind == TaskLifecycleRole::Settlement {
                        roles.settlement_phase() == crate::settlement::SettlementPhase::Retired
                    } else {
                        roles.phase(kind) == ExitRolePhase::Retired
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
                    roles.phase(TaskLifecycleRole::Finisher),
                    roles.phase(TaskLifecycleRole::WallTime),
                    roles.phase(TaskLifecycleRole::TerminalPublisher),
                    roles.settlement_phase(),
                );
                roles.try_retire();
                let after = (
                    roles.phase(TaskLifecycleRole::Finisher),
                    roles.phase(TaskLifecycleRole::WallTime),
                    roles.phase(TaskLifecycleRole::TerminalPublisher),
                    roles.settlement_phase(),
                );
                progress |= before != after;
            }
        }
        if let Some(statistics) = &mut self.resources.statistics {
            if !statistics.is_retired() {
                let turn = statistics.advance().map_err(TaskHostError::Statistics)?;
                progress |= turn.submitted || turn.consumed || turn.retired;
            }
        }
        for call in &self.resources.host_calls {
            if !call.is_retired() && call.actual_call_exited() {
                progress |= call
                    .retire_returned(ledger)
                    .map_err(TaskHostError::HostCall)?;
            }
        }
        self.observe_lifecycle_inputs()?;
        if !self.production_retired()
            || !self.resources.causal.is_empty()
            || !self.host_retired_pairs.is_empty()
        {
            return Ok(progress);
        }
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
                .deliver_next(self.core_generation)
                .map_err(TaskHostError::OpenSummary)?
                .is_some()
            {
                return Ok(true);
            }
        }
        if !ingress.open_summary_drained() {
            return Ok(progress);
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
            for holder in ingress
                .take_unused_pair(route)
                .map_err(|error| TaskHostError::Causal(error.into()))?
                .into_iter()
                .flatten()
            {
                holder.exit();
                progress = true;
            }
            for position in ingress
                .pair_inventory(route)
                .map_err(|error| TaskHostError::Causal(error.into()))?
            {
                let phase = position.custody.phase();
                if !position.custody.try_retire().map_err(|error| {
                    TaskHostError::CompletionAction(CausalInputError::Role(error))
                })? {
                    return Ok(progress);
                }
                // The Task's bounded reaper already ran earlier this turn.
                // A newly retired role can expose a ready original header;
                // keep one continuation to observe its actual reclamation.
                progress |= phase != ExitRolePhase::Retired;
            }
            let [a, b] = ingress
                .prepare_pair_reap(route)
                .map_err(|error| TaskHostError::Causal(error.into()))?
                .map(|probe| probe.observe_reaped());
            let (Ok(a), Ok(b)) = (a, b) else {
                return Ok(progress);
            };
            let mut detached = ingress
                .release_closed_route_pair(route, [a, b])
                .map_err(|(error, _)| TaskHostError::Causal(error.into()))?;
            for storage in &mut detached {
                storage.release()?;
            }
            self.normal_source_detached[index] = true;
            progress = true;
        }
        if !self.normal_lifetime_ended {
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
                    .any(|input| input.phase() != ExitRolePhase::Retired)
            {
                return Ok(progress);
            }
            self.resources.lifetime.body_exited()?;
            self.resources.lifetime.captures_exited()?;
            self.normal_lifetime_ended = true;
            progress = true;
        }
        if self.resources.lifetime.actual_exit_ready()? {
            if let Some(open) = &mut self.resources.open_summary {
                if !open.is_closed() {
                    open.close_after_exit(&self.resources.lifetime)
                        .map_err(TaskHostError::OpenSummary)?;
                    progress = true;
                }
            }
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
    pub(crate) fn retire_execution_on_host(&self, host: &dyn crate::TaskExecutionHost) -> bool {
        let descriptor = {
            let state = self.state.lock().expect("original execution retirement");
            if state.retired {
                return true;
            }
            if state.retirement_panic.is_some() {
                return false;
            }
            state.descriptor.clone()
        };
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            if let Some(descriptor) = &descriptor {
                host.remove_inbound_capability(descriptor);
                host.remove_receiver(descriptor);
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
        }));
        let mut state = self
            .state
            .lock()
            .expect("actual execution retirement result");
        match result {
            Ok(()) => {
                state.retired = true;
                true
            }
            Err(payload) => {
                state.retirement_panic = Some(ManuallyDrop::new(payload));
                false
            }
        }
    }
}
/// The actual removed Task owner remains in the Context's finite Task slot
/// until its physical payload destruction and original payment release finish.
/// Unexpected foreign Drop retains the same payment and panic inventory.
pub(crate) struct RetiredTaskDisposal {
    state: std::sync::Mutex<RetiredTaskDisposalState>,
}
struct RetiredTaskDisposalState {
    observation: Option<
        Box<
            ManuallyDrop<(
                crate::TaskExecutionPorts,
                TaskIdentity,
                novarocks_execution_contract::TaskState,
            )>,
        >,
    >,
    started: bool,
    payload: Option<ManuallyDrop<Box<dyn std::any::Any + Send>>>,
    payment: Option<(
        TaskLifetime,
        CapacityBundle,
        Option<CapacityBundle>,
        Option<CapacityBundle>,
    )>,
    panic: Option<ManuallyDrop<Box<dyn std::any::Any + Send>>>,
    complete: bool,
}
impl RetiredTaskDisposal {
    pub(crate) fn from_original(
        resources: TaskHostResources,
        ports: Option<crate::TaskExecutionPorts>,
        task: TaskIdentity,
        retained_state: novarocks_execution_contract::TaskState,
    ) -> Self {
        assert!(
            resources.seal_read_observation().is_ok(),
            "original Task inventory is still live"
        );
        let TaskHostResources {
            active,
            preparation_remainder,
            accepted_source_storage,
            lifetime,
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
        } = resources;
        Self {
            state: std::sync::Mutex::new(RetiredTaskDisposalState {
                observation: ports
                    .map(|ports| Box::new(ManuallyDrop::new((ports, task, retained_state)))),
                started: false,
                payload: Some(ManuallyDrop::new(Box::new((
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
                )))),
                payment: Some((
                    lifetime,
                    active,
                    preparation_remainder,
                    accepted_source_storage,
                )),
                panic: None,
                complete: false,
            }),
        }
    }
    pub(crate) fn key(
        &self,
    ) -> (
        novarocks_control_runtime_core::identity::ScopeId,
        novarocks_control_runtime_core::identity::WorkId,
    ) {
        let state = self.state.lock().expect("original Task disposal key");
        let (lifetime, ..) = state
            .payment
            .as_ref()
            .expect("original Task payment before physical handoff");
        (lifetime.scope().id(), lifetime.id())
    }
    pub(crate) fn complete(&self) -> bool {
        self.state.lock().expect("actual Task disposal").complete
    }
    pub(crate) fn run_on_host(&self) -> Result<(), RuntimeError> {
        let (payload, observation) = {
            let mut state = self.state.lock().expect("actual Task physical payload");
            if state.complete || state.panic.is_some() || state.started {
                return Ok(());
            }
            state.started = true;
            (state.payload.take(), state.observation.take())
        };
        if let Some(payload) = payload {
            if let Err(panic) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                drop(ManuallyDrop::into_inner(payload))
            })) {
                let mut state = self.state.lock().expect("original Task destruction panic");
                state.observation = observation;
                state.panic = Some(ManuallyDrop::new(panic));
                return Ok(());
            }
        }
        if let Some(mut observation) = observation {
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let (ports, task, retained_state) = &**observation;
                ports.observe(crate::TaskProtocolEvent::task_terminal_retained(
                    *task,
                    *retained_state,
                ));
                // SAFETY: this stable, original observer capture is destroyed
                // once on the physical worker. An unwind retains its allocation
                // without moving or retrying partially destroyed contents.
                unsafe { ManuallyDrop::drop(observation.as_mut()) };
            }));
            if let Err(panic) = result {
                let mut state = self.state.lock().expect("original Task observer panic");
                state.observation = Some(observation);
                state.panic = Some(ManuallyDrop::new(panic));
                return Ok(());
            }
            drop(observation);
        }
        let payment = {
            let mut state = self.state.lock().expect("actual Task payment exit");
            let (_, active, preparation, source) =
                state.payment.as_mut().expect("original Task payment");
            if let Some(capacity) = preparation {
                capacity.release(&capacity.resources()?)?;
            }
            if let Some(capacity) = source {
                capacity.release(&capacity.resources()?)?;
            }
            active.release(&active.resources()?)?;
            state.complete = true;
            state.payment.take()
        };
        drop(payment);
        Ok(())
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
                state.retirement_panic.is_some(),
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
