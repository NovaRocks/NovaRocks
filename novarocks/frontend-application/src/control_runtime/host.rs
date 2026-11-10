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

//! One bounded metadata host turn. Original Sources prepay result positions
//! before ticket/guard claims; only those Sources bind actual admitted Work.
//! This layer never manufactures a Work from a descriptor identity, never
//! executes an effect body, and never counts transport quota outside the ledger.

#[cfg(test)]
mod purpose_tests;

use super::{
    actor::{FrontendActor, FrontendActorKind, FrontendEffect, FrontendLaneKey},
    metrics::FrontendHostObservations,
};
use crate::task_execution::host::custody::{
    TransportRetirement, TransportRetirements, TransportSourceEvent,
};
use novarocks_control_runtime::{
    actor::ActorConfig,
    actor_turn::{ActorHostTurn, ActorRegistration, ActorRuntime, ActorTurnError, EffectTicket},
    causal_input::CausalInputObligation,
    effect_dispatch::{DispatchError, EffectDispatch, EffectLanePolicy, EffectRelease},
    exit_custody::ExitObligation,
    transaction::{RuntimeLedger, ScopeHandle},
};
use novarocks_control_runtime_core::identity::{InputId, ScopeId, Tick};
use std::{
    any::Any,
    collections::BTreeMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};
use tokio::sync::Notify;

/// Executor refusal returns this entire original job. Implementations only
/// submit through a credentialed carrier; they must not poll bodies, perform
/// I/O, or dispose rich captures on the poller. Filling the prepaid result and
/// returning/retiring its original Source role remain distinct from actual exit.
pub(crate) struct PreparedEffectJob {
    pub(crate) effect: FrontendEffect,
    pub(crate) exit: ExitObligation,
    pub(crate) result: CausalInputObligation<FrontendActor>,
    pub(crate) captures: Box<dyn Any + Send>,
    /// The same original transport Join owner moves to the host before
    /// handing off the launch credential and original result to the executor.
    pub(crate) transport: Option<Box<TransportRetirement>>,
}
pub(crate) trait EffectExecutor: Send + Sync + 'static {
    /// A temporary refusal must arrange the original host's real availability
    /// wake before returning the full job. Retrying uses no timer or new Work.
    /// Refusal concerns this executor's shared metadata handoff availability.
    /// Per-purpose workload/transport Full is handled by the already-admitted
    /// credentialed job, not by refusing it while accepting another purpose.
    fn handoff(&self, job: PreparedEffectJob) -> Result<(), PreparedEffectJob>;
}
/// A result position loan from the original admitted per-actor Source. The
/// callback returns None only after arranging an actual credit/retirement wake.
/// It may not issue Work by reconstructing a descriptor's numeric WorkId.
pub(crate) trait EffectSource: Send + 'static {
    /// The original, exact pending metadata selects its own already-priced
    /// purpose role. The host has not claimed its ticket or step guard yet.
    fn prepay_result(
        &mut self,
        effect: &FrontendEffect,
    ) -> Option<CausalInputObligation<FrontendActor>>;
    /// Binding is an already-preflighted original transfer: retrieve the exact
    /// step guard under this ticket, bind its original Work/report/captures and
    /// keep ExitCustody with this Source. Scope failure is handled by the later
    /// generation start gate; no new external admission occurs in this hook.
    fn bind_job(
        &mut self,
        runtime: &ActorRuntime<FrontendActor>,
        turn: &ActorHostTurn<FrontendActor>,
        ticket: &EffectTicket<FrontendEffect>,
        result: CausalInputObligation<FrontendActor>,
    ) -> PreparedEffectJob;
    /// A concurrent owner fence can refuse the original step guard after
    /// ticket claim. The failure owner must receive the original prepayment.
    fn try_bind_job(
        &mut self,
        runtime: &ActorRuntime<FrontendActor>,
        turn: &ActorHostTurn<FrontendActor>,
        ticket: &EffectTicket<FrontendEffect>,
        result: CausalInputObligation<FrontendActor>,
    ) -> Result<PreparedEffectJob, EffectBindRefusal> {
        Ok(self.bind_job(runtime, turn, ticket, result))
    }
    fn owner_failed_binding(
        &mut self,
        ticket: EffectTicket<FrontendEffect>,
        refused: EffectBindRefusal,
    ) {
        std::panic::resume_unwind(Box::new((ticket, refused)));
    }
    fn retain_transport_failure(&mut self, original: Box<dyn Any + Send>) {
        std::panic::resume_unwind(original);
    }
    /// Receives the actual unbound grant or retained payload for the
    /// same claimed cleanup effect. The binder must move it off the poller.
    fn retain_transport_release(&mut self, original: Box<dyn Any + Send>) {
        std::panic::resume_unwind(original);
    }
    fn return_unclaimed_result(&mut self, result: CausalInputObligation<FrontendActor>);
    /// Core consumption is proved by these original committed runtime inputs;
    /// it is not a capture, result-consumer, or producer-exit acknowledgement.
    /// If this makes already-exited work retireable, handle it now or notify
    /// this original host registration once; never wait for a periodic scan.
    fn core_consumed(&mut self, inputs: &[InputId]);
    /// A transport-enabled Source consumes these same originals. The host
    /// scheduler only routes them and does not interpret outcome or failure.
    fn transport_event(
        &mut self,
        _effect: novarocks_coordination_contract::AttemptActionId,
        original: TransportSourceEvent,
    ) {
        std::panic::resume_unwind(Box::new(original));
    }
    /// The immutable failure custodian takes these same originals; it must
    /// arrange actual unused withdrawal/cleanup and retain all partial custody.
    /// It may neither inspect the suspect Core nor drop the job as proof of exit.
    fn owner_failed_pending(&mut self, pending: PendingHandoff);
    /// Stop new external admission while draining already admitted effects.
    fn close_external(&mut self);
    /// True only when no new original result loan can be issued. Closing the
    /// external gate alone does not close loans needed by admitted effects.
    fn result_admission_closed(&self) -> bool;
    /// Progress only bounded original reports/inputs/custodies. Retired means
    /// every rich holder and Source role has actually exited, all report legs
    /// have retired and ready headers have reaped; only the actor slot remains.
    fn progress_retirement(
        &mut self,
        runtime: &ActorRuntime<FrontendActor>,
        turn: &ActorHostTurn<FrontendActor>,
        now: Tick,
        budget: usize,
    ) -> SourceProgress;
}
#[must_use]
pub(crate) struct EffectBindRefusal {
    pub(crate) error: ActorTurnError,
    pub(crate) original: Box<dyn Any + Send>,
}
pub(crate) struct SourceProgress {
    pub(crate) units: usize,
    /// Actual original state changed, including a purpose loan becoming
    /// available. Cursor inspection and self-notification alone are not
    /// progress. A real credit wake consumed during a partial host scan must
    /// preserve this fact so an earlier Full candidate can be revisited.
    pub(crate) progressed: bool,
    pub(crate) more: bool,
    pub(crate) retired: bool,
}
#[derive(Clone, Copy)]
pub(crate) struct FrontendLanePolicy {
    pub(crate) lane: fn(&FrontendEffect) -> Option<FrontendLaneKey>,
    pub(crate) per_step_inventory: usize,
}
impl FrontendLanePolicy {
    pub(crate) fn attempt(per_step_inventory: usize) -> Self {
        use novarocks_attempt_protocol::effect::EffectBody;
        Self {
            per_step_inventory,
            lane: |effect| {
                let FrontendEffect::Attempt(effect) = effect else {
                    return None;
                };
                match &effect.protocol.body {
                    EffectBody::SendBatch { batch, .. } => Some(FrontendLaneKey::Send {
                        target: batch.backend,
                        lane: batch.lane,
                        control_progress: batch.control_progress,
                    }),
                    EffectBody::OpenSubscription(request)
                    | EffectBody::AdjustReadWindow(request) => {
                        Some(FrontendLaneKey::Subscription {
                            participation: request.participation,
                            generation: request.generation,
                        })
                    }
                    EffectBody::CloseSubscription {
                        participation,
                        generation,
                    }
                    | EffectBody::ConfirmReadState {
                        participation,
                        generation,
                        ..
                    } => Some(FrontendLaneKey::Subscription {
                        participation: *participation,
                        generation: *generation,
                    }),
                    EffectBody::Publish(_) => Some(FrontendLaneKey::Publication),
                    // Independent effects keep their own ready marker. In
                    // particular, Withdraw must be able to reach an original
                    // send whose dispatch purpose has no available credit.
                    _ => None,
                }
            },
        }
    }
}
impl EffectLanePolicy<FrontendActor> for FrontendLanePolicy {
    type LaneKey = FrontendLaneKey;
    fn classify(&self, effect: &FrontendEffect) -> Option<(FrontendLaneKey, EffectRelease)> {
        (self.lane)(effect).map(|key| (key, EffectRelease::Completed))
    }
    fn effect_inventory_limit(&self) -> usize {
        self.per_step_inventory
    }
}
pub(crate) struct PendingHandoff {
    pub(crate) ticket: EffectTicket<FrontendEffect>,
    pub(crate) job: PreparedEffectJob,
}
struct HostMachine {
    withdrawals: Arc<super::transport_withdrawal::TransportWithdrawalRoutes>,
    dispatch: EffectDispatch<FrontendActor, FrontendLanePolicy>,
    policy: FrontendLanePolicy,
    source: Box<dyn EffectSource>,
    executor: Arc<dyn EffectExecutor>,
    transport: Option<TransportRetirements>,
    // A finite pass over original ready markers survives host-budget yield.
    // Resetting this to ready.len() on every turn would busy-loop when all
    // purposes are Full and ready markers outnumber one host allowance.
    scan_remaining: Option<usize>,
    rescan_requested: bool,
    pending: Option<PendingHandoff>,
    external_closed: bool,
}
struct HostEntry {
    scope: ScopeHandle,
    registration: ActorRegistration,
    kind: FrontendActorKind,
    budget: usize,
    close_requested: AtomicBool,
    // The exclusive host turn takes this original state out, then releases
    // the mutex before every Source/ledger/executor operation and Waker.
    machine: Mutex<Option<HostMachine>>,
}
pub(crate) struct HostBinding {
    pub(crate) scope: ScopeHandle,
    pub(crate) registration: ActorRegistration,
    pub(crate) kind: FrontendActorKind,
    pub(crate) config: ActorConfig,
    pub(crate) policy: FrontendLanePolicy,
    pub(crate) source: Box<dyn EffectSource>,
    pub(crate) executor: Arc<dyn EffectExecutor>,
    pub(crate) transport: Option<TransportRetirements>,
}
pub(crate) struct HostRegistrationRefusal {
    pub(crate) reason: &'static str,
    pub(crate) runtime_error: Option<ActorTurnError>,
    pub(crate) binding: HostBinding,
}
struct RegistryState {
    open: bool,
    entries: BTreeMap<ScopeId, Arc<HostEntry>>,
}
pub(crate) struct HostRegistry {
    runtime: ActorRuntime<FrontendActor>,
    ledger: RuntimeLedger,
    state: Mutex<RegistryState>,
    observations: Arc<FrontendHostObservations>,
    pub(crate) changed: Notify,
}
impl HostRegistry {
    pub(crate) fn new(
        runtime: ActorRuntime<FrontendActor>,
        ledger: RuntimeLedger,
        observations: Arc<FrontendHostObservations>,
    ) -> Self {
        Self {
            runtime,
            ledger,
            state: Mutex::new(RegistryState {
                open: true,
                entries: BTreeMap::new(),
            }),
            observations,
            changed: Notify::new(),
        }
    }
    pub(crate) fn register(&self, binding: HostBinding) -> Result<(), HostRegistrationRefusal> {
        if !binding.registration.matches_scope(&binding.scope)
            || binding.transport.as_ref().is_some_and(|transport| {
                !transport.matches_registration(&self.runtime, &binding.registration)
            })
        {
            return Err(HostRegistrationRefusal {
                reason: "original actor Scope binding differs",
                runtime_error: None,
                binding,
            });
        }
        match self
            .runtime
            .owns_scope(binding.registration.actor(), &binding.scope)
        {
            Ok(true) => {}
            Ok(false) => {
                return Err(HostRegistrationRefusal {
                    reason: "original actor runtime binding differs",
                    runtime_error: None,
                    binding,
                });
            }
            Err(error) => {
                return Err(HostRegistrationRefusal {
                    reason: "original actor runtime qualification failed",
                    runtime_error: Some(error),
                    binding,
                });
            }
        }
        if binding.policy.per_step_inventory != binding.config.max_effects_per_step {
            return Err(HostRegistrationRefusal {
                reason: "effect lane inventory differs from original actor configuration",
                runtime_error: None,
                binding,
            });
        }
        let Some(inventory) = binding
            .config
            .max_batches
            .checked_mul(binding.config.max_effects_per_step)
            .filter(|n| *n != 0)
        else {
            return Err(HostRegistrationRefusal {
                reason: "original effect inventory arithmetic",
                runtime_error: None,
                binding,
            });
        };
        // Indexing one entire atomic batch costs 1+max_effects. Its largest
        // admitted batch must fit even when the core's own turn budget is tiny.
        // The remainder is the original core turn allowance, not a new quota.
        let Some(budget) = binding
            .config
            .turn_budget
            .checked_add(binding.config.max_effects_per_step)
            .and_then(|n| n.checked_add(1))
        else {
            return Err(HostRegistrationRefusal {
                reason: "host turn work arithmetic",
                runtime_error: None,
                binding,
            });
        };
        let mut state = self.state.lock().expect("frontend host registry lock");
        if !state.open || state.entries.contains_key(&binding.scope.id()) {
            return Err(HostRegistrationRefusal {
                reason: "frontend actor registration closed or duplicated",
                runtime_error: None,
                binding,
            });
        }
        let dispatch = EffectDispatch::with_completion_runtime(
            self.runtime.clone(),
            binding.registration.actor(),
            inventory,
            binding.policy,
        );
        state.entries.insert(
            binding.scope.id(),
            Arc::new(HostEntry {
                scope: binding.scope,
                registration: binding.registration,
                kind: binding.kind,
                budget,
                close_requested: AtomicBool::new(false),
                machine: Mutex::new(Some(HostMachine {
                    withdrawals: Arc::default(),
                    dispatch,
                    policy: binding.policy,
                    source: binding.source,
                    executor: binding.executor,
                    transport: binding.transport,
                    scan_remaining: None,
                    rescan_requested: false,
                    pending: None,
                    external_closed: false,
                })),
            }),
        );
        drop(state);
        self.changed.notify_waiters();
        Ok(())
    }
    pub(crate) fn kind(&self, scope: ScopeId) -> Option<FrontendActorKind> {
        self.state
            .lock()
            .expect("frontend host registry lock")
            .entries
            .get(&scope)
            .map(|entry| entry.kind)
    }
    pub(super) fn observation_owners(&self) -> (&RuntimeLedger, &ActorRuntime<FrontendActor>) {
        (&self.ledger, &self.runtime)
    }
    /// One source per original HostMachine/runtime batch inventory. A host
    /// turn holding its machine or an unavailable runtime read returns Busy;
    /// the observer never reconstructs another pending/capacity table.
    pub(super) fn try_effect_observation(
        &self,
    ) -> Result<
        Option<super::metrics::PendingEffectObservation>,
        novarocks_control_runtime::observation::EffectObservationError,
    > {
        use novarocks_control_runtime::observation::EffectObservationError;
        let entries = {
            let state = match self.state.try_lock() {
                Ok(state) => state,
                Err(std::sync::TryLockError::WouldBlock) => return Ok(None),
                Err(std::sync::TryLockError::Poisoned(_)) => {
                    return Err(EffectObservationError::Poisoned);
                }
            };
            state.entries.values().cloned().collect::<Vec<_>>()
        };
        let (mut pending, mut claimed, mut unindexed) = (0_u64, 0_u64, 0_u64);
        for entry in entries {
            let machine = match entry.machine.try_lock() {
                Ok(machine) => machine,
                Err(std::sync::TryLockError::WouldBlock) => return Ok(None),
                Err(std::sync::TryLockError::Poisoned(_)) => {
                    return Err(EffectObservationError::Poisoned);
                }
            };
            let Some(machine) = machine.as_ref() else {
                return Ok(None);
            };
            let Some(sample) = machine
                .dispatch
                .try_observation(&self.runtime, &entry.registration)?
            else {
                return Ok(None);
            };
            pending = pending
                .checked_add(
                    u64::try_from(sample.runtime.states.pending)
                        .map_err(|_| EffectObservationError::InventoryOverflow)?,
                )
                .ok_or(EffectObservationError::InventoryOverflow)?;
            claimed = claimed
                .checked_add(u64::from(machine.pending.is_some()))
                .ok_or(EffectObservationError::InventoryOverflow)?;
            unindexed = unindexed
                .checked_add(
                    u64::try_from(sample.runtime.unindexed_batches)
                        .map_err(|_| EffectObservationError::InventoryOverflow)?,
                )
                .ok_or(EffectObservationError::InventoryOverflow)?;
        }
        Ok(Some(super::metrics::PendingEffectObservation {
            pending,
            claimed_not_handed_off: claimed,
            unindexed_inputs: unindexed,
        }))
    }
    pub(crate) fn close_registration(&self) {
        let entries = {
            let mut state = self.state.lock().expect("frontend host registry lock");
            state.open = false;
            state.entries.values().cloned().collect::<Vec<_>>()
        };
        for entry in entries {
            entry.close_requested.store(true, Ordering::Release);
            self.runtime
                .notify_host_actor(&entry.registration)
                .expect("actual closing actor registration");
        }
        self.changed.notify_waiters();
    }
    pub(crate) fn is_empty(&self) -> bool {
        self.state
            .lock()
            .expect("frontend host registry lock")
            .entries
            .is_empty()
    }
    pub(crate) fn drive(&self, now: Tick, turn: ActorHostTurn<FrontendActor>) {
        let entry = self
            .state
            .lock()
            .expect("frontend host registry lock")
            .entries
            .get(&turn.registration().scope_id())
            .cloned()
            .expect("every FE host turn has an original registered Source");
        assert!(
            entry.registration.same_registration(&turn.registration()),
            "host routing never revives equal numeric identities"
        );
        let mut machine = entry
            .machine
            .lock()
            .expect("frontend host state lock")
            .take()
            .expect("one original exclusive host turn");
        let began = self.observations.now();
        let progress = self
            .drive_inner(&entry, &mut machine, &turn, now)
            .unwrap_or_else(|error| {
                eprintln!("Frontend common host invariant failed: {error:?}");
                std::process::abort()
            });
        self.observations
            .host_turn(began, progress.units, progress.handoffs);
        if progress.retired {
            assert!(
                machine.pending.is_none() && machine.dispatch.is_empty(),
                "retired Source has returned every original host object"
            );
            // Source progress already destroyed rich captures and retired all
            // actual report/header duties. Only neutral route/Core state drops.
            match turn.retire_actor() {
                Ok(core) => drop(core),
                Err((
                    ActorTurnError::Scheduler(
                        novarocks_control_runtime_core::scheduler::SchedulerError::Busy,
                    ),
                    original_turn,
                )) => {
                    // A finite Core claim is still physically active. Its
                    // original scheduler state, rather than a timer/capacity
                    // guess, proves there is remaining driver work to retry.
                    *entry.machine.lock().expect("frontend host state lock") = Some(machine);
                    original_turn.release(true).unwrap_or_else(|error| {
                        eprintln!("Frontend busy retirement release failed: {error:?}");
                        std::process::abort()
                    });
                    self.changed.notify_waiters();
                    return;
                }
                Err((error, _original_turn)) => {
                    eprintln!("Frontend actor retirement invariant failed: {error:?}");
                    std::process::abort()
                }
            }
            self.ledger
                .remove_scope(entry.scope.id())
                .unwrap_or_else(|error| {
                    eprintln!("Frontend Scope retirement failed: {error:?}");
                    std::process::abort()
                });
            self.state
                .lock()
                .expect("frontend host registry lock")
                .entries
                .remove(&entry.scope.id());
        } else {
            *entry.machine.lock().expect("frontend host state lock") = Some(machine);
            turn.release(progress.more).unwrap_or_else(|error| {
                eprintln!("Frontend host turn release failed: {error:?}");
                std::process::abort()
            });
        }
        self.changed.notify_waiters();
    }
    fn drive_inner(
        &self,
        entry: &HostEntry,
        machine: &mut HostMachine,
        turn: &ActorHostTurn<FrontendActor>,
        now: Tick,
    ) -> Result<HostProgress, HostError> {
        let mut budget = entry.budget;
        let mut handoffs = 0;
        // Every dispatch observation can race the Core's real failure fence,
        // including indexing and defer after a Source callback. Preserve the
        // original custodian turn rather than escalating that race to abort.
        macro_rules! original_dispatch {
            ($operation:expr) => {
                match $operation {
                    Ok(value) => value,
                    Err(error) => {
                        if owner_failed_dispatch(&self.runtime, &entry.registration, &error)? {
                            machine.scan_remaining = None;
                            return Ok(HostProgress {
                                units: entry.budget - budget,
                                handoffs,
                                more: true,
                                retired: false,
                            });
                        }
                        return Err(error.into());
                    }
                }
            };
        }
        let failed = self.runtime.snapshot(entry.registration.actor())?.failed;
        if entry.close_requested.load(Ordering::Acquire) && !machine.external_closed {
            machine.source.close_external();
            machine.external_closed = true;
        }
        if failed && let Some(pending) = machine.pending.take() {
            machine.source.owner_failed_pending(pending);
        }
        let transport = machine.transport.as_mut().map(|transport| {
            // One original row visit per host turn leaves the original
            // indexing allowance available to make the Core and effects run.
            transport.progress(turn, 1, |effect, event| {
                machine.source.transport_event(effect, event);
            })
        });
        if let Some(transport) = &transport {
            budget -= transport.units;
        }
        let mut retired = machine
            .source
            .progress_retirement(&self.runtime, turn, now, budget);
        assert!(
            retired.units <= budget,
            "Source work uses the original finite host allowance"
        );
        budget -= retired.units;
        if let Some(transport) = transport {
            retired.progressed |= transport.progressed;
            retired.more |= transport.more;
            retired.retired &= machine.transport.as_ref().unwrap().is_empty();
        }
        if retired.progressed && machine.scan_remaining.is_some() {
            machine.rescan_requested = true;
        }
        if retired.retired {
            if failed && !machine.dispatch.is_empty() {
                let drained = machine
                    .dispatch
                    .drain_failed_index_turn(&self.runtime, budget)?;
                // The API returns completion rather than actual units. Charge
                // its entire bounded allowance conservatively, never add work.
                budget = 0;
                if !drained {
                    return Ok(HostProgress {
                        units: entry.budget,
                        handoffs,
                        more: true,
                        retired: false,
                    });
                }
            }
            return Ok(HostProgress {
                units: entry.budget - budget,
                handoffs,
                more: false,
                retired: true,
            });
        }
        if failed {
            return Ok(HostProgress {
                units: entry.budget - budget,
                handoffs,
                more: retired.more,
                retired: false,
            });
        }
        let mut handoff_refused = false;
        if budget != 0 {
            let indexed = original_dispatch!(machine.dispatch.index_committed_with(
                &self.runtime,
                budget,
                |metadata| {
                    if let FrontendEffect::Attempt(effect) = metadata {
                        machine.withdrawals.index(effect);
                    }
                }
            ));
            budget -= indexed.units;
            if !indexed.inputs.is_empty() && machine.scan_remaining.is_some() {
                machine.rescan_requested = true;
            }
            machine.source.core_consumed(&indexed.inputs);
            if !indexed.inputs.is_empty()
                && let Some(transport) = &machine.transport
            {
                transport.original_input_progress();
            }
        }
        while budget != 0 {
            if machine.pending.is_none() || handoff_refused {
                let remaining = machine
                    .scan_remaining
                    .get_or_insert_with(|| machine.dispatch.ready_positions());
                if *remaining == 0 {
                    break;
                }
                // A visit is paid even when this purpose is Full or the
                // original dependency/stop predicate currently blocks it.
                *remaining -= 1;
                budget -= 1;
                let candidate = match machine.dispatch.peek_candidate_with_withdrawal(
                    &self.runtime,
                    turn,
                    |metadata| match metadata {
                        FrontendEffect::Attempt(effect) => effect.handoff().cloned(),
                        _ => None,
                    },
                ) {
                    Ok(Some(candidate)) => candidate,
                    Ok(None) => {
                        machine.scan_remaining = Some(0);
                        break;
                    }
                    Err(error)
                        if owner_failed_dispatch(&self.runtime, &entry.registration, &error)? =>
                    {
                        machine.scan_remaining = None;
                        return Ok(HostProgress {
                            units: entry.budget - budget,
                            handoffs,
                            more: true,
                            retired: false,
                        });
                    }
                    Err(error) => return Err(error.into()),
                };
                let Some(metadata) = candidate.metadata() else {
                    original_dispatch!(machine.dispatch.defer_candidate(&candidate));
                    continue;
                };
                if let FrontendEffect::Attempt(effect) = metadata
                    && let novarocks_attempt_protocol::effect::EffectBody::Withdraw { effect } =
                        effect.protocol.body
                {
                    if let Some(ticket) = original_dispatch!(
                        machine.dispatch.claim_candidate(&self.runtime, &candidate)
                    ) {
                        machine.withdrawals.withdraw(effect);
                        self.runtime
                            .effect_completed(ticket)
                            .map_err(|refusal| HostError::Runtime(refusal.error))?;
                        if let Some(transport) = &machine.transport {
                            transport.original_input_progress();
                        }
                        machine.rescan_requested = true;
                    } else {
                        original_dispatch!(machine.dispatch.defer_candidate(&candidate));
                    }
                    continue;
                }
                if machine.pending.is_some() || machine.source.result_admission_closed() {
                    original_dispatch!(machine.dispatch.defer_candidate(&candidate));
                    continue;
                }
                if matches!(metadata, FrontendEffect::Attempt(effect) if effect.handoff().is_some())
                    && machine
                        .transport
                        .as_ref()
                        .is_some_and(|transport| !transport.has_room())
                {
                    original_dispatch!(machine.dispatch.defer_candidate(&candidate));
                    continue;
                }
                let Some(prepaid) = machine.source.prepay_result(metadata) else {
                    // Rotate this same original marker, never its same-lane
                    // tail. An independent purpose may have its own paid role.
                    original_dispatch!(machine.dispatch.defer_candidate(&candidate));
                    continue;
                };
                // Prepay precedes this exact ticket and its step-guard claim.
                // A gate changing after peek returns the same unclaimed role;
                // the next visit rechecks readiness before borrowing again.
                let ticket = match machine.dispatch.claim_candidate(&self.runtime, &candidate) {
                    Ok(Some(ticket)) => ticket,
                    Ok(None) => {
                        machine.source.return_unclaimed_result(prepaid);
                        original_dispatch!(machine.dispatch.defer_candidate(&candidate));
                        continue;
                    }
                    Err(error) => {
                        machine.source.return_unclaimed_result(prepaid);
                        if owner_failed_dispatch(&self.runtime, &entry.registration, &error)? {
                            machine.scan_remaining = None;
                            return Ok(HostProgress {
                                units: entry.budget - budget,
                                handoffs,
                                more: true,
                                retired: false,
                            });
                        }
                        return Err(error.into());
                    }
                };
                let mut job =
                    match machine
                        .source
                        .try_bind_job(&self.runtime, turn, &ticket, prepaid)
                    {
                        Ok(job) => job,
                        Err(refused)
                            if self.runtime.snapshot(entry.registration.actor())?.failed =>
                        {
                            machine.source.owner_failed_binding(ticket, refused);
                            machine.scan_remaining = None;
                            return Ok(HostProgress {
                                units: entry.budget - budget,
                                handoffs,
                                more: true,
                                retired: false,
                            });
                        }
                        Err(refused) => std::panic::resume_unwind(Box::new((ticket, refused))),
                    };
                if let Some(retirement) = job.transport.take() {
                    let FrontendEffect::Attempt(effect) = &ticket.effect else {
                        std::panic::resume_unwind(Box::new((job, retirement)));
                    };
                    let Some(transport) = machine.transport.as_mut() else {
                        std::panic::resume_unwind(Box::new((job, retirement)));
                    };
                    if let Err((_, retirement)) = transport.track(effect.clone(), retirement) {
                        std::panic::resume_unwind(Box::new((job, retirement)));
                    }
                }
                machine.pending = Some(PendingHandoff { ticket, job });
                if budget == 0 {
                    break;
                }
            }
            let pending = machine
                .pending
                .take()
                .expect("one original prepared handoff");
            budget -= 1;
            match machine.executor.handoff(pending.job) {
                Err(job) => {
                    machine.pending = Some(PendingHandoff {
                        ticket: pending.ticket,
                        job,
                    });
                    // Preserve this single prepared carrier and give the
                    // remaining bounded scan to host-local withdrawal.
                    handoff_refused = true;
                }
                Ok(()) => {
                    let lane = machine.policy.classify(&pending.ticket.effect);
                    let receipt = self
                        .runtime
                        .effect_completed_with_receipt(pending.ticket)
                        .map_err(|refusal| HostError::Runtime(refusal.error))?;
                    if let Some(transport) = &machine.transport {
                        transport.original_input_progress();
                    }
                    if let Some((key, EffectRelease::Completed)) = lane {
                        original_dispatch!(
                            machine
                                .dispatch
                                .completion_observed_with_receipt(key, &receipt)
                        );
                    }
                    if machine.scan_remaining.is_some() {
                        // This actual completion may expose a same-lane head
                        // or satisfy an earlier deferred head's dependency.
                        // Its wake can be consumed before a yielded old pass
                        // ends, so retain the real change through that cut.
                        machine.rescan_requested = true;
                    }
                    handoffs += 1;
                }
            }
        }
        let mut rescan = false;
        if machine.scan_remaining == Some(0) {
            machine.scan_remaining = None;
            rescan = std::mem::take(&mut machine.rescan_requested);
        }
        let more = retired.more
            || rescan
            || machine.dispatch.has_unindexed(&self.runtime)?
            || (budget == 0
                && (machine.scan_remaining.is_some_and(|n| n != 0)
                    || machine.pending.is_some() && !handoff_refused));
        Ok(HostProgress {
            units: entry.budget - budget,
            handoffs,
            more,
            retired: false,
        })
    }
}
fn owner_failed_dispatch(
    runtime: &ActorRuntime<FrontendActor>,
    registration: &ActorRegistration,
    error: &DispatchError,
) -> Result<bool, ActorTurnError> {
    match error {
        DispatchError::OwnerFailed | DispatchError::Runtime(ActorTurnError::ActorFailed) => {
            Ok(true)
        }
        // The original financial-author check can precede ActorFailed. Keep
        // that exact error when its author alone is invalid; only an actually
        // failed Core may enter the existing failure-custodian turn here.
        DispatchError::Runtime(ActorTurnError::InputAuthorInactive) => {
            Ok(runtime.snapshot(registration.actor())?.failed)
        }
        _ => Ok(false),
    }
}
struct HostProgress {
    units: usize,
    handoffs: usize,
    more: bool,
    retired: bool,
}
#[derive(Debug)]
enum HostError {
    Runtime(ActorTurnError),
    Dispatch(DispatchError),
}
impl From<ActorTurnError> for HostError {
    fn from(error: ActorTurnError) -> Self {
        Self::Runtime(error)
    }
}
impl From<DispatchError> for HostError {
    fn from(error: DispatchError) -> Self {
        Self::Dispatch(error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control_runtime::actor::{FrontendClock, FrontendInput, TestActor};
    use novarocks_control_runtime::{actor::ActorStep, actor_turn::PollerPool};
    use novarocks_control_runtime_core::{
        capacity::ResourceVector,
        identity::{ActorId, Generation},
        mailbox::{InputClass, InputEnvelope},
    };
    use novarocks_coordination_contract::{MonotonicClock, MonotonicInstant};
    use std::{
        sync::atomic::{AtomicUsize, Ordering},
        time::{Duration, Instant},
    };
    use tokio::sync::oneshot;

    #[test]
    fn attempt_lane_keeps_withdrawal_and_unrelated_sends_independent() {
        use novarocks_attempt_protocol::{
            capacity::ReservationToken,
            config::DispatchLane,
            effect::{AttemptEffect, DispatchBatch, EffectBody, EffectClass},
        };
        use novarocks_coordination_contract::AttemptActionId;
        use novarocks_types::BackendProcessId;
        use std::num::NonZeroU64;

        let policy = FrontendLanePolicy::attempt(3);
        let id = NonZeroU64::new(1).unwrap();
        let effect = |body| {
            FrontendEffect::Attempt(
                super::super::transport_withdrawal::AttemptEffectEnvelope::new(AttemptEffect {
                    id: AttemptActionId::new(id),
                    class: EffectClass::Business,
                    body,
                }),
            )
        };
        let send = |backend, lane, control_progress| {
            effect(EffectBody::SendBatch {
                batch: DispatchBatch {
                    backend,
                    lane,
                    control_progress,
                    operations: Box::new([]),
                    queued_bytes: 0,
                    queued_at: MonotonicInstant::from_origin(Duration::ZERO),
                    queued_inputs: Box::new([]),
                },
                reservation: ReservationToken(id),
            })
        };
        let first = BackendProcessId::new_v7();
        let other = BackendProcessId::new_v7();
        let classify = |target, lane, control| policy.classify(&send(target, lane, control));
        let create = classify(first, DispatchLane::Create, false);
        assert!(matches!(create, Some((_, EffectRelease::Completed))));
        assert_eq!(create, classify(first, DispatchLane::Create, false));
        assert_ne!(create, classify(other, DispatchLane::Create, false));
        assert_ne!(create, classify(first, DispatchLane::Update, false));
        assert_ne!(
            classify(first, DispatchLane::Update, false),
            classify(first, DispatchLane::Update, true)
        );
        assert!(
            policy
                .classify(&effect(EffectBody::Withdraw {
                    effect: AttemptActionId::new(id),
                }))
                .is_none()
        );
    }

    #[derive(Debug)]
    struct TestClock(Instant);
    impl MonotonicClock for TestClock {
        fn now(&self) -> MonotonicInstant {
            MonotonicInstant::from_origin(self.0.elapsed())
        }
    }
    struct CounterActor {
        inside: Arc<AtomicUsize>,
        calls: Arc<AtomicUsize>,
        expected: usize,
        finished: Option<oneshot::Sender<()>>,
    }
    impl TestActor for CounterActor {
        fn handle(
            &mut self,
            _: Tick,
            _: InputEnvelope,
            input: FrontendInput,
            _: &mut ActorStep<'_, FrontendEffect>,
        ) {
            if !matches!(input, FrontendInput::Test(_)) {
                return;
            }
            assert_eq!(
                self.inside.fetch_add(1, Ordering::SeqCst),
                0,
                "the original actor has only one active poller"
            );
            std::thread::yield_now();
            let complete = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
            assert_eq!(self.inside.fetch_sub(1, Ordering::SeqCst), 1);
            if complete == self.expected
                && let Some(finished) = self.finished.take()
            {
                let _ = finished.send(());
            }
        }
    }
    /// This actor emits no effects and owns no external work. Its Source only
    /// retires actual committed metadata inputs and the actual actor slot.
    struct MetadataSource {
        scope: ScopeHandle,
        closing: bool,
    }
    impl EffectSource for MetadataSource {
        fn prepay_result(
            &mut self,
            _: &FrontendEffect,
        ) -> Option<CausalInputObligation<FrontendActor>> {
            unreachable!("this metadata actor has no effect bodies")
        }
        fn bind_job(
            &mut self,
            _: &ActorRuntime<FrontendActor>,
            _: &ActorHostTurn<FrontendActor>,
            _: &EffectTicket<FrontendEffect>,
            _: CausalInputObligation<FrontendActor>,
        ) -> PreparedEffectJob {
            unreachable!("this metadata actor has no executor Work")
        }
        fn return_unclaimed_result(&mut self, _: CausalInputObligation<FrontendActor>) {
            unreachable!("no result role was issued")
        }
        fn core_consumed(&mut self, _: &[InputId]) {}
        fn owner_failed_pending(&mut self, _: PendingHandoff) {
            unreachable!("no handoff was prepared")
        }
        fn close_external(&mut self) {
            self.closing = true;
            self.scope.close_external().unwrap();
        }
        fn result_admission_closed(&self) -> bool {
            self.closing
        }
        fn progress_retirement(
            &mut self,
            runtime: &ActorRuntime<FrontendActor>,
            turn: &ActorHostTurn<FrontendActor>,
            _: Tick,
            budget: usize,
        ) -> SourceProgress {
            let actor = turn.registration().actor();
            let batches = runtime.committed_batches(actor, None, budget).unwrap();
            for (_, input) in &batches {
                runtime.retire_input(actor, *input).unwrap();
            }
            let more = !runtime
                .committed_batches(actor, None, 1)
                .unwrap()
                .is_empty();
            let retired = self.closing
                && runtime.snapshot(actor).unwrap().inputs == 0
                && self.scope.join_ready().unwrap();
            SourceProgress {
                units: batches.len(),
                progressed: !batches.is_empty(),
                more,
                retired,
            }
        }
    }
    struct NoExecutor;
    impl EffectExecutor for NoExecutor {
        fn handoff(&self, _: PreparedEffectJob) -> Result<(), PreparedEffectJob> {
            unreachable!("no effect ticket exists")
        }
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn three_actual_pollers_exclude_each_other_on_one_actor_and_drain_real_metadata() {
        const INPUTS: usize = 64;
        let clock: Arc<dyn novarocks_control_runtime::actor_turn::Clock> =
            Arc::new(FrontendClock(Arc::new(TestClock(Instant::now()))));
        let ledger = RuntimeLedger::new(ResourceVector::default());
        let scope = ledger
            .open_scope_next(None, Generation::new(1).unwrap())
            .unwrap();
        let runtime = ActorRuntime::new();
        let observations = Arc::new(FrontendHostObservations::new(clock.clone()));
        let host = Arc::new(HostRegistry::new(
            runtime.clone(),
            ledger.clone(),
            observations,
        ));
        let actor = ActorId::new(1).unwrap();
        let inside = Arc::new(AtomicUsize::new(0));
        let calls = Arc::new(AtomicUsize::new(0));
        let (finished, complete) = oneshot::channel();
        let config = ActorConfig {
            mailbox: [INPUTS; 4],
            max_batches: INPUTS,
            max_effects_per_step: 1,
            max_dependencies_per_effect: 0,
            turn_budget: 8,
            max_step_work: 1,
        };
        let core = FrontendActor::Test(Box::new(CounterActor {
            inside: inside.clone(),
            calls: calls.clone(),
            expected: INPUTS,
            finished: Some(finished),
        }));
        let kind = core.kind();
        let registration = runtime
            .register_bound(actor, scope.clone(), core, config)
            .unwrap_or_else(|_| panic!("one admitted test actor"));
        let source_scope = scope.clone();
        assert!(
            host.register(HostBinding {
                scope,
                registration: registration.clone(),
                kind,
                config,
                policy: FrontendLanePolicy {
                    lane: |_| None,
                    per_step_inventory: config.max_effects_per_step
                },
                source: Box::new(MetadataSource {
                    scope: source_scope,
                    closing: false
                }),
                executor: Arc::new(NoExecutor),
                transport: None,
            })
            .is_ok()
        );
        let owner = host.clone();
        let pollers = PollerPool::start_with_host(runtime.clone(), clock, 3, move |now, turn| {
            owner.drive(now, turn)
        })
        .unwrap();
        for _ in 0..INPUTS {
            runtime
                .reserve_input(actor, actor, InputClass::Ordinary)
                .unwrap()
                .fill(FrontendInput::Test(1))
                .unwrap();
        }
        let observed = tokio::time::timeout(Duration::from_secs(5), complete).await;
        host.close_registration();
        runtime.close_admission();
        ledger.close_admission();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let changed = host.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if host.is_empty() && runtime.is_empty() && ledger.is_empty() {
                break;
            }
            tokio::time::timeout_at(deadline, changed)
                .await
                .expect("actual actor/Scope drain");
        }
        assert!(pollers.shutdown().is_ok());
        assert!(observed.is_ok());
        assert_eq!(calls.load(Ordering::SeqCst), INPUTS);
        assert_eq!(inside.load(Ordering::SeqCst), 0);
    }
    use novarocks_control_runtime::{
        actor_turn::InputAuthor,
        completion::ReportSlot,
        exit_custody::{ExitCustody, ExitCustodyPhase},
        exit_work::{PanicSink, run_future},
        reported_work::ReportedWork,
    };
    use novarocks_control_runtime_core::{
        capacity::ReservationResources,
        completion::ConsumerKind,
        effect::{EffectDescriptor, EffectKind},
        identity::{ActionId, ConsumerId, ScopedWorkRef},
        owned_storage::OwnedSlotNeed,
        scope::{WorkAdmission, WorkKind},
    };
    struct EffectActor {
        work: ScopedWorkRef,
        results: Arc<AtomicUsize>,
    }
    impl TestActor for EffectActor {
        fn handle(
            &mut self,
            _: Tick,
            _: InputEnvelope,
            input: FrontendInput,
            step: &mut ActorStep<'_, FrontendEffect>,
        ) {
            match input {
                FrontendInput::Test(1) => step
                    .effect(
                        EffectDescriptor {
                            id: ActionId::new(1).unwrap(),
                            work: self.work,
                            kind: EffectKind::Business,
                            dependencies: vec![],
                        },
                        FrontendEffect::Test { value: 7 },
                    )
                    .unwrap(),
                FrontendInput::Test(2) => {
                    self.results.fetch_add(1, Ordering::SeqCst);
                }
                _ => {}
            }
        }
    }
    struct CaptureProbe(Arc<AtomicUsize>);
    impl Drop for CaptureProbe {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    struct ReturnedResult {
        role: Mutex<Option<CausalInputObligation<FrontendActor>>>,
        indexed: Mutex<Vec<InputId>>,
    }
    struct RealSource {
        scope: ScopeHandle,
        ledger: RuntimeLedger,
        runtime: ActorRuntime<FrontendActor>,
        registration: ActorRegistration,
        result: Option<CausalInputObligation<FrontendActor>>,
        exit: Option<ExitObligation>,
        captures: Option<Box<dyn Any + Send>>,
        custody: ExitCustody,
        report: ReportSlot,
        returned: Arc<ReturnedResult>,
        binds: Arc<AtomicUsize>,
        closing: bool,
        report_consumed: bool,
    }
    impl EffectSource for RealSource {
        fn prepay_result(
            &mut self,
            _: &FrontendEffect,
        ) -> Option<CausalInputObligation<FrontendActor>> {
            self.result.take()
        }
        fn bind_job(
            &mut self,
            _: &ActorRuntime<FrontendActor>,
            _: &ActorHostTurn<FrontendActor>,
            ticket: &EffectTicket<FrontendEffect>,
            result: CausalInputObligation<FrontendActor>,
        ) -> PreparedEffectJob {
            self.binds.fetch_add(1, Ordering::SeqCst);
            assert_eq!(
                ticket.descriptor.work,
                result.event_author().unwrap().work()
            );
            PreparedEffectJob {
                effect: ticket.effect.clone(),
                exit: self.exit.take().unwrap(),
                result,
                captures: self.captures.take().unwrap(),
                transport: None,
            }
        }
        fn return_unclaimed_result(&mut self, result: CausalInputObligation<FrontendActor>) {
            assert!(self.result.replace(result).is_none());
        }
        fn core_consumed(&mut self, inputs: &[InputId]) {
            self.returned
                .indexed
                .lock()
                .unwrap()
                .extend_from_slice(inputs);
            if !inputs.is_empty() {
                self.runtime.notify_host_actor(&self.registration).unwrap();
            }
        }
        fn owner_failed_pending(&mut self, _: PendingHandoff) {
            panic!("the controlled actor does not fail");
        }
        fn close_external(&mut self) {
            self.closing = true;
            self.scope.close_external().unwrap();
        }
        fn result_admission_closed(&self) -> bool {
            self.closing
        }
        fn progress_retirement(
            &mut self,
            runtime: &ActorRuntime<FrontendActor>,
            _: &ActorHostTurn<FrontendActor>,
            _: Tick,
            budget: usize,
        ) -> SourceProgress {
            let mut indexed = self.returned.indexed.lock().unwrap();
            let mut units = 0;
            while units < budget && !indexed.is_empty() {
                let input = indexed[0];
                if runtime
                    .effect_inventory(self.registration.actor(), input, None, 1)
                    .unwrap()
                    .iter()
                    .any(|(_, state)| {
                        !matches!(
                            state,
                            novarocks_control_runtime_core::effect::EffectState::Completed
                                | novarocks_control_runtime_core::effect::EffectState::AbandonedByOwnerFailure
                        )
                    })
                {
                    break;
                }
                indexed.remove(0);
                runtime
                    .retire_input(self.registration.actor(), input)
                    .unwrap();
                units += 1;
            }
            drop(indexed);
            if self.result.is_none() {
                self.result = self.returned.role.lock().unwrap().take();
            }
            if self.custody.actual_exit_ready().unwrap() {
                if let Some(role) = self.result.as_mut()
                    && role.in_flight().unwrap() == [None, None]
                {
                    role.close_after_exit().unwrap();
                    self.result.take();
                    if !self.report_consumed {
                        self.report.consume(ConsumerId::new(1).unwrap()).unwrap();
                        self.report_consumed = true;
                    }
                }
                if self.report_consumed {
                    self.custody.try_retire().unwrap();
                    units += self
                        .ledger
                        .reap_scope_ready(self.scope.id(), budget.saturating_sub(units))
                        .unwrap();
                }
            }
            let retired = self.closing && self.report_consumed && self.scope.join_ready().unwrap();
            SourceProgress {
                units,
                progressed: units != 0,
                more: units == budget && budget != 0,
                retired,
            }
        }
    }
    struct RefusingExecutor {
        accepting: AtomicBool,
        accepted: Mutex<Option<PreparedEffectJob>>,
        original: InputAuthor,
        captured: Arc<AtomicUsize>,
        attempts: AtomicUsize,
        changed: Notify,
    }
    impl EffectExecutor for RefusingExecutor {
        fn handoff(&self, job: PreparedEffectJob) -> Result<(), PreparedEffectJob> {
            assert!(
                job.result
                    .event_author()
                    .unwrap()
                    .same_binding(&self.original)
            );
            assert!(Arc::ptr_eq(
                &job.captures.downcast_ref::<CaptureProbe>().unwrap().0,
                &self.captured
            ));
            assert!(matches!(job.effect, FrontendEffect::Test { value: 7 }));
            self.attempts.fetch_add(1, Ordering::SeqCst);
            if !self.accepting.load(Ordering::SeqCst) {
                self.changed.notify_waiters();
                return Err(job);
            }
            assert!(self.accepted.lock().unwrap().replace(job).is_none());
            self.changed.notify_waiters();
            Ok(())
        }
    }
    async fn await_attempts(executor: &RefusingExecutor, expected: usize) {
        loop {
            let changed = executor.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if executor.attempts.load(Ordering::SeqCst) >= expected {
                return;
            }
            tokio::time::timeout(Duration::from_secs(5), changed)
                .await
                .expect("actual executor handoff attempt");
        }
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn executor_refusal_preserves_the_same_paid_job_until_real_result_and_capture_exit() {
        let ledger = RuntimeLedger::new(ResourceVector::default());
        let scope = ledger
            .open_scope_next(None, Generation::new(1).unwrap())
            .unwrap();
        let (work, mut capacity) = scope
            .reserve_next(
                Generation::new(1).unwrap(),
                ReservationResources::new(
                    ResourceVector::default(),
                    OwnedSlotNeed {
                        work_headers: 1,
                        completions: 1,
                        captures: 1,
                        ..Default::default()
                    },
                ),
                WorkKind::Business,
                WorkAdmission::External,
            )
            .unwrap()
            .bind(WorkKind::Business)
            .unwrap();
        let reference = work.reference();
        let runtime = ActorRuntime::new();
        let config = ActorConfig {
            mailbox: [4; 4],
            max_batches: 4,
            max_effects_per_step: 1,
            max_dependencies_per_effect: 0,
            turn_budget: 8,
            max_step_work: 1,
        };
        let results = Arc::new(AtomicUsize::new(0));
        let registration = runtime
            .register_bound(
                ActorId::new(1).unwrap(),
                scope.clone(),
                FrontendActor::Test(Box::new(EffectActor {
                    work: reference,
                    results: results.clone(),
                })),
                config,
            )
            .unwrap_or_else(|_| panic!("the actual admitted actor"));
        let role = CausalInputObligation::reserve_admitted(
            &runtime,
            &registration,
            registration.actor(),
            InputClass::Ordinary,
            &work,
        )
        .unwrap();
        let original = role.event_author().unwrap();
        let report = ReportSlot::reserve_next(
            &mut capacity,
            Generation::new(1).unwrap(),
            1,
            [(ConsumerId::new(1).unwrap(), ConsumerKind::Observer)],
        )
        .unwrap();
        let (custody, exit) = ExitCustody::from_admitted(
            ReportedWork::try_new(work, capacity, report.clone()).unwrap(),
        );
        let captures = Arc::new(AtomicUsize::new(0));
        let binds = Arc::new(AtomicUsize::new(0));
        let returned = Arc::new(ReturnedResult {
            role: Mutex::new(None),
            indexed: Mutex::new(vec![]),
        });
        let executor = Arc::new(RefusingExecutor {
            accepting: AtomicBool::new(false),
            accepted: Mutex::new(None),
            original,
            captured: captures.clone(),
            attempts: AtomicUsize::new(0),
            changed: Notify::new(),
        });
        let clock: Arc<dyn novarocks_control_runtime::actor_turn::Clock> =
            Arc::new(FrontendClock(Arc::new(TestClock(Instant::now()))));
        let host = Arc::new(HostRegistry::new(
            runtime.clone(),
            ledger.clone(),
            Arc::new(FrontendHostObservations::new(clock.clone())),
        ));
        assert!(
            host.register(HostBinding {
                scope: scope.clone(),
                registration: registration.clone(),
                kind: FrontendActorKind::Test,
                config,
                policy: FrontendLanePolicy {
                    lane: |_| Some(FrontendLaneKey::Test(1)),
                    per_step_inventory: 1
                },
                source: Box::new(RealSource {
                    scope: scope.clone(),
                    ledger: ledger.clone(),
                    runtime: runtime.clone(),
                    registration: registration.clone(),
                    result: Some(role),
                    exit: Some(exit),
                    captures: Some(Box::new(CaptureProbe(captures.clone()))),
                    custody: custody.clone(),
                    report: report.clone(),
                    returned: returned.clone(),
                    binds: binds.clone(),
                    closing: false,
                    report_consumed: false
                }),
                executor: executor.clone(),
                transport: None,
            })
            .is_ok()
        );
        let driver = host.clone();
        let pollers = PollerPool::start_with_host(runtime.clone(), clock, 3, move |now, turn| {
            driver.drive(now, turn)
        })
        .unwrap();
        runtime
            .reserve_input(
                registration.actor(),
                registration.actor(),
                InputClass::Ordinary,
            )
            .unwrap()
            .fill(FrontendInput::Test(1))
            .unwrap();
        await_attempts(&executor, 1).await;
        let sample = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let Some(sample) = host.try_effect_observation().unwrap() {
                    break sample;
                }
                // Busy can come from a runtime lock after the host released
                // its machine. That lock's release need not run another turn.
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("original host observation became readable");
        assert_eq!(sample.claimed_not_handed_off, 1);
        assert_eq!(binds.load(Ordering::SeqCst), 1);
        assert_eq!(captures.load(Ordering::SeqCst), 0);
        assert_eq!(custody.phase(), ExitCustodyPhase::Admitted);
        runtime.notify_host_actor(&registration).unwrap();
        await_attempts(&executor, 2).await;
        assert_eq!(binds.load(Ordering::SeqCst), 1);
        executor.accepting.store(true, Ordering::SeqCst);
        runtime.notify_host_actor(&registration).unwrap();
        let job = loop {
            let changed = executor.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if let Some(job) = executor.accepted.lock().unwrap().take() {
                break job;
            }
            tokio::time::timeout(Duration::from_secs(5), changed)
                .await
                .expect("accepted original job");
        };
        let PreparedEffectJob {
            exit,
            mut result,
            captures: original_captures,
            ..
        } = job;
        run_future(
            exit,
            async move {
                drop(original_captures);
            },
            PanicSink::new(),
        )
        .await
        .unwrap();
        assert_eq!(captures.load(Ordering::SeqCst), 1);
        assert_eq!(custody.phase(), ExitCustodyPhase::Exited);
        assert!(!custody.try_retire().unwrap());
        result
            .fill_with_receipt(None, FrontendInput::Test(2))
            .unwrap_or_else(|_| panic!("original prepaid result input"));
        *returned.role.lock().unwrap() = Some(result);
        runtime.notify_host_actor(&registration).unwrap();
        loop {
            let changed = host.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if results.load(Ordering::SeqCst) == 1 {
                break;
            }
            tokio::time::timeout(Duration::from_secs(5), changed)
                .await
                .expect("prepaid result woke a real poller");
        }
        host.close_registration();
        runtime.close_admission();
        ledger.close_admission();
        loop {
            let changed = host.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if host.is_empty() && runtime.is_empty() && ledger.is_empty() {
                break;
            }
            tokio::time::timeout(Duration::from_secs(5), changed)
                .await
                .expect("actual original Source drain");
        }
        assert!(pollers.shutdown().is_ok());
        assert!(host.is_empty());
        assert!(runtime.is_empty());
        assert!(ledger.is_empty());
        assert_eq!(custody.phase(), ExitCustodyPhase::Retired);
    }
    struct MemberServicePort(tokio::sync::watch::Sender<u64>);
    impl novarocks_query_application::api::BackendProcessObservationPort for MemberServicePort {
        fn subscribe_process_changes(&self) -> tokio::sync::watch::Receiver<u64> {
            self.0.subscribe()
        }
        fn observe_process_at_endpoint(
            &self,
            _: novarocks_types::BackendProcessId,
            _: &novarocks_execution_contract::RuntimeEndpoint,
        ) -> Result<
            novarocks_query_application::api::BackendProcessObservation,
            novarocks_query_application::api::BackendTopologyError,
        > {
            Ok(novarocks_query_application::api::BackendProcessObservation::Current)
        }
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn shutdown_timeout_keeps_original_roles_captures_and_joins_until_actual_exit() {
        use crate::control_runtime::{
            FrontendControlRuntime, FrontendControlRuntimeConfig, FrontendShutdownError,
        };
        let handle = tokio::runtime::Handle::current();
        let mut data = crate::native::data_runtime::FrontendDataRuntime::new(handle.clone());
        let clock: Arc<dyn MonotonicClock> = Arc::new(TestClock(Instant::now()));
        let mut owner = FrontendControlRuntime::start(
            FrontendControlRuntimeConfig {
                pollers: std::num::NonZeroUsize::new(handle.metrics().num_workers()).unwrap(),
                clock,
                transport: novarocks_task_codec::TransportBudget::DEFAULT,
                history: None,
            },
            &data,
            Arc::new(MemberServicePort(tokio::sync::watch::channel(0).0)),
        )
        .unwrap_or_else(|e| panic!("actual FE runtime start: {e:?}"));
        let runtime = owner.actor_runtime().clone();
        let ledger = owner.ledger().clone();
        let scope = ledger
            .open_scope_next(None, Generation::new(1).unwrap())
            .unwrap();
        let mut resources = owner
            .transport_capacity()
            .queue_resources(
                novarocks_types::BackendProcessId::new_v7(),
                crate::native::transport_capacity::TransportCapacityLane::Ordinary,
                1,
                256,
            )
            .unwrap();
        resources.owned_slots = OwnedSlotNeed {
            work_headers: 1,
            completions: 1,
            captures: 1,
            ..Default::default()
        };
        let (work, mut capacity) = scope
            .reserve_next(
                Generation::new(1).unwrap(),
                resources,
                WorkKind::Business,
                WorkAdmission::External,
            )
            .unwrap()
            .bind(WorkKind::Business)
            .unwrap();
        let config = ActorConfig {
            mailbox: [4; 4],
            max_batches: 4,
            max_effects_per_step: 1,
            max_dependencies_per_effect: 0,
            turn_budget: 8,
            max_step_work: 1,
        };
        let registration = runtime
            .register_bound(
                ActorId::new(1).unwrap(),
                scope.clone(),
                FrontendActor::Test(Box::new(EffectActor {
                    work: work.reference(),
                    results: Arc::new(AtomicUsize::new(0)),
                })),
                config,
            )
            .unwrap_or_else(|_| panic!("original runtime admission"));
        let role = CausalInputObligation::reserve_admitted(
            &runtime,
            &registration,
            registration.actor(),
            InputClass::Ordinary,
            &work,
        )
        .unwrap();
        let report = ReportSlot::reserve_next(
            &mut capacity,
            Generation::new(1).unwrap(),
            1,
            [(ConsumerId::new(1).unwrap(), ConsumerKind::Observer)],
        )
        .unwrap();
        let (custody, exit) = ExitCustody::from_admitted(
            ReportedWork::try_new(work, capacity, report.clone()).unwrap(),
        );
        let captures = Arc::new(AtomicUsize::new(0));
        let held_captures = CaptureProbe(captures.clone());
        assert!(
            owner
                .register(HostBinding {
                    scope: scope.clone(),
                    registration: registration.clone(),
                    kind: FrontendActorKind::Test,
                    config,
                    policy: FrontendLanePolicy {
                        lane: |_| None,
                        per_step_inventory: 1
                    },
                    source: Box::new(RealSource {
                        scope: scope.clone(),
                        ledger: ledger.clone(),
                        runtime: runtime.clone(),
                        registration: registration.clone(),
                        result: Some(role),
                        exit: None,
                        captures: None,
                        custody: custody.clone(),
                        report: report.clone(),
                        returned: Arc::new(ReturnedResult {
                            role: Mutex::new(None),
                            indexed: Mutex::new(vec![])
                        }),
                        binds: Arc::new(AtomicUsize::new(0)),
                        closing: false,
                        report_consumed: false
                    }),
                    executor: Arc::new(NoExecutor),
                    transport: None,
                })
                .is_ok()
        );
        let metric_registry = prometheus::Registry::new();
        let metrics = owner.metrics(&metric_registry).unwrap();
        metrics.sample();
        let gathered = metric_registry.gather();
        let process = gathered
            .iter()
            .find(|family| {
                family.get_name() == "novarocks_frontend_control_runtime_transport_process"
            })
            .unwrap();
        let used_items = process
            .get_metric()
            .iter()
            .find(|metric| {
                metric
                    .get_label()
                    .iter()
                    .any(|label| label.get_name() == "dimension" && label.get_value() == "1")
                    && metric
                        .get_label()
                        .iter()
                        .any(|label| label.get_name() == "resource" && label.get_value() == "used")
            })
            .unwrap();
        assert_eq!(
            used_items.get_gauge().get_value(),
            1.0,
            "metrics read the original charged process dimension"
        );
        let pending = owner
            .shutdown_until(Instant::now() + Duration::from_millis(20))
            .await;
        let Err(FrontendShutdownError::Pending(snapshot)) = pending else {
            panic!("physical holders must block shutdown");
        };
        assert!(
            !snapshot.actors_drained && !snapshot.ledger_drained && !snapshot.host_routes_drained
        );
        assert!(!snapshot.pollers_joined && !snapshot.membership_joined);
        assert_eq!(captures.load(Ordering::SeqCst), 0);
        assert_eq!(custody.phase(), ExitCustodyPhase::Admitted);
        assert_eq!(scope.snapshot().unwrap().unresolved, 0);
        assert_eq!(
            runtime
                .snapshot(registration.actor())
                .unwrap()
                .standing_inputs,
            2
        );
        run_future(
            exit,
            async move {
                drop(held_captures);
            },
            PanicSink::new(),
        )
        .await
        .unwrap();
        assert_eq!(captures.load(Ordering::SeqCst), 1);
        assert_eq!(custody.phase(), ExitCustodyPhase::Exited);
        runtime.notify_host_actor(&registration).unwrap();
        owner
            .shutdown_until(Instant::now() + Duration::from_secs(5))
            .await
            .unwrap();
        let snapshot = owner.shutdown_snapshot();
        assert!(
            snapshot.actors_drained
                && snapshot.ledger_drained
                && snapshot.host_routes_drained
                && snapshot.membership_routes_drained
                && snapshot.pollers_joined
                && snapshot.membership_joined
        );
        assert_eq!(custody.phase(), ExitCustodyPhase::Retired);
        assert!(ledger.is_empty());
        let deadline = Instant::now() + Duration::from_secs(5);
        data.drain_native_outgoing_until(deadline).await.unwrap();
        data.drain_original_apply_until(deadline).await.unwrap();
        data.drain_original_subscriptions_until(deadline)
            .await
            .unwrap();
    }
}
