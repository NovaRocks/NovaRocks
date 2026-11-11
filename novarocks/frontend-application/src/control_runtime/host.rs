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
    membership::AttemptMembershipSource,
    metrics::FrontendHostObservations,
};
use crate::task_execution::host::custody::{
    TransportRetirement, TransportRetirements, TransportSourceEvent,
};
use crate::task_execution::host::{
    adapter::AttemptCapacitySource,
    failure_publication::FailureContext,
    observations::{
        ObservationError,
        host_turn::{OrderedObservationFailure, OrderedObservationHost},
    },
};
use novarocks_control_runtime::{
    actor::ActorConfig,
    actor_turn::{
        ActorHostTurn, ActorRegistration, ActorRuntime, ActorTurnError, EffectTicket,
        FailureSourceState,
    },
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
        Arc, Mutex, Weak,
        atomic::{AtomicBool, Ordering},
    },
};
use tokio::sync::{Notify, watch};

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
    /// Transfer the one supervisor effect to its original cleanup owner.
    /// The owner launches its prepaid Work off the poller and retains the
    /// ticket until publication and actual exit allow the input to retire.
    fn owner_failed_cleanup(
        &mut self,
        ticket: EffectTicket<FrontendEffect>,
        context: Option<FailureContext>,
        observations: Option<OrderedObservationFailure>,
    ) {
        std::panic::resume_unwind(Box::new((ticket, context, observations)));
    }
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
    ordered_observations: Option<OrderedObservationHost>,
    startup: Option<crate::task_execution::host::startup::AttemptStartupSource>,
    capacity: Option<AttemptCapacitySource>,
    membership: Option<AttemptMembershipSource>,
    // A finite pass over original ready markers survives host-budget yield.
    // Resetting this to ready.len() on every turn would busy-loop when all
    // purposes are Full and ready markers outnumber one host allowance.
    scan_remaining: Option<usize>,
    rescan_requested: bool,
    pending: Option<PendingHandoff>,
    failure_handed_off: bool,
    external_closed: bool,
}
struct HostEntry {
    scope: ScopeHandle,
    registration: ActorRegistration,
    kind: FrontendActorKind,
    budget: usize,
    close_requested: AtomicBool,
    exit: watch::Sender<bool>,
    // The exclusive host turn takes this original state out, then releases
    // the mutex before every Source/ledger/executor operation and Waker.
    machine: Mutex<Option<HostMachine>>,
}

impl HostEntry {
    fn request_close(&self, runtime: &ActorRuntime<FrontendActor>) -> Result<(), ActorTurnError> {
        self.close_requested.store(true, Ordering::Release);
        match runtime.notify_host_actor(&self.registration) {
            // The matching registration can disappear between the close mark
            // and the wake. Only the exit receipt proves actual removal.
            Ok(())
            | Err(ActorTurnError::UnknownActor | ActorTurnError::EffectTicketOriginMismatch) => {
                Ok(())
            }
            Err(error) => Err(error),
        }
    }
}

/// Observation of one installed host's actual Actor and Scope removal.
pub(crate) struct HostExit {
    registration: ActorRegistration,
    runtime: ActorRuntime<FrontendActor>,
    original: Weak<HostEntry>,
    exited: watch::Receiver<bool>,
}
impl HostExit {
    pub(crate) fn matches(&self, registration: &ActorRegistration) -> bool {
        self.registration.same_registration(registration)
    }

    /// Request closure of this attempt alone. The borrowed wait below
    /// remains the only proof that the original actor and Scope are gone.
    pub(crate) fn request_close(&self) -> Result<(), ActorTurnError> {
        if let Some(original) = self.original.upgrade() {
            original.request_close(&self.runtime)
        } else if *self.exited.borrow() {
            Ok(())
        } else {
            Err(ActorTurnError::Closed)
        }
    }

    pub(crate) async fn wait(&mut self) -> Result<(), watch::error::RecvError> {
        self.exited.wait_for(|exited| *exited).await.map(|_| ())
    }
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
    pub(crate) ordered_observations: Option<OrderedObservationHost>,
    pub(crate) startup: Option<crate::task_execution::host::startup::AttemptStartupSource>,
    pub(crate) capacity: Option<AttemptCapacitySource>,
    pub(crate) membership: Option<AttemptMembershipSource>,
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
    pub(crate) fn register(
        &self,
        binding: HostBinding,
    ) -> Result<HostExit, HostRegistrationRefusal> {
        if !binding.registration.matches_scope(&binding.scope)
            || binding.transport.as_ref().is_some_and(|transport| {
                !transport.matches_registration(&self.runtime, &binding.registration)
            })
            || binding
                .ordered_observations
                .as_ref()
                .is_some_and(|observations| !observations.matches(&binding.registration))
            || binding
                .capacity
                .as_ref()
                .is_some_and(|capacity| !capacity.matches(&binding.registration))
            || binding.membership.as_ref().is_some_and(|membership| {
                !membership.matches(&binding.registration) || binding.startup.is_none()
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
        if binding
            .startup
            .as_ref()
            .is_some_and(|startup| !startup.matches(&binding.registration))
        {
            return Err(HostRegistrationRefusal {
                reason: "startup position belongs to another actor registration",
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
        if binding
            .ordered_observations
            .as_ref()
            .is_some_and(|observations| !observations.fits_host_turn(binding.config.turn_budget))
        {
            return Err(HostRegistrationRefusal {
                reason: "host turn cannot visit the original observation roles",
                runtime_error: None,
                binding,
            });
        }
        if binding
            .membership
            .as_ref()
            .is_some_and(|source| source.visit_units() > binding.config.turn_budget)
        {
            return Err(HostRegistrationRefusal {
                reason: "host turn cannot visit the original membership roster",
                runtime_error: None,
                binding,
            });
        }
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
        let (exit, exited) = watch::channel(false);
        let start_after_registration = binding.startup.is_some();
        let original = Arc::new(HostEntry {
            scope: binding.scope,
            registration: binding.registration,
            kind: binding.kind,
            budget,
            close_requested: AtomicBool::new(false),
            exit,
            machine: Mutex::new(Some(HostMachine {
                withdrawals: Arc::default(),
                dispatch,
                policy: binding.policy,
                source: binding.source,
                executor: binding.executor,
                transport: binding.transport,
                ordered_observations: binding.ordered_observations,
                startup: binding.startup,
                capacity: binding.capacity,
                membership: binding.membership,
                scan_remaining: None,
                rescan_requested: false,
                pending: None,
                failure_handed_off: false,
                external_closed: false,
            })),
        });
        let observed = HostExit {
            registration: original.registration.clone(),
            runtime: self.runtime.clone(),
            original: Arc::downgrade(&original),
            exited,
        };
        state.entries.insert(original.scope.id(), original);
        drop(state);
        if start_after_registration {
            self.runtime
                .notify_host_actor(&observed.registration)
                .expect("startup is scheduled on its installed original host");
        }
        self.changed.notify_waiters();
        Ok(observed)
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
            entry
                .request_close(&self.runtime)
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
            entry.exit.send_replace(true);
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
        if failed && !machine.failure_handed_off {
            let actor = entry.registration.actor();
            if let FailureSourceState::ClosedPending(input) =
                self.runtime.failure_source_state(actor)?
            {
                budget -= 1;
                // The failure fence becomes visible before the supervisor
                // batch commits. The Core turn wakes this host on commit.
                match self.runtime.effect_inventory(actor, input, None, 1) {
                    Ok(effects) => {
                        if let [(descriptor, _)] = effects.as_slice() {
                            let ticket = self.runtime.take_effect(actor, input, descriptor.id)?;
                            self.runtime.validate_failure_effect_ticket(&ticket)?;
                            let observations = machine
                                .ordered_observations
                                .as_mut()
                                .map(OrderedObservationHost::fence_failed);
                            machine
                                .source
                                .owner_failed_cleanup(ticket, None, observations);
                            machine.failure_handed_off = true;
                            handoffs += 1;
                        }
                    }
                    Err(ActorTurnError::UnknownInput) => {}
                    Err(error) => return Err(error.into()),
                }
            }
        }
        let mut startup_more = false;
        if let Some(startup) = &mut machine.startup {
            if budget == 0 {
                startup_more = true;
            } else {
                budget -= 1;
                startup_more = startup.progress(failed, machine.external_closed)?;
                if startup.is_closed() {
                    if startup.is_ready()
                        && let Some(membership) = &mut machine.membership
                    {
                        membership.startup_complete();
                    }
                    machine.startup = None;
                }
            }
        }
        let transport = machine.transport.as_mut().map(|transport| {
            // One original row visit per host turn leaves the original
            // indexing allowance available to make the Core and effects run.
            transport.progress(turn, budget.min(1), |effect, event| {
                machine.source.transport_event(effect, event);
            })
        });
        if let Some(transport) = &transport {
            budget -= transport.units;
        }
        if !failed && let Some(observations) = &mut machine.ordered_observations {
            let progress = (|| {
                if observations.has_indexed() {
                    budget -= 1;
                    observations.retire_ready(&self.runtime)?;
                }
                observations.retire_read_progress(&mut budget)
            })();
            match progress {
                Ok(()) => {}
                Err(ObservationError::Runtime(
                    ActorTurnError::ActorFailed | ActorTurnError::Closed,
                )) if self.runtime.snapshot(entry.registration.actor())?.failed => {
                    // The original failure fence marks this host again.
                }
                Err(error) => return Err(error.into()),
            }
        }
        if failed
            && machine.failure_handed_off
            && let Some(observations) = &machine.ordered_observations
        {
            observations.retire_failed(&self.runtime, &mut budget)?;
        }
        let mut capacity_more = false;
        let closing_capacity = machine.source.result_admission_closed();
        if let Some(capacity) = &mut machine.capacity
            && capacity.needs_progress(failed, closing_capacity)
        {
            if budget == 0 {
                capacity_more = true;
            } else {
                budget -= 1;
                capacity_more = capacity.progress(failed, closing_capacity)?;
                if capacity.is_closed() {
                    machine.capacity = None;
                }
            }
        }
        let mut membership_more = false;
        if let Some(membership) = &mut machine.membership
            && membership.needs_progress(failed, closing_capacity)
        {
            let units = membership.visit_units();
            if budget < units {
                membership_more = true;
            } else {
                budget -= units;
                membership_more = membership.progress(failed, closing_capacity)?;
                if membership.is_closed() {
                    machine.membership = None;
                }
            }
        }
        let mut retired = machine
            .source
            .progress_retirement(&self.runtime, turn, now, budget);
        assert!(
            retired.units <= budget,
            "Source work uses the original finite host allowance"
        );
        budget -= retired.units;
        // The original Source can finish admission in this progress call.
        // Schedule the retained capacity role for its separate final visit.
        if !closing_capacity && machine.source.result_admission_closed() {
            capacity_more |= machine.capacity.is_some();
            membership_more |= machine.membership.is_some();
        }
        if let Some(transport) = transport {
            retired.progressed |= transport.progressed;
            retired.more |= transport.more;
            retired.retired &= machine.transport.as_ref().unwrap().is_empty();
        }
        retired.retired &= machine
            .ordered_observations
            .as_ref()
            .is_none_or(OrderedObservationHost::is_retired);
        retired.retired &=
            machine.startup.is_none() && machine.capacity.is_none() && machine.membership.is_none();
        retired.more |= startup_more || capacity_more || membership_more;
        if retired.retired
            && matches!(
                self.runtime
                    .failure_source_state(entry.registration.actor())?,
                FailureSourceState::ClosedPending(_)
            )
        {
            retired.retired = false;
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
        let mut observation_advanced = false;
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
            if let Some(observations) = &mut machine.ordered_observations {
                observation_advanced =
                    observations.index_committed(&self.runtime, &indexed.inputs)?;
            }
            if let Some(startup) = &mut machine.startup {
                startup_more |= startup.indexed(&indexed.inputs);
            }
            if let Some(capacity) = &mut machine.capacity {
                capacity_more |= capacity.index_committed(&indexed.inputs)?;
            }
            if let Some(membership) = &mut machine.membership {
                membership_more |= membership.index_committed(&indexed.inputs)?;
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
                if let FrontendEffect::AttemptRootControlProcessed(input) = metadata {
                    if let Some(ticket) = original_dispatch!(
                        machine.dispatch.claim_candidate(&self.runtime, &candidate)
                    ) {
                        assert_eq!(*input, ticket.input, "original root-control step");
                        let observations = machine
                            .ordered_observations
                            .as_mut()
                            .ok_or(ObservationError::InvalidRootControl)?;
                        observations.root_control_processed(ticket.input)?;
                        self.runtime
                            .effect_completed(ticket)
                            .map_err(|refusal| HostError::Runtime(refusal.error))?;
                        observation_advanced = true;
                    } else {
                        original_dispatch!(machine.dispatch.defer_candidate(&candidate));
                    }
                    continue;
                }
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
                if matches!(metadata, FrontendEffect::Attempt(effect) if effect.uses_join_custody())
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
            || startup_more
            || capacity_more
            || membership_more
            || observation_advanced
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
    Observation(ObservationError),
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

impl From<ObservationError> for HostError {
    fn from(error: ObservationError) -> Self {
        Self::Observation(error)
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
        retire_batches: bool,
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
            let batches = if self.retire_batches {
                runtime.committed_batches(actor, None, budget).unwrap()
            } else {
                Vec::new()
            };
            for (_, input) in &batches {
                runtime.retire_input(actor, *input).unwrap();
            }
            let more = self.retire_batches
                && !runtime
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

    #[tokio::test]
    async fn installed_host_drives_startup_through_original_input_retirement() {
        drive_startup(false).await;
    }

    #[tokio::test]
    async fn startup_close_notifies_the_caller_while_retaining_its_queued_input() {
        drive_startup(true).await;
    }

    async fn drive_startup(close_before_bootstrap: bool) {
        use crate::task_execution::host::startup::{AttemptStartupSource, StartupInput};

        struct StartupProbe(Arc<Mutex<Vec<StartupInput>>>);
        impl TestActor for StartupProbe {
            fn handle(
                &mut self,
                _: Tick,
                envelope: InputEnvelope,
                input: FrontendInput,
                _: &mut ActorStep<'_, FrontendEffect>,
            ) {
                let FrontendInput::AttemptStartup(input) = input else {
                    panic!("the installed host supplies only its original startup inputs");
                };
                assert_eq!(envelope.class, InputClass::Lifecycle);
                self.0.lock().unwrap().push(input);
            }
        }

        let clock = Arc::new(FrontendClock(Arc::new(TestClock(Instant::now()))));
        let ledger = RuntimeLedger::new(ResourceVector::default());
        let scope = ledger
            .open_scope_next(None, Generation::new(1).unwrap())
            .unwrap();
        let runtime = ActorRuntime::new();
        let host = HostRegistry::new(
            runtime.clone(),
            ledger.clone(),
            Arc::new(FrontendHostObservations::new(clock)),
        );
        let actor = ActorId::new(1).unwrap();
        let observed = Arc::new(Mutex::new(Vec::new()));
        let config = ActorConfig {
            mailbox: [1; 4],
            max_batches: 1,
            max_effects_per_step: 1,
            max_dependencies_per_effect: 0,
            turn_budget: 8,
            max_step_work: 1,
        };
        let registration = runtime
            .register_bound(
                actor,
                scope.clone(),
                FrontendActor::Test(Box::new(StartupProbe(observed.clone()))),
                config,
            )
            .unwrap_or_else(|(error, _)| panic!("original runtime actor registration: {error:?}"));
        let position = runtime
            .reserve_input(actor, actor, InputClass::Lifecycle)
            .unwrap()
            .into_standing_input()
            .unwrap();
        let (source, mut startup) = AttemptStartupSource::new(registration.clone(), position);
        assert!(runtime.claim_host_actor().is_none());
        assert!(!startup.ready_for(&registration));

        let mut exit = host
            .register(HostBinding {
                scope: scope.clone(),
                registration: registration.clone(),
                kind: FrontendActorKind::Test,
                config,
                policy: FrontendLanePolicy {
                    lane: |_| None,
                    per_step_inventory: 1,
                },
                source: Box::new(MetadataSource {
                    scope,
                    closing: false,
                    retire_batches: false,
                }),
                executor: Arc::new(NoExecutor),
                transport: None,
                ordered_observations: None,
                startup: Some(source),
                capacity: None,
                membership: None,
            })
            .unwrap_or_else(|_| panic!("install original host startup"));
        let drive_host = || {
            host.drive(
                Tick(1),
                runtime.claim_host_actor().expect("original host wake"),
            )
        };
        drive_host();
        if close_before_bootstrap {
            exit.request_close().unwrap();
            drive_host();
            assert_eq!(startup.wait_ready().await, Err(ActorTurnError::Closed));
            assert_eq!(runtime.snapshot(actor).unwrap().inputs, 1);
            assert!(!startup.ready_for(&registration));
            assert!(runtime.poll_one(Tick(1)).unwrap());
            drive_host(); // Index the already queued Bootstrap.
            drive_host(); // Retire its actual occurrence.
            drive_host(); // Close the role and retire the original host.
            exit.wait().await.unwrap();
            assert_eq!(*observed.lock().unwrap(), [StartupInput::Bootstrap]);
            assert!(host.is_empty() && runtime.is_empty() && ledger.is_empty());
            return;
        }
        assert!(runtime.poll_one(Tick(1)).unwrap());
        assert_eq!(*observed.lock().unwrap(), [StartupInput::Bootstrap]);
        assert!(!startup.ready_for(&registration));
        assert!(!runtime.poll_one(Tick(1)).unwrap());

        drive_host(); // Index Bootstrap.
        drive_host(); // Retire Bootstrap.
        drive_host(); // Fill StartProtocol in the same paid position.
        assert!(runtime.poll_one(Tick(1)).unwrap());
        assert_eq!(
            *observed.lock().unwrap(),
            [StartupInput::Bootstrap, StartupInput::StartProtocol]
        );
        assert!(!startup.ready_for(&registration));
        {
            let wait = startup.wait_ready();
            tokio::pin!(wait);
            assert!(
                std::future::poll_fn(|cx| std::task::Poll::Ready(
                    wait.as_mut().poll(cx).is_pending()
                ))
                .await
            );
        }
        drive_host(); // Index StartProtocol.
        drive_host(); // Retire StartProtocol.
        drive_host(); // Return the original standing position.
        startup.wait_ready().await.unwrap();
        assert!(startup.ready_for(&registration));
        assert!(
            runtime
                .standing_input_inventory(actor, None, 1)
                .unwrap()
                .is_empty()
        );

        exit.request_close().unwrap();
        drive_host();
        exit.wait().await.unwrap();
        assert!(host.is_empty() && runtime.is_empty() && ledger.is_empty());
    }

    #[tokio::test]
    async fn installed_host_opens_membership_only_after_start_and_retires_each_indexed_view() {
        use crate::{
            control_runtime::membership::{
                FrozenMember, MembershipRegistry, MembershipTarget, membership_input,
            },
            task_execution::host::startup::{AttemptStartupSource, StartupInput},
        };
        use novarocks_attempt_protocol::input::{MembershipObservation, MembershipView};
        use novarocks_control_runtime::coalescing_input::CoalescingInput;
        use novarocks_execution_contract::{ParticipationRef, RuntimeEndpoint};
        use novarocks_query_application::api::{
            BackendProcessObservation, BackendProcessObservationPort, BackendTopologyError,
        };
        use novarocks_types::{
            AttemptId, BackendProcessId, FrontendProcessId, QueryExecutionId, QueryId,
        };

        struct Probe(Arc<Mutex<Vec<MembershipView>>>);
        impl TestActor for Probe {
            fn handle(
                &mut self,
                _: Tick,
                envelope: InputEnvelope,
                input: FrontendInput,
                _: &mut ActorStep<'_, FrontendEffect>,
            ) {
                match input {
                    FrontendInput::AttemptStartup(
                        StartupInput::Bootstrap | StartupInput::StartProtocol,
                    ) => {
                        assert_eq!(envelope.class, InputClass::Lifecycle);
                    }
                    FrontendInput::Membership(update) => {
                        assert_eq!(envelope.class, InputClass::Ordinary);
                        self.0.lock().unwrap().push(update.0);
                    }
                    _ => panic!("unexpected input to the installed membership host"),
                }
            }
        }

        struct Port(Result<BackendProcessObservation, BackendTopologyError>);
        impl BackendProcessObservationPort for Port {
            fn observe_process_at_endpoint(
                &self,
                _: BackendProcessId,
                _: &RuntimeEndpoint,
            ) -> Result<BackendProcessObservation, BackendTopologyError> {
                self.0.clone()
            }
        }

        let ledger = RuntimeLedger::new(ResourceVector::default());
        let scope = ledger
            .open_scope_next(None, Generation::new(1).unwrap())
            .unwrap();
        let runtime = ActorRuntime::new();
        let clock = Arc::new(FrontendClock(Arc::new(TestClock(Instant::now()))));
        let host = HostRegistry::new(
            runtime.clone(),
            ledger.clone(),
            Arc::new(FrontendHostObservations::new(clock)),
        );
        let actor = ActorId::new(1).unwrap();
        let seen = Arc::new(Mutex::new(Vec::new()));
        let config = ActorConfig {
            mailbox: [1; 4],
            max_batches: 1,
            max_effects_per_step: 1,
            max_dependencies_per_effect: 0,
            turn_budget: 8,
            max_step_work: 1,
        };
        let registration = runtime
            .register_bound(
                actor,
                scope.clone(),
                FrontendActor::Test(Box::new(Probe(seen.clone()))),
                config,
            )
            .unwrap_or_else(|(error, _)| panic!("original actor registration: {error:?}"));
        let standing = |source, class| {
            runtime
                .reserve_input(actor, source, class)
                .unwrap()
                .into_standing_input()
                .unwrap()
        };
        let (startup, mut ready) =
            AttemptStartupSource::new(registration.clone(), standing(actor, InputClass::Lifecycle));
        let participant = ParticipationRef::new(
            QueryExecutionId::new(QueryId::new(1, 1), AttemptId::new(1).unwrap()).unwrap(),
            FrontendProcessId::new_v7(),
            BackendProcessId::new_v7(),
        );
        let membership = MembershipTarget::register(
            registration.clone(),
            Arc::from([FrozenMember {
                participation: participant,
                endpoint: RuntimeEndpoint::new("127.0.0.1", 19091).unwrap(),
            }]),
            CoalescingInput::new(
                standing(ActorId::new(2).unwrap(), InputClass::Ordinary),
                membership_input,
            ),
        );
        let registry = Arc::new(MembershipRegistry::new());
        let mut exit = host
            .register(HostBinding {
                scope: scope.clone(),
                registration,
                kind: FrontendActorKind::Test,
                config,
                policy: FrontendLanePolicy {
                    lane: |_| None,
                    per_step_inventory: 1,
                },
                source: Box::new(MetadataSource {
                    scope,
                    closing: false,
                    retire_batches: false,
                }),
                executor: Arc::new(NoExecutor),
                transport: None,
                ordered_observations: None,
                startup: Some(startup),
                capacity: None,
                membership: Some(AttemptMembershipSource::new(
                    registry.clone(),
                    membership.target.clone(),
                )),
            })
            .unwrap_or_else(|_| panic!("install original membership host"));
        let drive = || {
            host.drive(
                Tick(1),
                runtime.claim_host_actor().expect("original host wake"),
            );
        };
        let poll = || assert!(runtime.poll_one(Tick(1)).unwrap());
        let current = Port(Ok(BackendProcessObservation::Current));
        registry.publish(0, &current);
        assert!(registry.is_empty());
        drive(); // Fill Bootstrap.
        poll();
        drive(); // Index Bootstrap.
        drive(); // Retire Bootstrap.
        drive(); // Fill StartProtocol.
        poll();
        drive(); // Index StartProtocol.
        drive(); // Retire StartProtocol.
        assert!(registry.is_empty());
        drive(); // Return the startup position and open delivery.
        ready.wait_ready().await.unwrap();
        assert!(!registry.is_empty());

        registry.publish(1, &current);
        let first = membership.target.input.in_flight().unwrap().unwrap();
        let original = BackendTopologyError::Unavailable {
            message: "original observer refusal".into(),
        };
        registry.publish(2, &Port(Err(original.clone())));
        assert_eq!(membership.errors.take(), [(participant, original)]);
        assert_eq!(membership.target.input.in_flight().unwrap(), Some(first));
        poll();
        drive();
        assert_eq!(membership.target.input.in_flight().unwrap(), Some(first));
        drive();
        let second = membership.target.input.in_flight().unwrap().unwrap();
        assert_ne!(second, first);

        exit.request_close().unwrap();
        drive();
        assert!(registry.is_empty());
        assert_eq!(membership.target.input.in_flight().unwrap(), Some(second));
        registry.publish(3, &current);
        poll();
        let views = seen.lock().unwrap();
        assert_eq!(views.len(), 2);
        assert_eq!(views[0].revision, 1);
        assert_eq!(
            views[0].processes[&participant],
            MembershipObservation::ExactOriginal
        );
        assert_eq!(views[1].revision, 2);
        assert_eq!(
            views[1].processes[&participant],
            MembershipObservation::Unobserved
        );
        drop(views);

        for _ in 0..8 {
            let Some(turn) = runtime.claim_host_actor() else {
                break;
            };
            host.drive(Tick(1), turn);
        }
        assert!(registry.is_empty() && host.is_empty() && runtime.is_empty() && ledger.is_empty());
        exit.wait().await.unwrap();
    }

    #[tokio::test]
    async fn installed_host_indexes_each_capacity_input_and_cancels_the_original_wait() {
        use novarocks_control_runtime::transaction::RuntimeError;
        use novarocks_control_runtime_core::{
            capacity::ReservationResources,
            identity::DimensionId,
            scope::{WorkAdmission, WorkKind},
        };
        use std::task::Waker;

        struct CapacityProbe(Arc<Mutex<Vec<InputId>>>);
        impl TestActor for CapacityProbe {
            fn handle(
                &mut self,
                _: Tick,
                envelope: InputEnvelope,
                input: FrontendInput,
                _: &mut ActorStep<'_, FrontendEffect>,
            ) {
                assert!(matches!(input, FrontendInput::CapacityReleased));
                assert_eq!(envelope.class, InputClass::Ordinary);
                self.0.lock().unwrap().push(envelope.input);
            }
        }

        let quota = ResourceVector::new([(DimensionId::new(1).unwrap(), 1)]).unwrap();
        let ledger = RuntimeLedger::new(quota.clone());
        let generation = Generation::new(1).unwrap();
        let scope = ledger.open_scope_next(None, generation).unwrap();
        let runtime = ActorRuntime::new();
        let clock = Arc::new(FrontendClock(Arc::new(TestClock(Instant::now()))));
        let host = HostRegistry::new(
            runtime.clone(),
            ledger.clone(),
            Arc::new(FrontendHostObservations::new(clock)),
        );
        let actor = ActorId::new(1).unwrap();
        let source = ActorId::new(2).unwrap();
        let observed = Arc::new(Mutex::new(Vec::new()));
        let config = ActorConfig {
            mailbox: [1; 4],
            max_batches: 1,
            max_effects_per_step: 1,
            max_dependencies_per_effect: 0,
            turn_budget: 8,
            max_step_work: 1,
        };
        let registration = runtime
            .register_bound(
                actor,
                scope.clone(),
                FrontendActor::Test(Box::new(CapacityProbe(observed.clone()))),
                config,
            )
            .unwrap_or_else(|(error, _)| panic!("original actor registration: {error:?}"));
        let position = runtime
            .reserve_input(actor, source, InputClass::Ordinary)
            .unwrap()
            .into_standing_input()
            .unwrap();
        let registrant = Arc::new(scope.capacity_registrant(Waker::noop().clone()).unwrap());
        let capacity =
            AttemptCapacitySource::new(registration.clone(), position, registrant.clone());
        let mut exit = host
            .register(HostBinding {
                scope: scope.clone(),
                registration,
                kind: FrontendActorKind::Test,
                config,
                policy: FrontendLanePolicy {
                    lane: |_| None,
                    per_step_inventory: 1,
                },
                source: Box::new(MetadataSource {
                    scope: scope.clone(),
                    closing: false,
                    retire_batches: false,
                }),
                executor: Arc::new(NoExecutor),
                transport: None,
                ordered_observations: None,
                startup: None,
                capacity: Some(capacity),
                membership: None,
            })
            .unwrap_or_else(|_| panic!("install original capacity Source"));
        let occupy = || {
            let blocker = scope
                .reserve_next(
                    generation,
                    ReservationResources::for_work(quota.clone()),
                    WorkKind::Business,
                    WorkAdmission::External,
                )
                .unwrap();
            assert!(matches!(
                scope.reserve_next_or_wait(
                    generation,
                    ReservationResources::for_work(quota.clone()),
                    WorkKind::Business,
                    WorkAdmission::External,
                    &registrant,
                ),
                Err(RuntimeError::CapacityDenied(_))
            ));
            blocker
        };
        let pending = || runtime.standing_input_inventory(actor, None, 1).unwrap()[0].in_flight;
        let drive = || {
            host.drive(
                Tick(1),
                runtime.claim_host_actor().expect("original host wake"),
            );
        };
        drop(occupy());
        let first = pending().unwrap();
        drop(occupy());
        assert_eq!(pending(), Some(first));
        assert!(runtime.poll_one(Tick(1)).unwrap());

        drive();
        assert_eq!(
            pending(),
            Some(first),
            "indexing retains the original occurrence"
        );
        drive();
        let second = pending().unwrap();
        assert_ne!(second, first);
        assert_eq!(*observed.lock().unwrap(), [first]);

        let blocker = occupy();
        let waits = || {
            ledger
                .try_capacity_waiter_snapshot()
                .unwrap()
                .unwrap()
                .logical_waiters
        };
        assert_eq!(waits(), 1);
        exit.request_close().unwrap();
        drive();
        assert_eq!(waits(), 0);
        assert_eq!(pending(), Some(second));
        drop(blocker);

        assert!(runtime.poll_one(Tick(1)).unwrap());
        assert_eq!(*observed.lock().unwrap(), [first, second]);
        for _ in 0..8 {
            let Some(turn) = runtime.claim_host_actor() else {
                break;
            };
            host.drive(Tick(1), turn);
        }
        assert!(host.is_empty() && runtime.is_empty() && ledger.is_empty());
        exit.wait().await.unwrap();
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
                    closing: false,
                    retire_batches: true,
                }),
                executor: Arc::new(NoExecutor),
                transport: None,
                startup: None,
                capacity: None,
                membership: None,
                ordered_observations: None,
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
                startup: None,
                capacity: None,
                membership: None,
                ordered_observations: None,
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
    struct MemberServicePort(Option<tokio::sync::watch::Sender<u64>>);
    impl novarocks_query_application::api::BackendProcessObservationPort for MemberServicePort {
        fn subscribe_process_changes(&self) -> tokio::sync::watch::Receiver<u64> {
            self.0.as_ref().map_or_else(
                || tokio::sync::watch::channel(0).1,
                |source| source.subscribe(),
            )
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

    #[tokio::test]
    async fn unexpected_membership_exit_remains_the_original_shutdown_failure() {
        use crate::control_runtime::{
            FrontendControlRuntime, FrontendControlRuntimeConfig, FrontendShutdownError,
            membership::MembershipShutdownError,
        };
        let handle = tokio::runtime::Handle::current();
        let mut data = crate::native::data_runtime::FrontendDataRuntime::new(handle.clone());
        let mut owner = FrontendControlRuntime::start(
            FrontendControlRuntimeConfig {
                pollers: std::num::NonZeroUsize::new(handle.metrics().num_workers()).unwrap(),
                clock: Arc::new(TestClock(Instant::now())),
                transport: novarocks_task_codec::TransportBudget::DEFAULT,
                workload_scope_limit: std::num::NonZeroUsize::new(
                    novarocks_workload_control::WorkloadConfig::default().scope_records_limit,
                )
                .unwrap(),
                history: None,
            },
            &data,
            Arc::new(MemberServicePort(None)),
        )
        .unwrap();
        let deadline = Instant::now() + Duration::from_secs(5);
        let detected = tokio::time::timeout_at(deadline.into(), async {
            while !owner.membership_has_exited() {
                tokio::task::yield_now().await;
            }
        })
        .await;
        let shutdown = owner
            .shutdown_until(Instant::now() + Duration::from_secs(5))
            .await;
        assert!(detected.is_ok());
        assert!(matches!(
            shutdown,
            Err(FrontendShutdownError::Membership(
                MembershipShutdownError::SourceClosed
            ))
        ));
        let snapshot = owner.shutdown_snapshot();
        assert!(snapshot.pollers_joined && snapshot.membership_joined);
        let deadline = Instant::now() + Duration::from_secs(5);
        data.drain_native_outgoing_until(deadline).await.unwrap();
        data.drain_original_apply_until(deadline).await.unwrap();
        data.drain_original_subscriptions_until(deadline)
            .await
            .unwrap();
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
                workload_scope_limit: std::num::NonZeroUsize::new(
                    novarocks_workload_control::WorkloadConfig::default().scope_records_limit,
                )
                .unwrap(),
                history: None,
            },
            &data,
            Arc::new(MemberServicePort(Some(tokio::sync::watch::channel(0).0))),
        )
        .unwrap_or_else(|e| panic!("actual FE runtime start: {e:?}"));
        let attempts = owner.attempts();
        let runtime = attempts.actor_runtime().clone();
        let ledger = attempts.ledger().clone();
        let scope = ledger
            .open_scope_next(None, Generation::new(1).unwrap())
            .unwrap();
        let mut resources = attempts
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
            attempts
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
                    startup: None,
                    capacity: None,
                    membership: None,
                    ordered_observations: None,
                })
                .is_ok()
        );
        let metric_registry = prometheus::Registry::new();
        let metrics = attempts.metrics(&metric_registry).unwrap();
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
