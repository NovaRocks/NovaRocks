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

//! FE composition of the common runtime. The process owns one actual ledger,
//! actor runtime, poller pool, membership service Join and host routing index.
//! N is supplied from the Server's actual data-runtime worker count; this module
//! introduces no poller setting, transport quota, global Work source or alternate
//! process topology. P10a supplies real already-admitted Source implementations.
//!
//! Metrics read the original CR wait and pending-effect observations. Busy
//! reads remain unavailable; no history reconstruction or default count exists.

//! Integration handoff:
//!
//! * P10a implements EffectSource from the actual admitted Source owner. Result
//!   credit is obtained before take_ready; bind_job only moves original guards,
//!   reports, captures and ExitObligation. No descriptor signs another Work.
//! * Executor refusal retains the entire original job/ticket and waits for a
//!   real availability wake. Handoff completion is distinct from physical exit.
//! * Tests exercise three real pollers' same-actor exclusion and paid result
//!   delivery, repeated full-job refusal, original report/custody retirement,
//!   latest membership revision delivery and in-flight member-input closure.
//! * The shutdown test retains original paid roles/captures and both joins
//!   through timeout, then observes real work/role/report exit before joining.
//!   Timeout, cancellation and handoff never supply physical exit evidence.
//! * P11 must connect unexpected member-service Join failure to the existing
//!   application health supervisor while preserving the original Join outcome.
//!   This module does not classify that diagnostic as replacement evidence.
//!
pub(crate) mod actor;
pub(crate) mod host;
pub(crate) mod membership;
pub(crate) mod metrics;

use crate::native::{
    data_runtime::FrontendDataRuntime, transport_capacity::NativeTransportCapacity,
};
use crate::task_execution::bridge::{
    control::AttemptOwnerControls,
    terminal::{RegisteredAttemptTerminal, TerminalOutbox},
};
use crate::task_execution::host::adapter::{
    AttemptActorRegistration, AttemptRegistrationError, PreparedAttemptHost,
};
use crate::task_execution::host::failure_bridge::PendingFailureBridge;
use crate::task_execution::host::funding::AttemptHostFunding;
use crate::task_execution::host::observations::{
    ObservationBudget, ObservationError, backing::ObservationBackingQuote,
};
use crate::task_execution::host::startup::AttemptStartup;
use crate::task_execution::host::transport::AttemptCallSource;
use actor::{FrontendActor, FrontendClock};
use host::{HostBinding, HostExit, HostRegistrationRefusal, HostRegistry};
use membership::{
    AttemptMembershipSource, MembershipErrors, MembershipObserver, MembershipRegistry,
    MembershipShutdownError,
};
use metrics::{FrontendHostObservations, FrontendRuntimeMetrics};
use novarocks_control_runtime::{
    actor_turn::{ActorRuntime, Clock, PollerPool},
    history::HistoryCapture,
    obligation::{ReservationObligation, WorkObligation},
    observation::RuntimeObservation,
    transaction::{RuntimeError, RuntimeLedger},
};
use novarocks_control_runtime_core::{
    capacity::ReservationResources,
    identity::{Generation, ScopeId},
    scope::{WorkAdmission, WorkKind},
};
use novarocks_coordination_contract::MonotonicClock;
use novarocks_query_application::api::BackendProcessObservationPort;
use prometheus::Registry;
use std::{
    any::Any,
    collections::BTreeMap,
    num::NonZeroUsize,
    sync::{Arc, Mutex, MutexGuard},
    time::Instant,
};

pub(crate) struct FrontendControlRuntimeConfig {
    /// The actual worker count of the role's data runtime.
    pub(crate) pollers: NonZeroUsize,
    pub(crate) clock: Arc<dyn MonotonicClock>,
    pub(crate) workload_scope_limit: NonZeroUsize,
    pub(crate) history: Option<HistoryCapture>,
}
#[derive(Debug)]
#[allow(
    dead_code,
    reason = "The application reports the original typed startup failure through Debug."
)]
pub(crate) enum FrontendControlStartError {
    Ledger(RuntimeError),
    Observation(ObservationError),
    Pollers(std::io::Error),
}
#[derive(Clone, Copy, Debug)]
#[allow(
    dead_code,
    reason = "The shutdown diagnostic reports each original owner and join state through Debug."
)]
pub(crate) struct FrontendShutdownSnapshot {
    pub(crate) actors_drained: bool,
    pub(crate) ledger_drained: bool,
    pub(crate) host_routes_drained: bool,
    pub(crate) membership_routes_drained: bool,
    pub(crate) pollers_joined: bool,
    pub(crate) membership_joined: bool,
}
#[derive(Debug)]
pub(crate) enum FrontendShutdownError {
    Pending(FrontendShutdownSnapshot),
    Membership(MembershipShutdownError),
}
/// The non-cloneable process owner retains every original physical Join.
pub(crate) struct FrontendControlRuntime {
    attempts: Arc<FrontendAttemptRuntime>,
    member_observer: MembershipObserver,
    pollers: Option<PollerPool<FrontendActor>>,
    membership_joined: bool,
    shutdown_started: bool,
}

/// Attempt registration and its original runtime authorities. Logical
/// launchers borrow this process registration without owning service joins.
pub(crate) struct FrontendAttemptRuntime {
    registration_open: Mutex<bool>,
    runtime: ActorRuntime<FrontendActor>,
    ledger: RuntimeLedger,
    layout: NativeTransportCapacity,
    host: Arc<HostRegistry>,
    members: Arc<MembershipRegistry>,
    backend_processes: Arc<dyn BackendProcessObservationPort>,
    clock: Arc<dyn Clock>,
    observation: RuntimeObservation,
    host_observation: Arc<FrontendHostObservations>,
    unresolved_replacements: Mutex<BTreeMap<ScopeId, UnresolvedReplacementQualification>>,
}

/// Keeps process shutdown from fencing the CR and host registries between
/// the original attempt's scope admission and publication of its host.
pub(crate) struct AttemptRegistrationGuard<'a> {
    owner: &'a FrontendAttemptRuntime,
    _open: MutexGuard<'a, bool>,
}

#[derive(Debug)]
pub(crate) struct AttemptRegistrationClosed;

#[derive(Debug)]
pub(crate) enum ReplacementQualificationScopeError {
    Closed,
    Runtime(RuntimeError),
}

/// The original admission parent for a single logical qualification. The
/// logical qualification future drives all its physical sends to actual exit.
#[must_use = "the FE ledger keeps the qualification until its original Scope is removed"]
pub(crate) struct ReplacementQualificationScope {
    parent: WorkObligation,
    payer: ReservationObligation,
}

impl ReplacementQualificationScope {
    pub(crate) const GENERATION: Generation = Generation::new(1).unwrap();

    pub(crate) fn parent(&self) -> &WorkObligation {
        &self.parent
    }

    fn retire(&mut self, ledger: &RuntimeLedger) -> Result<(), RuntimeError> {
        self.parent.scope().close_external()?;
        self.parent.body_exited()?;
        self.parent.captures_exited()?;
        self.payer.finish()?;
        let scope = self.parent.scope();
        if !scope.reap_observed_header(&self.parent.retirement_observer())? {
            return Err(RuntimeError::ScopeNotRetired);
        }
        ledger.remove_scope(scope.id())
    }
}

struct UnresolvedReplacementQualification {
    _original: ReplacementQualificationScope,
    _failure: Box<dyn Any + Send>,
}

impl FrontendAttemptRuntime {
    pub(crate) fn open_replacement_qualification(
        &self,
    ) -> Result<ReplacementQualificationScope, ReplacementQualificationScopeError> {
        use ReplacementQualificationScopeError::{Closed, Runtime};

        let open = self
            .registration_open
            .lock()
            .expect("frontend registration gate");
        if !*open {
            return Err(Closed);
        }
        let generation = ReplacementQualificationScope::GENERATION;
        let scope = self
            .ledger
            .open_scope_next(None, generation)
            .map_err(Runtime)?;
        let root = match scope.reserve_next(
            generation,
            ReservationResources::work_only(),
            WorkKind::Settlement,
            WorkAdmission::External,
        ) {
            Ok(root) => root,
            Err(error) => {
                scope
                    .close_external()
                    .expect("unpublished qualification scope");
                self.ledger
                    .remove_scope(scope.id())
                    .expect("empty unpublished qualification scope");
                return Err(Runtime(error));
            }
        };
        let (mut parent, payer) = root
            .bind(WorkKind::Settlement)
            .expect("original qualification parent");
        parent
            .start()
            .expect("original qualification registration is open");
        Ok(ReplacementQualificationScope { parent, payer })
    }

    pub(crate) fn finish_replacement_qualification(
        &self,
        mut original: ReplacementQualificationScope,
    ) -> Result<(), RuntimeError> {
        assert!(original.parent.scope().belongs_to_ledger(&self.ledger));
        match original.retire(&self.ledger) {
            Ok(()) => {
                self.host.changed.notify_waiters();
                Ok(())
            }
            Err(error) => {
                self.retain_replacement_failure(original, error);
                Err(error)
            }
        }
    }

    pub(crate) fn retain_replacement_failure(
        &self,
        original: ReplacementQualificationScope,
        failure: impl Any + Send,
    ) {
        let scope = original.parent.scope();
        assert!(scope.belongs_to_ledger(&self.ledger));
        let _ = scope.close_external();
        original.parent.mark_unresolved();
        let id = scope.id();
        let retained = UnresolvedReplacementQualification {
            _original: original,
            _failure: Box::new(failure),
        };
        assert!(
            self.unresolved_replacements
                .lock()
                .expect("replacement qualification custody")
                .insert(id, retained)
                .is_none()
        );
    }

    pub(crate) fn begin_attempt_registration(
        &self,
    ) -> Result<AttemptRegistrationGuard<'_>, AttemptRegistrationClosed> {
        let open = self
            .registration_open
            .lock()
            .expect("frontend registration gate");
        if !*open {
            return Err(AttemptRegistrationClosed);
        }
        Ok(AttemptRegistrationGuard {
            owner: self,
            _open: open,
        })
    }
}

type AttemptRegistrationRefusal = (
    HostRegistrationRefusal,
    AttemptStartup,
    AttemptOwnerControls,
    PendingFailureBridge,
    Arc<TerminalOutbox>,
    MembershipErrors,
    AttemptHostFunding,
);

impl AttemptRegistrationGuard<'_> {
    pub(crate) fn register_binding(
        &self,
        binding: HostBinding,
        funding: &mut Option<AttemptHostFunding>,
    ) -> Result<HostExit, HostRegistrationRefusal> {
        self.owner.host.register_with_funding(binding, funding)
    }

    /// Install the real host route before that host can post Bootstrap.
    /// Refusal returns the Source and its still-unpublished original position.
    pub(crate) fn register_attempt(
        &self,
        prepared: PreparedAttemptHost,
        funding: AttemptHostFunding,
    ) -> Result<
        (
            AttemptStartup,
            AttemptOwnerControls,
            RegisteredAttemptTerminal,
        ),
        AttemptRegistrationRefusal,
    > {
        let PreparedAttemptHost {
            mut binding,
            startup,
            controls,
            membership,
            failure,
            terminal,
        } = prepared;
        assert!(binding.membership.is_none());
        binding.membership = Some(AttemptMembershipSource::new(
            self.owner.members.clone(),
            membership.target,
        ));
        let mut funding = Some(funding);
        match self.register_binding(binding, &mut funding) {
            Ok(exit) => Ok((
                startup,
                controls,
                RegisteredAttemptTerminal::new(
                    terminal,
                    failure.bind(exit, self.owner.backend_processes.clone()),
                    Some(membership.errors),
                ),
            )),
            Err(refusal) => Err((
                refusal,
                startup,
                controls,
                failure,
                terminal,
                membership.errors,
                funding.expect("registration refusal returns the original funding"),
            )),
        }
    }
}

impl FrontendControlRuntime {
    pub(crate) fn start(
        config: FrontendControlRuntimeConfig,
        data: &FrontendDataRuntime,
        backend_processes: Arc<dyn BackendProcessObservationPort>,
    ) -> Result<Self, FrontendControlStartError> {
        let layout = data.transport_capacity().clone();
        let clock: Arc<dyn Clock> = Arc::new(FrontendClock(config.clock));
        let observation = RuntimeObservation::new(Some(clock.clone()));
        let budget = ObservationBudget::for_task_attempt();
        let observation_windows = budget
            .byte_windows(config.workload_scope_limit)
            .map_err(FrontendControlStartError::Ledger)?;
        let observation_backing =
            ObservationBackingQuote::process_limit(budget, config.workload_scope_limit)
                .map_err(FrontendControlStartError::Observation)?;
        let limits = layout
            .process_limits()
            .checked_add(&observation_windows)
            .and_then(|limits| limits.checked_add(&observation_backing))
            .map_err(|error| FrontendControlStartError::Ledger(error.into()))?;
        let ledger = RuntimeLedger::with_keyed_families_and_observation(
            limits,
            layout.keyed_families(),
            [],
            config.history.clone(),
            observation.clone(),
        )
        .map_err(FrontendControlStartError::Ledger)?;
        let runtime = ActorRuntime::with_observation(config.history, observation.clone());
        let host_observation = Arc::new(FrontendHostObservations::new(clock.clone()));
        let host = Arc::new(HostRegistry::new(
            runtime.clone(),
            ledger.clone(),
            host_observation.clone(),
        ));
        let members = Arc::new(MembershipRegistry::new());
        let dispatch = host.clone();
        let pollers = PollerPool::start_with_host(
            runtime.clone(),
            clock.clone(),
            config.pollers.get(),
            move |now, turn| dispatch.drive(now, turn),
        )
        .map_err(FrontendControlStartError::Pollers)?;
        // The service's actual Join is stored directly in this same owner;
        // creating it adds no query Work that could block ledger drain.
        let member_observer =
            MembershipObserver::start(data, backend_processes.clone(), members.clone());
        let attempts = Arc::new(FrontendAttemptRuntime {
            registration_open: Mutex::new(true),
            runtime,
            ledger,
            layout,
            host,
            members,
            backend_processes,
            clock,
            observation,
            host_observation,
            unresolved_replacements: Mutex::new(BTreeMap::new()),
        });
        Ok(Self {
            attempts,
            member_observer,
            pollers: Some(pollers),
            membership_joined: false,
            shutdown_started: false,
        })
    }

    pub(crate) fn attempts(&self) -> Arc<FrontendAttemptRuntime> {
        self.attempts.clone()
    }

    /// Fence new attempts while existing owners can still publish member
    /// evidence and finish their original workload convergence.
    pub(crate) fn close_registration(&self) {
        *self
            .attempts
            .registration_open
            .lock()
            .expect("frontend registration gate") = false;
    }
}

impl FrontendAttemptRuntime {
    pub(crate) fn prepare_attempt_process_source(
        &self,
        registration: &AttemptActorRegistration,
        parent: &novarocks_control_runtime::obligation::WorkObligation,
        payer: &novarocks_control_runtime::obligation::ReservationObligation,
        observer: novarocks_control_runtime_core::identity::ConsumerId,
    ) -> Result<AttemptCallSource, AttemptRegistrationError> {
        registration.prepare_process_source(
            &self.runtime,
            parent,
            payer,
            observer,
            self.backend_processes.clone(),
            self.clock.clone(),
        )
    }

    pub(crate) fn actor_runtime(&self) -> &ActorRuntime<FrontendActor> {
        &self.runtime
    }
    pub(crate) fn ledger(&self) -> &RuntimeLedger {
        &self.ledger
    }
    pub(crate) fn transport_capacity(&self) -> &NativeTransportCapacity {
        &self.layout
    }
    pub(crate) fn metrics(&self, registry: &Registry) -> Result<FrontendRuntimeMetrics, String> {
        FrontendRuntimeMetrics::new(
            registry,
            self.observation.clone(),
            self.host_observation.clone(),
            &self.host,
            self.layout.clone(),
        )
    }
    fn drained(&self) -> bool {
        self.host.is_empty()
            && self.members.is_empty()
            && self.runtime.is_empty()
            && self.ledger.is_empty()
    }
}

impl FrontendControlRuntime {
    fn begin_shutdown(&mut self) {
        if self.shutdown_started {
            return;
        }
        self.close_registration();
        self.attempts.members.close_delivery();
        self.attempts.host.close_registration();
        self.attempts.runtime.close_admission();
        self.attempts.ledger.close_admission();
        self.shutdown_started = true;
    }

    pub(crate) fn membership_has_exited(&self) -> bool {
        !self.shutdown_started && self.member_observer.has_exited()
    }

    /// The Server has exhausted graceful shutdown and committed to process
    /// exit. Preserve the original owners if their actual exit is unresolved.
    pub(crate) fn retain_until_process_exit(mut self) {
        self.begin_shutdown();
        self.member_observer.request_stop();
        std::mem::forget(self);
    }

    pub(crate) fn shutdown_snapshot(&self) -> FrontendShutdownSnapshot {
        FrontendShutdownSnapshot {
            actors_drained: self.attempts.runtime.is_empty(),
            ledger_drained: self.attempts.ledger.is_empty(),
            host_routes_drained: self.attempts.host.is_empty(),
            membership_routes_drained: self.attempts.members.is_empty(),
            pollers_joined: self.pollers.is_none(),
            membership_joined: self.membership_joined,
        }
    }
    /// A timeout stops waiting and retains this same owner's Sources, scopes,
    /// pollers and original membership Join. It never fabricates physical exit.
    pub(crate) async fn shutdown_until(
        &mut self,
        deadline: Instant,
    ) -> Result<(), FrontendShutdownError> {
        self.begin_shutdown();
        loop {
            let changed = self.attempts.host.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.attempts.drained() {
                break;
            }
            tokio::select! {
                _=changed.as_mut()=>{},
                _=tokio::time::sleep_until(tokio::time::Instant::from_std(deadline))=>return Err(FrontendShutdownError::Pending(self.shutdown_snapshot())),
            }
        }
        if Instant::now() >= deadline {
            return Err(FrontendShutdownError::Pending(self.shutdown_snapshot()));
        }
        if let Some(pollers) = self.pollers.take() {
            match pollers.shutdown() {
                Ok(()) => {}
                Err(original) => {
                    self.pollers = Some(original);
                    return Err(FrontendShutdownError::Pending(self.shutdown_snapshot()));
                }
            }
        }
        match self.member_observer.shutdown_until(deadline).await {
            Ok(()) => {
                self.membership_joined = true;
                Ok(())
            }
            Err(MembershipShutdownError::Pending) => {
                Err(FrontendShutdownError::Pending(self.shutdown_snapshot()))
            }
            Err(error) => {
                self.membership_joined = true;
                Err(FrontendShutdownError::Membership(error))
            }
        }
    }
}

pub(crate) mod transport_withdrawal;
