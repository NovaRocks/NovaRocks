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
use crate::task_execution::host::adapter::{AttemptStartup, PreparedAttemptHost};
use actor::{FrontendActor, FrontendClock};
use host::{HostBinding, HostRegistrationRefusal, HostRegistry};
use membership::{
    MembershipObserver, MembershipRegistry, MembershipShutdownError, MembershipTarget,
};
use metrics::{FrontendHostObservations, FrontendRuntimeMetrics};
use novarocks_control_runtime::{
    actor_turn::{ActorRuntime, Clock, PollerPool},
    history::HistoryCapture,
    observation::RuntimeObservation,
    transaction::{RuntimeError, RuntimeLedger},
};
use novarocks_coordination_contract::MonotonicClock;
use novarocks_query_application::api::BackendProcessObservationPort;
use novarocks_task_codec::TransportBudget;
use prometheus::Registry;
use std::{
    num::NonZeroUsize,
    sync::{Arc, Mutex},
    time::Instant,
};

pub(crate) struct FrontendControlRuntimeConfig {
    /// Exactly actual_data_runtime_threads(), supplied by the real Server.
    pub(crate) pollers: NonZeroUsize,
    pub(crate) clock: Arc<dyn MonotonicClock>,
    pub(crate) transport: TransportBudget,
    pub(crate) history: Option<HistoryCapture>,
}
#[derive(Debug)]
pub(crate) enum FrontendControlStartError {
    Transport(String),
    Ledger(RuntimeError),
    Pollers(std::io::Error),
}
pub(crate) struct RegistrationRefusal {
    pub(crate) host: HostRegistrationRefusal,
    pub(crate) membership: Option<Arc<MembershipTarget>>,
}
#[derive(Clone, Copy, Debug)]
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
/// Reference projections below neither clone joins nor authorize new Work.
pub(crate) struct FrontendControlRuntime {
    registration_open: Mutex<bool>,
    runtime: ActorRuntime<FrontendActor>,
    ledger: RuntimeLedger,
    layout: NativeTransportCapacity,
    host: Arc<HostRegistry>,
    members: Arc<MembershipRegistry>,
    member_observer: MembershipObserver,
    pollers: Option<PollerPool<FrontendActor>>,
    membership_joined: bool,
    observation: RuntimeObservation,
    host_observation: Arc<FrontendHostObservations>,
}
impl FrontendControlRuntime {
    /// Install the real host route before the application can post Bootstrap.
    /// Refusal returns the Source and its still-unpublished original position.
    pub(crate) fn register_attempt(
        &self,
        prepared: PreparedAttemptHost,
    ) -> Result<AttemptStartup, (HostRegistrationRefusal, AttemptStartup)> {
        let PreparedAttemptHost { binding, startup } = prepared;
        match self.register(binding, None) {
            Ok(()) => Ok(startup),
            Err(refusal) => Err((refusal.host, startup)),
        }
    }

    pub(crate) fn start(
        config: FrontendControlRuntimeConfig,
        data: &FrontendDataRuntime,
        backend_processes: Arc<dyn BackendProcessObservationPort>,
    ) -> Result<Self, FrontendControlStartError> {
        let layout = NativeTransportCapacity::from_transport(config.transport)
            .map_err(FrontendControlStartError::Transport)?;
        let clock: Arc<dyn Clock> = Arc::new(FrontendClock(config.clock));
        let observation = RuntimeObservation::new(Some(clock.clone()));
        let ledger = RuntimeLedger::with_keyed_families_and_observation(
            layout.process_limits(),
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
            clock,
            config.pollers.get(),
            move |now, turn| dispatch.drive(now, turn),
        )
        .map_err(FrontendControlStartError::Pollers)?;
        // The service's actual Join is stored directly in this same owner;
        // creating it adds no query Work that could block ledger drain.
        let member_observer = MembershipObserver::start(data, backend_processes, members.clone());
        Ok(Self {
            registration_open: Mutex::new(true),
            runtime,
            ledger,
            layout,
            host,
            members,
            member_observer,
            pollers: Some(pollers),
            membership_joined: false,
            observation,
            host_observation,
        })
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
    pub(crate) fn memberships(&self) -> &Arc<MembershipRegistry> {
        &self.members
    }
    /// The Source and both registrations already belong to the real admitted
    /// attempt. This method indexes their originals; it allocates no Scope or
    /// work credentials. Ordinary refusal returns every caller-owned object.
    pub(crate) fn register(
        &self,
        binding: HostBinding,
        membership: Option<Arc<MembershipTarget>>,
    ) -> Result<(), RegistrationRefusal> {
        let open = self
            .registration_open
            .lock()
            .expect("frontend registration gate");
        if !*open {
            return Err(RegistrationRefusal {
                host: HostRegistrationRefusal {
                    reason: "frontend registration is closed",
                    runtime_error: None,
                    binding,
                },
                membership,
            });
        }
        if membership
            .as_ref()
            .is_some_and(|target| !target.registration.same_registration(&binding.registration))
        {
            return Err(RegistrationRefusal {
                host: HostRegistrationRefusal {
                    reason: "membership Source has another original registration",
                    runtime_error: None,
                    binding,
                },
                membership,
            });
        }
        if let Err(host) = self.host.register(binding) {
            return Err(RegistrationRefusal { host, membership });
        }
        if let Some(target) = membership {
            self.members.insert(target).unwrap_or_else(|_| {
                eprintln!("Frontend original membership routing installation failed");
                std::process::abort()
            });
        }
        Ok(())
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
    pub(crate) fn begin_shutdown(&self) {
        let mut open = self
            .registration_open
            .lock()
            .expect("frontend registration gate");
        if !*open {
            return;
        }
        *open = false;
        // In-flight metadata registration has completed before this lock was
        // acquired. Stop new member deliveries before asking Sources to drain.
        self.members.close_delivery();
        self.host.close_registration();
        self.runtime.close_admission();
        self.ledger.close_admission();
    }
    fn drained(&self) -> bool {
        self.host.is_empty()
            && self.members.is_empty()
            && self.runtime.is_empty()
            && self.ledger.is_empty()
    }
    pub(crate) fn shutdown_snapshot(&self) -> FrontendShutdownSnapshot {
        FrontendShutdownSnapshot {
            actors_drained: self.runtime.is_empty(),
            ledger_drained: self.ledger.is_empty(),
            host_routes_drained: self.host.is_empty(),
            membership_routes_drained: self.members.is_empty(),
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
            let changed = self.host.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.drained() {
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
