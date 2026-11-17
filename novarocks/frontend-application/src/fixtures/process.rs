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

//! Manual progression of the same FE process owner, without production pollers.
use super::*;
use crate::control_runtime::{
    FrontendAttemptRuntime, actor::FrontendClock, membership::MembershipRegistry,
    metrics::FrontendHostObservations,
};
use crate::native::transport_capacity::NativeTransportCapacity;
use crate::task_execution::host::observations::{
    ObservationBudget, backing::ObservationBackingQuote,
};
use novarocks_control_runtime::{history::HistoryCapture, observation::RuntimeObservation};
use novarocks_execution_contract::RuntimeEndpoint;
use novarocks_query_application::api::{
    BackendProcessObservation, BackendProcessObservationPort, BackendTopologyError,
};
use novarocks_task_codec::TransportBudget;
use novarocks_types::BackendProcessId;
use std::{collections::BTreeMap, num::NonZeroUsize};

struct ScriptTopology;
impl BackendProcessObservationPort for ScriptTopology {
    fn observe_process_at_endpoint(
        &self,
        _: BackendProcessId,
        _: &RuntimeEndpoint,
    ) -> Result<BackendProcessObservation, BackendTopologyError> {
        Err(BackendTopologyError::Unavailable {
            message: "Script backend has no transport observation".to_owned(),
        })
    }
}
pub(super) struct Process {
    pub(super) original: Arc<FrontendAttemptRuntime>,
    pub(super) clock: Arc<crate::task_execution::clock::ManualClock>,
    pub(super) history: HistoryCapture,
    pub(super) workload: Option<novarocks_workload_control::WorkloadControl>,
}
impl Process {
    pub(super) fn new() -> Self {
        // Identical original registration/ledger/Host assembly to
        // installed_subscription_tests::manual_process. This first backend has
        // no Native I/O; P12 replaces it behind the same four fixture methods.
        let layout = NativeTransportCapacity::from_transport(TransportBudget::DEFAULT).unwrap();
        let clock = Arc::new(crate::task_execution::clock::ManualClock::new());
        let host_clock: Arc<dyn Clock> = Arc::new(FrontendClock(clock.clone()));
        let history = HistoryCapture::new(1_000_000);
        let observation = RuntimeObservation::new(Some(host_clock.clone()));
        let budget = ObservationBudget::for_task_attempt();
        let limit = NonZeroUsize::new(8).unwrap();
        let limits = layout
            .process_limits()
            .checked_add(&budget.byte_windows(limit).unwrap())
            .unwrap()
            .checked_add(&ObservationBackingQuote::process_limit(budget, limit).unwrap())
            .unwrap();
        let ledger = RuntimeLedger::with_keyed_families_and_observation(
            limits,
            layout.keyed_families(),
            [],
            Some(history.clone()),
            observation.clone(),
        )
        .unwrap();
        let runtime = ActorRuntime::with_observation(Some(history.clone()), observation.clone());
        let host_observation = Arc::new(FrontendHostObservations::new(host_clock.clone()));
        let host = Arc::new(HostRegistry::new(
            runtime.clone(),
            ledger.clone(),
            host_observation.clone(),
        ));
        let workload = novarocks_workload_control::WorkloadControl::try_new(
            novarocks_workload_control::WorkloadConfig::default(),
            novarocks_workload_control::ResourceConfig {
                total_bytes: 128,
                control_bytes: 16,
                per_scope_bytes: 112,
            },
        )
        .unwrap();
        workload.mark_ready().unwrap();
        Self {
            original: Arc::new(FrontendAttemptRuntime {
                registration_open: Mutex::new(true),
                runtime,
                ledger,
                layout,
                host,
                members: Arc::new(MembershipRegistry::new()),
                backend_processes: Arc::new(ScriptTopology),
                clock: host_clock,
                observation,
                host_observation,
                unresolved_replacements: Mutex::new(BTreeMap::new()),
            }),
            clock,
            history,
            workload: Some(workload),
        }
    }
    pub(super) fn now(&self) -> Tick {
        self.original.clock.now()
    }
}

/// A real claimed LP turn is held before its SAME Core handle. No queue item is
/// removed, consumed or replaced by the selector; all other actors keep using
/// the unchanged public poll_one path.
#[derive(Default)]
pub(super) struct TurnGate {
    state: Mutex<(bool, bool)>,
    changed: std::sync::Condvar,
}
impl TurnGate {
    pub(super) fn pause(&self) {
        self.state.lock().unwrap().0 = true;
    }
    pub(super) fn entered(&self) -> bool {
        self.state.lock().unwrap().1
    }
    pub(super) fn release(&self) {
        self.state.lock().unwrap().0 = false;
        self.changed.notify_all();
    }
    fn before_turn(&self) {
        let mut state = self.state.lock().unwrap();
        if state.0 {
            state.1 = true;
            self.changed.notify_all();
            while state.0 {
                state = self.changed.wait(state).unwrap();
            }
            state.1 = false;
        }
    }
}
pub(super) struct PausedLogicalActor {
    pub(super) original: LogicalActor,
    pub(super) gate: Arc<TurnGate>,
    pub(super) panic: Arc<AtomicBool>,
}
impl crate::control_runtime::actor::TestActor for PausedLogicalActor {
    fn handle(
        &mut self,
        now: Tick,
        input: novarocks_control_runtime_core::mailbox::InputEnvelope,
        mut value: FrontendInput,
        step: &mut novarocks_control_runtime::actor::ActorStep<'_, FrontendEffect>,
    ) {
        self.gate.before_turn();
        if self.panic.swap(false, Ordering::AcqRel) {
            value = FrontendInput::Test(u64::MAX);
        }
        self.original.handle(now, input, value, step);
    }
    fn next_deadline(&self) -> Option<Tick> {
        self.original.next_deadline()
    }
}

// Test-only observation/cleanup of this SAME original process. No raw host or
// membership authority is exposed to component callers.
impl FrontendAttemptRuntime {
    pub(crate) fn close_original_host_registration_for_fixture(&self) {
        self.host.close_registration();
    }
    pub(crate) fn original_registries_empty_for_fixture(&self) -> bool {
        self.host.is_empty()
            && self.runtime.is_empty()
            && self.ledger.is_empty()
            && self.members.is_empty()
    }
}
