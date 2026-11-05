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

//! Test-only transport authors demand grants and custody observations; it never
//! authors Consumed. Blocked calls retain the real sender activity until exit.

use super::*;
use crate::runtime::execution_runtime::ExecutionRuntime;
use crate::runtime::task::io::exchange_unit::*;
use novarocks_execution_contract::{ExchangeStreamIdentity, TaskFailurePhase, TaskIdentity};
use novarocks_failure_contract::FailureFact;
use std::collections::BTreeMap;
use tokio_util::sync::CancellationToken;

/// Each test owns this exact Task, runtime and scripted transport. No runner
/// isolation or thread reuse can substitute another test's terminal state.
pub(super) struct Fixture {
    runtime: Arc<ExecutionRuntime>,
    error: Arc<RuntimeErrorState>,
    state: RuntimeState,
    transport: Arc<UnitTransport>,
}
impl Fixture {
    pub(super) fn new() -> Self {
        let owner = crate::runtime::task_control::test_support::Owner::new();
        Self::with_error(Arc::new(RuntimeErrorState::for_task(Arc::new(
            owner.view(TaskFailurePhase::Execution),
        ))))
    }
    pub(super) fn with_error(error: Arc<RuntimeErrorState>) -> Self {
        let runtime = crate::runtime::execution_runtime::test_execution_runtime();
        Self::with_error_and_runtime(error, runtime)
    }
    pub(super) fn with_runtime(
        runtime: Arc<ExecutionRuntime>,
    ) -> (Self, Arc<crate::runtime::task_control::test_support::Owner>) {
        let owner = crate::runtime::task_control::test_support::Owner::new();
        let error = Arc::new(RuntimeErrorState::for_task(Arc::new(
            owner.view(TaskFailurePhase::Execution),
        )));
        (Self::with_error_and_runtime(error, runtime), owner)
    }
    fn with_error_and_runtime(
        error: Arc<RuntimeErrorState>,
        runtime: Arc<ExecutionRuntime>,
    ) -> Self {
        let source = error.task_identity().expect("original Task owner");
        let state = RuntimeState::new(
            None,
            None,
            Some(source.query_execution_id().query_id()),
            Some(UniqueId::new(1, 2)),
            Some(0),
            None,
            Some(runtime.clone()),
            error.clone(),
        );
        Self {
            runtime,
            error,
            state,
            transport: Arc::new(UnitTransport::default()),
        }
    }
    pub(super) fn state(&self) -> RuntimeState {
        self.state.clone()
    }
    pub(super) fn state_with_backend(&self, backend: i32) -> RuntimeState {
        let source = self.error.task_identity().unwrap();
        RuntimeState::new(
            None,
            None,
            Some(source.query_execution_id().query_id()),
            Some(UniqueId::new(1, 2)),
            Some(backend),
            self.state.mem_tracker(),
            Some(self.runtime.clone()),
            self.error.clone(),
        )
    }
    pub(super) fn transport(&self) -> Arc<UnitTransport> {
        self.transport.clone()
    }
    pub(super) fn destination_task(&self, ordinal: u32) -> TaskIdentity {
        let source = self.error.task_identity().unwrap();
        TaskIdentity::new(
            source.query_execution_id(),
            novarocks_types::identity::StageId::new(2).unwrap(),
            novarocks_types::identity::TaskId::new(ordinal).unwrap(),
            source.backend_process_id(),
        )
    }
}
pub(super) fn bind_test_operator(fixture: &Fixture, op: &mut DataStreamSinkOperator) {
    let error = op.error_state.clone().expect("original Task control");
    assert!(
        Arc::ptr_eq(&error, &fixture.error),
        "sink and context must retain the same original Task"
    );
    let source = fixture.error.task_identity().unwrap();
    for destination in &op.input.destinations {
        let target = destination
            .task_identity()
            .expect("complete frozen destination Task");
        assert_eq!(target.query_execution_id(), source.query_execution_id());
    }
    if op.edge_gates.is_none() {
        gate_destinations(op, op.input.destinations.clone());
        let edges = op
            .edge_gates
            .as_ref()
            .unwrap()
            .edges()
            .map(|(edge, _)| edge)
            .collect::<Vec<_>>();
        if !edges.is_empty() {
            open_edges(op, &edges);
        }
    }
    let state = RuntimeState::new(
        None,
        None,
        Some(source.query_execution_id().query_id()),
        Some(op.task_key),
        Some(op.be_number),
        fixture.state.mem_tracker(),
        Some(fixture.runtime.clone()),
        error,
    );
    op.bind_runtime_state(&state)
        .expect("real runtime, Task, exact roster and gate binding");
}
pub(super) fn share_factory_binding(
    first: &DataStreamSinkOperator,
    second: &mut DataStreamSinkOperator,
) {
    second.send_group = first.send_group.clone();
    second.send_observable = first.send_observable.clone();
    second.transmitter = first.transmitter.clone();
    second.edge_gates = first.edge_gates.clone();
    second.error_state = first.error_state.clone();
    second.finish_state = first.finish_state.clone();
    second.input.destinations = first.input.destinations.clone();
}

type Retry = Box<dyn FnOnce() + Send + 'static>;
#[derive(Default)]
struct TransportState {
    frames: Vec<ExchangeUnitFrame>,
    facts: BTreeMap<ExchangeStreamIdentity, ExchangeStreamFacts>,
    watches: BTreeMap<ExchangeStreamIdentity, ExchangeGrantWake>,
    demands: Vec<ExchangeDemandEntry>,
    retries: VecDeque<Retry>,
    refusal: Option<FailureFact>,
}
pub(super) struct UnitTransport {
    state: Mutex<TransportState>,
    grant: AtomicBool,
}
impl Default for UnitTransport {
    fn default() -> Self {
        Self {
            state: Mutex::default(),
            grant: AtomicBool::new(true),
        }
    }
}
impl UnitTransport {
    pub(super) fn frames(&self) -> Vec<ExchangeUnitFrame> {
        self.state.lock().unwrap().frames.clone()
    }
    pub(super) fn take_frames(&self) -> Vec<ExchangeUnitFrame> {
        std::mem::take(&mut self.state.lock().unwrap().frames)
    }
    pub(super) fn call_count(&self) -> usize {
        self.state.lock().unwrap().frames.len()
    }
    pub(super) fn fail_delivery(&self, fact: FailureFact) {
        self.state.lock().unwrap().refusal = Some(fact);
    }
    pub(super) fn withhold_grants(&self) {
        self.grant.store(false, Ordering::Release);
    }
    pub(super) fn grant_pending(&self) {
        self.grant.store(true, Ordering::Release);
        let wakes = {
            let mut state = self.state.lock().unwrap();
            for entry in state.demands.clone() {
                if let ExchangeDemandOperation::Register(demand) = entry.operation {
                    let facts = state.facts.entry(entry.stream).or_default();
                    facts.granted_through = facts.granted_through.max(demand.sequence.get());
                }
            }
            state
                .watches
                .iter()
                .map(|(stream, wake)| {
                    (
                        wake.clone(),
                        state.facts.get(stream).copied().unwrap_or_default(),
                    )
                })
                .collect::<Vec<_>>()
        };
        for (wake, facts) in wakes {
            wake(ExchangeGrantNotice::Facts(facts));
        }
    }
    pub(super) fn demands(&self) -> Vec<ExchangeDemandEntry> {
        self.state.lock().unwrap().demands.clone()
    }
    fn complete(
        &self,
        frame: ExchangeUnitFrame,
        activity: ExchangeCallActivity,
        done: ExchangeDeliveryCallback,
    ) {
        let outcome = {
            let mut state = self.state.lock().unwrap();
            state.frames.push(frame.clone());
            if let Some(fact) = state.refusal {
                ExchangeCallOutcome::Refused(fact)
            } else {
                let facts = state.facts.entry(frame.receiver.stream).or_default();
                match frame.receiver.item {
                    ExchangeUnitItem::Unit { demand, .. } => {
                        assert_eq!(demand.wire_bytes, frame.receiver.payload.len() as u64);
                        assert!(facts.granted_through >= demand.sequence.get());
                    }
                    ExchangeUnitItem::End(_) => assert!(frame.receiver.payload.is_empty()),
                }
                facts.custody_through = facts
                    .custody_through
                    .max(frame.receiver.item.sequence().get());
                if self.grant.load(Ordering::Acquire)
                    && let Some(demand) = frame.receiver.next_demand
                {
                    facts.granted_through = facts.granted_through.max(demand.sequence.get());
                }
                ExchangeCallOutcome::Observed(ExchangeUnitReceipt {
                    verdict: ExchangeAdmissionVerdict::Applied,
                    facts: *facts,
                })
            }
        };
        done(outcome);
        drop(activity);
    }
}
impl ExchangeFrameTransmitter for UnitTransport {
    fn deliver_unit(
        &self,
        frame: ExchangeUnitFrame,
        _stop: CancellationToken,
        activity: ExchangeCallActivity,
        done: ExchangeDeliveryCallback,
    ) {
        self.complete(frame, activity, done);
    }
    fn register_unit_demands(
        &self,
        _destination: RuntimeEndpoint,
        entries: Vec<ExchangeDemandEntry>,
        _stop: CancellationToken,
        done: ExchangeDemandCallback,
    ) {
        let facts = {
            let mut state = self.state.lock().unwrap();
            state.demands.extend(entries.iter().copied());
            entries
                .into_iter()
                .map(|entry| {
                    let facts = state.facts.entry(entry.stream).or_default();
                    match entry.operation {
                        ExchangeDemandOperation::Register(demand)
                            if self.grant.load(Ordering::Acquire) =>
                        {
                            facts.granted_through = facts.granted_through.max(demand.sequence.get())
                        }
                        ExchangeDemandOperation::Register(_) => {}
                        ExchangeDemandOperation::StopSender => facts.sender_stopped = true,
                    }
                    (entry.stream, Ok(*facts))
                })
                .collect()
        };
        done(ExchangeDemandCallOutcome::Observed(facts));
    }
    fn watch_unit_facts(
        &self,
        _destination: RuntimeEndpoint,
        stream: ExchangeStreamIdentity,
        wake: ExchangeGrantWake,
    ) -> Result<(), FailureFact> {
        self.state.lock().unwrap().watches.insert(stream, wake);
        Ok(())
    }
    fn forget_unit_facts(&self, stream: ExchangeStreamIdentity) {
        self.state.lock().unwrap().watches.remove(&stream);
    }
    fn schedule_unit_retry(&self, _delay: Duration, task: Retry) -> Result<(), FailureFact> {
        self.state.lock().unwrap().retries.push_back(task);
        Ok(())
    }
}

pub(super) struct BlockingEosTransmitter {
    pub(super) transport: Arc<UnitTransport>,
    pub(super) entered: mpsc::Sender<()>,
    pub(super) release: Arc<Mutex<mpsc::Receiver<()>>>,
    pub(super) calls: AtomicUsize,
}
impl ExchangeFrameTransmitter for BlockingEosTransmitter {
    fn deliver_unit(
        &self,
        frame: ExchangeUnitFrame,
        _stop: CancellationToken,
        activity: ExchangeCallActivity,
        done: ExchangeDeliveryCallback,
    ) {
        assert!(
            matches!(frame.receiver.item, ExchangeUnitItem::End(_)),
            "empty sink sends only End"
        );
        self.calls.fetch_add(1, Ordering::AcqRel);
        let release = self.release.clone();
        let transport = self.transport.clone();
        let entered = self.entered.clone();
        std::thread::spawn(move || {
            entered.send(()).expect("observe actual active call");
            let _ = release.lock().unwrap().recv();
            transport.complete(frame, activity, done);
        });
    }
    fn register_unit_demands(
        &self,
        destination: RuntimeEndpoint,
        entries: Vec<ExchangeDemandEntry>,
        stop: CancellationToken,
        done: ExchangeDemandCallback,
    ) {
        self.transport
            .register_unit_demands(destination, entries, stop, done);
    }
    fn watch_unit_facts(
        &self,
        destination: RuntimeEndpoint,
        stream: ExchangeStreamIdentity,
        wake: ExchangeGrantWake,
    ) -> Result<(), FailureFact> {
        self.transport.watch_unit_facts(destination, stream, wake)
    }
    fn forget_unit_facts(&self, stream: ExchangeStreamIdentity) {
        self.transport.forget_unit_facts(stream);
    }
    fn schedule_unit_retry(&self, delay: Duration, task: Retry) -> Result<(), FailureFact> {
        self.transport.schedule_unit_retry(delay, task)
    }
}
