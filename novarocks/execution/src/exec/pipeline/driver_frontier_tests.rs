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

//! Actual driver scheduling over reported steps, independent of row kernels.
use super::*;
use crate::exec::pipeline::operator::OperatorLifecycle;
use crate::exec::pipeline::{
    input::AcceptedInput,
    operator_factory::OperatorFactory,
    step::{StepResult, WaitReason},
};
use std::sync::atomic::AtomicUsize;

#[derive(Clone, Copy)]
enum Role {
    Source,
    Receiver,
    Pass,
    Gate,
}
struct Probe {
    role: Role,
    event: Arc<Observable>,
    ready: Arc<AtomicBool>,
    next: NextStep,
    input: Option<AcceptedInput>,
    page: Option<Chunk>,
    calls: Arc<AtomicUsize>,
    accepts: Arc<AtomicUsize>,
    deadline: Option<DriverBlockDeadline>,
}
impl Probe {
    fn new(role: Role, event: Arc<Observable>) -> Self {
        Self {
            role,
            event,
            ready: Arc::new(AtomicBool::new(false)),
            next: if matches!(role, Role::Source) {
                NextStep::Runnable
            } else {
                NextStep::NeedInput
            },
            input: None,
            page: matches!(role, Role::Source).then(Chunk::default),
            calls: Arc::new(AtomicUsize::new(0)),
            accepts: Arc::new(AtomicUsize::new(0)),
            deadline: None,
        }
    }
    fn wait(&self) -> WaitFact {
        WaitFact {
            observable: self.event.clone(),
            observed_generation: self.event.generation(),
            reason: if matches!(self.role, Role::Gate) {
                WaitReason::Output
            } else {
                WaitReason::Input
            },
            deadline: self.deadline,
        }
    }
    fn already_waiting(mut self) -> Self {
        self.next = NextStep::Wait(self.wait());
        self.page = None;
        self
    }
}
impl OperatorLifecycle for Probe {
    fn name(&self) -> &str {
        "REPORTED_FRONTIER_PROBE"
    }
}
impl StepOperator for Probe {
    fn initial_step(&self) -> NextStep {
        self.next.clone()
    }
    fn try_push(&mut self, _: &mut DriverStep<'_>, input: RawInput) -> InputResult {
        assert!(!matches!(self.role, Role::Source));
        if self.input.is_some() || matches!(self.next, NextStep::Wait(_)) {
            return InputResult::Deferred {
                input,
                next: AdmissionNext::ResumeExisting,
            };
        }
        if matches!(self.role, Role::Gate) && !self.ready.load(Ordering::Acquire) {
            return InputResult::Deferred {
                input,
                next: AdmissionNext::Wait(self.wait()),
            };
        }
        self.accepts.fetch_add(1, Ordering::AcqRel);
        let input = input.accept(None);
        match self.role {
            Role::Receiver if !self.ready.load(Ordering::Acquire) => {
                self.input = Some(input);
                self.next = NextStep::Wait(self.wait());
                InputResult::Accepted(Ok(StepResult::new(None, self.next.clone(), 0)))
            }
            Role::Pass => {
                let output = input.prepare_original().into_uncovered_chunk().unwrap();
                InputResult::Accepted(Ok(StepResult::new(Some(output), NextStep::NeedInput, 0)))
            }
            _ => {
                drop(input);
                InputResult::Accepted(Ok(StepResult::new(None, NextStep::NeedInput, 0)))
            }
        }
    }
    fn drive(&mut self, _: &mut DriverStep<'_>) -> ExecutionResult<StepResult> {
        self.calls.fetch_add(1, Ordering::AcqRel);
        match self.role {
            Role::Source => {
                self.next = NextStep::Wait(self.wait());
                Ok(StepResult::new(self.page.take(), self.next.clone(), 0))
            }
            Role::Receiver => {
                assert!(
                    self.ready.load(Ordering::Acquire),
                    "accepted work ran before its event"
                );
                drop(self.input.take());
                self.next = NextStep::NeedInput;
                Ok(StepResult::new(None, self.next.clone(), 0))
            }
            _ => panic!("input-only probe has no continuation"),
        }
    }
    fn finish(&mut self, _: &mut DriverStep<'_>) -> ExecutionResult<()> {
        self.next = NextStep::Finished;
        Ok(())
    }
}
fn driver(operators: Vec<Box<dyn Operator>>) -> PipelineDriver {
    PipelineDriver::new(
        1,
        operators,
        None,
        vec![],
        Arc::new(RuntimeState::default()),
        None,
    )
}
fn sink() -> Box<dyn Operator> {
    crate::exec::operators::NoopSinkFactory::new().create(1, 0)
}
fn blocked(
    driver: &mut PipelineDriver,
    reason: BlockedReason,
) -> (Arc<Observable>, u64, Option<DriverBlockDeadline>) {
    assert_eq!(
        driver.process(Duration::from_secs(1)),
        DriverState::Blocked(reason)
    );
    driver.blocked_observable_snapshot().unwrap()
}

#[test]
fn accepted_work_wait_survives_an_empty_source_and_another_raw_delivery() {
    let source_event = Arc::new(Observable::new());
    let work_event = Arc::new(Observable::new());
    let source = Probe::new(Role::Source, source_event.clone());
    let source_calls = source.calls.clone();
    let receiver = Probe::new(Role::Receiver, work_event.clone());
    let ready = receiver.ready.clone();
    let calls = receiver.calls.clone();
    let accepts = receiver.accepts.clone();
    let mut driver = driver(vec![Box::new(source), Box::new(receiver), sink()]);
    let (event, observed, _) = blocked(&mut driver, BlockedReason::InputEmpty);
    assert_eq!(accepts.load(Ordering::Acquire), 1);
    assert_eq!(calls.load(Ordering::Acquire), 0);
    // A second original page may already be buffered while the first is owned
    // by its receiver. ResumeExisting must not discard the receiver's WaitFact.
    driver.edge_chunks[0] = Some(RawInput::new(Chunk::default(), None));
    source_event.notify_observers();
    assert_eq!(
        event.generation(),
        observed,
        "an ineligible source is not a wake dependency"
    );
    driver.set_ready();
    let (same, observed, _) = blocked(&mut driver, BlockedReason::InputEmpty);
    assert!(Arc::ptr_eq(&event, &same));
    assert!(driver.edge_chunks[0].is_some());
    assert_eq!(source_calls.load(Ordering::Acquire), 1);
    assert_eq!(accepts.load(Ordering::Acquire), 1);
    assert_eq!(calls.load(Ordering::Acquire), 0);
    ready.store(true, Ordering::Release);
    work_event.notify_observers();
    assert_ne!(same.generation(), observed);
    driver.set_ready();
    let (endpoint, observed, _) = blocked(&mut driver, BlockedReason::InputEmpty);
    assert!(Arc::ptr_eq(&event, &endpoint));
    work_event.notify_observers();
    assert_eq!(
        endpoint.generation(),
        observed,
        "completed work releases its wait subscription"
    );
    source_event.notify_observers();
    assert_ne!(endpoint.generation(), observed);
    assert!(driver.edge_chunks[0].is_none());
    assert_eq!(accepts.load(Ordering::Acquire), 2);
    assert_eq!(calls.load(Ordering::Acquire), 1);
}

#[test]
fn independent_waits_share_a_reusable_cover_and_keep_the_earliest_deadline() {
    let source_event = Arc::new(Observable::try_bounded(1).unwrap());
    let work_event = Arc::new(Observable::try_bounded(1).unwrap());
    let mut source = Probe::new(Role::Source, source_event.clone()).already_waiting();
    let mut receiver = Probe::new(Role::Receiver, work_event.clone()).already_waiting();
    let later = DriverBlockDeadline::new(Instant::now() + Duration::from_secs(90), 81);
    let earlier = DriverBlockDeadline::new(Instant::now() + Duration::from_secs(60), 82);
    source.deadline = Some(later);
    source.next = NextStep::Wait(source.wait());
    receiver.deadline = Some(earlier);
    receiver.next = NextStep::Wait(receiver.wait());
    let source_calls = source.calls.clone();
    let work_calls = receiver.calls.clone();
    let ready = receiver.ready.clone();
    let pass = Probe::new(Role::Pass, Arc::new(Observable::new()));
    let mut driver = driver(vec![
        Box::new(source),
        Box::new(pass),
        Box::new(receiver),
        sink(),
    ]);
    let (cover, generation, deadline) = blocked(&mut driver, BlockedReason::InputEmpty);
    assert!(!Arc::ptr_eq(&cover, &source_event) && !Arc::ptr_eq(&cover, &work_event));
    assert_eq!(deadline, Some(earlier));
    source_event.notify_observers();
    assert_ne!(cover.generation(), generation);
    driver.set_ready();
    let (same, generation, deadline) = blocked(&mut driver, BlockedReason::InputEmpty);
    assert!(Arc::ptr_eq(&cover, &same));
    assert_eq!(deadline, Some(earlier));
    assert_eq!(source_calls.load(Ordering::Acquire), 1);
    assert_eq!(work_calls.load(Ordering::Acquire), 0);
    ready.store(true, Ordering::Release);
    work_event.notify_observers();
    assert_ne!(same.generation(), generation);
    driver.set_ready();
    let (event, _, deadline) = blocked(&mut driver, BlockedReason::InputEmpty);
    assert!(Arc::ptr_eq(&event, &cover));
    assert_eq!(deadline, Some(later));
    assert_eq!(work_calls.load(Ordering::Acquire), 1);
    let _released = work_event
        .try_subscribe(Arc::new(|| {}))
        .expect("inactive branch releases its only slot");
    drop(driver);
    let _released_source = source_event
        .try_subscribe(Arc::new(|| {}))
        .expect("retirement releases the cover");
}

#[test]
fn a_full_output_edge_waits_for_its_receiver_and_retires_the_old_admission_fact() {
    let source_event = Arc::new(Observable::new());
    let inactive_event = Arc::new(Observable::try_bounded(1).unwrap());
    let capacity_event = Arc::new(Observable::try_bounded(1).unwrap());
    let source = Probe::new(Role::Source, source_event).already_waiting();
    let receiver = Probe::new(Role::Receiver, inactive_event.clone()).already_waiting();
    let inactive_calls = receiver.calls.clone();
    let receiver_ready = receiver.ready.clone();
    let gate = Probe::new(Role::Gate, capacity_event.clone());
    let gate_ready = gate.ready.clone();
    let accepts = gate.accepts.clone();
    let mut driver = driver(vec![Box::new(source), Box::new(receiver), Box::new(gate)]);
    driver.edge_chunks[1] = Some(RawInput::new(Chunk::default(), None));
    let (event, observed, _) = blocked(&mut driver, BlockedReason::OutputFull);
    assert_eq!(inactive_calls.load(Ordering::Acquire), 0);
    receiver_ready.store(true, Ordering::Release);
    inactive_event.notify_observers();
    assert_eq!(
        event.generation(),
        observed,
        "a full output edge excludes further producer work"
    );
    driver.set_ready();
    let (same, observed, _) = blocked(&mut driver, BlockedReason::OutputFull);
    assert!(Arc::ptr_eq(&same, &event));
    assert_eq!(inactive_calls.load(Ordering::Acquire), 0);
    gate_ready.store(true, Ordering::Release);
    capacity_event.notify_observers();
    assert_ne!(same.generation(), observed);
    driver.set_ready();
    blocked(&mut driver, BlockedReason::InputEmpty);
    assert_eq!(accepts.load(Ordering::Acquire), 1);
    assert_eq!(inactive_calls.load(Ordering::Acquire), 1);
    assert!(driver.operators[2].admission_next().is_none());
    assert!(driver.edge_chunks[1].is_none());
}
