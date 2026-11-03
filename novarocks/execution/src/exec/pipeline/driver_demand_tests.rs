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

//! Driver-owned edge custody and normal demand withdrawal. The production
//! Limit and common receiving shape consume literal rows. A source's real
//! asynchronous exit remains independently pending, including after its last
//! demanded output drains. These fixtures use no external services or controls.

use super::*;
use crate::exec::chunk::{ChunkSchema, ChunkSlotSchema};
use crate::exec::operators::LimitProcessorFactory;
use crate::exec::pipeline::input::PreparedInput;
use crate::exec::pipeline::operator::{
    ConstructionFailure, ExitFailure, ExitObservation, InputEncodingPolicy, OperatorLifecycle,
};
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::exec::pipeline::shape::{
    ControlStep, SinkKernel, SinkStep, Source, SourceKernel, Transform,
};
use crate::exec::pipeline::step::{StepResult, WaitReason};
use crate::runtime::profile::{OperatorProfiles, RuntimeProfile};
use arrow::array::{Array, ArrayRef, DictionaryArray, Int32Array};
use arrow::datatypes::{DataType, Field, Int32Type, Schema};
use arrow::record_batch::RecordBatch;
use novarocks_type_contract::{
    TypedValueBinding, ValueKind, ValueLayout, ValueTypeRef, ValueTypeStorageLifetime,
};
use novarocks_types::SlotId;
use std::collections::VecDeque;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

fn integer(value: i32) -> (Chunk, Weak<dyn Array>) {
    let array = Arc::new(Int32Array::from(vec![value]));
    let storage = ValueTypeStorageLifetime::pure();
    let binding = TypedValueBinding::try_new(
        ValueTypeRef::try_new(ValueKind::Scalar(DataType::Int32), false, storage.clone()).unwrap(),
        ValueLayout::try_new(&DataType::Int32, storage.clone()).unwrap(),
    )
    .unwrap();
    let slot = ChunkSlotSchema::try_new_typed(
        SlotId::new(1),
        Arc::new(Field::new("value", DataType::Int32, false)),
        binding,
        None,
        storage.clone(),
    )
    .unwrap();
    let schema = Arc::new(ChunkSchema::try_new_typed(vec![slot], storage).unwrap());
    let chunk = Chunk::try_new_typed_with_columns_observed::<ConstructionFailure, _>(
        schema,
        vec![array as ArrayRef],
        1,
        |_| Ok(()),
    )
    .unwrap();
    let weak = Arc::downgrade(&chunk.columns()[0]);
    (chunk, weak)
}

fn encoded() -> (Chunk, Weak<dyn Array>) {
    let array = Arc::new(
        [Some("undemanded")]
            .into_iter()
            .collect::<DictionaryArray<Int32Type>>(),
    );
    let schema = Arc::new(
        ChunkSchema::try_new(vec![ChunkSlotSchema::new_with_field(
            SlotId::new(1),
            Field::new("value", DataType::Utf8, true),
            None,
            None,
        )])
        .unwrap(),
    );
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            true,
        )])),
        vec![array as ArrayRef],
    )
    .unwrap();
    let chunk = Chunk::try_new_with_chunk_schema(batch, schema).unwrap();
    let weak = Arc::downgrade(&chunk.columns()[0]);
    (chunk, weak)
}

#[derive(Default)]
struct SourceOwner {
    polls: AtomicUsize,
    exits_started: AtomicUsize,
    cancels: AtomicUsize,
    exited: AtomicBool,
    event: Arc<Observable>,
}
struct Pages {
    chunks: VecDeque<Chunk>,
    owner: Arc<SourceOwner>,
    exit_failure: Option<ExecutionFailure>,
}
impl OperatorLifecycle for Pages {
    fn name(&self) -> &str {
        "DEMAND_SOURCE"
    }
    fn begin_exit(&mut self) -> ExecutionResult<()> {
        self.owner.exits_started.fetch_add(1, Ordering::AcqRel);
        self.chunks.clear();
        Ok(())
    }
    fn cancel(&mut self) {
        self.owner.cancels.fetch_add(1, Ordering::AcqRel);
        self.chunks.clear();
    }
    fn observe_exit(&mut self) -> ExitObservation {
        if !self.owner.exited.load(Ordering::Acquire) {
            ExitObservation::pending(FinishWatch::Notify(self.owner.event.clone()))
        } else if let Some(failure) = self.exit_failure.take() {
            ExitObservation::exited().with_failure(ExitFailure::Required(failure))
        } else {
            ExitObservation::exited()
        }
    }
}
impl SourceKernel for Pages {
    fn control_observable(&self) -> Option<Arc<Observable>> {
        Some(self.owner.event.clone())
    }
    fn poll_output(&mut self, ctx: &mut DriverStep<'_>) -> ExecutionResult<StepResult> {
        if !ctx.work().try_consume(1) {
            return Ok(StepResult::new(None, NextStep::Yield, 0));
        }
        self.owner.polls.fetch_add(1, Ordering::AcqRel);
        let output = self.chunks.pop_front();
        let next = if output.is_some() {
            NextStep::Runnable
        } else {
            NextStep::Finished
        };
        Ok(StepResult::new(output, next, 0))
    }
}

struct Collector {
    open: Arc<AtomicBool>,
    event: Arc<Observable>,
    values: Arc<Mutex<Vec<i32>>>,
    finishes: Arc<AtomicUsize>,
}
impl OperatorLifecycle for Collector {
    fn name(&self) -> &str {
        "DEMAND_COLLECTOR"
    }
}
impl InputEncodingPolicy for Collector {}
impl SinkKernel for Collector {
    fn admission(
        &self,
        _: &mut DriverStep<'_>,
        _: &RawInput,
    ) -> ExecutionResult<Option<AdmissionNext>> {
        let observed_generation = self.event.generation();
        Ok((!self.open.load(Ordering::Acquire)).then(|| {
            AdmissionNext::Wait(WaitFact {
                reason: WaitReason::Output,
                observable: self.event.clone(),
                observed_generation,
                deadline: None,
            })
        }))
    }
    fn consume(
        &mut self,
        _: &mut DriverStep<'_>,
        input: PreparedInput,
    ) -> ExecutionResult<SinkStep> {
        let chunk = input.into_uncovered_chunk()?;
        let column = chunk.columns()[0]
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        self.values
            .lock()
            .unwrap()
            .extend(column.values().iter().copied());
        Ok(SinkStep::new(NextStep::NeedInput, chunk.len() as u64))
    }
    fn finish(&mut self, _: &mut DriverStep<'_>) -> ExecutionResult<SinkStep> {
        self.finishes.fetch_add(1, Ordering::AcqRel);
        Ok(SinkStep::new(NextStep::Finished, 0))
    }
}

#[test]
fn limit_retires_undemanded_input_but_drains_its_last_output_before_eos_and_actual_exit() {
    for late_failure in [
        None,
        Some(ExecutionFailure::from(
            novarocks_functions::KernelFailure::ResourceExhausted,
        )),
    ] {
        let state = Arc::new(RuntimeState::default());
        let owner = Arc::new(SourceOwner::default());
        let (first, _) = integer(7);
        let (mut second, second_array) = encoded();
        assert!(
            second_array.upgrade().is_some(),
            "observe the returned raw carrier"
        );
        let raw_tracker = MemTracker::new_root("undemanded raw prefetch");
        second.transfer_to(&raw_tracker);
        assert!(raw_tracker.current() > 0);
        let source = Source::new(Pages {
            chunks: VecDeque::from([first, second]),
            owner: owner.clone(),
            exit_failure: late_failure.clone(),
        });
        let mut limit = LimitProcessorFactory::new(1, Some(1), 0).create(1, 0);
        let profiles = OperatorProfiles::new(RuntimeProfile::new("demand limit"));
        limit.set_profiles(profiles.clone());
        limit.set_construction_factory(crate::exec::pipeline::operator::input_construction::try_runtime_input_construction_factory(
            crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(),
            MemTracker::new_root("demand limit creator"), state.error_state(),
        ).unwrap());
        limit.prepare().unwrap();
        let open = Arc::new(AtomicBool::new(false));
        let event = Arc::new(Observable::new());
        let values = Arc::new(Mutex::new(Vec::new()));
        let finishes = Arc::new(AtomicUsize::new(0));
        let collector = Transform::hydrating(Collector {
            open: open.clone(),
            event: event.clone(),
            values: values.clone(),
            finishes: finishes.clone(),
        });
        let mut driver = PipelineDriver::new(
            1,
            vec![Box::new(source), limit, Box::new(collector)],
            None,
            vec![],
            state,
            None,
        );
        assert_eq!(
            driver.process(Duration::from_secs(1)),
            DriverState::Blocked(BlockedReason::OutputFull)
        );
        assert!(
            second_array.upgrade().is_none(),
            "normal withdrawal releases undemanded original input"
        );
        assert_eq!(raw_tracker.current(), 0);
        assert_eq!(
            profiles.unique.counter_value("DictHydratedColumns"),
            Some(0)
        );
        assert!(
            driver.edge_chunks[1].is_some(),
            "the final demanded output remains on its edge"
        );
        assert!(values.lock().unwrap().is_empty());
        assert_eq!(
            finishes.load(Ordering::Acquire),
            0,
            "EOS cannot overtake that output"
        );
        assert_eq!(owner.exits_started.load(Ordering::Acquire), 1);
        assert_eq!(
            owner.cancels.load(Ordering::Acquire),
            0,
            "normal withdrawal is not Task cancellation"
        );
        open.store(true, Ordering::Release);
        event.notify_observers();
        driver.set_ready();
        assert_eq!(
            driver.process(Duration::from_secs(1)),
            DriverState::PendingFinish
        );
        assert_eq!(*values.lock().unwrap(), vec![7]);
        assert_eq!(finishes.load(Ordering::Acquire), 1);
        owner.exited.store(true, Ordering::Release);
        owner.event.notify_observers();
        let terminal = driver.process(Duration::from_secs(1));
        match &late_failure {
            None => assert_eq!(terminal, DriverState::Finished),
            Some(expected) => match terminal {
                DriverState::Failed(actual) => assert!(actual.same_occurrence(expected)),
                other => panic!("the actual final owner failure precedes success: {other:?}"),
            },
        }
        assert_eq!(
            owner.exits_started.load(Ordering::Acquire),
            1,
            "exit starts once across all driver revisits"
        );
    }
}

struct Withdraw {
    // Demand withdrawal ends data work; the independently observed control
    // owner stays live until it has reported its possible late failure.
    exited: Arc<AtomicBool>,
    closed: Arc<AtomicBool>,
    event: Arc<Observable>,
    retired: bool,
    late_failure: Arc<Mutex<Option<ExecutionFailure>>>,
}
impl OperatorLifecycle for Withdraw {
    fn name(&self) -> &str {
        "DEMAND_WITHDRAWAL"
    }
    fn begin_exit(&mut self) -> ExecutionResult<()> {
        self.retired = true;
        Ok(())
    }
    fn pending_finish(&self) -> Option<crate::exec::pipeline::operator::FinishWatch> {
        (!self.exited.load(Ordering::Acquire))
            .then(|| crate::exec::pipeline::operator::FinishWatch::Notify(self.event.clone()))
    }
}
impl StepOperator for Withdraw {
    fn initial_step(&self) -> NextStep {
        NextStep::NeedInput
    }
    fn control_observable(&self) -> Option<Arc<Observable>> {
        Some(self.event.clone())
    }
    fn poll_control(&mut self, _: &mut DriverStep<'_>) -> ExecutionResult<ControlStep> {
        if let Some(failure) = self.late_failure.lock().unwrap().clone() {
            return Err(failure);
        }
        Ok(if self.retired {
            ControlStep::Runnable
        } else if self.closed.load(Ordering::Acquire) {
            ControlStep::Withdrawn
        } else {
            ControlStep::Active
        })
    }
    fn try_push(&mut self, _: &mut DriverStep<'_>, input: RawInput) -> InputResult {
        let observed_generation = self.event.generation();
        InputResult::Deferred {
            input,
            next: AdmissionNext::Wait(WaitFact {
                reason: WaitReason::Output,
                observable: self.event.clone(),
                observed_generation,
                deadline: None,
            }),
        }
    }
    fn drive(&mut self, _: &mut DriverStep<'_>) -> ExecutionResult<StepResult> {
        panic!("no input was accepted")
    }
    fn finish(&mut self, _: &mut DriverStep<'_>) -> ExecutionResult<()> {
        panic!("withdrawal has no upstream EOS")
    }
}

#[test]
fn intermediate_control_withdrawal_retires_raw_custody_without_spending_a_data_turn() {
    for late_failure in [
        None,
        Some(ExecutionFailure::from(
            novarocks_functions::KernelFailure::ResourceExhausted,
        )),
    ] {
        let owner = Arc::new(SourceOwner::default());
        let (mut chunk, backing) = integer(11);
        let tracker = MemTracker::new_root("raw input awaiting withdrawn receiver");
        chunk.transfer_to(&tracker);
        let source = Source::new(Pages {
            chunks: VecDeque::from([chunk]),
            owner: owner.clone(),
            exit_failure: None,
        });
        let closed = Arc::new(AtomicBool::new(false));
        let event = Arc::new(Observable::new());
        let withdrawal_exited = Arc::new(AtomicBool::new(false));
        let receiver = Withdraw {
            exited: withdrawal_exited.clone(),
            closed: closed.clone(),
            event: event.clone(),
            retired: false,
            late_failure: Arc::new(Mutex::new(None)),
        };
        let control_failure = receiver.late_failure.clone();
        let finishes = Arc::new(AtomicUsize::new(0));
        let collector = Transform::hydrating(Collector {
            open: Arc::new(AtomicBool::new(true)),
            event: Arc::new(Observable::new()),
            values: Arc::new(Mutex::new(Vec::new())),
            finishes: finishes.clone(),
        });
        let mut driver = PipelineDriver::new(
            1,
            vec![Box::new(source), Box::new(receiver), Box::new(collector)],
            None,
            vec![],
            Arc::new(RuntimeState::default()),
            None,
        );
        assert_eq!(
            driver.process(Duration::from_secs(1)),
            DriverState::Blocked(BlockedReason::OutputFull)
        );
        assert!(backing.upgrade().is_some());
        assert!(tracker.current() > 0);
        closed.store(true, Ordering::Release);
        event.notify_observers();
        driver.set_ready();
        assert_eq!(driver.process(Duration::ZERO), DriverState::Ready);
        assert!(
            backing.upgrade().is_none(),
            "a control withdrawal retires edge custody before another data turn"
        );
        assert_eq!(tracker.current(), 0);
        assert_eq!(owner.polls.load(Ordering::Acquire), 1);
        assert_eq!(owner.exits_started.load(Ordering::Acquire), 1);
        assert_eq!(owner.cancels.load(Ordering::Acquire), 0);
        assert_eq!(
            finishes.load(Ordering::Acquire),
            0,
            "control does not execute the surviving sink's finalizer"
        );
        assert_eq!(
            driver.process(Duration::from_secs(1)),
            DriverState::PendingFinish
        );
        assert_eq!(finishes.load(Ordering::Acquire), 1);
        if let Some(failure) = &late_failure {
            *control_failure.lock().unwrap() = Some(failure.clone());
            event.notify_observers();
            assert_eq!(
                driver.process(Duration::from_secs(1)),
                DriverState::PendingFinish
            );
        }
        withdrawal_exited.store(true, Ordering::Release);
        event.notify_observers();
        owner.exited.store(true, Ordering::Release);
        owner.event.notify_observers();
        let terminal = driver.process(Duration::from_secs(1));
        match late_failure {
            None => assert_eq!(terminal, DriverState::Finished),
            Some(expected) => match terminal {
                DriverState::Failed(actual) => assert!(actual.same_occurrence(&expected)),
                other => panic!("retired owner control failure must precede success: {other:?}"),
            },
        }
        assert_eq!(owner.exits_started.load(Ordering::Acquire), 1);
    }
}
