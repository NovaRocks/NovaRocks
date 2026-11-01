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
//! Sink side of local in-process exchange.
//!
//! Responsibilities:
//! - Pushes chunks into local exchange channels according to partitioning policy.
//! - Coordinates producer completion and wake-up notifications for source operators.
//!
//! Key exported interfaces:
//! - Types: `LocalExchangeSinkFactory`.
//!
//! Current limitations:
//! - Implements only the execution semantics currently wired by novarocks plan lowering and pipeline builder.
//! - Unsupported states should be surfaced as explicit runtime errors instead of fallback behavior.

use crate::exec::operators::local_exchanger::LocalExchanger;
use crate::exec::pipeline::operator::OperatorLifecycle;
use crate::exec::pipeline::{
    input::{PreparedInput, RawInput},
    operator::{InputEncodingPolicy, Operator},
    operator_factory::OperatorFactory,
    schedule::observer::Observable,
    shape::{ControlStep, SinkKernel, SinkStep, Transform},
    step::{AdmissionNext, NextStep, WaitFact, WaitReason},
    step_context::StepContext,
};
use crate::runtime::fragment::ExecutionResult;
use crate::runtime::{mem_tracker::MemTracker, runtime_state::RuntimeState};
use std::sync::Arc;
use tracing::debug;

/// Factory for local-exchange sink operators that partition and enqueue chunks locally.
pub struct LocalExchangeSinkFactory {
    name: String,
    owner_node_id: i32,
    exchanger: Arc<LocalExchanger>,
}
impl LocalExchangeSinkFactory {
    pub(crate) fn new(owner_node_id: i32, exchanger: Arc<LocalExchanger>) -> Self {
        let name = if owner_node_id >= 0 {
            format!("LOCAL_EXCHANGE_SINK (id={owner_node_id})")
        } else {
            "LOCAL_EXCHANGE_SINK".to_string()
        };
        Self {
            name,
            owner_node_id,
            exchanger,
        }
    }
}
impl OperatorFactory for LocalExchangeSinkFactory {
    fn name(&self) -> &str {
        &self.name
    }
    fn create(&self, _dop: i32, driver_id: i32) -> Box<dyn Operator> {
        Box::new(Transform::hydrating(LocalExchangeSinkOperator {
            name: self.name.clone(),
            owner_node_id: self.owner_node_id,
            driver_id,
            exchanger: self.exchanger.clone(),
            producer_finished: false,
            bypass_cursor: 0,
            logged_first_input: false,
            queue_tracker: None,
            construction: None,
        }))
    }
    fn is_sink(&self) -> bool {
        true
    }
}
struct LocalExchangeSinkOperator {
    construction: Option<Arc<dyn crate::exec::pipeline::operator::InputConstructionFactory>>,
    name: String,
    owner_node_id: i32,
    driver_id: i32,
    exchanger: Arc<LocalExchanger>,
    producer_finished: bool,
    bypass_cursor: usize,
    logged_first_input: bool,
    // Outer Option records installation, including an explicitly untracked
    // kernel-only fixture. Production binding supplies its query owner.
    queue_tracker: Option<Option<Arc<MemTracker>>>,
}
impl LocalExchangeSinkOperator {
    fn finish_producer(&mut self) {
        if !self.producer_finished {
            self.producer_finished = true;
            self.exchanger.finish_producer();
        }
    }
}
impl OperatorLifecycle for LocalExchangeSinkOperator {
    fn creates_typed_storage(&self) -> bool {
        true
    }
    fn set_construction_factory(
        &mut self,
        factory: Arc<dyn crate::exec::pipeline::operator::InputConstructionFactory>,
    ) {
        self.construction = Some(factory);
    }

    fn name(&self) -> &str {
        &self.name
    }
    fn bind_runtime_state(&mut self, state: &RuntimeState) -> ExecutionResult<()> {
        self.queue_tracker = Some(self.exchanger.queue_mem_tracker(state));
        Ok(())
    }
    fn begin_exit(&mut self) -> ExecutionResult<()> {
        self.finish_producer();
        Ok(())
    }
    fn close(&mut self) -> ExecutionResult<()> {
        self.begin_exit()
    }
    fn cancel(&mut self) {
        self.finish_producer();
    }
}
impl InputEncodingPolicy for LocalExchangeSinkOperator {
    fn retains_encoded_path(
        &self,
        _: crate::exec::pipeline::operator::EncodedValueOccurrence<'_>,
    ) -> bool {
        self.exchanger.retains_encoded_partition_input()
    }
}
impl SinkKernel for LocalExchangeSinkOperator {
    fn poll_control(&mut self, _: &mut StepContext<'_>) -> ExecutionResult<ControlStep> {
        if self.exchanger.all_consumers_closed() {
            self.finish_producer();
            Ok(ControlStep::Withdrawn)
        } else {
            Ok(ControlStep::Active)
        }
    }
    fn control_observable(&self) -> Option<Arc<Observable>> {
        Some(self.exchanger.closed_observable())
    }
    fn admission(
        &self,
        _: &mut StepContext<'_>,
        _: &RawInput,
    ) -> ExecutionResult<Option<AdmissionNext>> {
        let observable = self.exchanger.sink_observable();
        let observed_generation = observable.generation();
        if !self.exchanger.need_input() && !self.exchanger.all_consumers_closed() {
            Ok(Some(AdmissionNext::Wait(WaitFact {
                reason: WaitReason::Output,
                observable,
                observed_generation,
                deadline: None,
            })))
        } else {
            Ok(None)
        }
    }
    fn consume(
        &mut self,
        ctx: &mut StepContext<'_>,
        input: PreparedInput,
    ) -> ExecutionResult<SinkStep> {
        let chunk = input.into_uncovered_chunk()?;
        if self.exchanger.all_consumers_closed() {
            drop(chunk);
            self.finish_producer();
            return Ok(SinkStep::new(NextStep::Finished, 0));
        }
        let rows = u64::try_from(chunk.len()).expect("supported row count fits u64");
        if !self.logged_first_input && !chunk.is_empty() {
            self.logged_first_input = true;
            debug!(
                "LocalExchangeSink first chunk: exchange_id={} owner_node_id={} driver_id={} rows={}",
                self.exchanger.exchange_id(),
                self.owner_node_id,
                self.driver_id,
                rows
            );
        }
        let tracker = self.queue_tracker.as_ref().ok_or_else(|| {
            crate::runtime::fragment::ExecutionFailure::from_domain(LocalExchangeBindingFailure)
        })?;
        let factory = self
            .construction
            .as_deref()
            .ok_or(crate::exec::pipeline::operator::ConstructionFailure::MissingOwner)?;
        self.exchanger.accept_created(
            tracker.as_ref(),
            chunk,
            factory,
            ctx,
            &mut self.bypass_cursor,
        )?;
        Ok(SinkStep::new(NextStep::NeedInput, rows))
    }
    fn finish(&mut self, _: &mut StepContext<'_>) -> ExecutionResult<SinkStep> {
        self.finish_producer();
        Ok(SinkStep::new(NextStep::Finished, 0))
    }
}
#[derive(Debug)]
struct LocalExchangeBindingFailure;
impl std::fmt::Display for LocalExchangeBindingFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("local exchange sink has no installed queue ownership binding")
    }
}
impl std::error::Error for LocalExchangeBindingFailure {}
impl novarocks_failure_contract::FailureProjection for LocalExchangeBindingFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use crate::runtime::fragment::ExecutionReason;
        use novarocks_failure_contract::{FailureClass, FailureFact};
        FailureFact::new(
            FailureClass::Invariant,
            ExecutionReason::LocalExchangeMissingBinding.reason(),
        )
    }
}

#[cfg(test)]
mod tests {
    fn install_actual_creator(
        operator: &mut dyn crate::exec::pipeline::operator::Operator,
        state: &RuntimeState,
    ) {
        operator.set_construction_factory(
            crate::exec::pipeline::operator::input_construction::try_runtime_input_construction_factory(
                crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(),
                crate::runtime::mem_tracker::MemTracker::new_root("local exchange fixture construction"),
                state.error_state(),
            ).unwrap(),
        );
    }

    use std::sync::Arc;

    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    use crate::exec::chunk::Chunk;
    use crate::exec::operators::local_exchanger::LocalExchangePartitionSpec;
    use crate::exec::operators::{LocalExchangeSinkFactory, LocalExchangeSourceFactory};
    use crate::exec::pipeline::operator_factory::OperatorFactory;
    use crate::runtime::runtime_state::RuntimeState;
    use novarocks_failure_contract::FailureProjection;
    use novarocks_types::SlotId;

    use crate::exec::operators::local_exchanger::LocalExchanger;

    fn chunk_of(values: &[i32]) -> Chunk {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
        let array = Arc::new(Int32Array::from(values.to_vec())) as arrow::array::ArrayRef;
        let batch = RecordBatch::try_new(schema, vec![array]).expect("record batch");
        crate::exec::chunk::typed_fixture::chunk(
            batch,
            &[SlotId::new(1)],
            &[novarocks_type_contract::FunctionValueType::new(
                DataType::Int32,
                false,
            )],
        )
    }

    #[test]
    fn local_exchange_forwards_chunks() {
        let rt = RuntimeState::default();
        let exchanger = LocalExchanger::new_handoff(1, 1, 2);
        let sink_factory = LocalExchangeSinkFactory::new(-1, Arc::clone(&exchanger));
        let source_factory = LocalExchangeSourceFactory::new(-1, 1, Arc::clone(&exchanger));

        let mut sink = sink_factory.create(1, 0);
        install_actual_creator(sink.as_mut(), &rt);
        let mut source = source_factory.create(1, 0);

        let c1 = chunk_of(&[1]);
        let c2 = chunk_of(&[2]);

        push_chunk(&mut sink, &rt, c1);

        push_chunk(&mut sink, &rt, c2);

        finish_sink(&mut sink, &rt);

        let out1 = pull(&mut source, &rt).expect("chunk1");
        assert_eq!(out1.len(), 1);

        let out2 = pull(&mut source, &rt).expect("chunk2");
        assert_eq!(out2.len(), 1);

        let out3 = pull(&mut source, &rt);
        assert!(out3.is_none());
    }

    fn source_step(
        source: &mut Box<dyn crate::exec::pipeline::operator::Operator>,
        rt: &RuntimeState,
    ) -> crate::exec::pipeline::step::StepResult {
        use crate::exec::pipeline::step_context::{DEFAULT_TURN_WORK_UNITS, StepContext, TurnWork};
        let control = rt.error_state();
        let mut work = TurnWork::new(std::num::NonZeroU64::MIN, DEFAULT_TURN_WORK_UNITS);
        source
            .as_mut()
            .drive(&mut StepContext::new(
                &control,
                &mut work,
                &mut novarocks_value_arrays::decode::ValueConversionContext::default(),
            ))
            .expect("source work")
    }

    fn pull(
        source: &mut Box<dyn crate::exec::pipeline::operator::Operator>,
        rt: &RuntimeState,
    ) -> Option<Chunk> {
        source_step(source, rt).output
    }

    fn push(
        sink: &mut Box<dyn crate::exec::pipeline::operator::Operator>,
        rt: &RuntimeState,
        values: &[i32],
    ) {
        push_chunk(sink, rt, chunk_of(values));
    }
    fn push_chunk(
        sink: &mut Box<dyn crate::exec::pipeline::operator::Operator>,
        rt: &RuntimeState,
        chunk: Chunk,
    ) {
        use crate::exec::pipeline::{
            input::RawInput,
            step::InputResult,
            step_context::{StepContext, TurnWork},
        };
        sink.bind_runtime_state(rt).expect("install queue binding");
        let control = rt.error_state();
        let mut work = TurnWork::new(std::num::NonZeroU64::MIN, 64);
        let mut value_conversions =
            novarocks_value_arrays::decode::ValueConversionContext::default();
        let mut ctx = StepContext::new(&control, &mut work, &mut value_conversions);
        assert!(matches!(
            sink.as_mut().try_push(&mut ctx, RawInput::for_test(chunk)),
            InputResult::Accepted(Ok(_))
        ));
        let step = sink
            .as_mut()
            .drive(&mut ctx)
            .expect("enqueue accepted input");
        assert!(step.output.is_none());
    }
    fn finish_sink(
        sink: &mut Box<dyn crate::exec::pipeline::operator::Operator>,
        rt: &RuntimeState,
    ) {
        use crate::exec::pipeline::{
            step::NextStep,
            step_context::{StepContext, TurnWork},
        };
        let control = rt.error_state();
        let mut work = TurnWork::new(std::num::NonZeroU64::MIN, 64);
        let mut value_conversions =
            novarocks_value_arrays::decode::ValueConversionContext::default();
        let mut ctx = StepContext::new(&control, &mut work, &mut value_conversions);
        sink.as_mut().finish(&mut ctx).unwrap();
        let step = sink.as_mut().drive(&mut ctx).unwrap();
        assert!(step.output.is_none());
        assert!(matches!(step.next, NextStep::Finished));
    }
    fn poll_control(
        sink: &mut Box<dyn crate::exec::pipeline::operator::Operator>,
        rt: &RuntimeState,
    ) {
        use crate::exec::pipeline::step_context::{StepContext, TurnWork};
        let control = rt.error_state();
        let mut work = TurnWork::new(std::num::NonZeroU64::MIN, 0);
        sink.as_mut()
            .poll_control(&mut StepContext::new(
                &control,
                &mut work,
                &mut novarocks_value_arrays::decode::ValueConversionContext::default(),
            ))
            .unwrap();
    }
    fn reject_after_withdrawal(
        sink: &mut Box<dyn crate::exec::pipeline::operator::Operator>,
        rt: &RuntimeState,
        chunk: Chunk,
    ) {
        use crate::exec::pipeline::{
            input::RawInput,
            step::InputResult,
            step_context::{StepContext, TurnWork},
        };
        let original = chunk.columns()[0].clone();
        let control = rt.error_state();
        let mut work = TurnWork::new(std::num::NonZeroU64::MIN, 0);
        match sink.as_mut().try_push(
            &mut StepContext::new(
                &control,
                &mut work,
                &mut novarocks_value_arrays::decode::ValueConversionContext::default(),
            ),
            RawInput::for_test(chunk),
        ) {
            InputResult::Rejected { input, failure } => {
                assert!(Arc::ptr_eq(
                    &original,
                    &input.original_for_test().columns()[0]
                ));
                assert_eq!(failure.failure_fact().reason().code(), 27);
            }
            _ => panic!("finished receiver must reject the same raw input"),
        }
    }

    fn first_value(chunk: &Chunk) -> i32 {
        chunk.columns()[0]
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("int32 column")
            .value(0)
    }

    #[test]
    fn full_queue_defers_the_same_raw_input_until_capacity_changes() {
        use crate::exec::pipeline::{
            input::RawInput,
            step::{AdmissionNext, InputResult, NextStep},
            step_context::{StepContext, TurnWork},
        };
        let rt = RuntimeState::default();
        let exchanger = LocalExchanger::new_handoff(1, 1, 1);
        let mut sink = LocalExchangeSinkFactory::new(-1, exchanger.clone()).create(1, 0);
        install_actual_creator(sink.as_mut(), &rt);
        let mut source = LocalExchangeSourceFactory::new(-1, 1, exchanger.clone()).create(1, 0);
        push(&mut sink, &rt, &[7]);
        let tracker = crate::runtime::mem_tracker::MemTracker::new_root("deferred raw input");
        let mut chunk = chunk_of(&[8, 9]);
        chunk.transfer_to(&tracker);
        let original = chunk.columns()[0].clone();
        let bytes = tracker.current();
        let control = rt.error_state();
        let mut work = TurnWork::new(std::num::NonZeroU64::MIN, 64);
        let mut value_conversions =
            novarocks_value_arrays::decode::ValueConversionContext::default();
        let mut ctx = StepContext::new(&control, &mut work, &mut value_conversions);
        let mut raw = RawInput::for_test(chunk);
        let mut observed = None;
        for _ in 0..2 {
            raw = match sink.as_mut().try_push(&mut ctx, raw) {
                InputResult::Deferred {
                    input,
                    next: AdmissionNext::Wait(wait),
                } => {
                    assert!(Arc::ptr_eq(
                        &original,
                        &input.original_for_test().columns()[0]
                    ));
                    assert!(Arc::ptr_eq(&wait.observable, &exchanger.sink_observable()));
                    if let Some(generation) = observed {
                        assert_eq!(wait.observed_generation, generation);
                    }
                    observed = Some(wait.observed_generation);
                    input
                }
                _ => panic!("full queue must refuse before preparation"),
            };
            assert_eq!(
                tracker.current(),
                bytes,
                "refusal preserves edge responsibility"
            );
        }
        assert_eq!(first_value(&pull(&mut source, &rt).unwrap()), 7);
        assert!(matches!(
            sink.as_mut().try_push(&mut ctx, raw),
            InputResult::Accepted(Ok(_))
        ));
        let step = sink.as_mut().drive(&mut ctx).unwrap();
        assert!(step.output.is_none());
        assert!(matches!(step.next, NextStep::NeedInput));
        assert_eq!(step.committed_rows, 2);
        let output = pull(&mut source, &rt).unwrap();
        assert!(Arc::ptr_eq(&original, &output.columns()[0]));
        assert_eq!(output.len(), 2);
        drop(original);
        drop(output);
        assert_eq!(tracker.current(), 0);
        finish_sink(&mut sink, &rt);
        assert!(matches!(
            source_step(&mut source, &rt).next,
            NextStep::Finished
        ));
    }

    #[test]
    fn handoff_queue_backpressures_at_its_chunk_bound_and_wakes_on_pop() {
        let rt = RuntimeState::default();
        let exchanger = LocalExchanger::new_handoff(1, 3, 2);
        let mut sink = LocalExchangeSinkFactory::new(-1, Arc::clone(&exchanger)).create(1, 0);
        install_actual_creator(sink.as_mut(), &rt);
        let mut source =
            LocalExchangeSourceFactory::new(-1, 1, Arc::clone(&exchanger)).create(3, 0);

        assert!(exchanger.need_input());
        push(&mut sink, &rt, &[1]);
        assert!(
            exchanger.need_input(),
            "one queued chunk is below the bound of two"
        );
        push(&mut sink, &rt, &[2, 3]);
        assert!(
            !exchanger.need_input(),
            "the whole queue holds at most two chunks"
        );

        let capacity = exchanger.sink_observable();
        let before = capacity.generation();
        assert!(pull(&mut source, &rt).is_some());
        assert!(exchanger.need_input());
        assert!(
            capacity.generation() > before,
            "a pop that frees the bound must wake a producer parked on the sink"
        );
    }

    #[test]
    fn handoff_consumers_share_one_queue_and_take_each_chunk_once() {
        let rt = RuntimeState::default();
        let exchanger = LocalExchanger::new_handoff(1, 3, 8);
        let sink_factory = LocalExchangeSinkFactory::new(-1, Arc::clone(&exchanger));
        let source_factory = LocalExchangeSourceFactory::new(-1, 1, Arc::clone(&exchanger));
        let mut sink = sink_factory.create(1, 0);
        install_actual_creator(sink.as_mut(), &rt);
        let mut consumers = (0..3)
            .map(|index| source_factory.create(3, index))
            .collect::<Vec<_>>();
        for value in 0..6 {
            push(&mut sink, &rt, &[value]);
        }
        finish_sink(&mut sink, &rt);

        // Consumer 2 takes everything while the others are idle: no chunk is
        // reserved for a consumer that is not running.
        let mut taken = Vec::new();
        while let Some(chunk) = pull(&mut consumers[2], &rt) {
            taken.push(first_value(&chunk));
        }
        assert_eq!(taken, vec![0, 1, 2, 3, 4, 5]);
        for consumer in &mut consumers {
            assert!(pull(consumer, &rt).is_none());
            assert!(
                matches!(
                    source_step(consumer, &rt).next,
                    crate::exec::pipeline::step::NextStep::Finished
                ),
                "an empty queue with no producer is end of stream"
            );
        }
    }

    #[test]
    fn racing_handoff_consumers_neither_duplicate_nor_lose_chunks() {
        const CHUNKS: i32 = 2_000;
        let rt = RuntimeState::default();
        let exchanger = LocalExchanger::new_handoff(1, 3, 2);
        let sink_factory = LocalExchangeSinkFactory::new(-1, Arc::clone(&exchanger));
        let source_factory = LocalExchangeSourceFactory::new(-1, 1, Arc::clone(&exchanger));
        let mut sink = sink_factory.create(1, 0);
        install_actual_creator(sink.as_mut(), &rt);
        let consumers = (0..3)
            .map(|index| source_factory.create(3, index))
            .collect::<Vec<_>>();

        let mut taken = std::thread::scope(|scope| {
            let rt = &rt;
            let readers = consumers
                .into_iter()
                .map(|mut consumer| {
                    scope.spawn(move || {
                        let mut values = Vec::new();
                        loop {
                            let step = source_step(&mut consumer, rt);
                            if let Some(chunk) = step.output {
                                values.push(first_value(&chunk));
                            }
                            if matches!(step.next, crate::exec::pipeline::step::NextStep::Finished)
                            {
                                return values;
                            }
                            std::thread::yield_now();
                        }
                    })
                })
                .collect::<Vec<_>>();
            for value in 0..CHUNKS {
                while !exchanger.need_input() {
                    std::thread::yield_now();
                }
                push(&mut sink, rt, &[value]);
            }
            finish_sink(&mut sink, rt);
            readers
                .into_iter()
                .flat_map(|reader| reader.join().expect("consumer thread"))
                .collect::<Vec<_>>()
        });

        taken.sort_unstable();
        assert_eq!(taken, (0..CHUNKS).collect::<Vec<_>>());
    }

    #[test]
    fn buffered_queue_wakes_on_push_capacity_recovery_and_last_producer_eos() {
        let rt = RuntimeState::default();
        let exchanger =
            LocalExchanger::new_with_limits(1, 2, LocalExchangePartitionSpec::Single, 1, -1);
        let sink_factory = LocalExchangeSinkFactory::new(-1, Arc::clone(&exchanger));
        let mut first = sink_factory.create(2, 0);
        install_actual_creator(first.as_mut(), &rt);
        let mut second = sink_factory.create(2, 1);
        install_actual_creator(second.as_mut(), &rt);
        let mut source =
            LocalExchangeSourceFactory::new(-1, 1, Arc::clone(&exchanger)).create(1, 0);
        let readable = exchanger.source_observable();
        let writable = exchanger.sink_observable();
        let before_push = readable.generation();
        let idle = source_step(&mut source, &rt);
        assert!(idle.output.is_none());
        assert!(matches!(
            idle.next,
            crate::exec::pipeline::step::NextStep::Wait(_)
        ));
        push(&mut first, &rt, &[7]);
        assert!(readable.generation() > before_push);
        assert!(!exchanger.need_input());
        assert!(!exchanger.need_input());

        let before_pop = writable.generation();
        assert_eq!(
            first_value(&pull(&mut source, &rt).expect("queued chunk")),
            7
        );
        assert!(writable.generation() > before_pop);
        assert!(exchanger.need_input());
        assert!(exchanger.need_input());
        finish_sink(&mut first, &rt);
        let idle = source_step(&mut source, &rt);
        assert!(idle.output.is_none());
        assert!(matches!(
            idle.next,
            crate::exec::pipeline::step::NextStep::Wait(_)
        ));
        let before_eos = readable.generation();
        finish_sink(&mut second, &rt);
        assert!(readable.generation() > before_eos);
        let eos = source_step(&mut source, &rt);
        assert!(eos.output.is_none());
        assert!(matches!(
            eos.next,
            crate::exec::pipeline::step::NextStep::Finished
        ));
        assert!(matches!(
            source_step(&mut source, &rt).next,
            crate::exec::pipeline::step::NextStep::Finished
        ));
    }

    #[test]
    fn cancelling_the_last_consumer_releases_queue_owners_in_both_capacity_policies() {
        for handoff in [false, true] {
            let rt = RuntimeState::default();
            let exchanger = if handoff {
                LocalExchanger::new_handoff(1, 1, 1)
            } else {
                LocalExchanger::new_with_limits(1, 1, LocalExchangePartitionSpec::Single, 1, -1)
            };
            let mut sink = LocalExchangeSinkFactory::new(-1, Arc::clone(&exchanger)).create(1, 0);
            install_actual_creator(sink.as_mut(), &rt);
            let mut source =
                LocalExchangeSourceFactory::new(-1, 1, Arc::clone(&exchanger)).create(1, 0);
            let tracker = crate::runtime::mem_tracker::MemTracker::new_root("queued test input");
            let mut chunk = chunk_of(&[1, 2]);
            let array = Arc::downgrade(&chunk.columns()[0]);
            chunk.transfer_to(&tracker);
            let input_bytes = tracker.current();
            assert!(input_bytes > 0);
            push_chunk(&mut sink, &rt, chunk);
            assert!(array.upgrade().is_some());
            assert_eq!(tracker.current(), input_bytes);
            assert!(!exchanger.need_input());
            let writable = exchanger.sink_observable();
            let closed = exchanger.closed_observable();
            let before_writable = writable.generation();
            let before_closed = closed.generation();
            source.cancel();
            poll_control(&mut sink, &rt);
            assert!(matches!(
                source_step(&mut source, &rt).next,
                crate::exec::pipeline::step::NextStep::Finished
            ));
            assert!(matches!(
                source_step(&mut sink, &rt).next,
                crate::exec::pipeline::step::NextStep::Finished
            ));
            assert!(array.upgrade().is_none());
            assert_eq!(
                tracker.current(),
                0,
                "cancel releases the live input charge"
            );
            assert!(writable.generation() > before_writable);
            assert!(closed.generation() > before_closed);
            let after_closed = closed.generation();
            source.cancel();
            source.close().expect("idempotent close after cancel");
            assert_eq!(closed.generation(), after_closed);
            let late = chunk_of(&[3]);
            let late_array = Arc::downgrade(&late.columns()[0]);
            reject_after_withdrawal(&mut sink, &rt, late);
            assert!(late_array.upgrade().is_none());
            assert_eq!(
                exchanger.partition_buffered_chunks(0).map(|(n, _)| n),
                Some(0)
            );
        }
    }

    #[test]
    fn partial_consumer_close_keeps_the_shared_queue_for_the_others() {
        let rt = RuntimeState::default();
        let exchanger = LocalExchanger::new_handoff(1, 2, 8);
        let sink_factory = LocalExchangeSinkFactory::new(-1, Arc::clone(&exchanger));
        let source_factory = LocalExchangeSourceFactory::new(-1, 1, Arc::clone(&exchanger));
        let mut sink = sink_factory.create(1, 0);
        install_actual_creator(sink.as_mut(), &rt);
        let mut first = source_factory.create(2, 0);
        let mut second = source_factory.create(2, 1);
        push(&mut sink, &rt, &[7]);

        first.close().expect("close first consumer");
        first.close().expect("a repeated close is a no-op");
        assert!(!exchanger.all_consumers_closed());
        assert!(!matches!(
            source_step(&mut sink, &rt).next,
            crate::exec::pipeline::step::NextStep::Finished
        ));
        assert!(exchanger.need_input());
        let chunk = pull(&mut second, &rt).expect("the remaining consumer still reads");
        assert_eq!(first_value(&chunk), 7);
    }

    #[test]
    fn last_consumer_close_finishes_producers_and_drops_the_queue() {
        let rt = RuntimeState::default();
        let exchanger = LocalExchanger::new_handoff(1, 2, 8);
        let sink_factory = LocalExchangeSinkFactory::new(-1, Arc::clone(&exchanger));
        let source_factory = LocalExchangeSourceFactory::new(-1, 1, Arc::clone(&exchanger));
        let mut sink = sink_factory.create(1, 0);
        install_actual_creator(sink.as_mut(), &rt);
        let mut first = source_factory.create(2, 0);
        let mut second = source_factory.create(2, 1);
        push(&mut sink, &rt, &[1]);
        push(&mut sink, &rt, &[2]);

        let early_finish = sink
            .as_ref()
            .control_observable()
            .expect("an open handoff sink exposes its early-finish observable");
        let before = early_finish.generation();
        first.close().expect("close first consumer");
        assert_eq!(
            early_finish.generation(),
            before,
            "one consumer is still reading"
        );
        second.close().expect("close second consumer");
        poll_control(&mut sink, &rt);

        assert!(exchanger.all_consumers_closed());
        assert!(
            early_finish.generation() > before,
            "producers must be woken"
        );
        assert!(
            matches!(
                source_step(&mut sink, &rt).next,
                crate::exec::pipeline::step::NextStep::Finished
            ),
            "nothing a producer pushes can be read any more"
        );
        assert!(!exchanger.need_input());
        assert_eq!(
            exchanger
                .partition_buffered_chunks(0)
                .map(|(chunks, _)| chunks),
            Some(0)
        );
        // No new input is accepted after control withdrawal.
        reject_after_withdrawal(&mut sink, &rt, chunk_of(&[3]));
        assert_eq!(
            exchanger
                .partition_buffered_chunks(0)
                .map(|(chunks, _)| chunks),
            Some(0)
        );
        assert_eq!(exchanger.consumer_count(), 2);
    }
    #[test]
    fn local_exchange_limit_zero_wakes_source_blocked_remote_driver() {
        use crate::exec::operators::exchange_source::ExchangeSourceFactory;
        use crate::exec::operators::limit_processor::LimitProcessorFactory;
        use crate::exec::pipeline::binding::ExchangeBinding;
        use crate::exec::pipeline::driver::{DriverState, PipelineDriver};
        use crate::exec::pipeline::operator::BlockedReason;
        use crate::runtime::exchange::ExchangeKey;
        use crate::runtime::fragment::io::exchange::test_support::Fixture;
        use std::time::Duration;

        let fixture = Fixture::new();
        let exchanger = LocalExchanger::new(1, 1, LocalExchangePartitionSpec::Single);
        let sink = LocalExchangeSinkFactory::new(-1, Arc::clone(&exchanger));
        let local_source = LocalExchangeSourceFactory::new(-1, 1, Arc::clone(&exchanger));
        let key = ExchangeKey {
            finst_id_hi: 97001,
            finst_id_lo: 97002,
            node_id: 97003,
        };
        let receiver_key = crate::runtime::fragment::io::ExchangeReceiverKey {
            fragment_instance_id: novarocks_types::UniqueId::new(key.finst_id_hi, key.finst_id_lo),
            node_id: key.node_id,
        };
        let state = fixture.state(receiver_key.fragment_instance_id);
        let receiver_port = fixture.port.clone();
        let creation_factory = fixture.creation(receiver_key.fragment_instance_id);
        let layout = crate::exec::chunk::typed_fixture::layout(
            Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)])),
            &[SlotId::new(1)],
            &[novarocks_type_contract::FunctionValueType::new(
                DataType::Int32,
                false,
            )],
        );
        let schema = creation_factory
            .create_compiled_chunk_schema(&layout)
            .unwrap();
        receiver_port
            .register(crate::runtime::fragment::io::ExchangeReceiverRegistration {
                key: receiver_key,
                expected_senders: 1,
                expected_chunk_schema: schema,
                column_binding: crate::runtime::exchange::ExchangeColumnBinding::Positional,
                creation_factory,
            })
            .expect("register remote receiver");
        fixture.bind(receiver_key, &[novarocks_types::UniqueId::new(97000, 1)]);
        let remote = ExchangeSourceFactory::new_compiled(
            key.node_id,
            ExchangeBinding {
                key,
                expected_senders: 1,
                receiver_port,
            },
        );
        let (mut source, source_observable) = remote.create_source(0);
        source.prepare().expect("prepare remote source");
        source
            .bind_runtime_state(&state)
            .expect("bind remote source");
        let mut producer = PipelineDriver::new(
            0,
            vec![source, sink.create(1, 0)],
            None,
            Vec::new(),
            Arc::clone(&state),
            None,
        );
        assert_eq!(
            producer.process(Duration::from_secs(1)),
            DriverState::Blocked(BlockedReason::InputEmpty)
        );
        let (wait, generation, deadline) =
            producer.blocked_observable_snapshot().expect("input wait");
        assert!(
            deadline.is_none(),
            "remote receive waits have no idle deadline"
        );
        let (control_wait, control_generation) =
            producer.blocked_terminal_snapshot().expect("control wait");
        let source_observers = source_observable.num_observers();
        let sink_observers = exchanger.sink_observable().num_observers();
        producer.set_ready();
        assert_eq!(
            producer.process(Duration::from_secs(1)),
            DriverState::Blocked(BlockedReason::InputEmpty)
        );
        let (same_wait, same_generation, _) = producer.blocked_observable_snapshot().unwrap();
        assert!(Arc::ptr_eq(&wait, &same_wait));
        assert_eq!(generation, same_generation);
        assert_eq!(source_observers, source_observable.num_observers());
        assert_eq!(sink_observers, exchanger.sink_observable().num_observers());
        drop(same_wait);
        let source_generation = source_observable.generation();
        let limit = LimitProcessorFactory::new(-1, Some(0), 0);
        let mut consumer = PipelineDriver::new(
            1,
            vec![local_source.create(1, 0), limit.create(1, 0)],
            None,
            Vec::new(),
            state,
            None,
        );
        assert_eq!(
            consumer.process(Duration::from_secs(1)),
            DriverState::Finished
        );
        assert_eq!(
            source_observable.generation(),
            source_generation,
            "no remote chunk or EOS is needed"
        );
        assert!(
            Arc::ptr_eq(&wait, &control_wait),
            "data and control share the driver's stable endpoint"
        );
        assert!(
            wait.generation() > generation,
            "consumer withdrawal invalidates the data wait too"
        );
        assert!(
            control_wait.generation() > control_generation,
            "consumer close invalidates the independently subscribed control wait"
        );
        producer.set_ready();
        assert_eq!(
            producer.process(Duration::ZERO),
            DriverState::Finished,
            "sink-only notification permits actual driver completion"
        );
        // Finished data work releases its old source subscription even while
        // the driver object and its stable control endpoint still exist.
        let retired_generation = wait.generation();
        source_observable.notify_observers();
        assert_eq!(wait.generation(), retired_generation);
        assert!(exchanger.all_consumers_closed());
        let weak_control = Arc::downgrade(&control_wait);
        drop(control_wait);
        drop(wait);
        drop(producer);
        assert!(
            weak_control.upgrade().is_none(),
            "forwarding does not retain the driver control wait"
        );
        assert_eq!(
            exchanger.closed_observable().num_observers(),
            0,
            "driver exit removes its scoped control subscription"
        );
    }

    #[test]
    fn local_exchange_closed_partition_does_not_block_necessary_sibling() {
        let state = RuntimeState::default();
        let exchanger = LocalExchanger::new_with_limits(
            2,
            1,
            LocalExchangePartitionSpec::InputSlotIds(vec![SlotId::new(1)]),
            1024 * 1024,
            1,
        );
        let sink_factory = LocalExchangeSinkFactory::new(-1, Arc::clone(&exchanger));
        let source_factory = LocalExchangeSourceFactory::new(-1, 2, Arc::clone(&exchanger));
        let mut sink = sink_factory.create(1, 0);
        install_actual_creator(sink.as_mut(), &state);
        let mut departed = source_factory.create(2, 0);
        let mut sibling = source_factory.create(2, 1);
        push_chunk(&mut sink, &state, chunk_of(&(0..64).collect::<Vec<_>>()));
        let before = exchanger.stats_snapshot();
        assert!(before.partitions[0].buffered_chunks > 0);
        let sibling_rows = before.partitions[1].pushed_rows;
        assert!(sibling_rows > 0);
        let wake = exchanger.sink_observable();
        let generation = wake.generation();
        departed.close().unwrap();
        departed.cancel();
        departed.begin_exit().unwrap();
        assert!(
            !matches!(
                source_step(&mut sink, &state).next,
                crate::exec::pipeline::step::NextStep::Finished
            ),
            "a necessary sibling keeps its producer live"
        );
        assert_eq!(
            wake.generation(),
            generation + 1,
            "close/cancel/exit settle only once"
        );
        assert_eq!(exchanger.stats_snapshot().partitions[0].buffered_chunks, 0);
        let mut rows = 0;
        while let Some(chunk) = pull(&mut sibling, &state) {
            rows += chunk.len() as u64;
        }
        assert_eq!(rows, sibling_rows);
        assert!(
            exchanger.need_input(),
            "departed queue releases backpressure"
        );
        push_chunk(&mut sink, &state, chunk_of(&(64..128).collect::<Vec<_>>()));
        assert_eq!(
            exchanger.stats_snapshot().partitions[0].buffered_chunks,
            0,
            "late partition output is discarded"
        );
        let expected = exchanger.stats_snapshot().partitions[1].pushed_rows;
        finish_sink(&mut sink, &state);
        while let Some(chunk) = pull(&mut sibling, &state) {
            rows += chunk.len() as u64;
        }
        assert_eq!(rows, expected);
        assert!(
            matches!(
                source_step(&mut sibling, &state).next,
                crate::exec::pipeline::step::NextStep::Finished
            ),
            "necessary sibling receives normal EOS"
        );
        sibling.close().unwrap();
        assert!(exchanger.all_consumers_closed());
    }
    #[test]
    fn conditional_bypass_cursor_is_per_producer_and_persists_across_batches() {
        use crate::exec::operators::local_exchanger::LocalExchangePartitionSpec;
        use arrow::array::{ArrayRef, BooleanArray, Int32Array};
        use arrow::datatypes::DataType;
        use novarocks_type_contract::FunctionValueType;
        use novarocks_types::SlotId;
        fn bypass(values: Vec<i32>) -> Chunk {
            let count = values.len();
            crate::exec::chunk::typed_fixture::columns(
                &["row", "target", "applies"],
                &[SlotId::new(1), SlotId::new(2), SlotId::new(3)],
                &[
                    FunctionValueType::new(DataType::Int32, false),
                    FunctionValueType::new(DataType::Int32, true),
                    FunctionValueType::new(DataType::Boolean, true),
                ],
                vec![
                    Arc::new(Int32Array::from(values)) as ArrayRef,
                    Arc::new(Int32Array::from(vec![None::<i32>; count])),
                    Arc::new(BooleanArray::from(vec![Some(false); count])),
                ],
                count,
            )
        }
        let rt = RuntimeState::default();
        let exchanger = LocalExchanger::new_with_limits(
            3,
            2,
            LocalExchangePartitionSpec::ConditionalKey {
                applies: SlotId::new(3),
                keys: vec![(
                    SlotId::new(2),
                    novarocks_type_contract::FunctionValueType::new(DataType::Int32, true),
                )],
            },
            1024 * 1024,
            -1,
        );
        let factory = LocalExchangeSinkFactory::new(-1, exchanger.clone());
        let mut first = factory.create(2, 0);
        let mut second = factory.create(2, 1);
        install_actual_creator(first.as_mut(), &rt);
        install_actual_creator(second.as_mut(), &rt);
        push_chunk(&mut first, &rt, bypass(vec![0, 1]));
        push_chunk(&mut second, &rt, bypass(vec![100]));
        push_chunk(&mut first, &rt, bypass(vec![2, 3]));
        push_chunk(&mut second, &rt, bypass(vec![101]));
        finish_sink(&mut first, &rt);
        finish_sink(&mut second, &rt);
        let source = LocalExchangeSourceFactory::new(-1, 3, exchanger);
        let mut found = Vec::new();
        for partition in 0..3 {
            let mut consumer = source.create(3, partition);
            let mut rows = Vec::new();
            while let Some(chunk) = pull(&mut consumer, &rt) {
                rows.extend_from_slice(
                    chunk.columns()[0]
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .unwrap()
                        .values(),
                );
            }
            rows.sort_unstable();
            found.push(rows);
        }
        assert_eq!(found, vec![vec![0, 3, 100], vec![1, 101], vec![2]]);
    }
}
