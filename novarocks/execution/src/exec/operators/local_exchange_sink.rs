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

use crate::runtime::fragment::ExecutionResult;

use crate::exec::chunk::Chunk;
use crate::exec::pipeline::operator::{Operator, ProcessorOperator};
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::exec::pipeline::schedule::observer::Observable;
use std::sync::Arc;

use crate::exec::operators::local_exchanger::LocalExchanger;
use crate::runtime::runtime_state::RuntimeState;
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
        debug!(
            "LocalExchangeSinkFactory created: exchange_id={} owner_node_id={}",
            exchanger.exchange_id(),
            owner_node_id,
        );
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
        Box::new(LocalExchangeSinkOperator {
            name: self.name.clone(),
            owner_node_id: self.owner_node_id,
            driver_id,
            exchanger: Arc::clone(&self.exchanger),
            finished: false,
            logged_first_input: false,
            construction: None,
        })
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
    finished: bool,
    logged_first_input: bool,
}

impl Operator for LocalExchangeSinkOperator {
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

    fn as_processor_mut(&mut self) -> Option<&mut dyn ProcessorOperator> {
        Some(self)
    }

    fn as_processor_ref(&self) -> Option<&dyn ProcessorOperator> {
        Some(self)
    }

    fn is_finished(&self) -> bool {
        // Once every consumer has left, nothing this producer pushes can be
        // read; reporting finished ends the producer pipeline.
        self.finished || self.exchanger.all_consumers_closed()
    }
}

impl ProcessorOperator for LocalExchangeSinkOperator {
    fn need_input(&self) -> bool {
        !self.is_finished() && self.exchanger.need_input()
    }

    fn has_output(&self) -> bool {
        false
    }

    fn push_chunk(&mut self, state: &RuntimeState, chunk: Chunk) -> ExecutionResult<()> {
        if self.is_finished() {
            return Ok(());
        }
        if chunk.is_empty() {
            return Ok(());
        }
        if !self.logged_first_input {
            self.logged_first_input = true;
            debug!(
                "LocalExchangeSink first chunk: exchange_id={} owner_node_id={} driver_id={} rows={}",
                self.exchanger.exchange_id(),
                self.owner_node_id,
                self.driver_id,
                chunk.len()
            );
        }
        Ok(self.exchanger.accept_created(
            state,
            chunk,
            self.driver_id as usize,
            self.construction.as_deref(),
        )?)
    }

    fn pull_chunk(&mut self, _state: &RuntimeState) -> ExecutionResult<Option<Chunk>> {
        Ok(None)
    }

    fn set_finishing(&mut self, _state: &RuntimeState) -> ExecutionResult<()> {
        if self.finished {
            return Ok(());
        }
        debug!(
            "LocalExchangeSink set_finishing begin: exchange_id={} owner_node_id={} driver_id={} remaining_producers={}",
            self.exchanger.exchange_id(),
            self.owner_node_id,
            self.driver_id,
            self.exchanger.remaining_producers()
        );
        self.finished = true;
        let remaining_before = self.exchanger.remaining_producers();
        let finished_all = self.exchanger.finish_producer();
        let remaining_after = self.exchanger.remaining_producers();
        debug!(
            "LocalExchangeSink producer finished: exchange_id={} owner_node_id={} driver_id={} remaining_before={} remaining_after={} finished_all={}",
            self.exchanger.exchange_id(),
            self.owner_node_id,
            self.driver_id,
            remaining_before,
            remaining_after,
            finished_all
        );
        if finished_all {
            use tracing::info;
            info!("LocalExchangeSink: all producers finished");
            let stats = self.exchanger.stats_snapshot();
            for part in stats.partitions {
                debug!(
                    "LocalExchange stats: exchange_id={} partition={} pushed_rows={} popped_rows={} pushed_chunks={} popped_chunks={} buffered_chunks={} remaining_producers={}",
                    stats.exchange_id,
                    part.partition,
                    part.pushed_rows,
                    part.popped_rows,
                    part.pushed_chunks,
                    part.popped_chunks,
                    part.buffered_chunks,
                    stats.remaining_producers
                );
            }
        }
        Ok(())
    }

    fn sink_observable(&self) -> Option<Arc<Observable>> {
        if self.is_finished() {
            return None;
        }
        Some(self.exchanger.sink_observable())
    }

    fn early_finish_observable(&self) -> Option<Arc<Observable>> {
        if self.is_finished() {
            return None;
        }
        Some(self.exchanger.closed_observable())
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
    use crate::exec::expr::ExprArena;
    use crate::exec::operators::local_exchanger::LocalExchangePartitionSpec;
    use crate::exec::operators::{LocalExchangeSinkFactory, LocalExchangeSourceFactory};
    use crate::exec::pipeline::operator_factory::OperatorFactory;
    use crate::runtime::runtime_state::RuntimeState;
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
        let arena = Arc::new(ExprArena::default());
        let exchanger =
            LocalExchanger::new(1, 1, LocalExchangePartitionSpec::Single, Arc::clone(&arena));
        let sink_factory = LocalExchangeSinkFactory::new(-1, Arc::clone(&exchanger));
        let source_factory = LocalExchangeSourceFactory::new(-1, 1, Arc::clone(&exchanger));

        let mut sink = sink_factory.create(1, 0);
        install_actual_creator(sink.as_mut(), &rt);
        let mut source = source_factory.create(1, 0);

        let c1 = chunk_of(&[1]);
        let c2 = chunk_of(&[2]);

        sink.as_processor_mut()
            .expect("sink op")
            .push_chunk(&rt, c1)
            .expect("push c1");

        sink.as_processor_mut()
            .expect("sink op")
            .push_chunk(&rt, c2)
            .expect("push c2");

        sink.as_processor_mut()
            .expect("sink op")
            .set_finishing(&rt)
            .expect("set_finishing should finish producer");

        let out1 = source
            .as_processor_mut()
            .expect("source op")
            .pull_chunk(&rt)
            .expect("pull c1")
            .expect("chunk1");
        assert_eq!(out1.len(), 1);

        let out2 = source
            .as_processor_mut()
            .expect("source op")
            .pull_chunk(&rt)
            .expect("pull c2")
            .expect("chunk2");
        assert_eq!(out2.len(), 1);

        let out3 = source
            .as_processor_mut()
            .expect("source op")
            .pull_chunk(&rt)
            .expect("pull done");
        assert!(out3.is_none());
    }

    fn pull(
        source: &mut Box<dyn crate::exec::pipeline::operator::Operator>,
        rt: &RuntimeState,
    ) -> Option<Chunk> {
        source
            .as_processor_mut()
            .expect("source op")
            .pull_chunk(rt)
            .expect("pull")
    }

    fn push(
        sink: &mut Box<dyn crate::exec::pipeline::operator::Operator>,
        rt: &RuntimeState,
        values: &[i32],
    ) {
        sink.as_processor_mut()
            .expect("sink op")
            .push_chunk(rt, chunk_of(values))
            .expect("push");
    }

    fn need_input(sink: &dyn crate::exec::pipeline::operator::Operator) -> bool {
        sink.as_processor_ref().expect("sink op").need_input()
    }

    fn first_value(chunk: &Chunk) -> i32 {
        chunk.columns()[0]
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("int32 column")
            .value(0)
    }

    #[test]
    fn handoff_queue_backpressures_at_its_chunk_bound_and_wakes_on_pop() {
        let rt = RuntimeState::default();
        let exchanger = LocalExchanger::new_handoff(1, 3, 2, Arc::new(ExprArena::default()));
        let mut sink = LocalExchangeSinkFactory::new(-1, Arc::clone(&exchanger)).create(1, 0);
        let mut source =
            LocalExchangeSourceFactory::new(-1, 1, Arc::clone(&exchanger)).create(3, 0);

        assert!(need_input(sink.as_ref()));
        push(&mut sink, &rt, &[1]);
        assert!(
            need_input(sink.as_ref()),
            "one queued chunk is below the bound of two"
        );
        push(&mut sink, &rt, &[2, 3]);
        assert!(
            !need_input(sink.as_ref()),
            "the whole queue holds at most two chunks"
        );

        let capacity = exchanger.sink_observable();
        let before = capacity.generation();
        assert!(pull(&mut source, &rt).is_some());
        assert!(need_input(sink.as_ref()));
        assert!(
            capacity.generation() > before,
            "a pop that frees the bound must wake a producer parked on the sink"
        );
    }

    #[test]
    fn handoff_consumers_share_one_queue_and_take_each_chunk_once() {
        let rt = RuntimeState::default();
        let exchanger = LocalExchanger::new_handoff(1, 3, 8, Arc::new(ExprArena::default()));
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
        sink.as_processor_mut()
            .expect("sink op")
            .set_finishing(&rt)
            .expect("finish producer");

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
                consumer.is_finished(),
                "an empty queue with no producer is end of stream"
            );
        }
    }

    #[test]
    fn racing_handoff_consumers_neither_duplicate_nor_lose_chunks() {
        const CHUNKS: i32 = 2_000;
        let rt = RuntimeState::default();
        let exchanger = LocalExchanger::new_handoff(1, 3, 2, Arc::new(ExprArena::default()));
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
                            if let Some(chunk) = pull(&mut consumer, rt) {
                                values.push(first_value(&chunk));
                            } else if consumer.is_finished() {
                                return values;
                            } else {
                                std::thread::yield_now();
                            }
                        }
                    })
                })
                .collect::<Vec<_>>();
            for value in 0..CHUNKS {
                while !need_input(sink.as_ref()) {
                    std::thread::yield_now();
                }
                push(&mut sink, rt, &[value]);
            }
            sink.as_processor_mut()
                .expect("sink op")
                .set_finishing(rt)
                .expect("finish producer");
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
        let exchanger = LocalExchanger::new_with_limits(
            1,
            2,
            LocalExchangePartitionSpec::Single,
            Arc::new(ExprArena::default()),
            1,
            -1,
        );
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
        assert!(!source.as_processor_ref().expect("source").has_output());
        push(&mut first, &rt, &[7]);
        assert!(readable.generation() > before_push);
        assert!(!need_input(first.as_ref()));
        assert!(!need_input(second.as_ref()));

        let before_pop = writable.generation();
        assert_eq!(
            first_value(&pull(&mut source, &rt).expect("queued chunk")),
            7
        );
        assert!(writable.generation() > before_pop);
        assert!(need_input(first.as_ref()));
        assert!(need_input(second.as_ref()));
        first
            .as_processor_mut()
            .expect("first")
            .set_finishing(&rt)
            .expect("finish first");
        assert!(!source.as_processor_ref().expect("source").has_output());
        let before_eos = readable.generation();
        second
            .as_processor_mut()
            .expect("second")
            .set_finishing(&rt)
            .expect("finish second");
        assert!(readable.generation() > before_eos);
        assert!(source.as_processor_ref().expect("source").has_output());
        assert!(pull(&mut source, &rt).is_none());
        assert!(source.is_finished());
    }

    #[test]
    fn cancelling_the_last_consumer_releases_queue_owners_in_both_capacity_policies() {
        for handoff in [false, true] {
            let rt = RuntimeState::default();
            let arena = Arc::new(ExprArena::default());
            let exchanger = if handoff {
                LocalExchanger::new_handoff(1, 1, 1, arena)
            } else {
                LocalExchanger::new_with_limits(
                    1,
                    1,
                    LocalExchangePartitionSpec::Single,
                    arena,
                    1,
                    -1,
                )
            };
            let mut sink = LocalExchangeSinkFactory::new(-1, Arc::clone(&exchanger)).create(1, 0);
            let mut source =
                LocalExchangeSourceFactory::new(-1, 1, Arc::clone(&exchanger)).create(1, 0);
            let tracker = crate::runtime::mem_tracker::MemTracker::new_root("queued test input");
            let mut chunk = chunk_of(&[1, 2]);
            let array = Arc::downgrade(&chunk.columns()[0]);
            chunk.transfer_to(&tracker);
            let input_bytes = tracker.current();
            assert!(input_bytes > 0);
            sink.as_processor_mut()
                .expect("sink")
                .push_chunk(&rt, chunk)
                .expect("push input");
            assert!(array.upgrade().is_some());
            assert_eq!(tracker.current(), input_bytes);
            assert!(!need_input(sink.as_ref()));
            let writable = exchanger.sink_observable();
            let closed = exchanger.closed_observable();
            let before_writable = writable.generation();
            let before_closed = closed.generation();
            source.cancel();
            assert!(source.is_finished());
            assert!(sink.is_finished());
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
            sink.as_processor_mut()
                .expect("sink")
                .push_chunk(&rt, late)
                .expect("late push");
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
        let exchanger = LocalExchanger::new_handoff(1, 2, 8, Arc::new(ExprArena::default()));
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
        assert!(!sink.is_finished());
        assert!(need_input(sink.as_ref()));
        let chunk = pull(&mut second, &rt).expect("the remaining consumer still reads");
        assert_eq!(first_value(&chunk), 7);
    }

    #[test]
    fn last_consumer_close_finishes_producers_and_drops_the_queue() {
        let rt = RuntimeState::default();
        let exchanger = LocalExchanger::new_handoff(1, 2, 8, Arc::new(ExprArena::default()));
        let sink_factory = LocalExchangeSinkFactory::new(-1, Arc::clone(&exchanger));
        let source_factory = LocalExchangeSourceFactory::new(-1, 1, Arc::clone(&exchanger));
        let mut sink = sink_factory.create(1, 0);
        install_actual_creator(sink.as_mut(), &rt);
        let mut first = source_factory.create(2, 0);
        let mut second = source_factory.create(2, 1);
        push(&mut sink, &rt, &[1]);
        push(&mut sink, &rt, &[2]);

        let early_finish = sink
            .as_processor_ref()
            .expect("sink op")
            .early_finish_observable()
            .expect("an open handoff sink exposes its early-finish observable");
        let before = early_finish.generation();
        first.close().expect("close first consumer");
        assert_eq!(
            early_finish.generation(),
            before,
            "one consumer is still reading"
        );
        second.close().expect("close second consumer");

        assert!(exchanger.all_consumers_closed());
        assert!(
            early_finish.generation() > before,
            "producers must be woken"
        );
        assert!(
            sink.is_finished(),
            "nothing a producer pushes can be read any more"
        );
        assert!(!need_input(sink.as_ref()));
        assert_eq!(
            exchanger
                .partition_buffered_chunks(0)
                .map(|(chunks, _)| chunks),
            Some(0)
        );
        // A late push after every consumer left is dropped, not queued.
        push(&mut sink, &rt, &[3]);
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
        use crate::exec::node::exchange_source::ExchangeSourceNode;
        use crate::exec::operators::exchange_source::ExchangeSourceFactory;
        use crate::exec::operators::limit_processor::LimitProcessorFactory;
        use crate::exec::pipeline::binding::ExchangeBinding;
        use crate::exec::pipeline::driver::{DriverState, PipelineDriver};
        use crate::exec::pipeline::operator::BlockedReason;
        use crate::runtime::exchange::ExchangeKey;
        use crate::runtime::fragment::io::exchange::in_process_test_exchange_receiver_port;
        use std::time::Duration;

        let state = Arc::new(RuntimeState::default());
        let arena = Arc::new(ExprArena::default());
        let exchanger =
            LocalExchanger::new(1, 1, LocalExchangePartitionSpec::Single, Arc::clone(&arena));
        let sink = LocalExchangeSinkFactory::new(-1, Arc::clone(&exchanger));
        let local_source = LocalExchangeSourceFactory::new(-1, 1, Arc::clone(&exchanger));
        let schema = chunk_of(&[1]).chunk_schema_ref();
        let key = ExchangeKey {
            finst_id_hi: 97001,
            finst_id_lo: 97002,
            node_id: 97003,
        };
        let remote = ExchangeSourceFactory::new_native(
            ExchangeSourceNode::new(key.node_id, Duration::from_secs(60), schema),
            ExchangeBinding {
                key,
                expected_senders: 1,
                receiver_port: in_process_test_exchange_receiver_port(),
            },
            arena,
        )
        .expect("remote source");
        let mut source = remote.create(1, 0);
        source.prepare().expect("prepare remote source");
        let source_observable = source
            .as_processor_ref()
            .unwrap()
            .source_observable()
            .unwrap();
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
        assert!(deadline.is_some(), "remote idle deadline is preserved");
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
            wait.generation() > generation,
            "consumer close must invalidate the input wait"
        );
        producer.set_ready();
        assert_eq!(
            producer.process(Duration::from_secs(1)),
            DriverState::Finished,
            "sink-only notification permits actual driver completion"
        );
        // The same target also receives source events, including an event
        // overlapping downstream completion. Neither event replaces the other.
        source_observable.notify_observers();
        assert!(wait.generation() > generation + 1);
        assert!(exchanger.all_consumers_closed());
        let weak_wait = Arc::downgrade(&wait);
        drop(wait);
        drop(producer);
        assert!(
            weak_wait.upgrade().is_none(),
            "forwarding does not retain the driver wait"
        );
    }

    #[test]
    fn local_exchange_closed_partition_does_not_block_necessary_sibling() {
        let state = RuntimeState::default();
        let exchanger = LocalExchanger::new_with_limits(
            2,
            1,
            LocalExchangePartitionSpec::InputSlotIds(vec![SlotId::new(1)]),
            Arc::new(ExprArena::default()),
            1024 * 1024,
            1,
        );
        let sink_factory = LocalExchangeSinkFactory::new(-1, Arc::clone(&exchanger));
        let source_factory = LocalExchangeSourceFactory::new(-1, 2, Arc::clone(&exchanger));
        let mut sink = sink_factory.create(1, 0);
        install_actual_creator(sink.as_mut(), &state);
        let mut departed = source_factory.create(2, 0);
        let mut sibling = source_factory.create(2, 1);
        sink.as_processor_mut()
            .unwrap()
            .push_chunk(&state, chunk_of(&(0..64).collect::<Vec<_>>()))
            .unwrap();
        let before = exchanger.stats_snapshot();
        assert!(before.partitions[0].buffered_chunks > 0);
        let sibling_rows = before.partitions[1].pushed_rows;
        assert!(sibling_rows > 0);
        let wake = exchanger.sink_observable();
        let generation = wake.generation();
        departed.close().unwrap();
        departed.cancel();
        departed
            .as_processor_mut()
            .unwrap()
            .set_finishing(&state)
            .unwrap();
        assert!(
            !sink.is_finished(),
            "a necessary sibling keeps its producer live"
        );
        assert_eq!(
            wake.generation(),
            generation + 1,
            "close/cancel/finishing settle only once"
        );
        assert_eq!(exchanger.stats_snapshot().partitions[0].buffered_chunks, 0);
        let mut rows = 0;
        while let Some(chunk) = sibling
            .as_processor_mut()
            .unwrap()
            .pull_chunk(&state)
            .unwrap()
        {
            rows += chunk.len() as u64;
        }
        assert_eq!(rows, sibling_rows);
        assert!(
            sink.as_processor_ref().unwrap().need_input(),
            "departed queue releases backpressure"
        );
        sink.as_processor_mut()
            .unwrap()
            .push_chunk(&state, chunk_of(&(64..128).collect::<Vec<_>>()))
            .unwrap();
        assert_eq!(
            exchanger.stats_snapshot().partitions[0].buffered_chunks,
            0,
            "late partition output is discarded"
        );
        let expected = exchanger.stats_snapshot().partitions[1].pushed_rows;
        sink.as_processor_mut()
            .unwrap()
            .set_finishing(&state)
            .unwrap();
        while let Some(chunk) = sibling
            .as_processor_mut()
            .unwrap()
            .pull_chunk(&state)
            .unwrap()
        {
            rows += chunk.len() as u64;
        }
        assert_eq!(rows, expected);
        assert!(
            sibling.is_finished(),
            "necessary sibling receives normal EOS"
        );
        sibling.close().unwrap();
        assert!(exchanger.all_consumers_closed());
    }
}
