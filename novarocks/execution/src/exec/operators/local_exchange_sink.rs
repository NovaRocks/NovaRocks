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
        })
    }

    fn is_sink(&self) -> bool {
        true
    }
}

struct LocalExchangeSinkOperator {
    name: String,
    owner_node_id: i32,
    driver_id: i32,
    exchanger: Arc<LocalExchanger>,
    finished: bool,
    logged_first_input: bool,
}

impl Operator for LocalExchangeSinkOperator {
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
        self.finished || self.exchanger.is_all_sources_finished()
    }
}

impl ProcessorOperator for LocalExchangeSinkOperator {
    fn need_input(&self) -> bool {
        !self.is_finished() && self.exchanger.need_input()
    }

    fn has_output(&self) -> bool {
        false
    }

    fn push_chunk(&mut self, state: &RuntimeState, chunk: Chunk) -> Result<(), String> {
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
        self.exchanger.accept(state, chunk, self.driver_id as usize)
    }

    fn pull_chunk(&mut self, _state: &RuntimeState) -> Result<Option<Chunk>, String> {
        Ok(None)
    }

    fn set_finishing(&mut self, _state: &RuntimeState) -> Result<(), String> {
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
        if self.finished {
            return None;
        }
        Some(self.exchanger.sink_observable())
    }
}

#[cfg(test)]
mod tests {
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
        {
            let batch = batch;
            let chunk_schema = crate::exec::chunk::ChunkSchema::try_ref_from_schema_and_slot_ids(
                batch.schema().as_ref(),
                &[SlotId::new(1)],
            )
            .expect("chunk schema");
            Chunk::new_with_chunk_schema(batch, chunk_schema)
        }
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
        assert!(exchanger.is_all_sources_finished());
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
        assert!(exchanger.is_all_sources_finished());
    }
}
