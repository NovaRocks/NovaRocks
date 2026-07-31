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

//! Internal pipeline sink for a bounded statistics fragment partial.
//!
//! It intentionally has no `FragmentResultSession`: statistic collection is
//! not a client result and must never materialize MySQL rows. The caller takes
//! the encoded partial only after every local driver has finished.

use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::{Arc, Mutex};

use arrow::datatypes::SchemaRef;
use bytes::Bytes;
use novarocks_spi::connector::StatisticsMetricRequest;

use crate::exec::chunk::Chunk;
use crate::exec::pipeline::operator::{Operator, ProcessorOperator};
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::query_execution::statistics::StatisticsBatchCollector;
use crate::runtime::runtime_state::RuntimeState;

/// Read-only completion handle retained by the fragment host. A terminal
/// report may consume the partial exactly once after the local pipeline has
/// completed; no StateStore or cross-query registry stores this handle.
#[derive(Clone)]
pub struct StatisticsSinkHandle {
    shared: Arc<StatisticsSinkShared>,
}

impl StatisticsSinkHandle {
    pub fn take_fragment_payload(&self) -> Result<Option<Bytes>, String> {
        let mut completion = self
            .shared
            .completion
            .lock()
            .map_err(|_| "statistics sink completion lock poisoned".to_string())?;
        match completion.take() {
            Some(Ok(payload)) => Ok(Some(payload)),
            Some(Err(error)) => Err(error),
            None => Ok(None),
        }
    }
}

struct StatisticsSinkShared {
    remaining_drivers: AtomicI32,
    collector: Mutex<Option<StatisticsBatchCollector>>,
    completion: Mutex<Option<Result<Bytes, String>>>,
}

impl StatisticsSinkShared {
    fn new(collector: StatisticsBatchCollector) -> Self {
        Self {
            remaining_drivers: AtomicI32::new(-1),
            collector: Mutex::new(Some(collector)),
            completion: Mutex::new(None),
        }
    }

    fn init_driver_count(&self, dop: i32) {
        let dop = dop.max(1);
        let _ =
            self.remaining_drivers
                .compare_exchange(-1, dop, Ordering::AcqRel, Ordering::Acquire);
    }

    fn collect(&self, chunk: &Chunk) -> Result<(), String> {
        let mut collector = self
            .collector
            .lock()
            .map_err(|_| "statistics sink collector lock poisoned".to_string())?;
        collector
            .as_mut()
            .ok_or_else(|| "statistics sink received data after completion".to_string())?
            .push_batch(&chunk.batch)
            .map_err(|error| error.to_string())
    }

    fn finish_once(&self) -> Result<(), String> {
        let collector = self
            .collector
            .lock()
            .map_err(|_| "statistics sink collector lock poisoned".to_string())?
            .take()
            .ok_or_else(|| "statistics sink completed more than once".to_string())?;
        let payload = collector
            .finish_fragment_payload()
            .map_err(|error| error.to_string());
        if payload.is_ok() {
            // The cross-process statistics acceptance suite consumes stdout/stderr
            // only. Keep this marker at the exact BE-local completion boundary so
            // it proves a non-empty collection partial was produced here.
            eprintln!("NOVAROCKS_STATISTICS_FRAGMENT_COLLECTED");
        }
        let mut completion = self
            .completion
            .lock()
            .map_err(|_| "statistics sink completion lock poisoned".to_string())?;
        *completion = Some(payload.clone());
        payload.map(|_| ())
    }
}

/// Factory for the terminal sink of one local statistics fragment. The shared
/// collector synchronizes all pipeline drivers into one bounded partial.
pub struct StatisticsSinkFactory {
    name: String,
    shared: Arc<StatisticsSinkShared>,
}

impl StatisticsSinkFactory {
    pub fn try_new(
        schema: SchemaRef,
        metrics: StatisticsMetricRequest,
        plan_node_id: Option<i32>,
    ) -> Result<(Self, StatisticsSinkHandle), String> {
        let collector = StatisticsBatchCollector::try_new(schema, metrics)
            .map_err(|error| error.to_string())?;
        let plan_node_id = plan_node_id.filter(|id| *id >= 0).unwrap_or(-1);
        let shared = Arc::new(StatisticsSinkShared::new(collector));
        Ok((
            Self {
                name: format!("STATISTICS_SINK (plan_node_id={plan_node_id})"),
                shared: Arc::clone(&shared),
            },
            StatisticsSinkHandle { shared },
        ))
    }
}

impl OperatorFactory for StatisticsSinkFactory {
    fn name(&self) -> &str {
        &self.name
    }

    fn create(&self, dop: i32, _driver_id: i32) -> Box<dyn Operator> {
        self.shared.init_driver_count(dop);
        Box::new(StatisticsSinkOperator {
            name: self.name.clone(),
            shared: Arc::clone(&self.shared),
            finished: false,
        })
    }

    fn is_sink(&self) -> bool {
        true
    }
}

struct StatisticsSinkOperator {
    name: String,
    shared: Arc<StatisticsSinkShared>,
    finished: bool,
}

impl Operator for StatisticsSinkOperator {
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
        self.finished
    }
}

impl ProcessorOperator for StatisticsSinkOperator {
    fn need_input(&self) -> bool {
        !self.finished
    }

    fn has_output(&self) -> bool {
        false
    }

    fn push_chunk(&mut self, _state: &RuntimeState, chunk: Chunk) -> Result<(), String> {
        if self.finished || chunk.is_empty() {
            return Ok(());
        }
        self.shared.collect(&chunk)
    }

    fn pull_chunk(&mut self, _state: &RuntimeState) -> Result<Option<Chunk>, String> {
        Ok(None)
    }

    fn set_finishing(&mut self, _state: &RuntimeState) -> Result<(), String> {
        if self.finished {
            return Ok(());
        }
        self.finished = true;
        let previous = self.shared.remaining_drivers.fetch_sub(1, Ordering::AcqRel);
        if previous <= 0 {
            return Err("STATISTICS_SINK driver count underflow".to_string());
        }
        if previous == 1 {
            self.shared.finish_once()?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use novarocks_spi::connector::{StatisticsMetric, StatisticsMetricRequest};

    use super::*;
    use crate::common::ids::SlotId;
    use crate::exec::chunk::ChunkSchema;
    use crate::exec::pipeline::operator::Operator;

    fn chunk(schema: SchemaRef, values: Vec<i64>) -> Chunk {
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(values))],
        )
        .expect("batch");
        let chunk_schema =
            ChunkSchema::try_ref_from_schema_and_slot_ids(schema.as_ref(), &[SlotId::new(1)])
                .expect("chunk schema");
        Chunk::new_with_chunk_schema(batch, chunk_schema)
    }

    #[test]
    fn sink_emits_one_bounded_terminal_partial_after_all_drivers_finish() {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let metrics = StatisticsMetricRequest::try_new(vec![
            StatisticsMetric::RowCount,
            StatisticsMetric::ThetaNdv { column: "v".into() },
        ])
        .expect("metrics");
        let (factory, handle) =
            StatisticsSinkFactory::try_new(schema.clone(), metrics, Some(7)).expect("factory");
        let state = RuntimeState::new(None, None, None, None, None, None, None, None);
        let mut first = factory.create(2, 0);
        let mut second = factory.create(2, 1);
        first
            .as_processor_mut()
            .expect("processor")
            .push_chunk(&state, chunk(schema.clone(), vec![1, 2]))
            .expect("collect first");
        second
            .as_processor_mut()
            .expect("processor")
            .push_chunk(&state, chunk(schema, vec![2, 3]))
            .expect("collect second");
        first
            .as_processor_mut()
            .expect("processor")
            .set_finishing(&state)
            .expect("finish first");
        assert!(handle.take_fragment_payload().expect("pending").is_none());
        second
            .as_processor_mut()
            .expect("processor")
            .set_finishing(&state)
            .expect("finish second");
        let payload = handle
            .take_fragment_payload()
            .expect("completion")
            .expect("payload");
        let finalizer = crate::query_execution::statistics::StatisticsCollectionFinalizer::
            try_from_fragment_payload(&payload)
            .expect("decode partial");
        assert!(matches!(
            finalizer
                .metric_states(
                    &StatisticsMetricRequest::try_new(vec![StatisticsMetric::RowCount])
                        .expect("row metric"),
                )
                .get(&StatisticsMetric::RowCount),
            Some(novarocks_spi::connector::StatisticsMetricState::Available(
                novarocks_spi::connector::StatisticsMetricValue::U64(4)
            ))
        ));
        assert!(handle.take_fragment_payload().expect("consumed").is_none());
    }
}
