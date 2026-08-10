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
//! Split stream sink for partitioned exchange output.
//!
//! Responsibilities:
//! - Splits incoming chunks into disjoint per-destination subsets based on split expressions.
//! - Sends split chunks to dedicated downstream data stream sinks.
//! - Preserves sink completion signaling.

use std::sync::Arc;

use arrow::array::{Array, BooleanArray};
use arrow::compute::filter_record_batch;

use crate::exec::chunk::Chunk;
use crate::exec::expr::{ExprArena, ExprId};
use crate::exec::pipeline::operator::{Operator, ProcessorOperator};
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::exec::pipeline::schedule::observer::Observable;
use crate::runtime::fragment::io::ExchangeFrameTransmitter;
use crate::runtime::mem_tracker::MemTracker;
use crate::runtime::profile::OperatorProfiles;
use crate::runtime::runtime_state::RuntimeState;
use novarocks_types::UniqueId;

use super::DataStreamSinkFactory;
use crate::exec::fragment::sink::DataStreamSinkFactoryInput;

struct InnerSinkSpec {
    factory: DataStreamSinkFactory,
}

/// Factory for split stream sinks that partition rows to multiple remote channels.
pub struct SplitDataStreamSinkFactory {
    name: String,
    init_error: Option<String>,
    split_arena: Arc<ExprArena>,
    split_exprs: Vec<ExprId>,
    fanout: bool,
    sinks: Vec<InnerSinkSpec>,
}

impl SplitDataStreamSinkFactory {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        sinks: Vec<DataStreamSinkFactoryInput>,
        fragment_instance_id: UniqueId,
        sender_id: Option<i32>,
        partition_arena: ExprArena,
        plan_node_id: i32,
        split_arena: Arc<ExprArena>,
        split_exprs: Vec<ExprId>,
        fanout: bool,
        transmitter: Arc<dyn ExchangeFrameTransmitter>,
    ) -> Self {
        let name = if plan_node_id >= 0 {
            format!("SPLIT_DATA_STREAM_SINK (id={plan_node_id})")
        } else {
            "SPLIT_DATA_STREAM_SINK".to_string()
        };

        let mut init_error = None;
        let mut sinks_out = Vec::new();

        if sinks.is_empty() {
            init_error = Some("SPLIT_DATA_STREAM_SINK requires at least one sink".to_string());
        } else if split_exprs.len() != sinks.len() {
            init_error = Some(format!(
                "SPLIT_DATA_STREAM_SINK: split_exprs size {} != sinks size {}",
                split_exprs.len(),
                sinks.len()
            ));
        } else {
            for sink in sinks {
                sinks_out.push(InnerSinkSpec {
                    factory: DataStreamSinkFactory::new(
                        sink,
                        fragment_instance_id,
                        sender_id,
                        plan_node_id,
                        partition_arena.clone(),
                        Arc::clone(&transmitter),
                    ),
                });
            }
        }

        Self {
            name,
            init_error,
            split_arena,
            split_exprs,
            fanout,
            sinks: sinks_out,
        }
    }
}

impl OperatorFactory for SplitDataStreamSinkFactory {
    fn name(&self) -> &str {
        &self.name
    }

    fn create(&self, dop: i32, driver_id: i32) -> Box<dyn Operator> {
        let mut sinks = Vec::with_capacity(self.sinks.len());
        for spec in &self.sinks {
            sinks.push(InnerSinkRuntime {
                op: spec.factory.create(dop, driver_id),
            });
        }

        Box::new(SplitDataStreamSinkOperator {
            name: self.name.clone(),
            init_error: self.init_error.clone(),
            split_arena: Arc::clone(&self.split_arena),
            split_exprs: self.split_exprs.clone(),
            fanout: self.fanout,
            sinks,
            finishing: false,
        })
    }

    fn is_sink(&self) -> bool {
        true
    }
}

struct InnerSinkRuntime {
    op: Box<dyn Operator>,
}

struct SplitDataStreamSinkOperator {
    name: String,
    init_error: Option<String>,
    split_arena: Arc<ExprArena>,
    split_exprs: Vec<ExprId>,
    fanout: bool,
    sinks: Vec<InnerSinkRuntime>,
    finishing: bool,
}

impl Operator for SplitDataStreamSinkOperator {
    fn name(&self) -> &str {
        &self.name
    }

    fn set_mem_tracker(&mut self, tracker: Arc<MemTracker>) {
        for sink in &mut self.sinks {
            sink.op.set_mem_tracker(Arc::clone(&tracker));
        }
    }

    fn set_profiles(&mut self, profiles: OperatorProfiles) {
        for sink in &mut self.sinks {
            sink.op.set_profiles(profiles.clone());
        }
    }

    fn bind_runtime_state(&mut self, state: &RuntimeState) -> Result<(), String> {
        for sink in &mut self.sinks {
            sink.op.bind_runtime_state(state)?;
        }
        Ok(())
    }

    fn prepare(&mut self) -> Result<(), String> {
        for sink in &mut self.sinks {
            sink.op.prepare()?;
        }
        Ok(())
    }

    fn close(&mut self) -> Result<(), String> {
        for sink in &mut self.sinks {
            sink.op.close()?;
        }
        Ok(())
    }

    fn cancel(&mut self) {
        for sink in &mut self.sinks {
            sink.op.cancel();
        }
    }

    fn as_processor_mut(&mut self) -> Option<&mut dyn ProcessorOperator> {
        Some(self)
    }

    fn as_processor_ref(&self) -> Option<&dyn ProcessorOperator> {
        Some(self)
    }

    fn is_finished(&self) -> bool {
        self.finishing && self.sinks.iter().all(|sink| sink.op.is_finished())
    }
}

impl ProcessorOperator for SplitDataStreamSinkOperator {
    fn need_input(&self) -> bool {
        if self.is_finished() || self.finishing {
            return false;
        }
        for sink in &self.sinks {
            let Some(inner) = sink.op.as_processor_ref() else {
                return false;
            };
            if !inner.need_input() {
                return false;
            }
        }
        true
    }

    fn has_output(&self) -> bool {
        false
    }

    fn push_chunk(&mut self, state: &RuntimeState, chunk: Chunk) -> Result<(), String> {
        if let Some(err) = self.init_error.as_ref() {
            return Err(err.clone());
        }
        if self.is_finished() || self.finishing {
            return Ok(());
        }
        if chunk.is_empty() || self.sinks.is_empty() {
            return Ok(());
        }

        let split_chunks =
            split_chunk_by_exprs(&self.split_arena, &self.split_exprs, chunk, self.fanout)?;
        if split_chunks.len() != self.sinks.len() {
            return Err(format!(
                "split chunk output size {} != sink size {}",
                split_chunks.len(),
                self.sinks.len()
            ));
        }
        for (sink, part) in self.sinks.iter_mut().zip(split_chunks.into_iter()) {
            let Some(part) = part else {
                continue;
            };
            let inner = sink
                .op
                .as_processor_mut()
                .ok_or_else(|| "inner data stream op missing processor operator".to_string())?;
            inner.push_chunk(state, part)?;
        }
        Ok(())
    }

    fn pull_chunk(&mut self, _state: &RuntimeState) -> Result<Option<Chunk>, String> {
        Ok(None)
    }

    fn set_finishing(&mut self, state: &RuntimeState) -> Result<(), String> {
        if let Some(err) = self.init_error.as_ref() {
            return Err(err.clone());
        }
        if self.finishing {
            return Ok(());
        }
        for sink in &mut self.sinks {
            let inner = sink
                .op
                .as_processor_mut()
                .ok_or_else(|| "inner data stream op missing processor operator".to_string())?;
            inner.set_finishing(state)?;
        }
        self.finishing = true;
        Ok(())
    }

    fn sink_observable(&self) -> Option<Arc<Observable>> {
        if self.is_finished() {
            return None;
        }
        // Return the first inner sink's observable unconditionally.
        // Checking need_input() here would be a TOCTOU race: by the time the
        // scheduler calls sink_observable(), the blocking inner sink may already
        // be ready, causing a spurious None and a fragment failure.
        for sink in &self.sinks {
            let Some(inner) = sink.op.as_processor_ref() else {
                continue;
            };
            if let Some(obs) = inner.sink_observable() {
                return Some(obs);
            }
        }
        None
    }
}

fn split_chunk_by_exprs(
    arena: &ExprArena,
    split_exprs: &[ExprId],
    chunk: Chunk,
    fanout: bool,
) -> Result<Vec<Option<Chunk>>, String> {
    if split_exprs.is_empty() {
        return Ok(vec![]);
    }

    if fanout {
        // A row-mutation route is a filter, not an exclusive partition.  In
        // particular, a logical Replace must reach both the delete and
        // replacement-data routes.
        return split_exprs
            .iter()
            .map(|expr| {
                eval_split_mask(arena, *expr, &chunk)
                    .and_then(|mask| filter_chunk_by_mask(&chunk, &mask))
            })
            .collect();
    }

    let mut out = vec![None; split_exprs.len()];
    let mut remaining = chunk;

    for idx in (1..split_exprs.len()).rev() {
        if remaining.is_empty() {
            break;
        }
        let mask = eval_split_mask(arena, split_exprs[idx], &remaining)?;
        let matched = mask.iter().filter(|flag| **flag).count();
        if matched == 0 {
            continue;
        }
        if matched == remaining.len() {
            out[idx] = Some(remaining);
            return Ok(out);
        }

        out[idx] = filter_chunk_by_mask(&remaining, &mask)?;
        let remaining_mask = mask.into_iter().map(|flag| !flag).collect::<Vec<_>>();
        remaining = filter_chunk_by_mask(&remaining, &remaining_mask)?
            .ok_or_else(|| "split sink produced empty remaining chunk unexpectedly".to_string())?;
    }

    if !remaining.is_empty() {
        let mask = eval_split_mask(arena, split_exprs[0], &remaining)?;
        out[0] = filter_chunk_by_mask(&remaining, &mask)?;
    }
    Ok(out)
}

fn eval_split_mask(arena: &ExprArena, expr_id: ExprId, chunk: &Chunk) -> Result<Vec<bool>, String> {
    let predicate_array = arena.eval(expr_id, chunk)?;
    let mask = predicate_array
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| "split expr must return BOOLEAN".to_string())?;
    if mask.len() != chunk.len() {
        return Err(format!(
            "split expr result length {} != chunk length {}",
            mask.len(),
            chunk.len()
        ));
    }

    let mut out = Vec::with_capacity(mask.len());
    for i in 0..mask.len() {
        out.push(mask.is_valid(i) && mask.value(i));
    }
    Ok(out)
}

fn filter_chunk_by_mask(chunk: &Chunk, mask: &[bool]) -> Result<Option<Chunk>, String> {
    if mask.len() != chunk.len() {
        return Err(format!(
            "filter mask length {} != chunk length {}",
            mask.len(),
            chunk.len()
        ));
    }

    let selected = mask.iter().filter(|flag| **flag).count();
    if selected == 0 {
        return Ok(None);
    }
    if selected == chunk.len() {
        return Ok(Some(chunk.clone()));
    }

    let predicate = BooleanArray::from(mask.to_vec());
    let filtered = filter_record_batch(&chunk.batch, &predicate)
        .map_err(|e| format!("split sink filter chunk failed: {e}"))?;
    Ok(Some(Chunk::new_like(filtered, chunk)))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    use arrow::array::{BooleanArray, Int32Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    use super::split_chunk_by_exprs;
    use crate::exec::chunk::{Chunk, ChunkSchema};
    use crate::exec::expr::{ExprArena, ExprNode};
    use crate::exec::pipeline::operator::{Operator, ProcessorOperator};
    use crate::exec::pipeline::schedule::observer::Observable;
    use crate::runtime::runtime_state::RuntimeState;
    use novarocks_types::SlotId;

    use super::{InnerSinkRuntime, SplitDataStreamSinkOperator};

    struct PendingFinishSink {
        name: String,
        finishing: bool,
        finished: Arc<AtomicBool>,
        observable: Arc<Observable>,
    }

    impl PendingFinishSink {
        fn new(name: &str, finished: Arc<AtomicBool>) -> Self {
            Self {
                name: name.to_string(),
                finishing: false,
                finished,
                observable: Arc::new(Observable::new()),
            }
        }
    }

    impl Operator for PendingFinishSink {
        fn name(&self) -> &str {
            &self.name
        }

        fn is_finished(&self) -> bool {
            self.finished.load(Ordering::SeqCst)
        }

        fn as_processor_mut(&mut self) -> Option<&mut dyn ProcessorOperator> {
            Some(self)
        }

        fn as_processor_ref(&self) -> Option<&dyn ProcessorOperator> {
            Some(self)
        }
    }

    impl ProcessorOperator for PendingFinishSink {
        fn need_input(&self) -> bool {
            !self.finishing && !self.is_finished()
        }

        fn has_output(&self) -> bool {
            false
        }

        fn push_chunk(&mut self, _state: &RuntimeState, _chunk: Chunk) -> Result<(), String> {
            Ok(())
        }

        fn pull_chunk(&mut self, _state: &RuntimeState) -> Result<Option<Chunk>, String> {
            Ok(None)
        }

        fn set_finishing(&mut self, _state: &RuntimeState) -> Result<(), String> {
            self.finishing = true;
            Ok(())
        }

        fn sink_observable(&self) -> Option<Arc<Observable>> {
            (!self.is_finished()).then(|| Arc::clone(&self.observable))
        }
    }

    #[test]
    fn split_sink_waits_for_inner_sinks_to_finish() {
        let first_done = Arc::new(AtomicBool::new(false));
        let second_done = Arc::new(AtomicBool::new(false));
        let mut op = SplitDataStreamSinkOperator {
            name: "SPLIT_DATA_STREAM_SINK(test)".to_string(),
            init_error: None,
            split_arena: Arc::new(ExprArena::default()),
            split_exprs: Vec::new(),
            fanout: false,
            sinks: vec![
                InnerSinkRuntime {
                    op: Box::new(PendingFinishSink::new("first", Arc::clone(&first_done))),
                },
                InnerSinkRuntime {
                    op: Box::new(PendingFinishSink::new("second", Arc::clone(&second_done))),
                },
            ],
            finishing: false,
        };

        let state = RuntimeState::default();
        op.set_finishing(&state).expect("set finishing");
        assert!(!op.is_finished(), "wrapper must wait for async inner sinks");
        assert!(op.sink_observable().is_some());

        first_done.store(true, Ordering::SeqCst);
        assert!(!op.is_finished(), "wrapper must wait for every inner sink");

        second_done.store(true, Ordering::SeqCst);
        assert!(op.is_finished());
        assert!(op.sink_observable().is_none());
    }

    #[test]
    fn split_sink_applies_the_first_branch_predicate_instead_of_using_it_as_fallback() {
        let predicate_slot = SlotId::new(1);
        let value_slot = SlotId::new(2);
        let schema = Arc::new(Schema::new(vec![
            Field::new("selected", DataType::Boolean, false),
            Field::new("value", DataType::Int32, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(BooleanArray::from(vec![true, false])),
                Arc::new(Int32Array::from(vec![10, 20])),
            ],
        )
        .expect("input batch");
        let chunk_schema = ChunkSchema::try_ref_from_schema_and_slot_ids(
            schema.as_ref(),
            &[predicate_slot, value_slot],
        )
        .expect("input chunk schema");
        let chunk = Chunk::new_with_chunk_schema(batch, chunk_schema);

        let mut arena = ExprArena::default();
        let predicate = arena.push_typed(ExprNode::SlotId(predicate_slot), DataType::Boolean);
        let split = split_chunk_by_exprs(&arena, &[predicate], chunk, false).expect("split chunk");
        let selected = split[0].as_ref().expect("matching first branch rows");

        assert_eq!(selected.len(), 1);
        let values = selected
            .batch
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("value column");
        assert_eq!(values.values(), &[10]);
    }
}
