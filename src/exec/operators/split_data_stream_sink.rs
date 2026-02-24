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
use crate::lower::layout::Layout;
use crate::runtime::mem_tracker::MemTracker;
use crate::runtime::profile::OperatorProfiles;
use crate::runtime::runtime_state::RuntimeState;
use crate::{data_sinks, internal_service, types};

use super::DataStreamSinkFactory;

struct InnerSinkSpec {
    factory: DataStreamSinkFactory,
}

/// Factory for split stream sinks that partition rows to multiple remote channels.
pub(crate) struct SplitDataStreamSinkFactory {
    name: String,
    init_error: Option<String>,
    split_arena: Arc<ExprArena>,
    split_exprs: Vec<ExprId>,
    sinks: Vec<InnerSinkSpec>,
}

impl SplitDataStreamSinkFactory {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        split: data_sinks::TSplitDataStreamSink,
        exec_params: internal_service::TPlanFragmentExecParams,
        layout: Layout,
        plan_node_id: i32,
        last_query_id: Option<String>,
        fe_addr: Option<types::TNetworkAddress>,
        split_arena: Arc<ExprArena>,
        split_exprs: Vec<ExprId>,
    ) -> Self {
        let name = if plan_node_id >= 0 {
            format!("SPLIT_DATA_STREAM_SINK (id={plan_node_id})")
        } else {
            "SPLIT_DATA_STREAM_SINK".to_string()
        };

        let mut init_error = None;
        let mut sinks_out = Vec::new();

        let sinks = split.sinks.unwrap_or_default();
        let destinations = split.destinations.unwrap_or_default();

        if sinks.is_empty() {
            init_error = Some("SPLIT_DATA_STREAM_SINK requires at least one sink".to_string());
        } else if sinks.len() != destinations.len() {
            init_error = Some(format!(
                "SPLIT_DATA_STREAM_SINK: sinks size {} != destinations size {}",
                sinks.len(),
                destinations.len()
            ));
        } else if split_exprs.len() != sinks.len() {
            init_error = Some(format!(
                "SPLIT_DATA_STREAM_SINK: split_exprs size {} != sinks size {}",
                split_exprs.len(),
                sinks.len()
            ));
        } else {
            for (sink, destinations) in sinks.into_iter().zip(destinations.into_iter()) {
                let mut params = exec_params.clone();
                params.destinations = Some(destinations);

                sinks_out.push(InnerSinkSpec {
                    factory: DataStreamSinkFactory::new(
                        sink,
                        params,
                        layout.clone(),
                        plan_node_id,
                        last_query_id.clone(),
                        fe_addr.clone(),
                    ),
                });
            }
        }

        Self {
            name,
            init_error,
            split_arena,
            split_exprs,
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
            sinks,
            finished: false,
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
    sinks: Vec<InnerSinkRuntime>,
    finished: bool,
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
        self.finished
    }
}

impl ProcessorOperator for SplitDataStreamSinkOperator {
    fn need_input(&self) -> bool {
        if self.finished {
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
        if self.finished {
            return Ok(());
        }
        if chunk.is_empty() || self.sinks.is_empty() {
            return Ok(());
        }

        let split_chunks = split_chunk_by_exprs(&self.split_arena, &self.split_exprs, chunk)?;
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
        if self.finished {
            return Ok(());
        }
        for sink in &mut self.sinks {
            let inner = sink
                .op
                .as_processor_mut()
                .ok_or_else(|| "inner data stream op missing processor operator".to_string())?;
            inner.set_finishing(state)?;
        }
        self.finished = true;
        Ok(())
    }

    fn sink_observable(&self) -> Option<Arc<Observable>> {
        if self.finished {
            return None;
        }
        for sink in &self.sinks {
            let Some(inner) = sink.op.as_processor_ref() else {
                return None;
            };
            if !inner.need_input() {
                return inner.sink_observable();
            }
        }
        None
    }
}

fn split_chunk_by_exprs(
    arena: &ExprArena,
    split_exprs: &[ExprId],
    chunk: Chunk,
) -> Result<Vec<Option<Chunk>>, String> {
    if split_exprs.is_empty() {
        return Ok(vec![]);
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
        out[0] = Some(remaining);
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
    Ok(Some(Chunk::new(filtered)))
}
