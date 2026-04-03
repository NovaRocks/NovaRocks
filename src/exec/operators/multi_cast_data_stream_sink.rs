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
//! Multi-cast stream sink for replicated exchange output.
//!
//! Responsibilities:
//! - Sends the same serialized chunk stream to multiple remote destinations.
//! - Coordinates per-destination transport state and completion signaling.
//!
//! Key exported interfaces:
//! - Types: `MultiCastDataStreamSinkFactory`.
//!
//! Current limitations:
//! - Implements only the execution semantics currently wired by novarocks plan lowering and pipeline builder.
//! - Unsupported states should be surfaced as explicit runtime errors instead of fallback behavior.

use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

use crate::exec::chunk::Chunk;
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
    limit_remaining: Option<Arc<AtomicI64>>,
    factory: DataStreamSinkFactory,
}

/// Factory for multi-cast stream sinks that replicate output to multiple remote channels.
pub(crate) struct MultiCastDataStreamSinkFactory {
    name: String,
    init_error: Option<String>,
    sinks: Vec<InnerSinkSpec>,
}

impl MultiCastDataStreamSinkFactory {
    pub(crate) fn new(
        multi_cast: data_sinks::TMultiCastDataStreamSink,
        exec_params: internal_service::TPlanFragmentExecParams,
        layout: Layout,
        plan_node_id: i32,
        last_query_id: Option<String>,
        fe_addr: Option<types::TNetworkAddress>,
    ) -> Self {
        let name = if plan_node_id >= 0 {
            format!("MULTI_CAST_DATA_STREAM_SINK (id={plan_node_id})")
        } else {
            "MULTI_CAST_DATA_STREAM_SINK".to_string()
        };
        let mut init_error = None;
        let mut sinks = Vec::new();

        if multi_cast.sinks.len() != multi_cast.destinations.len() {
            init_error = Some(format!(
                "MULTI_CAST_DATA_STREAM_SINK: sinks size {} != destinations size {}",
                multi_cast.sinks.len(),
                multi_cast.destinations.len()
            ));
        } else {
            for (sink, destinations) in multi_cast
                .sinks
                .into_iter()
                .zip(multi_cast.destinations.into_iter())
            {
                let limit_remaining = match sink.limit {
                    Some(v) if v != -1 && v >= 0 => Some(Arc::new(AtomicI64::new(v))),
                    _ => None,
                };

                let mut params = exec_params.clone();
                params.destinations = Some(destinations);

                sinks.push(InnerSinkSpec {
                    limit_remaining,
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
            sinks,
        }
    }
}

impl OperatorFactory for MultiCastDataStreamSinkFactory {
    fn name(&self) -> &str {
        &self.name
    }

    fn create(&self, dop: i32, driver_id: i32) -> Box<dyn Operator> {
        let mut sinks = Vec::with_capacity(self.sinks.len());
        for spec in &self.sinks {
            sinks.push(InnerSinkRuntime {
                limit_remaining: spec.limit_remaining.clone(),
                op: spec.factory.create(dop, driver_id),
            });
        }

        Box::new(MultiCastDataStreamSinkOperator {
            name: self.name.clone(),
            init_error: self.init_error.clone(),
            sinks,
            finished: false,
        })
    }

    fn is_sink(&self) -> bool {
        true
    }
}

struct InnerSinkRuntime {
    limit_remaining: Option<Arc<AtomicI64>>,
    op: Box<dyn Operator>,
}

struct MultiCastDataStreamSinkOperator {
    name: String,
    init_error: Option<String>,
    sinks: Vec<InnerSinkRuntime>,
    finished: bool,
}

impl Operator for MultiCastDataStreamSinkOperator {
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

impl ProcessorOperator for MultiCastDataStreamSinkOperator {
    fn need_input(&self) -> bool {
        if self.finished {
            return false;
        }
        for sink in &self.sinks {
            let allowed = sink
                .limit_remaining
                .as_ref()
                .map(|remaining| remaining.load(Ordering::SeqCst) > 0)
                .unwrap_or(true);
            if !allowed {
                continue;
            }
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

        let input_chunk = chunk;
        let mut should_send = Vec::with_capacity(self.sinks.len());
        for sink in &mut self.sinks {
            let allowed = sink
                .limit_remaining
                .as_ref()
                .map(|remaining| remaining.load(Ordering::SeqCst) > 0)
                .unwrap_or(true);
            should_send.push(allowed);
        }

        let mut per_sink_chunks = Vec::with_capacity(self.sinks.len());
        for (sink, allowed) in self.sinks.iter_mut().zip(should_send.iter().copied()) {
            if !allowed {
                per_sink_chunks.push(None);
                continue;
            }
            let limited = apply_limit(&input_chunk, sink.limit_remaining.as_ref())
                .map_err(|e| e.to_string())?;
            per_sink_chunks.push(limited);
        }

        for (sink, chunk) in self.sinks.iter_mut().zip(per_sink_chunks.into_iter()) {
            let Some(chunk) = chunk else {
                continue;
            };
            let inner = sink
                .op
                .as_processor_mut()
                .ok_or_else(|| "inner data stream op missing processor operator".to_string())?;
            inner.push_chunk(state, chunk)?;
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
            let allowed = sink
                .limit_remaining
                .as_ref()
                .map(|remaining| remaining.load(Ordering::SeqCst) > 0)
                .unwrap_or(true);
            if !allowed {
                continue;
            }
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

fn apply_limit(chunk: &Chunk, remaining: Option<&Arc<AtomicI64>>) -> Result<Option<Chunk>, String> {
    let Some(remaining) = remaining else {
        return Ok(Some(chunk.clone()));
    };

    let rows = chunk.len() as i64;
    if rows <= 0 {
        return Ok(None);
    }

    loop {
        let cur = remaining.load(Ordering::SeqCst);
        if cur <= 0 {
            return Ok(None);
        }
        let send = rows.min(cur);
        if remaining
            .compare_exchange(cur, cur - send, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            let send = send as usize;
            if send == 0 {
                return Ok(None);
            }
            if send == chunk.len() {
                return Ok(Some(chunk.clone()));
            }
            return Ok(Some(chunk.slice(0, send)));
        }
    }
}
