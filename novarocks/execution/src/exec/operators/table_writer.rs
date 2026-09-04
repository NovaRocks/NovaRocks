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

//! The `TableWriter` operator: one writer per pipeline driver.
//!
//! `bind_runtime_state` starts a task-local actor which opens the driver's own
//! [`ConnectorBatchWriter`](novarocks_spi::connector::write_stack::ConnectorBatchWriter)
//! with a physical context that includes that driver's
//! id. Nothing is shared between drivers: the append path takes no cross-driver
//! lock, no driver counts the others, and a failing driver cooperatively aborts
//! only the writer its actor owns.
//!
//! While open or draining, ordinary partial aggregation may emit zero or more
//! byte-bounded sparse `AGGREGATE_PARTIAL` batches. Only after both children
//! finish successfully does the operator emit exactly one `ROW_COUNT` row and
//! zero or more `COMMIT_FRAGMENT` rows. Canonical fragment bytes are produced
//! by the encoder port, never by this layer, and each fragment is charged
//! against the frozen single-fragment budget at egress.
//!
//! Design: ADR-0135 (docs/adr/ADR-0135-ordinary-aggregate-statistics-dataflow.md)

use std::collections::VecDeque;
use std::mem::size_of;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use arrow::array::{
    Array, ArrayRef, BinaryArray, BinaryBuilder, Int8Array, Int32Array, Int64Array, new_null_array,
};
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;

use novarocks_spi::connector::ConnectorRequestContext;
use novarocks_spi::connector::write_stack::{
    ConnectorOpenWriterRequest, ConnectorWriteExecution, ConnectorWriterHandle,
    MAX_CONNECTOR_COMMIT_FRAGMENT_BYTES, WriteTargetOrdinal, WriterRowKind, row_count_to_wire,
    target_ordinal_to_wire,
};

use crate::exec::chunk::{Chunk, ChunkSchema, record_batch_additional_bytes, record_batch_bytes};
use crate::exec::expr::agg::SealedExecutionFunctionSet;
use crate::exec::expr::{ExprArena, ExprNode};
use crate::exec::node::aggregate::{AggFunction, AggTypeSignature};
use crate::exec::node::table_write_relation::{
    ConnectorCommitFragmentEncoder, WriterMultiplexRelationSchema,
};
#[cfg(debug_assertions)]
use crate::exec::node::table_write_relation::{
    TableWriteAggregateBoundary, TableWriteAggregateGuard,
};
use crate::exec::node::table_writer::{
    TableWriterInputProjection, TableWriterNode, TableWriterPhysicalContextTemplate,
};
use crate::exec::operators::AggregateProcessorFactory;
use crate::exec::operators::blocked_duration::BlockedDuration;
use crate::exec::pipeline::async_writer::{AsyncWriterOwner, AsyncWriterQueueConfig};
use crate::exec::pipeline::operator::{Operator, ProcessorOperator};
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::exec::pipeline::schedule::observer::Observable;
use crate::runtime::mem_tracker::{MemTracker, TrackedBytes};
use crate::runtime::profile::OperatorProfiles;
use crate::runtime::runtime_state::RuntimeState;

/// Hard ceiling for one indivisible writer-relation row. The runtime exchange
/// batching setting is only a flush target: just like `DataStreamSink`, a
/// single row may exceed that target, but it must remain safely below the
/// 64 MiB native gRPC message limit.
const MAX_WRITER_MULTIPLEX_ROW_BYTES: usize = 16 * 1024 * 1024;

/// The immutable per-node facts every driver copies when it opens its writer.
struct TableWriterPlan {
    handle: ConnectorWriterHandle,
    target: WriteTargetOrdinal,
    execution: Arc<dyn ConnectorWriteExecution>,
    expected_schema: arrow::datatypes::SchemaRef,
    projection: TableWriterInputProjection,
    physical_template: TableWriterPhysicalContextTemplate,
    request_context: ConnectorRequestContext,
    fragment_encoder: Arc<dyn ConnectorCommitFragmentEncoder>,
    writer_multiplex_schema: WriterMultiplexRelationSchema,
    partial_aggregate_factory: Option<AggregateProcessorFactory>,
    #[cfg(debug_assertions)]
    aggregate_guard: Arc<dyn TableWriteAggregateGuard>,
}

/// Factory for per-driver table writers.
///
/// It deliberately holds no writer, no mutex, and no driver accounting: every
/// piece of mutable writer state lives inside the one operator that owns it.
pub struct TableWriterOperatorFactory {
    name: String,
    plan: Arc<TableWriterPlan>,
}

impl TableWriterOperatorFactory {
    /// Construct a composite writer with the exact process function set used
    /// to bind every ordinary partial aggregate in its typed tail.
    pub fn try_new(
        node: &TableWriterNode,
        function_set: Arc<SealedExecutionFunctionSet>,
    ) -> Result<Self, String> {
        let name = if node.node_id >= 0 {
            format!("TABLE_WRITER (id={})", node.node_id)
        } else {
            "TABLE_WRITER".to_string()
        };
        let partial_aggregate_factory = build_partial_aggregate_factory(node, function_set)?;
        Ok(Self {
            name,
            plan: Arc::new(TableWriterPlan {
                handle: node.handle().clone(),
                target: node.target(),
                execution: Arc::clone(node.execution()),
                expected_schema: Arc::clone(node.expected_schema()),
                projection: node.projection().clone(),
                physical_template: node.physical_template(),
                request_context: node.request_context().clone(),
                fragment_encoder: Arc::clone(node.fragment_encoder()),
                writer_multiplex_schema: node.writer_multiplex_schema().clone(),
                partial_aggregate_factory,
                #[cfg(debug_assertions)]
                aggregate_guard: Arc::clone(node.aggregate_guard()),
            }),
        })
    }
}

fn build_partial_aggregate_factory(
    node: &TableWriterNode,
    function_set: Arc<SealedExecutionFunctionSet>,
) -> Result<Option<AggregateProcessorFactory>, String> {
    let calls = &node.partial_aggregate_plan().calls;
    if calls.is_empty() {
        return Ok(None);
    }
    let mut arena = ExprArena::default();
    let mut functions = Vec::with_capacity(calls.len());
    let mut resolved = Vec::with_capacity(calls.len());
    let mut output_slots = Vec::with_capacity(calls.len());
    for call in calls {
        if call.resolved.argument_types.len() != 1 {
            return Err(format!(
                "writer partial aggregate {} requires exactly one planned input type, got {}",
                call.function_name,
                call.resolved.argument_types.len()
            ));
        }
        let input_type = call
            .resolved
            .argument_types
            .first()
            .ok_or_else(|| "writer partial aggregate input type is missing".to_string())?
            .clone();
        let input = arena.push_typed(ExprNode::SlotId(call.input_slot_id), input_type.clone());
        functions.push(AggFunction {
            name: call.function_name.to_string(),
            inputs: vec![input],
            input_is_intermediate: false,
            types: Some(AggTypeSignature {
                intermediate_type: Some(call.resolved.intermediate_type.clone()),
                output_type: Some(call.resolved.output_type.clone()),
                input_arg_type: Some(input_type),
            }),
            order: Default::default(),
        });
        resolved.push(call.resolved.clone());
        let output_slot = node
            .writer_multiplex_schema()
            .chunk_schema()
            .slot(call.intermediate_slot_id)
            .cloned()
            .ok_or_else(|| {
                format!(
                    "writer partial aggregate output slot {} is absent from the multiplex schema",
                    call.intermediate_slot_id
                )
            })?;
        if output_slot.data_type() != &call.resolved.intermediate_type {
            return Err(format!(
                "writer partial aggregate output slot {} type mismatch: relation={:?}, resolved={:?}",
                call.intermediate_slot_id,
                output_slot.data_type(),
                call.resolved.intermediate_type
            ));
        }
        output_slots.push(output_slot);
    }
    let output_chunk_schema = Arc::new(ChunkSchema::try_new(output_slots)?);
    AggregateProcessorFactory::new_native(
        node.node_id,
        Arc::new(arena),
        Vec::new(),
        functions,
        function_set,
        resolved,
        true,
        false,
        output_chunk_schema,
        Vec::new(),
        None,
        1,
        None,
    )
    .map(Some)
}

impl TableWriterOperatorFactory {
    fn create_operator(&self, dop: i32, driver_id: i32) -> TableWriterOperator {
        let plan = &self.plan;
        let physical = plan
            .physical_template
            .for_driver(u32::try_from(driver_id.max(0)).unwrap_or(u32::MAX));
        let request = ConnectorOpenWriterRequest {
            handle: plan.handle.clone(),
            target: plan.target,
            expected_schema: Arc::clone(&plan.expected_schema),
            physical,
            context: plan.request_context.clone(),
        };
        let target = plan.target;
        let fragment_encoder = Arc::clone(&plan.fragment_encoder);
        let result_tracker = Arc::new(Mutex::new(None));
        let result_tracker_for_actor = Arc::clone(&result_tracker);
        let writer = AsyncWriterOwner::new(
            Arc::clone(&plan.execution),
            request,
            AsyncWriterQueueConfig::default(),
            Box::new(move |accepted_rows, fragments| {
                encode_writer_completion(
                    target,
                    fragment_encoder.as_ref(),
                    accepted_rows,
                    fragments,
                    result_tracker_for_actor
                        .lock()
                        .expect("table writer result tracker lock")
                        .clone(),
                )
            }),
        );
        let partial_aggregate = plan
            .partial_aggregate_factory
            .as_ref()
            .map(|factory| factory.create(dop, driver_id));
        let readiness_observable = Arc::new(Observable::new());
        forward_observable(&writer.observable(), &readiness_observable);
        if let Some(partial) = partial_aggregate.as_ref()
            && let Some(processor) = partial.as_processor_ref()
        {
            if let Some(observable) = processor.sink_observable() {
                forward_observable(&observable, &readiness_observable);
            }
            if let Some(observable) = processor.source_observable() {
                forward_observable(&observable, &readiness_observable);
            }
        }
        TableWriterOperator {
            name: self.name.clone(),
            projection: plan.projection.clone(),
            writer,
            target,
            relation: plan.writer_multiplex_schema.clone(),
            partial_aggregate,
            #[cfg(debug_assertions)]
            aggregate_guard: Arc::clone(&plan.aggregate_guard),
            state: TableWriterState::Open,
            logical_rows: 0,
            aggregate_output: None,
            aggregate_output_row: 0,
            writer_completion: None,
            emitted_row_count: false,
            next_auxiliary: 0,
            result_tracker,
            pending_output_tracker: None,
            aggregate_tracker: None,
            profiles: None,
            blocked_checks: AtomicUsize::new(0),
            writer_queue_blocked_time: BlockedDuration::default(),
            composite_blocked_time: BlockedDuration::default(),
            partial_rows: 0,
            partial_bytes: 0,
            partial_batches: 0,
            partial_channels: 0,
            sparse_partial_rows: 0,
            target_multiplex_batch_bytes: MAX_WRITER_MULTIPLEX_ROW_BYTES,
            readiness_observable,
        }
    }
}

impl OperatorFactory for TableWriterOperatorFactory {
    fn name(&self) -> &str {
        &self.name
    }

    fn create(&self, dop: i32, driver_id: i32) -> Box<dyn Operator> {
        Box::new(self.create_operator(dop, driver_id))
    }

    /// A table writer has output, so it is never the pipeline's terminal sink.
    fn is_sink(&self) -> bool {
        false
    }
}

fn forward_observable(source: &Arc<Observable>, target: &Arc<Observable>) {
    let target = Arc::downgrade(target);
    source.add_observer(Arc::new(move || {
        if let Some(target) = target.upgrade() {
            target.defer_notify().arm();
        }
    }));
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TableWriterState {
    Open,
    Draining,
    Producing,
    Failed,
    Aborting,
    Finished,
}

struct EncodedFragment {
    bytes: Vec<u8>,
    _accounting: Option<TrackedBytes>,
}

struct WriterCompletion {
    accepted_rows: u64,
    fragments: VecDeque<EncodedFragment>,
    _index_accounting: Option<TrackedBytes>,
}

struct TableWriterOperator {
    name: String,
    projection: TableWriterInputProjection,
    writer: AsyncWriterOwner<WriterCompletion>,
    target: WriteTargetOrdinal,
    relation: WriterMultiplexRelationSchema,
    partial_aggregate: Option<Box<dyn Operator>>,
    #[cfg(debug_assertions)]
    aggregate_guard: Arc<dyn TableWriteAggregateGuard>,
    state: TableWriterState,
    logical_rows: u64,
    aggregate_output: Option<Chunk>,
    aggregate_output_row: usize,
    writer_completion: Option<WriterCompletion>,
    emitted_row_count: bool,
    next_auxiliary: usize,
    result_tracker: Arc<Mutex<Option<Arc<MemTracker>>>>,
    pending_output_tracker: Option<Arc<MemTracker>>,
    aggregate_tracker: Option<Arc<MemTracker>>,
    profiles: Option<OperatorProfiles>,
    blocked_checks: AtomicUsize,
    writer_queue_blocked_time: BlockedDuration,
    composite_blocked_time: BlockedDuration,
    partial_rows: usize,
    partial_bytes: usize,
    partial_batches: usize,
    partial_channels: usize,
    sparse_partial_rows: usize,
    target_multiplex_batch_bytes: usize,
    readiness_observable: Arc<Observable>,
}

fn encode_writer_completion(
    target: WriteTargetOrdinal,
    fragment_encoder: &dyn ConnectorCommitFragmentEncoder,
    accepted_rows: u64,
    fragments: Vec<novarocks_spi::connector::write_stack::ConnectorCommitFragment>,
    tracker: Option<Arc<MemTracker>>,
) -> Result<WriterCompletion, String> {
    let mut encoded = VecDeque::with_capacity(fragments.len());
    for fragment in fragments {
        let bytes = fragment_encoder
            .encode(target, &fragment)
            .map_err(|error| format!("encode table writer commit fragment: {error}"))?;
        if bytes.len() > MAX_CONNECTOR_COMMIT_FRAGMENT_BYTES {
            return Err(format!(
                "table writer commit fragment of {} bytes exceeds the frozen single-fragment budget of {} bytes",
                bytes.len(),
                MAX_CONNECTOR_COMMIT_FRAGMENT_BYTES
            ));
        }
        let accounting = tracker
            .as_ref()
            .map(|tracker| TrackedBytes::try_new(bytes.capacity(), Arc::clone(tracker)))
            .transpose()
            .map_err(|error| {
                format!("ResourceExhausted: table writer fragment result memory: {error}")
            })?;
        encoded.push_back(EncodedFragment {
            bytes,
            _accounting: accounting,
        });
    }
    let index_bytes = encoded
        .capacity()
        .saturating_mul(size_of::<EncodedFragment>());
    let index_accounting = tracker
        .map(|tracker| TrackedBytes::try_new(index_bytes, tracker))
        .transpose()
        .map_err(|error| {
            format!("ResourceExhausted: table writer fragment index memory: {error}")
        })?;
    Ok(WriterCompletion {
        accepted_rows,
        fragments: encoded,
        _index_accounting: index_accounting,
    })
}

impl Operator for TableWriterOperator {
    fn name(&self) -> &str {
        &self.name
    }

    fn set_mem_tracker(&mut self, tracker: Arc<MemTracker>) {
        self.writer.set_mem_tracker(Arc::clone(&tracker));
        let result_tracker = MemTracker::new_child("ConnectorWriterResult", &tracker);
        *self
            .result_tracker
            .lock()
            .expect("table writer result tracker lock") = Some(result_tracker);
        self.pending_output_tracker =
            Some(MemTracker::new_child("WriterMultiplexOutput", &tracker));
        if let Some(partial) = self.partial_aggregate.as_mut() {
            let aggregate_tracker = MemTracker::new_child("WriterPartialAggregate", &tracker);
            partial.set_mem_tracker(Arc::clone(&aggregate_tracker));
            self.aggregate_tracker = Some(aggregate_tracker);
        }
    }

    fn set_profiles(&mut self, profiles: OperatorProfiles) {
        if let Some(partial) = self.partial_aggregate.as_mut() {
            partial.set_profiles(profiles.clone());
        }
        self.profiles = Some(profiles);
        self.sync_metrics();
    }

    fn prepare(&mut self) -> Result<(), String> {
        if let Some(partial) = self.partial_aggregate.as_mut()
            && let Err(error) = partial.prepare()
        {
            self.state = TableWriterState::Failed;
            return Err(error);
        }
        Ok(())
    }

    fn bind_runtime_state(&mut self, state: &RuntimeState) -> Result<(), String> {
        self.target_multiplex_batch_bytes = state
            .execution_runtime()
            .map(|runtime| runtime.config().exchange_max_transmit_batched_bytes)
            .unwrap_or(MAX_WRITER_MULTIPLEX_ROW_BYTES)
            .min(MAX_WRITER_MULTIPLEX_ROW_BYTES)
            .max(1);
        if let Some(partial) = self.partial_aggregate.as_mut()
            && let Err(error) = partial.bind_runtime_state(state)
        {
            self.state = TableWriterState::Failed;
            return Err(error);
        }
        let result = state
            .sink_io_executor()
            .and_then(|executor| self.writer.bind(executor, state.error_state()));
        if result.is_err() {
            self.state = TableWriterState::Failed;
        }
        result
    }

    fn close(&mut self) -> Result<(), String> {
        self.finish_blocked_intervals();
        self.sync_metrics();
        if let Some(partial) = self.partial_aggregate.as_mut() {
            partial.close()?;
        }
        Ok(())
    }

    fn cancel(&mut self) {
        if matches!(
            self.state,
            TableWriterState::Finished | TableWriterState::Aborting
        ) {
            return;
        }
        if let Some(partial) = self.partial_aggregate.as_mut() {
            partial.cancel();
        }
        self.writer.request_abort();
        self.aggregate_output = None;
        self.writer_completion = None;
        self.state = TableWriterState::Aborting;
        self.finish_blocked_intervals();
        self.sync_metrics();
    }

    fn on_driver_failure(&mut self) {
        if let Some(partial) = self.partial_aggregate.as_mut() {
            partial.on_driver_failure();
        }
        self.cancel();
    }

    fn is_finished(&self) -> bool {
        matches!(self.state, TableWriterState::Finished)
            || (matches!(
                self.state,
                TableWriterState::Failed | TableWriterState::Aborting
            ) && self.writer.is_done()
                && !self.writer.has_output())
    }

    fn pending_finish(&self) -> bool {
        match self.state {
            TableWriterState::Draining => {
                self.partial_child_finished()
                    && !self.partial_output_available()
                    && !self.writer.is_done()
            }
            TableWriterState::Aborting => !self.writer.is_done(),
            _ => false,
        }
    }

    fn as_processor_mut(&mut self) -> Option<&mut dyn ProcessorOperator> {
        Some(self)
    }

    fn as_processor_ref(&self) -> Option<&dyn ProcessorOperator> {
        Some(self)
    }
}

impl ProcessorOperator for TableWriterOperator {
    fn need_input(&self) -> bool {
        if self.state != TableWriterState::Open {
            self.writer_queue_blocked_time.observe(false);
            self.composite_blocked_time.observe(false);
            return false;
        }
        let aggregate_ready = self
            .partial_aggregate
            .as_ref()
            .and_then(|partial| partial.as_processor_ref())
            .is_none_or(ProcessorOperator::need_input);
        let writer_ready = self.writer.can_accept();
        let ready = aggregate_ready
            && self.aggregate_output.is_none()
            && !self.partial_child_has_output()
            && self.writer_completion.is_none()
            && writer_ready;
        self.writer_queue_blocked_time.observe(!writer_ready);
        self.composite_blocked_time.observe(!ready);
        if !ready {
            self.blocked_checks.fetch_add(1, Ordering::Relaxed);
        }
        ready
    }

    fn has_output(&self) -> bool {
        match self.state {
            TableWriterState::Open => self.partial_output_available(),
            TableWriterState::Draining => {
                self.partial_output_available()
                    || self.writer.error().is_some()
                    || (self.partial_child_finished()
                        && (self.writer.has_output() || self.writer.is_done()))
            }
            TableWriterState::Producing => self.has_remaining_output(),
            _ => false,
        }
    }

    fn push_chunk(&mut self, state: &RuntimeState, mut chunk: Chunk) -> Result<(), String> {
        let result = (|| {
            if !self.need_input() {
                return Err("composite table writer received input while not ready".to_string());
            }
            if chunk.is_empty() {
                return Ok(());
            }
            let rows = u64::try_from(chunk.len())
                .map_err(|_| "table writer input row count does not fit u64".to_string())?;
            let next_rows = self
                .logical_rows
                .checked_add(rows)
                .ok_or_else(|| "table writer logical row count overflowed u64".to_string())?;
            let projected = self.projection.project(&chunk)?;
            let batch = projected.batch.clone();
            let retained_bytes = record_batch_bytes(&batch);
            let additional_bytes = record_batch_additional_bytes(&batch, &chunk.batch);
            let accounting = chunk.take_memory_lease();
            let reservation = self.writer.try_reserve_input(
                batch.num_rows(),
                retained_bytes,
                accounting,
                additional_bytes,
            )?;
            if let Some(partial) = self.partial_aggregate.as_mut() {
                #[cfg(debug_assertions)]
                self.aggregate_guard
                    .check(TableWriteAggregateBoundary::PartialUpdate)
                    .map_err(|error| format!("update writer partial aggregate: {error}"))?;
                let processor = partial
                    .as_processor_mut()
                    .ok_or_else(|| "writer partial aggregate is not a processor".to_string())?;
                processor
                    .push_chunk(state, projected)
                    .map_err(|error| format!("update writer partial aggregate: {error}"))?;
            }
            reservation.send(batch);
            self.logical_rows = next_rows;
            Ok(())
        })();
        if result.is_err() {
            self.fail_attempt();
        }
        result
    }

    fn pull_chunk(&mut self, state: &RuntimeState) -> Result<Option<Chunk>, String> {
        if matches!(
            self.state,
            TableWriterState::Open | TableWriterState::Draining
        ) && self.partial_output_available()
        {
            match self.take_partial_output(state) {
                Ok(Some(output)) => {
                    self.sync_metrics();
                    return Ok(Some(output));
                }
                Ok(None) => {}
                Err(error) => {
                    self.fail_attempt();
                    return Err(error);
                }
            }
        }
        if self.state == TableWriterState::Open {
            return Ok(None);
        }
        if self.state == TableWriterState::Draining {
            if let Some(error) = self.writer.error() {
                self.fail_attempt();
                return Err(error);
            }
            if !self.partial_child_finished() || self.aggregate_output.is_some() {
                return Ok(None);
            }
            let Some(completion) = self.writer.take_output() else {
                return Ok(None);
            };
            if completion.accepted_rows != self.logical_rows {
                self.state = TableWriterState::Failed;
                self.sync_metrics();
                return Err(format!(
                    "composite table writer row count drift: aggregate accepted {}, writer accepted {}",
                    self.logical_rows, completion.accepted_rows
                ));
            }
            self.writer_completion = Some(completion);
            self.state = TableWriterState::Producing;
        }
        if self.state != TableWriterState::Producing {
            return Ok(None);
        }

        let output = if !self.emitted_row_count {
            self.emitted_row_count = true;
            let output = self.build_prefix_output(Some(self.logical_rows), &[])?;
            let bytes = self.output_size(&output)?;
            if bytes > MAX_WRITER_MULTIPLEX_ROW_BYTES {
                return Err(format!(
                    "ResourceExhausted: table writer row-count output requires {bytes} bytes, limit is {}",
                    MAX_WRITER_MULTIPLEX_ROW_BYTES
                ));
            }
            Some(output)
        } else if self
            .writer_completion
            .as_ref()
            .is_some_and(|completion| !completion.fragments.is_empty())
        {
            Some(self.take_fragment_output()?)
        } else {
            None
        };

        if !self.has_remaining_output() {
            self.aggregate_output = None;
            self.writer_completion = None;
            self.state = TableWriterState::Finished;
        }
        self.sync_metrics();
        Ok(output)
    }

    fn set_finishing(&mut self, state: &RuntimeState) -> Result<(), String> {
        if self.state != TableWriterState::Open {
            return Ok(());
        }
        self.state = TableWriterState::Draining;
        self.finish_blocked_intervals();
        if let Some(partial) = self.partial_aggregate.as_mut() {
            #[cfg(debug_assertions)]
            if let Err(error) = self
                .aggregate_guard
                .check(TableWriteAggregateBoundary::PartialFinalize)
            {
                self.state = TableWriterState::Failed;
                self.writer.request_abort();
                self.state = TableWriterState::Aborting;
                return Err(format!("finish writer partial aggregate: {error}"));
            }
            let processor = partial
                .as_processor_mut()
                .ok_or_else(|| "writer partial aggregate is not a processor".to_string())?;
            if let Err(error) = processor.set_finishing(state) {
                self.state = TableWriterState::Failed;
                self.writer.request_abort();
                self.state = TableWriterState::Aborting;
                return Err(format!("finish writer partial aggregate: {error}"));
            }
        }
        if let Err(error) = self.writer.request_finish() {
            self.state = TableWriterState::Failed;
            self.writer.request_abort();
            self.state = TableWriterState::Aborting;
            return Err(error);
        }
        Ok(())
    }

    fn sink_observable(&self) -> Option<Arc<Observable>> {
        Some(Arc::clone(&self.readiness_observable))
    }

    fn source_observable(&self) -> Option<Arc<Observable>> {
        Some(Arc::clone(&self.readiness_observable))
    }
}

impl TableWriterOperator {
    #[cfg(test)]
    fn replace_partial_aggregate_for_test(&mut self, partial: Box<dyn Operator>) {
        if let Some(processor) = partial.as_processor_ref() {
            if let Some(observable) = processor.sink_observable() {
                forward_observable(&observable, &self.readiness_observable);
            }
            if let Some(observable) = processor.source_observable() {
                forward_observable(&observable, &self.readiness_observable);
            }
        }
        self.partial_aggregate = Some(partial);
    }

    fn partial_child_has_output(&self) -> bool {
        self.partial_aggregate
            .as_ref()
            .and_then(|partial| partial.as_processor_ref())
            .is_some_and(ProcessorOperator::has_output)
    }

    fn partial_child_finished(&self) -> bool {
        self.partial_aggregate
            .as_ref()
            .is_none_or(|partial| partial.is_finished())
    }

    fn partial_output_available(&self) -> bool {
        self.aggregate_output.is_some() || self.partial_child_has_output()
    }

    fn has_remaining_output(&self) -> bool {
        !self.emitted_row_count
            || self
                .writer_completion
                .as_ref()
                .is_some_and(|completion| !completion.fragments.is_empty())
    }

    fn next_non_null_auxiliary(&self) -> Option<usize> {
        let aggregate = self.aggregate_output.as_ref()?;
        if self.aggregate_output_row >= aggregate.len() {
            return None;
        }
        self.relation
            .contract()
            .auxiliary_channels()
            .iter()
            .enumerate()
            .skip(self.next_auxiliary)
            .find_map(|(index, channel)| {
                let array = aggregate
                    .column_by_slot_id(novarocks_types::SlotId::new(channel.slot_id()))
                    .ok()?;
                (array.len() > self.aggregate_output_row
                    && !array.is_null(self.aggregate_output_row))
                .then_some(index)
            })
    }

    fn build_prefix_output(
        &self,
        row_count: Option<u64>,
        fragments: &[&[u8]],
    ) -> Result<Chunk, String> {
        let target = target_ordinal_to_wire(self.target)
            .map_err(|error| format!("table writer target ordinal: {error}"))?;
        let rows = usize::from(row_count.is_some()) + fragments.len();
        let mut kinds = Vec::with_capacity(rows);
        let mut ordinals = Vec::with_capacity(rows);
        let mut row_counts = Vec::with_capacity(rows);
        let mut payloads = BinaryBuilder::new();
        if let Some(row_count) = row_count {
            kinds.push(WriterRowKind::RowCount.to_wire());
            ordinals.push(target);
            row_counts
                .push(Some(row_count_to_wire(row_count).map_err(|error| {
                    format!("table writer row count: {error}")
                })?));
            payloads.append_null();
        }
        for fragment in fragments {
            kinds.push(WriterRowKind::CommitFragment.to_wire());
            ordinals.push(target);
            row_counts.push(None);
            payloads.append_value(fragment);
        }
        let mut columns: Vec<ArrayRef> = vec![
            Arc::new(Int8Array::from(kinds)),
            Arc::new(Int32Array::from(ordinals)),
            Arc::new(Int64Array::from(row_counts)),
            Arc::new(payloads.finish()),
        ];
        columns.extend(
            self.relation
                .contract()
                .auxiliary_channels()
                .iter()
                .map(|channel| new_null_array(channel.data_type(), rows)),
        );
        self.track_output(
            RecordBatch::try_new(Arc::clone(self.relation.contract().arrow_schema()), columns)
                .map_err(|error| format!("build table writer prefix output: {error}"))?,
        )
    }

    fn take_fragment_output(&mut self) -> Result<Chunk, String> {
        let available = self
            .writer_completion
            .as_ref()
            .map(|completion| completion.fragments.len())
            .unwrap_or(0);
        let mut low = 1usize;
        let mut high = available;
        let mut best = None;
        while low <= high {
            let middle = low + (high - low) / 2;
            let refs = self
                .writer_completion
                .as_ref()
                .expect("writer completion")
                .fragments
                .iter()
                .take(middle)
                .map(|fragment| fragment.bytes.as_slice())
                .collect::<Vec<_>>();
            let candidate = self.build_prefix_output(None, &refs)?;
            let bytes = self.output_size(&candidate)?;
            if bytes <= self.target_multiplex_batch_bytes
                || (middle == 1 && bytes <= MAX_WRITER_MULTIPLEX_ROW_BYTES)
            {
                best = Some((middle, candidate));
                if bytes > self.target_multiplex_batch_bytes {
                    break;
                }
                low = middle + 1;
            } else if middle == 1 {
                return Err(format!(
                    "ResourceExhausted: one table writer fragment row requires {bytes} bytes, limit is {}",
                    MAX_WRITER_MULTIPLEX_ROW_BYTES
                ));
            } else {
                high = middle - 1;
            }
        }
        let (count, output) = best.expect("one fragment was validated or rejected");
        let completion = self.writer_completion.as_mut().expect("writer completion");
        for _ in 0..count {
            completion.fragments.pop_front();
        }
        Ok(output)
    }

    fn take_partial_output(&mut self, state: &RuntimeState) -> Result<Option<Chunk>, String> {
        loop {
            if self.aggregate_output.is_none() {
                let Some(partial) = self.partial_aggregate.as_mut() else {
                    return Ok(None);
                };
                let processor = partial
                    .as_processor_mut()
                    .ok_or_else(|| "writer partial aggregate is not a processor".to_string())?;
                if !processor.has_output() {
                    return Ok(None);
                }
                let Some(mut output) = processor.pull_chunk(state)? else {
                    return Err(
                        "writer partial aggregate advertised output but returned none".to_string(),
                    );
                };
                if let Some(tracker) = self.aggregate_tracker.as_ref() {
                    output.try_transfer_to(tracker)?;
                }
                self.aggregate_output = Some(output);
                self.aggregate_output_row = 0;
                self.next_auxiliary = 0;
            }

            let Some(first) = self.next_non_null_auxiliary() else {
                self.advance_partial_row();
                continue;
            };
            let candidates = self
                .relation
                .contract()
                .auxiliary_channels()
                .iter()
                .enumerate()
                .skip(first)
                .filter_map(|(index, channel)| {
                    let aggregate = self.aggregate_output.as_ref()?;
                    let array = aggregate
                        .column_by_slot_id(novarocks_types::SlotId::new(channel.slot_id()))
                        .ok()?;
                    (array.len() > self.aggregate_output_row
                        && !array.is_null(self.aggregate_output_row))
                    .then_some(index)
                })
                .collect::<Vec<_>>();
            let mut low = 1usize;
            let mut high = candidates.len();
            let mut best_count = None;
            let mut best_bytes = 0;
            while low <= high {
                let middle = low + (high - low) / 2;
                let candidate = self.build_partial_output(&candidates[..middle])?;
                let bytes = self.output_size(&candidate)?;
                drop(candidate);
                if bytes <= self.target_multiplex_batch_bytes
                    || (middle == 1 && bytes <= MAX_WRITER_MULTIPLEX_ROW_BYTES)
                {
                    best_count = Some(middle);
                    best_bytes = bytes;
                    if bytes > self.target_multiplex_batch_bytes {
                        break;
                    }
                    low = middle + 1;
                } else if middle == 1 {
                    return Err(format!(
                        "ResourceExhausted: one writer aggregate intermediate value requires {bytes} bytes, limit is {}",
                        MAX_WRITER_MULTIPLEX_ROW_BYTES
                    ));
                } else {
                    high = middle - 1;
                }
            }
            let count = best_count.expect("one partial value was validated or rejected");
            let output = self.build_partial_output(&candidates[..count])?;
            self.next_auxiliary = candidates[count - 1] + 1;
            self.partial_rows = self.partial_rows.saturating_add(1);
            self.partial_bytes = self.partial_bytes.saturating_add(best_bytes);
            self.partial_batches = self.partial_batches.saturating_add(1);
            self.partial_channels = self.partial_channels.saturating_add(count);
            let sparse_rows =
                usize::from(count < self.relation.contract().auxiliary_channels().len());
            self.sparse_partial_rows = self.sparse_partial_rows.saturating_add(sparse_rows);
            crate::runtime::table_writer_metrics::observe_partial_output(
                output.len(),
                best_bytes,
                count,
                sparse_rows,
            );
            if self.next_non_null_auxiliary().is_none() {
                self.advance_partial_row();
            }
            return Ok(Some(output));
        }
    }

    fn advance_partial_row(&mut self) {
        let Some(output) = self.aggregate_output.as_ref() else {
            return;
        };
        self.aggregate_output_row = self.aggregate_output_row.saturating_add(1);
        self.next_auxiliary = 0;
        if self.aggregate_output_row >= output.len() {
            self.aggregate_output = None;
            self.aggregate_output_row = 0;
        }
    }

    fn build_partial_output(&self, selected: &[usize]) -> Result<Chunk, String> {
        let aggregate = self
            .aggregate_output
            .as_ref()
            .ok_or_else(|| "writer partial aggregate output is missing".to_string())?;
        let target = target_ordinal_to_wire(self.target)
            .map_err(|error| format!("table writer target ordinal: {error}"))?;
        let mut columns: Vec<ArrayRef> = vec![
            Arc::new(Int8Array::from(vec![
                WriterRowKind::AggregatePartial.to_wire(),
            ])),
            Arc::new(Int32Array::from(vec![target])),
            Arc::new(Int64Array::from(vec![None])),
            Arc::new(BinaryArray::from(vec![None::<&[u8]>])),
        ];
        for (index, channel) in self
            .relation
            .contract()
            .auxiliary_channels()
            .iter()
            .enumerate()
        {
            if selected.binary_search(&index).is_ok() {
                columns.push(
                    aggregate
                        .column_by_slot_id(novarocks_types::SlotId::new(channel.slot_id()))?
                        .slice(self.aggregate_output_row, 1),
                );
            } else {
                columns.push(new_null_array(channel.data_type(), 1));
            }
        }
        self.track_output(
            RecordBatch::try_new(Arc::clone(self.relation.contract().arrow_schema()), columns)
                .map_err(|error| format!("build writer aggregate partial output: {error}"))?,
        )
    }

    fn track_output(&self, batch: RecordBatch) -> Result<Chunk, String> {
        let mut chunk =
            Chunk::try_new_with_chunk_schema(batch, Arc::clone(self.relation.chunk_schema()))?;
        if let Some(tracker) = self.pending_output_tracker.as_ref() {
            chunk.try_transfer_to(tracker)?;
        }
        Ok(chunk)
    }

    fn output_size(&self, chunk: &Chunk) -> Result<usize, String> {
        let mut encoded = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut encoded, &chunk.schema())
                .map_err(|error| format!("table writer output size encoding: {error}"))?;
            writer
                .write(&chunk.batch)
                .map_err(|error| format!("table writer output size encoding: {error}"))?;
            writer
                .finish()
                .map_err(|error| format!("table writer output size encoding: {error}"))?;
        }
        let _accounting = self
            .pending_output_tracker
            .as_ref()
            .map(|tracker| TrackedBytes::try_new(encoded.capacity(), Arc::clone(tracker)))
            .transpose()?;
        Ok(chunk
            .estimated_bytes()
            .max(chunk.logical_bytes())
            .max(encoded.len()))
    }

    fn fail_attempt(&mut self) {
        self.state = TableWriterState::Failed;
        if let Some(partial) = self.partial_aggregate.as_mut() {
            partial.on_driver_failure();
        }
        self.writer.request_abort();
        self.aggregate_output = None;
        self.writer_completion = None;
        self.state = TableWriterState::Aborting;
        self.finish_blocked_intervals();
        self.sync_metrics();
    }

    fn finish_blocked_intervals(&self) {
        self.writer_queue_blocked_time.observe(false);
        self.composite_blocked_time.observe(false);
    }

    fn sync_metrics(&self) {
        let Some(profiles) = self.profiles.as_ref() else {
            return;
        };
        let writer = self.writer.metrics();
        profiles.common.counter_set_unit(
            "WriterQueueBlockedChecks",
            i64::try_from(writer.queue_blocked_checks).unwrap_or(i64::MAX),
        );
        profiles.common.counter_set_unit(
            "CompositeWriterBlockedChecks",
            i64::try_from(self.blocked_checks.load(Ordering::Relaxed)).unwrap_or(i64::MAX),
        );
        profiles.common.counter_set(
            "WriterQueueBlockedTime",
            crate::runtime::profile::ProfileUnit::TimeNs,
            crate::runtime::profile::clamp_u128_to_i64(self.writer_queue_blocked_time.elapsed_ns()),
        );
        profiles.common.counter_set_unit(
            "WriterQueueBlockedIntervals",
            i64::try_from(self.writer_queue_blocked_time.intervals()).unwrap_or(i64::MAX),
        );
        profiles.common.counter_set(
            "CompositeWriterBlockedTime",
            crate::runtime::profile::ProfileUnit::TimeNs,
            crate::runtime::profile::clamp_u128_to_i64(self.composite_blocked_time.elapsed_ns()),
        );
        profiles.common.counter_set_unit(
            "CompositeWriterBlockedIntervals",
            i64::try_from(self.composite_blocked_time.intervals()).unwrap_or(i64::MAX),
        );
        profiles.common.counter_set_unit(
            "WriterQueuePeakBatches",
            i64::try_from(writer.queue_peak_batches).unwrap_or(i64::MAX),
        );
        profiles.common.counter_set_unit(
            "WriterQueuePeakRows",
            i64::try_from(writer.queue_peak_rows).unwrap_or(i64::MAX),
        );
        profiles.common.counter_set_bytes(
            "WriterQueuePeakBytes",
            i64::try_from(writer.queue_peak_bytes).unwrap_or(i64::MAX),
        );
        profiles.common.counter_set_unit(
            "WriterAbortRequests",
            i64::try_from(writer.abort_requests).unwrap_or(i64::MAX),
        );
        profiles.common.counter_set_unit(
            "WriterPartialRows",
            i64::try_from(self.partial_rows).unwrap_or(i64::MAX),
        );
        profiles.common.counter_set_bytes(
            "WriterPartialBytes",
            i64::try_from(self.partial_bytes).unwrap_or(i64::MAX),
        );
        profiles.common.counter_set_unit(
            "WriterPartialBatches",
            i64::try_from(self.partial_batches).unwrap_or(i64::MAX),
        );
        profiles.common.counter_set_unit(
            "WriterPartialChannels",
            i64::try_from(self.partial_channels).unwrap_or(i64::MAX),
        );
        profiles.common.counter_set_unit(
            "WriterSparsePartialRows",
            i64::try_from(self.sparse_partial_rows).unwrap_or(i64::MAX),
        );
        if let Some(tracker) = self.aggregate_tracker.as_ref() {
            profiles
                .common
                .counter_set_bytes("WriterPartialAggregatePeakBytes", tracker.peak());
        }
    }
}

/// Read one `TableWriter` output relation back into typed rows. It exists so
/// `TableFinish` and the tests share exactly one reader of the frozen relation.
pub(crate) struct TableWriteRelationColumns<'chunk> {
    pub kinds: &'chunk Int8Array,
    pub ordinals: &'chunk Int32Array,
    pub row_counts: &'chunk Int64Array,
    pub fragments: &'chunk BinaryArray,
}

impl<'chunk> TableWriteRelationColumns<'chunk> {
    pub fn try_from_chunk(chunk: &'chunk Chunk) -> Result<Self, String> {
        use crate::exec::node::table_write_relation::{
            WRITE_RELATION_FRAGMENT_SLOT, WRITE_RELATION_KIND_SLOT, WRITE_RELATION_ROW_COUNT_SLOT,
            WRITE_RELATION_TARGET_SLOT,
        };

        let schema = chunk.chunk_schema();
        let index = |slot| {
            schema.index_of(slot).ok_or_else(|| {
                format!("table write relation is missing slot {slot}: unexpected input shape")
            })
        };
        let kind_index = index(WRITE_RELATION_KIND_SLOT)?;
        let ordinal_index = index(WRITE_RELATION_TARGET_SLOT)?;
        let row_count_index = index(WRITE_RELATION_ROW_COUNT_SLOT)?;
        let fragment_index = index(WRITE_RELATION_FRAGMENT_SLOT)?;

        let column = |position: usize, name: &str| {
            chunk.columns().get(position).ok_or_else(|| {
                format!("table write relation column {name} is outside its record batch")
            })
        };
        let kinds = column(kind_index, "kind")?
            .as_any()
            .downcast_ref::<Int8Array>()
            .ok_or_else(|| "table write relation kind column is not Int8".to_string())?;
        let ordinals = column(ordinal_index, "write_target_ordinal")?
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or_else(|| {
                "table write relation write_target_ordinal column is not Int32".to_string()
            })?;
        let row_counts = column(row_count_index, "row_count")?
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| "table write relation row_count column is not Int64".to_string())?;
        let fragments = column(fragment_index, "commit_fragment")?
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| {
                "table write relation commit_fragment column is not Binary".to_string()
            })?;
        Ok(Self {
            kinds,
            ordinals,
            row_counts,
            fragments,
        })
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::time::{Duration, Instant};

    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use novarocks_spi::connector::write_stack::{
        ConnectorBatchWriter, ConnectorCommitFragment, ProviderWriteRuntime, WriteRuntimeAdapter,
        WriterAuxiliaryChannel, WriterMultiplexSchema,
    };
    use novarocks_spi::connector::{
        CatalogHandle, CatalogVersion, ConnectorCancellation, ConnectorError, ConnectorErrorKind,
        ConnectorInstanceDescriptor, ConnectorInstanceId, ConnectorProviderId,
        ConnectorRequestContext, MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
        MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
    };
    use novarocks_types::SlotId;
    use tokio::sync::Notify;

    use super::*;
    use crate::exec::chunk::ChunkSchema;
    use crate::exec::expr::{ExprArena, ExprNode};
    use crate::exec::node::ExecNode;
    use crate::exec::node::table_write_aggregate::{
        WriterPartialAggregateCall, WriterPartialAggregatePlan,
    };
    use crate::exec::node::table_write_relation::WriterMultiplexRelationSchema;
    use crate::exec::node::values::ValuesNode;
    use crate::exec::pipeline::driver::{DriverState, PipelineDriver};
    use crate::exec::pipeline::fragment_context::FragmentContext;
    use crate::exec::pipeline::global_driver_executor::{DriverTask, FragmentCompletion};
    use crate::runtime::mem_tracker::MemTracker;

    fn poll_until<F: Fn() -> bool>(pred: F, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            if pred() {
                return true;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        pred()
    }

    #[cfg(debug_assertions)]
    struct RejectAggregateBoundary(TableWriteAggregateBoundary);

    #[cfg(debug_assertions)]
    impl TableWriteAggregateGuard for RejectAggregateBoundary {
        fn check(&self, boundary: TableWriteAggregateBoundary) -> Result<(), ConnectorError> {
            if boundary == self.0 {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::Internal,
                    format!("injected {boundary:?}"),
                ));
            }
            Ok(())
        }
    }

    fn test_runtime_state() -> RuntimeState {
        test_runtime_state_with_mem(None)
    }

    fn test_runtime_state_with_mem(mem_tracker: Option<Arc<MemTracker>>) -> RuntimeState {
        test_runtime_state_with_packet_budget(MAX_WRITER_MULTIPLEX_ROW_BYTES, mem_tracker)
    }

    fn test_runtime_state_with_packet_budget(
        max_packet_bytes: usize,
        mem_tracker: Option<Arc<MemTracker>>,
    ) -> RuntimeState {
        let runtime = Arc::new(
            crate::runtime::ExecutionRuntime::new(
                crate::runtime::ExecutionRuntimeConfig {
                    driver_threads: 1,
                    scan_threads: 1,
                    scan_queue_capacity: 8,
                    spill_io_threads: 1,
                    spill_io_queue_capacity: 8,
                    spill_storage:
                        crate::runtime::execution_runtime::ExecutionSpillStorageConfig::default(),
                    exchange_wait_ms: 120_000,
                    exchange_io_threads: 1,
                    exchange_io_max_inflight_bytes: 1024,
                    exchange_max_transmit_batched_bytes: max_packet_bytes,
                    operator_buffer_chunks: 1,
                    local_exchange_buffer_mem_limit_per_driver: 1024,
                    local_exchange_max_buffered_rows: 1024,
                    connector_io_tasks_per_scan_operator: 1,
                    scan_submit_fail_max: 1,
                    scan_submit_fail_timeout_ms: 1,
                    runtime_filter_scan_wait_time_ms_override: None,
                    runtime_filter_wait_timeout_ms_override: None,
                    sink_io_worker_threads: 1,
                    sink_io_max_blocking_threads: 1,
                },
                crate::runtime::execution_runtime::test_execution_function_set(),
            )
            .expect("test execution runtime"),
        );
        RuntimeState::new(
            None,
            None,
            None,
            None,
            None,
            mem_tracker,
            None,
            None,
            Some(runtime),
            None,
        )
    }

    fn bind(operator: &mut Box<dyn Operator>, state: &RuntimeState) {
        operator.prepare().expect("prepare");
        operator
            .bind_runtime_state(state)
            .expect("bind writer actor");
    }

    fn wait_for_output(
        operator: &mut Box<dyn Operator>,
        state: &RuntimeState,
    ) -> Result<Vec<Chunk>, String> {
        let ready = poll_until(
            || {
                state.error().is_some()
                    || operator.as_processor_ref().expect("processor").has_output()
            },
            Duration::from_secs(5),
        );
        if !ready {
            return Err("timed out waiting for table writer output".to_string());
        }
        if let Some(error) = state.error() {
            return Err(error);
        }
        Ok(drain(operator, state))
    }

    struct OneChunkSource {
        chunk: Option<Chunk>,
        finished: bool,
    }

    impl Operator for OneChunkSource {
        fn name(&self) -> &str {
            "one_chunk_source"
        }

        fn is_finished(&self) -> bool {
            self.finished
        }

        fn as_processor_mut(&mut self) -> Option<&mut dyn ProcessorOperator> {
            Some(self)
        }

        fn as_processor_ref(&self) -> Option<&dyn ProcessorOperator> {
            Some(self)
        }
    }

    impl ProcessorOperator for OneChunkSource {
        fn need_input(&self) -> bool {
            false
        }

        fn has_output(&self) -> bool {
            self.chunk.is_some()
        }

        fn push_chunk(&mut self, _state: &RuntimeState, _chunk: Chunk) -> Result<(), String> {
            Err("source does not accept input".to_string())
        }

        fn pull_chunk(&mut self, _state: &RuntimeState) -> Result<Option<Chunk>, String> {
            let chunk = self.chunk.take();
            self.finished = chunk.is_some();
            Ok(chunk)
        }

        fn set_finishing(&mut self, _state: &RuntimeState) -> Result<(), String> {
            self.finished = true;
            Ok(())
        }
    }

    struct CollectSink {
        chunks: Arc<std::sync::Mutex<Vec<Chunk>>>,
        finished: bool,
    }

    struct ProjectedInputObserver {
        values: Arc<std::sync::Mutex<Vec<i64>>>,
        finishing: bool,
        cancelled: bool,
    }

    impl Operator for ProjectedInputObserver {
        fn name(&self) -> &str {
            "projected_input_observer"
        }

        fn cancel(&mut self) {
            self.cancelled = true;
        }

        fn is_finished(&self) -> bool {
            self.cancelled || self.finishing
        }

        fn as_processor_mut(&mut self) -> Option<&mut dyn ProcessorOperator> {
            Some(self)
        }

        fn as_processor_ref(&self) -> Option<&dyn ProcessorOperator> {
            Some(self)
        }
    }

    impl ProcessorOperator for ProjectedInputObserver {
        fn need_input(&self) -> bool {
            !self.finishing && !self.cancelled
        }

        fn has_output(&self) -> bool {
            false
        }

        fn push_chunk(&mut self, _state: &RuntimeState, chunk: Chunk) -> Result<(), String> {
            let values = chunk
                .column_by_slot_id(SlotId::new(1))?
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| "embedded aggregate did not receive projected Int64".to_string())?
                .values()
                .to_vec();
            self.values.lock().expect("observed values").extend(values);
            Ok(())
        }

        fn pull_chunk(&mut self, _state: &RuntimeState) -> Result<Option<Chunk>, String> {
            Ok(None)
        }

        fn set_finishing(&mut self, _state: &RuntimeState) -> Result<(), String> {
            self.finishing = true;
            Ok(())
        }
    }

    impl Operator for CollectSink {
        fn name(&self) -> &str {
            "collect_sink"
        }

        fn is_finished(&self) -> bool {
            self.finished
        }

        fn as_processor_mut(&mut self) -> Option<&mut dyn ProcessorOperator> {
            Some(self)
        }

        fn as_processor_ref(&self) -> Option<&dyn ProcessorOperator> {
            Some(self)
        }
    }

    impl ProcessorOperator for CollectSink {
        fn need_input(&self) -> bool {
            !self.finished
        }

        fn has_output(&self) -> bool {
            false
        }

        fn push_chunk(&mut self, _state: &RuntimeState, chunk: Chunk) -> Result<(), String> {
            self.chunks.lock().expect("collected chunks").push(chunk);
            Ok(())
        }

        fn pull_chunk(&mut self, _state: &RuntimeState) -> Result<Option<Chunk>, String> {
            Ok(None)
        }

        fn set_finishing(&mut self, _state: &RuntimeState) -> Result<(), String> {
            self.finished = true;
            Ok(())
        }
    }

    struct StreamingPartial {
        slot_id: SlotId,
        outputs: VecDeque<Chunk>,
        accepted_rows: Arc<AtomicUsize>,
        finishing: bool,
        cancelled: bool,
        observable: Arc<Observable>,
    }

    impl StreamingPartial {
        fn new(slot_id: SlotId, accepted_rows: Arc<AtomicUsize>) -> Self {
            Self {
                slot_id,
                outputs: VecDeque::new(),
                accepted_rows,
                finishing: false,
                cancelled: false,
                observable: Arc::new(Observable::new()),
            }
        }

        fn output(&self, value: i64) -> Chunk {
            let schema = Arc::new(Schema::new(vec![Field::new(
                "partial",
                DataType::Int64,
                true,
            )]));
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(Int64Array::from(vec![Some(value)]))],
            )
            .expect("streaming partial batch");
            let chunk_schema =
                ChunkSchema::try_ref_from_schema_and_slot_ids(schema.as_ref(), &[self.slot_id])
                    .expect("streaming partial chunk schema");
            Chunk::new_with_chunk_schema(batch, chunk_schema)
        }
    }

    impl Operator for StreamingPartial {
        fn name(&self) -> &str {
            "streaming_partial"
        }

        fn cancel(&mut self) {
            self.cancelled = true;
            self.outputs.clear();
            self.observable.defer_notify().arm();
        }

        fn is_finished(&self) -> bool {
            self.cancelled || (self.finishing && self.outputs.is_empty())
        }

        fn as_processor_mut(&mut self) -> Option<&mut dyn ProcessorOperator> {
            Some(self)
        }

        fn as_processor_ref(&self) -> Option<&dyn ProcessorOperator> {
            Some(self)
        }
    }

    impl ProcessorOperator for StreamingPartial {
        fn need_input(&self) -> bool {
            !self.finishing && !self.cancelled && self.outputs.is_empty()
        }

        fn has_output(&self) -> bool {
            !self.outputs.is_empty()
        }

        fn push_chunk(&mut self, _state: &RuntimeState, chunk: Chunk) -> Result<(), String> {
            if !self.need_input() {
                return Err("streaming partial received input while blocked".to_string());
            }
            self.accepted_rows.fetch_add(chunk.len(), Ordering::Relaxed);
            self.outputs.push_back(self.output(10));
            self.outputs.push_back(self.output(20));
            self.observable.defer_notify().arm();
            Ok(())
        }

        fn pull_chunk(&mut self, _state: &RuntimeState) -> Result<Option<Chunk>, String> {
            let output = self.outputs.pop_front();
            if self.outputs.is_empty() {
                self.observable.defer_notify().arm();
            }
            Ok(output)
        }

        fn set_finishing(&mut self, _state: &RuntimeState) -> Result<(), String> {
            self.finishing = true;
            self.observable.defer_notify().arm();
            Ok(())
        }

        fn sink_observable(&self) -> Option<Arc<Observable>> {
            Some(Arc::clone(&self.observable))
        }

        fn source_observable(&self) -> Option<Arc<Observable>> {
            Some(Arc::clone(&self.observable))
        }
    }

    struct OneShotPartial {
        output_on_push: Option<Chunk>,
        output: Option<Chunk>,
        finishing: bool,
        cancelled: bool,
        observable: Arc<Observable>,
    }

    impl OneShotPartial {
        fn new(output: Chunk) -> Self {
            Self {
                output_on_push: Some(output),
                output: None,
                finishing: false,
                cancelled: false,
                observable: Arc::new(Observable::new()),
            }
        }
    }

    impl Operator for OneShotPartial {
        fn name(&self) -> &str {
            "one_shot_partial"
        }

        fn cancel(&mut self) {
            self.cancelled = true;
            self.output = None;
            self.observable.defer_notify().arm();
        }

        fn is_finished(&self) -> bool {
            self.cancelled || (self.finishing && self.output.is_none())
        }

        fn as_processor_mut(&mut self) -> Option<&mut dyn ProcessorOperator> {
            Some(self)
        }

        fn as_processor_ref(&self) -> Option<&dyn ProcessorOperator> {
            Some(self)
        }
    }

    impl ProcessorOperator for OneShotPartial {
        fn need_input(&self) -> bool {
            !self.finishing && !self.cancelled && self.output.is_none()
        }

        fn has_output(&self) -> bool {
            self.output.is_some()
        }

        fn push_chunk(&mut self, _state: &RuntimeState, _chunk: Chunk) -> Result<(), String> {
            self.output = self.output_on_push.take();
            self.observable.defer_notify().arm();
            Ok(())
        }

        fn pull_chunk(&mut self, _state: &RuntimeState) -> Result<Option<Chunk>, String> {
            Ok(self.output.take())
        }

        fn set_finishing(&mut self, _state: &RuntimeState) -> Result<(), String> {
            self.finishing = true;
            Ok(())
        }

        fn sink_observable(&self) -> Option<Arc<Observable>> {
            Some(Arc::clone(&self.observable))
        }

        fn source_observable(&self) -> Option<Arc<Observable>> {
            Some(Arc::clone(&self.observable))
        }
    }

    /// A minimal provider that owns nothing but a marker payload, so the tests
    /// exercise the operator contract rather than a provider implementation.
    pub(crate) struct TestWriteRuntime {
        descriptor: ConnectorInstanceDescriptor,
        catalog_handle: CatalogHandle,
    }

    impl TestWriteRuntime {
        fn new() -> Arc<Self> {
            let instance_id = ConnectorInstanceId::parse("test_connector").expect("instance id");
            Arc::new(Self {
                descriptor: ConnectorInstanceDescriptor {
                    provider_id: ConnectorProviderId::parse("test").expect("provider id"),
                    instance_id: instance_id.clone(),
                },
                catalog_handle: CatalogHandle::new(
                    instance_id,
                    CatalogVersion::from_bytes([9; 32]),
                ),
            })
        }
    }

    impl ProviderWriteRuntime for TestWriteRuntime {
        type CommitHandle = ();
        type WriterHandle = TestWriterRecipe;
        type CommitFragment = TestFragment;

        fn descriptor(&self) -> &ConnectorInstanceDescriptor {
            &self.descriptor
        }

        fn catalog_handle(&self) -> &CatalogHandle {
            &self.catalog_handle
        }
    }

    #[derive(Clone, Debug)]
    pub(crate) struct TestWriterRecipe;

    #[derive(Clone, Debug)]
    pub(crate) struct TestFragment {
        pub bytes: Vec<u8>,
    }

    pub(crate) fn adapter() -> WriteRuntimeAdapter<TestWriteRuntime> {
        WriteRuntimeAdapter::new(TestWriteRuntime::new())
    }

    pub(crate) fn catalog_handle() -> CatalogHandle {
        adapter().binding().catalog_handle().clone()
    }

    pub(crate) fn writer_handle() -> ConnectorWriterHandle {
        adapter().wrap_writer_handle(TestWriterRecipe)
    }

    pub(crate) fn commit_fragment(bytes: Vec<u8>) -> ConnectorCommitFragment {
        adapter().wrap_commit_fragment(TestFragment { bytes })
    }

    /// Encoder port stub: the backend owns the real canonical codec, so a test
    /// only has to hand back the bytes the provider fragment carries.
    pub(crate) struct TestFragmentEncoder;

    impl ConnectorCommitFragmentEncoder for TestFragmentEncoder {
        fn encode(
            &self,
            _target: WriteTargetOrdinal,
            fragment: &ConnectorCommitFragment,
        ) -> Result<Vec<u8>, ConnectorError> {
            let adapter = adapter();
            let value = adapter.commit_fragment(fragment)?;
            Ok(value.bytes.clone())
        }
    }

    #[derive(Default)]
    pub(crate) struct NeverCancelled;

    impl ConnectorCancellation for NeverCancelled {
        fn is_cancelled(&self) -> bool {
            false
        }
    }

    pub(crate) fn request_context() -> ConnectorRequestContext {
        ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(60),
            Arc::new(NeverCancelled),
            MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
            MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
        )
        .expect("request context")
    }

    pub(crate) fn target(value: u32) -> WriteTargetOrdinal {
        WriteTargetOrdinal::try_new(value).expect("bounded ordinal")
    }

    #[derive(Default)]
    pub(crate) struct WriteExecutionStats {
        pub opened: AtomicUsize,
        pub finished: AtomicUsize,
        pub aborted: AtomicUsize,
    }

    pub(crate) struct TestWriteExecution {
        catalog_handle: CatalogHandle,
        stats: Arc<WriteExecutionStats>,
        fragments_per_writer: usize,
        fragment_bytes: usize,
        /// Every driver id this execution has been asked to open a writer for.
        pub driver_ids: std::sync::Mutex<Vec<u32>>,
        pub writer_rows: Arc<std::sync::Mutex<Vec<(u32, usize)>>>,
    }

    impl TestWriteExecution {
        pub fn new(stats: Arc<WriteExecutionStats>) -> Self {
            Self {
                catalog_handle: catalog_handle(),
                stats,
                fragments_per_writer: 1,
                fragment_bytes: 8,
                driver_ids: std::sync::Mutex::new(Vec::new()),
                writer_rows: Arc::new(std::sync::Mutex::new(Vec::new())),
            }
        }

        pub fn with_fragments(mut self, count: usize, bytes: usize) -> Self {
            self.fragments_per_writer = count;
            self.fragment_bytes = bytes;
            self
        }
    }

    #[async_trait::async_trait]
    impl ConnectorWriteExecution for TestWriteExecution {
        fn catalog_handle(&self) -> &CatalogHandle {
            &self.catalog_handle
        }

        async fn open_writer(
            &self,
            request: ConnectorOpenWriterRequest,
        ) -> Result<Box<dyn ConnectorBatchWriter>, ConnectorError> {
            self.stats.opened.fetch_add(1, Ordering::Relaxed);
            let driver_id = request.physical.driver_id();
            self.driver_ids
                .lock()
                .expect("driver id log")
                .push(driver_id);
            Ok(Box::new(TestBatchWriter {
                driver_id,
                rows: 0,
                stats: Arc::clone(&self.stats),
                writer_rows: Arc::clone(&self.writer_rows),
                fragments_per_writer: self.fragments_per_writer,
                fragment_bytes: self.fragment_bytes,
            }))
        }
    }

    struct TestBatchWriter {
        driver_id: u32,
        rows: usize,
        stats: Arc<WriteExecutionStats>,
        writer_rows: Arc<std::sync::Mutex<Vec<(u32, usize)>>>,
        fragments_per_writer: usize,
        fragment_bytes: usize,
    }

    #[async_trait::async_trait]
    impl ConnectorBatchWriter for TestBatchWriter {
        async fn append(&mut self, batch: RecordBatch) -> Result<(), ConnectorError> {
            self.rows += batch.num_rows();
            Ok(())
        }

        async fn finish(&mut self) -> Result<Vec<ConnectorCommitFragment>, ConnectorError> {
            self.stats.finished.fetch_add(1, Ordering::Relaxed);
            self.writer_rows
                .lock()
                .expect("writer row log")
                .push((self.driver_id, self.rows));
            Ok((0..self.fragments_per_writer)
                .map(|index| {
                    commit_fragment(vec![
                        u8::try_from(index % 251).unwrap_or_default();
                        self.fragment_bytes
                    ])
                })
                .collect())
        }

        async fn abort(&mut self) -> Result<(), ConnectorError> {
            self.stats.aborted.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
    }

    pub(crate) fn writer_input_schema() -> arrow::datatypes::SchemaRef {
        Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]))
    }

    pub(crate) fn identity_projection() -> TableWriterInputProjection {
        let mut arena = ExprArena::default();
        let expr = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Int32);
        TableWriterInputProjection::try_new(arena, vec![expr], writer_input_schema())
            .expect("projection")
    }

    pub(crate) fn input_chunk(values: Vec<i32>) -> Chunk {
        let schema = writer_input_schema();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(values)) as ArrayRef],
        )
        .expect("batch");
        let chunk_schema =
            ChunkSchema::try_ref_from_schema_and_slot_ids(schema.as_ref(), &[SlotId::new(1)])
                .expect("chunk schema");
        Chunk::new_with_chunk_schema(batch, chunk_schema)
    }

    fn values_input() -> Box<ExecNode> {
        Box::new(ExecNode {
            kind: crate::exec::node::ExecNodeKind::Values(ValuesNode {
                chunk: Chunk::default(),
                node_id: 1,
            }),
        })
    }

    pub(crate) fn writer_node(execution: Arc<dyn ConnectorWriteExecution>) -> TableWriterNode {
        writer_node_over(values_input(), execution)
    }

    fn writer_factory(node: &TableWriterNode) -> TableWriterOperatorFactory {
        TableWriterOperatorFactory::try_new(
            node,
            crate::runtime::execution_runtime::test_execution_function_set(),
        )
        .expect("table writer factory")
    }

    pub(crate) fn writer_node_with_count_partials(
        execution: Arc<dyn ConnectorWriteExecution>,
        count: usize,
    ) -> (TableWriterNode, Arc<SealedExecutionFunctionSet>) {
        let function_set = crate::runtime::execution_runtime::test_execution_function_set();
        let resolved = function_set
            .catalog()
            .resolve_aggregate_trusted("count", &[DataType::Int32])
            .expect("count(Int32)");
        let channels = (0..count)
            .map(|index| {
                WriterAuxiliaryChannel::try_new(
                    10_000 + u32::try_from(index).expect("channel index"),
                    format!("partial_{index}"),
                    resolved.intermediate_type.clone(),
                )
                .expect("auxiliary channel")
            })
            .collect::<Vec<_>>();
        let contract = WriterMultiplexSchema::try_new(channels.clone()).expect("multiplex schema");
        let relation = WriterMultiplexRelationSchema::try_new(contract).expect("relation schema");
        let partial = WriterPartialAggregatePlan {
            calls: channels
                .iter()
                .map(|channel| WriterPartialAggregateCall {
                    input_slot_id: SlotId::new(1),
                    function_name: Arc::from("count"),
                    resolved: resolved.clone(),
                    intermediate_slot_id: SlotId::new(channel.slot_id()),
                })
                .collect(),
        };
        let node = TableWriterNode::try_new_with_relation(
            values_input(),
            2,
            writer_handle(),
            target(0),
            execution,
            writer_input_schema(),
            identity_projection(),
            TableWriterPhysicalContextTemplate::new([1; 16], 4, [2; 16], 0),
            request_context(),
            Arc::new(TestFragmentEncoder),
            relation,
            partial,
        )
        .expect("composite table writer node");
        (node, function_set)
    }

    /// A writer node over a caller-supplied input, for tests that care about the
    /// pipeline shape the input implies rather than the writer's own behavior.
    pub(crate) fn writer_node_over(
        input: Box<ExecNode>,
        execution: Arc<dyn ConnectorWriteExecution>,
    ) -> TableWriterNode {
        TableWriterNode::try_new(
            input,
            2,
            writer_handle(),
            target(0),
            execution,
            writer_input_schema(),
            identity_projection(),
            TableWriterPhysicalContextTemplate::new([1; 16], 4, [2; 16], 0),
            request_context(),
            Arc::new(TestFragmentEncoder),
        )
        .expect("table writer node")
    }

    fn drain(operator: &mut Box<dyn Operator>, state: &RuntimeState) -> Vec<Chunk> {
        let processor = operator.as_processor_mut().expect("processor");
        let mut out = Vec::new();
        while processor.has_output() {
            match processor.pull_chunk(state).expect("pull") {
                Some(chunk) => out.push(chunk),
                None => break,
            }
        }
        out
    }

    #[test]
    fn table_writer_node_rejects_a_foreign_catalog_generation() {
        struct ForeignExecution(CatalogHandle);
        #[async_trait::async_trait]
        impl ConnectorWriteExecution for ForeignExecution {
            fn catalog_handle(&self) -> &CatalogHandle {
                &self.0
            }
            async fn open_writer(
                &self,
                _request: ConnectorOpenWriterRequest,
            ) -> Result<Box<dyn ConnectorBatchWriter>, ConnectorError> {
                unreachable!("a foreign catalog generation never opens a writer")
            }
        }

        let foreign = CatalogHandle::new(
            ConnectorInstanceId::parse("test_connector").expect("instance id"),
            CatalogVersion::from_bytes([8; 32]),
        );
        let error = TableWriterNode::try_new(
            values_input(),
            2,
            writer_handle(),
            target(0),
            Arc::new(ForeignExecution(foreign)),
            writer_input_schema(),
            identity_projection(),
            TableWriterPhysicalContextTemplate::new([1; 16], 4, [2; 16], 0),
            request_context(),
            Arc::new(TestFragmentEncoder),
        )
        .expect_err("a foreign catalog generation must be rejected before any writer opens");
        assert!(
            error
                .to_string()
                .contains("catalog handle does not match its query-leased write execution")
        );
    }

    #[test]
    fn table_writer_opens_one_independent_writer_per_driver() {
        let stats = Arc::new(WriteExecutionStats::default());
        let execution = Arc::new(TestWriteExecution::new(Arc::clone(&stats)));
        let writer_rows = Arc::clone(&execution.writer_rows);
        let factory = writer_factory(&writer_node(execution.clone()));

        let dop = 4;
        let mut operators: Vec<Box<dyn Operator>> =
            (0..dop).map(|driver| factory.create(dop, driver)).collect();
        let state = test_runtime_state();
        for (driver, operator) in operators.iter_mut().enumerate() {
            bind(operator, &state);
            let processor = operator.as_processor_mut().expect("processor");
            processor
                .push_chunk(&state, input_chunk(vec![driver as i32; driver + 1]))
                .expect("append");
            processor.set_finishing(&state).expect("finish");
        }
        assert!(poll_until(
            || stats.finished.load(Ordering::Relaxed) == dop as usize,
            Duration::from_secs(5)
        ));
        assert_eq!(stats.opened.load(Ordering::Relaxed), dop as usize);
        let mut driver_ids = execution.driver_ids.lock().expect("driver ids").clone();
        driver_ids.sort_unstable();
        assert_eq!(driver_ids, vec![0, 1, 2, 3]);

        // Each driver finished its own writer with only its own rows.
        let mut rows = writer_rows.lock().expect("writer rows").clone();
        rows.sort_unstable();
        assert_eq!(rows, vec![(0, 1), (1, 2), (2, 3), (3, 4)]);
        assert_eq!(stats.finished.load(Ordering::Relaxed), dop as usize);
        assert_eq!(stats.aborted.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn connector_writer_and_partial_aggregate_share_the_projected_target_page() {
        let stats = Arc::new(WriteExecutionStats::default());
        let execution = Arc::new(TestWriteExecution::new(Arc::clone(&stats)).with_fragments(0, 0));
        let writer_rows = Arc::clone(&execution.writer_rows);
        let expected_schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let mut arena = ExprArena::default();
        let input = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Int32);
        let projection =
            TableWriterInputProjection::try_new(arena, vec![input], Arc::clone(&expected_schema))
                .expect("target projection");
        let node = TableWriterNode::try_new(
            values_input(),
            2,
            writer_handle(),
            target(0),
            execution,
            expected_schema,
            projection,
            TableWriterPhysicalContextTemplate::new([1; 16], 4, [2; 16], 0),
            request_context(),
            Arc::new(TestFragmentEncoder),
        )
        .expect("projecting writer node");
        let mut operator = writer_factory(&node).create_operator(1, 0);
        let observed = Arc::new(std::sync::Mutex::new(Vec::new()));
        operator.replace_partial_aggregate_for_test(Box::new(ProjectedInputObserver {
            values: Arc::clone(&observed),
            finishing: false,
            cancelled: false,
        }));
        let state = test_runtime_state();
        operator.prepare().expect("prepare");
        operator.bind_runtime_state(&state).expect("bind");
        ProcessorOperator::push_chunk(&mut operator, &state, input_chunk(vec![7, 11]))
            .expect("push projected page");
        assert_eq!(
            observed.lock().expect("observed values").as_slice(),
            &[7_i64, 11_i64]
        );
        ProcessorOperator::set_finishing(&mut operator, &state).expect("finish");
        let mut boxed: Box<dyn Operator> = Box::new(operator);
        wait_for_output(&mut boxed, &state).expect("writer output");
        assert_eq!(
            writer_rows.lock().expect("writer rows").as_slice(),
            &[(0, 2)]
        );
    }

    #[cfg(debug_assertions)]
    #[test]
    fn partial_aggregate_guard_rejects_update_before_either_consumer_accepts_the_page() {
        let stats = Arc::new(WriteExecutionStats::default());
        let execution = Arc::new(TestWriteExecution::new(Arc::clone(&stats)).with_fragments(0, 0));
        let writer_rows = Arc::clone(&execution.writer_rows);
        let (node, function_set) = writer_node_with_count_partials(execution, 1);
        let node = node.with_aggregate_guard(Arc::new(RejectAggregateBoundary(
            TableWriteAggregateBoundary::PartialUpdate,
        )));
        let mut operator = TableWriterOperatorFactory::try_new(&node, function_set)
            .expect("writer factory")
            .create(1, 0);
        let state = test_runtime_state();
        bind(&mut operator, &state);
        let error = operator
            .as_processor_mut()
            .expect("processor")
            .push_chunk(&state, input_chunk(vec![1, 2, 3]))
            .expect_err("partial update fault");
        assert!(error.contains("PartialUpdate"), "{error}");
        assert!(
            writer_rows.lock().expect("writer rows").is_empty(),
            "the reserved page must not reach the provider writer"
        );
        assert!(!operator.as_processor_ref().expect("processor").has_output());
    }

    #[cfg(debug_assertions)]
    #[test]
    fn partial_aggregate_guard_rejects_finalize_without_success_output() {
        let stats = Arc::new(WriteExecutionStats::default());
        let execution = Arc::new(TestWriteExecution::new(Arc::clone(&stats)).with_fragments(0, 0));
        let (node, function_set) = writer_node_with_count_partials(execution, 1);
        let node = node.with_aggregate_guard(Arc::new(RejectAggregateBoundary(
            TableWriteAggregateBoundary::PartialFinalize,
        )));
        let mut operator = TableWriterOperatorFactory::try_new(&node, function_set)
            .expect("writer factory")
            .create(1, 0);
        let state = test_runtime_state();
        bind(&mut operator, &state);
        operator
            .as_processor_mut()
            .expect("processor")
            .push_chunk(&state, input_chunk(vec![1, 2, 3]))
            .expect("input before finalize fault");
        let error = operator
            .as_processor_mut()
            .expect("processor")
            .set_finishing(&state)
            .expect_err("partial finalize fault");
        assert!(error.contains("PartialFinalize"), "{error}");
        assert!(!operator.as_processor_ref().expect("processor").has_output());
    }

    #[test]
    fn table_writer_emits_one_row_count_row_and_one_row_per_fragment() {
        for fragment_count in [0usize, 1, 3] {
            let stats = Arc::new(WriteExecutionStats::default());
            let execution = Arc::new(
                TestWriteExecution::new(Arc::clone(&stats)).with_fragments(fragment_count, 4),
            );
            let factory = writer_factory(&writer_node(execution));
            let mut operator = factory.create(1, 0);
            let state = test_runtime_state();
            bind(&mut operator, &state);
            let processor = operator.as_processor_mut().expect("processor");
            processor
                .push_chunk(&state, input_chunk(vec![1, 2, 3, 4, 5]))
                .expect("append");
            assert!(!processor.has_output(), "nothing is emitted before finish");
            processor.set_finishing(&state).expect("finish");

            let chunks = wait_for_output(&mut operator, &state).expect("writer output");
            assert_eq!(
                chunks.iter().map(Chunk::len).sum::<usize>(),
                fragment_count + 1
            );
            let mut row = 0usize;
            for chunk in &chunks {
                let columns = TableWriteRelationColumns::try_from_chunk(chunk).expect("columns");
                for local in 0..chunk.len() {
                    if row == 0 {
                        assert_eq!(
                            columns.kinds.value(local),
                            WriterRowKind::RowCount.to_wire()
                        );
                        assert_eq!(columns.ordinals.value(local), 0);
                        assert_eq!(columns.row_counts.value(local), 5);
                        assert!(columns.fragments.is_null(local));
                    } else {
                        assert_eq!(
                            columns.kinds.value(local),
                            WriterRowKind::CommitFragment.to_wire()
                        );
                        assert_eq!(columns.ordinals.value(local), 0);
                        assert!(columns.row_counts.is_null(local));
                        assert_eq!(columns.fragments.value(local).len(), 4);
                    }
                    row += 1;
                }
            }
            assert!(operator.is_finished());
        }
    }

    #[test]
    fn composite_writer_counts_every_page_on_both_sides_and_restores_1024_channels() {
        let stats = Arc::new(WriteExecutionStats::default());
        let execution = Arc::new(TestWriteExecution::new(Arc::clone(&stats)).with_fragments(0, 0));
        let writer_rows = Arc::clone(&execution.writer_rows);
        let (node, function_set) = writer_node_with_count_partials(execution, 1_024);
        let factory =
            TableWriterOperatorFactory::try_new(&node, function_set).expect("composite factory");
        let mut operator = factory.create(1, 0);
        let state = test_runtime_state();
        bind(&mut operator, &state);
        let processor = operator.as_processor_mut().expect("processor");
        processor
            .push_chunk(&state, input_chunk(vec![1, 2]))
            .expect("first page");
        processor
            .push_chunk(&state, input_chunk(vec![3, 4, 5]))
            .expect("second page");
        processor.set_finishing(&state).expect("finish");

        let chunks = wait_for_output(&mut operator, &state).expect("multiplex output");
        assert_eq!(
            writer_rows.lock().expect("writer rows").as_slice(),
            &[(0, 5)],
            "the provider writer must see exactly the pages counted by aggregation"
        );
        let mut row_count_rows = 0usize;
        let mut observed_channels = vec![false; 1_024];
        for chunk in &chunks {
            assert_eq!(
                chunk.schema().as_ref(),
                node.writer_multiplex_schema()
                    .contract()
                    .arrow_schema()
                    .as_ref(),
            );
            let prefix = TableWriteRelationColumns::try_from_chunk(chunk).expect("prefix");
            for row in 0..chunk.len() {
                match WriterRowKind::from_wire(prefix.kinds.value(row)).expect("kind") {
                    WriterRowKind::RowCount => {
                        row_count_rows += 1;
                        assert_eq!(prefix.row_counts.value(row), 5);
                    }
                    WriterRowKind::AggregatePartial => {
                        let mut non_null = 0usize;
                        for (index, channel) in node
                            .writer_multiplex_schema()
                            .contract()
                            .auxiliary_channels()
                            .iter()
                            .enumerate()
                        {
                            let array = chunk
                                .column_by_slot_id(SlotId::new(channel.slot_id()))
                                .expect("auxiliary column");
                            if !array.is_null(row) {
                                let value = array
                                    .as_any()
                                    .downcast_ref::<Int64Array>()
                                    .expect("count intermediate")
                                    .value(row);
                                assert_eq!(value, 5);
                                assert!(!observed_channels[index]);
                                observed_channels[index] = true;
                                non_null += 1;
                            }
                        }
                        assert!(non_null > 0, "a sparse partial row cannot be empty");
                    }
                    WriterRowKind::CommitFragment => panic!("no fragments expected"),
                }
            }
        }
        assert_eq!(row_count_rows, 1);
        assert!(observed_channels.into_iter().all(|seen| seen));
        assert!(operator.is_finished());
    }

    #[test]
    fn streaming_partial_outputs_block_input_then_resume_and_continue_while_draining() {
        let stats = Arc::new(WriteExecutionStats::default());
        let execution = Arc::new(TestWriteExecution::new(Arc::clone(&stats)).with_fragments(0, 0));
        let writer_rows = Arc::clone(&execution.writer_rows);
        let (node, function_set) = writer_node_with_count_partials(execution, 1);
        let factory =
            TableWriterOperatorFactory::try_new(&node, function_set).expect("composite factory");
        let slot_id = SlotId::new(
            node.writer_multiplex_schema()
                .contract()
                .auxiliary_channels()[0]
                .slot_id(),
        );
        let aggregate_rows = Arc::new(AtomicUsize::new(0));
        let mut operator = factory.create_operator(1, 0);
        operator.replace_partial_aggregate_for_test(Box::new(StreamingPartial::new(
            slot_id,
            Arc::clone(&aggregate_rows),
        )));
        let state = test_runtime_state();
        operator.prepare().expect("prepare");
        operator.bind_runtime_state(&state).expect("bind");

        ProcessorOperator::push_chunk(&mut operator, &state, input_chunk(vec![1, 2]))
            .expect("first input page");
        assert!(!ProcessorOperator::need_input(&operator));
        assert!(ProcessorOperator::has_output(&operator));
        let first = ProcessorOperator::pull_chunk(&mut operator, &state)
            .expect("first running output")
            .expect("first running partial");
        assert!(!ProcessorOperator::need_input(&operator));
        let second = ProcessorOperator::pull_chunk(&mut operator, &state)
            .expect("second running output")
            .expect("second running partial");
        assert!(ProcessorOperator::need_input(&operator));

        ProcessorOperator::push_chunk(&mut operator, &state, input_chunk(vec![3]))
            .expect("second input page");
        ProcessorOperator::set_finishing(&mut operator, &state).expect("finish");
        assert!(
            !operator.pending_finish(),
            "pending partial output must remain dataflow-drivable while the writer finishes"
        );
        let mut outputs = vec![first, second];
        let deadline = Instant::now() + Duration::from_secs(5);
        while !operator.is_finished() {
            if ProcessorOperator::has_output(&operator) {
                if let Some(output) =
                    ProcessorOperator::pull_chunk(&mut operator, &state).expect("drain output")
                {
                    outputs.push(output);
                }
            } else if Instant::now() >= deadline {
                panic!("streaming composite writer did not finish");
            } else {
                std::thread::sleep(Duration::from_millis(2));
            }
        }

        assert_eq!(aggregate_rows.load(Ordering::Relaxed), 3);
        assert_eq!(
            writer_rows.lock().expect("writer rows").as_slice(),
            &[(0, 3)]
        );
        let mut partial_values = Vec::new();
        let mut row_count = None;
        for output in &outputs {
            let prefix = TableWriteRelationColumns::try_from_chunk(output).expect("prefix");
            for row in 0..output.len() {
                match WriterRowKind::from_wire(prefix.kinds.value(row)).expect("kind") {
                    WriterRowKind::AggregatePartial => {
                        partial_values.push(
                            output
                                .column_by_slot_id(slot_id)
                                .expect("partial channel")
                                .as_any()
                                .downcast_ref::<Int64Array>()
                                .expect("partial Int64")
                                .value(row),
                        );
                    }
                    WriterRowKind::RowCount => row_count = Some(prefix.row_counts.value(row)),
                    WriterRowKind::CommitFragment => panic!("no fragments expected"),
                }
            }
        }
        assert_eq!(partial_values, vec![10, 20, 10, 20]);
        assert_eq!(row_count, Some(3));
    }

    #[test]
    fn one_oversized_partial_value_fails_the_attempt_without_success_output() {
        const PACKET_BYTES: usize = 64 * 1024;

        let stats = Arc::new(WriteExecutionStats::default());
        let execution = Arc::new(TestWriteExecution::new(Arc::clone(&stats)).with_fragments(0, 0));
        let channel = WriterAuxiliaryChannel::try_new(20_000, "huge", DataType::Binary)
            .expect("binary auxiliary channel");
        let relation = WriterMultiplexRelationSchema::try_new(
            WriterMultiplexSchema::try_new(vec![channel.clone()]).expect("multiplex contract"),
        )
        .expect("multiplex relation");
        let node = TableWriterNode::try_new_with_relation(
            values_input(),
            2,
            writer_handle(),
            target(0),
            execution,
            writer_input_schema(),
            identity_projection(),
            TableWriterPhysicalContextTemplate::new([1; 16], 4, [2; 16], 0),
            request_context(),
            Arc::new(TestFragmentEncoder),
            relation,
            WriterPartialAggregatePlan::default(),
        )
        .expect("writer node");
        let schema = Arc::new(Schema::new(vec![Field::new(
            "huge",
            DataType::Binary,
            true,
        )]));
        let value = vec![7u8; MAX_WRITER_MULTIPLEX_ROW_BYTES];
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(BinaryArray::from(vec![Some(value.as_slice())]))],
        )
        .expect("oversized partial batch");
        let slot_id = SlotId::new(channel.slot_id());
        let chunk_schema =
            ChunkSchema::try_ref_from_schema_and_slot_ids(schema.as_ref(), &[slot_id])
                .expect("oversized partial schema");
        let partial = Chunk::new_with_chunk_schema(batch, chunk_schema);

        let factory = writer_factory(&node);
        let mut operator = factory.create_operator(1, 0);
        operator.replace_partial_aggregate_for_test(Box::new(OneShotPartial::new(partial)));
        let state = test_runtime_state_with_packet_budget(PACKET_BYTES, None);
        operator.prepare().expect("prepare");
        operator.bind_runtime_state(&state).expect("bind");
        assert!(poll_until(
            || stats.opened.load(Ordering::Relaxed) == 1,
            Duration::from_secs(5)
        ));
        ProcessorOperator::push_chunk(&mut operator, &state, input_chunk(vec![1]))
            .expect("input reaches both children");
        let error = ProcessorOperator::pull_chunk(&mut operator, &state)
            .expect_err("one oversized value must fail before any sparse row is emitted");
        assert!(error.contains("ResourceExhausted"), "{error}");
        assert!(error.contains("one writer aggregate intermediate value"));
        assert!(
            error.contains(&format!("limit is {MAX_WRITER_MULTIPLEX_ROW_BYTES}")),
            "{error}"
        );
        assert!(!ProcessorOperator::has_output(&operator));
        assert!(poll_until(
            || stats.aborted.load(Ordering::Relaxed) == 1,
            Duration::from_secs(5)
        ));
    }

    #[test]
    fn sparse_packer_splits_1024_typed_channels_across_multiple_batches() {
        const CHANNELS: usize = 1_024;
        const VALUE_BYTES: usize = 20 * 1024;
        const PACKET_BYTES: usize = 320 * 1024;

        let stats = Arc::new(WriteExecutionStats::default());
        let execution = Arc::new(TestWriteExecution::new(Arc::clone(&stats)).with_fragments(0, 0));
        let channels = (0..CHANNELS)
            .map(|index| {
                WriterAuxiliaryChannel::try_new(
                    30_000 + u32::try_from(index).expect("channel index"),
                    format!("binary_partial_{index}"),
                    DataType::Binary,
                )
                .expect("binary channel")
            })
            .collect::<Vec<_>>();
        let relation = WriterMultiplexRelationSchema::try_new(
            WriterMultiplexSchema::try_new(channels.clone()).expect("multiplex contract"),
        )
        .expect("multiplex relation");
        let node = TableWriterNode::try_new_with_relation(
            values_input(),
            2,
            writer_handle(),
            target(0),
            execution,
            writer_input_schema(),
            identity_projection(),
            TableWriterPhysicalContextTemplate::new([1; 16], 4, [2; 16], 0),
            request_context(),
            Arc::new(TestFragmentEncoder),
            relation,
            WriterPartialAggregatePlan::default(),
        )
        .expect("writer node");
        let fields = channels
            .iter()
            .map(|channel| Field::new(channel.name(), DataType::Binary, true))
            .collect::<Vec<_>>();
        let slot_ids = channels
            .iter()
            .map(|channel| SlotId::new(channel.slot_id()))
            .collect::<Vec<_>>();
        let arrays = (0..CHANNELS)
            .map(|index| {
                let value = vec![u8::try_from(index % 251).expect("byte"); VALUE_BYTES];
                Arc::new(BinaryArray::from(vec![Some(value.as_slice())])) as ArrayRef
            })
            .collect::<Vec<_>>();
        let schema = Arc::new(Schema::new(fields));
        let batch = RecordBatch::try_new(Arc::clone(&schema), arrays).expect("wide partial batch");
        let chunk_schema =
            ChunkSchema::try_ref_from_schema_and_slot_ids(schema.as_ref(), &slot_ids)
                .expect("wide partial schema");
        let partial = Chunk::new_with_chunk_schema(batch, chunk_schema);

        let factory = writer_factory(&node);
        let mut operator = factory.create_operator(1, 0);
        operator.replace_partial_aggregate_for_test(Box::new(OneShotPartial::new(partial)));
        let state = test_runtime_state_with_packet_budget(PACKET_BYTES, None);
        operator.prepare().expect("prepare");
        operator.bind_runtime_state(&state).expect("bind");
        assert!(poll_until(
            || stats.opened.load(Ordering::Relaxed) == 1,
            Duration::from_secs(5)
        ));
        ProcessorOperator::push_chunk(&mut operator, &state, input_chunk(vec![1]))
            .expect("input reaches both children");

        let mut output_batches = 0usize;
        let mut seen = vec![false; CHANNELS];
        while ProcessorOperator::has_output(&operator) {
            let output = ProcessorOperator::pull_chunk(&mut operator, &state)
                .expect("sparse output")
                .expect("sparse batch");
            output_batches += 1;
            assert_eq!(output.len(), 1);
            assert!(
                operator.output_size(&output).expect("encoded output size") <= PACKET_BYTES,
                "every sparse batch must honor the runtime packet budget"
            );
            let prefix = TableWriteRelationColumns::try_from_chunk(&output).expect("prefix");
            assert_eq!(
                WriterRowKind::from_wire(prefix.kinds.value(0)).expect("kind"),
                WriterRowKind::AggregatePartial
            );
            let mut populated = 0usize;
            for (index, slot_id) in slot_ids.iter().enumerate() {
                let array = output
                    .column_by_slot_id(*slot_id)
                    .expect("sparse auxiliary channel");
                if !array.is_null(0) {
                    populated += 1;
                    assert!(!seen[index], "channel {index} was emitted twice");
                    let value = array
                        .as_any()
                        .downcast_ref::<BinaryArray>()
                        .expect("binary partial")
                        .value(0);
                    assert_eq!(value.len(), VALUE_BYTES);
                    assert_eq!(value[0], u8::try_from(index % 251).expect("byte"));
                    seen[index] = true;
                }
            }
            assert!(populated > 0 && populated < CHANNELS);
        }
        assert!(output_batches > 1, "the wide row must be split by bytes");
        assert_eq!(operator.partial_batches, output_batches);
        assert_eq!(operator.sparse_partial_rows, output_batches);
        assert_eq!(operator.partial_channels, CHANNELS);
        assert!(seen.into_iter().all(|channel| channel));
        assert!(ProcessorOperator::need_input(&operator));
        operator.cancel();
        assert!(poll_until(
            || operator.is_finished(),
            Duration::from_secs(5)
        ));
    }

    #[test]
    fn table_writer_reports_zero_rows_when_it_wrote_nothing() {
        let stats = Arc::new(WriteExecutionStats::default());
        let execution = Arc::new(TestWriteExecution::new(Arc::clone(&stats)).with_fragments(0, 0));
        let factory = writer_factory(&writer_node(execution));
        let mut operator = factory.create(1, 0);
        let state = test_runtime_state();
        bind(&mut operator, &state);
        operator
            .as_processor_mut()
            .expect("processor")
            .set_finishing(&state)
            .expect("finish");
        let chunks = wait_for_output(&mut operator, &state).expect("writer output");
        let columns = TableWriteRelationColumns::try_from_chunk(&chunks[0]).expect("columns");
        assert_eq!(chunks[0].len(), 1);
        assert_eq!(columns.row_counts.value(0), 0);
    }

    #[test]
    fn table_writer_rejects_a_fragment_over_the_frozen_single_fragment_budget() {
        // Exactly at the limit is accepted.
        let stats = Arc::new(WriteExecutionStats::default());
        let execution = Arc::new(
            TestWriteExecution::new(Arc::clone(&stats))
                .with_fragments(1, MAX_CONNECTOR_COMMIT_FRAGMENT_BYTES),
        );
        let factory = writer_factory(&writer_node(execution));
        let mut operator = factory.create(1, 0);
        let state = test_runtime_state();
        bind(&mut operator, &state);
        operator
            .as_processor_mut()
            .expect("processor")
            .set_finishing(&state)
            .expect("the exact single-fragment budget is legal");
        wait_for_output(&mut operator, &state).expect("exact-budget output");

        // One byte more is a typed rejection, and the writer is aborted.
        let stats = Arc::new(WriteExecutionStats::default());
        let execution = Arc::new(
            TestWriteExecution::new(Arc::clone(&stats))
                .with_fragments(1, MAX_CONNECTOR_COMMIT_FRAGMENT_BYTES + 1),
        );
        let factory = writer_factory(&writer_node(execution));
        let mut operator = factory.create(1, 0);
        let state = test_runtime_state();
        bind(&mut operator, &state);
        operator
            .as_processor_mut()
            .expect("processor")
            .set_finishing(&state)
            .expect("finish request is asynchronous");
        let error =
            wait_for_output(&mut operator, &state).expect_err("over the single-fragment budget");
        assert!(error.contains("exceeds the frozen single-fragment budget"));
        assert!(poll_until(
            || stats.aborted.load(Ordering::Relaxed) == 1,
            Duration::from_secs(5)
        ));
        assert_eq!(stats.aborted.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn cancelling_one_driver_aborts_only_its_own_writer() {
        let stats = Arc::new(WriteExecutionStats::default());
        let execution = Arc::new(TestWriteExecution::new(Arc::clone(&stats)));
        let factory = writer_factory(&writer_node(execution));
        let mut first = factory.create(2, 0);
        let mut second = factory.create(2, 1);
        let state = test_runtime_state();
        bind(&mut first, &state);
        bind(&mut second, &state);
        assert!(poll_until(
            || stats.opened.load(Ordering::Relaxed) == 2,
            Duration::from_secs(5)
        ));

        first.cancel();
        assert!(poll_until(
            || stats.aborted.load(Ordering::Relaxed) == 1,
            Duration::from_secs(5)
        ));
        assert_eq!(stats.aborted.load(Ordering::Relaxed), 1);
        // A second cancel of the same driver is idempotent.
        first.cancel();
        assert_eq!(stats.aborted.load(Ordering::Relaxed), 1);

        // The other driver is untouched and still finishes normally.
        second
            .as_processor_mut()
            .expect("processor")
            .set_finishing(&state)
            .expect("second driver finishes independently");
        assert!(poll_until(
            || stats.finished.load(Ordering::Relaxed) == 1,
            Duration::from_secs(5)
        ));
        assert_eq!(stats.finished.load(Ordering::Relaxed), 1);
        assert_eq!(stats.aborted.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn a_failed_writer_open_fails_the_driver_asynchronously() {
        struct FailingExecution(CatalogHandle);
        #[async_trait::async_trait]
        impl ConnectorWriteExecution for FailingExecution {
            fn catalog_handle(&self) -> &CatalogHandle {
                &self.0
            }
            async fn open_writer(
                &self,
                _request: ConnectorOpenWriterRequest,
            ) -> Result<Box<dyn ConnectorBatchWriter>, ConnectorError> {
                Err(ConnectorError::new(
                    novarocks_spi::connector::ConnectorErrorKind::Unavailable,
                    "provider refused to open a writer",
                ))
            }
        }

        let execution = Arc::new(FailingExecution(catalog_handle()));
        let factory = writer_factory(&writer_node(execution));
        let mut operator = factory.create(1, 0);
        let state = test_runtime_state();
        bind(&mut operator, &state);
        assert!(poll_until(
            || state.error().is_some(),
            Duration::from_secs(5)
        ));
        let error = state
            .error()
            .expect("a failed writer open must fail its driver");
        assert!(error.contains("open connector writer"));
    }

    #[test]
    fn writer_finish_error_without_a_result_drives_draining_to_terminal_failure() {
        struct FailFinishExecution {
            catalog_handle: CatalogHandle,
            aborted: Arc<AtomicUsize>,
        }
        struct FailFinishWriter(Arc<AtomicUsize>);

        #[async_trait::async_trait]
        impl ConnectorWriteExecution for FailFinishExecution {
            fn catalog_handle(&self) -> &CatalogHandle {
                &self.catalog_handle
            }

            async fn open_writer(
                &self,
                _request: ConnectorOpenWriterRequest,
            ) -> Result<Box<dyn ConnectorBatchWriter>, ConnectorError> {
                Ok(Box::new(FailFinishWriter(Arc::clone(&self.aborted))))
            }
        }

        #[async_trait::async_trait]
        impl ConnectorBatchWriter for FailFinishWriter {
            async fn append(&mut self, _batch: RecordBatch) -> Result<(), ConnectorError> {
                Ok(())
            }

            async fn finish(&mut self) -> Result<Vec<ConnectorCommitFragment>, ConnectorError> {
                Err(ConnectorError::new(
                    novarocks_spi::connector::ConnectorErrorKind::Unavailable,
                    "injected finish failure",
                ))
            }

            async fn abort(&mut self) -> Result<(), ConnectorError> {
                self.0.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
        }

        let aborted = Arc::new(AtomicUsize::new(0));
        let execution = Arc::new(FailFinishExecution {
            catalog_handle: catalog_handle(),
            aborted: Arc::clone(&aborted),
        });
        let factory = writer_factory(&writer_node(execution));
        let mut operator = factory.create_operator(1, 0);
        let state = test_runtime_state();
        operator.prepare().expect("prepare");
        operator.bind_runtime_state(&state).expect("bind");
        ProcessorOperator::set_finishing(&mut operator, &state).expect("request finish");

        assert!(poll_until(
            || ProcessorOperator::has_output(&operator),
            Duration::from_secs(5)
        ));
        let error = ProcessorOperator::pull_chunk(&mut operator, &state)
            .expect_err("actor error must be pulled instead of leaving the driver OutputFull");
        assert!(error.contains("finish connector writer"), "{error}");
        assert_eq!(aborted.load(Ordering::Relaxed), 1);
        assert!(!ProcessorOperator::has_output(&operator));
        assert!(poll_until(
            || operator.is_finished(),
            Duration::from_secs(5)
        ));
    }

    #[test]
    fn driver_resumes_after_async_writer_finish_to_deliver_its_output() {
        let stats = Arc::new(WriteExecutionStats::default());
        let execution = Arc::new(TestWriteExecution::new(Arc::clone(&stats)));
        let factory = writer_factory(&writer_node(execution));
        let runtime_state = Arc::new(test_runtime_state());
        let mut writer = factory.create(1, 0);
        bind(&mut writer, runtime_state.as_ref());
        let collected = Arc::new(std::sync::Mutex::new(Vec::new()));
        let mut driver = PipelineDriver::new(
            0,
            vec![
                Box::new(OneChunkSource {
                    chunk: Some(input_chunk(vec![1, 2, 3])),
                    finished: false,
                }),
                writer,
                Box::new(CollectSink {
                    chunks: Arc::clone(&collected),
                    finished: false,
                }),
            ],
            None,
            Vec::new(),
            Arc::clone(&runtime_state),
            None,
        );

        let mut saw_pending_finish = false;
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            match driver.process(Duration::from_millis(10)) {
                DriverState::PendingFinish => saw_pending_finish = true,
                DriverState::Finished => break,
                DriverState::Failed(error) => panic!("driver failed: {error}"),
                state if Instant::now() >= deadline => {
                    panic!("driver did not finish before timeout: {state:?}")
                }
                _ => std::thread::sleep(Duration::from_millis(2)),
            }
        }

        assert!(saw_pending_finish);
        let chunks = collected.lock().expect("collected chunks");
        assert_eq!(chunks.len(), 2);
        let columns = TableWriteRelationColumns::try_from_chunk(&chunks[0]).expect("columns");
        assert_eq!(columns.row_counts.value(0), 3);
        assert_eq!(stats.finished.load(Ordering::Relaxed), 1);
        assert_eq!(stats.aborted.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn slow_writer_backpressure_is_part_of_composite_need_input() {
        struct SlowExecution {
            catalog_handle: CatalogHandle,
            append_started: Arc<AtomicBool>,
            gate: Arc<Notify>,
            rows: Arc<AtomicUsize>,
        }
        struct SlowWriter {
            first: bool,
            append_started: Arc<AtomicBool>,
            gate: Arc<Notify>,
            rows: Arc<AtomicUsize>,
        }

        #[async_trait::async_trait]
        impl ConnectorWriteExecution for SlowExecution {
            fn catalog_handle(&self) -> &CatalogHandle {
                &self.catalog_handle
            }

            async fn open_writer(
                &self,
                _request: ConnectorOpenWriterRequest,
            ) -> Result<Box<dyn ConnectorBatchWriter>, ConnectorError> {
                Ok(Box::new(SlowWriter {
                    first: true,
                    append_started: Arc::clone(&self.append_started),
                    gate: Arc::clone(&self.gate),
                    rows: Arc::clone(&self.rows),
                }))
            }
        }

        #[async_trait::async_trait]
        impl ConnectorBatchWriter for SlowWriter {
            async fn append(&mut self, batch: RecordBatch) -> Result<(), ConnectorError> {
                if self.first {
                    self.first = false;
                    self.append_started.store(true, Ordering::Release);
                    self.gate.notified().await;
                }
                self.rows.fetch_add(batch.num_rows(), Ordering::Relaxed);
                Ok(())
            }

            async fn finish(&mut self) -> Result<Vec<ConnectorCommitFragment>, ConnectorError> {
                Ok(Vec::new())
            }

            async fn abort(&mut self) -> Result<(), ConnectorError> {
                Ok(())
            }
        }

        let append_started = Arc::new(AtomicBool::new(false));
        let gate = Arc::new(Notify::new());
        let rows = Arc::new(AtomicUsize::new(0));
        let execution = Arc::new(SlowExecution {
            catalog_handle: catalog_handle(),
            append_started: Arc::clone(&append_started),
            gate: Arc::clone(&gate),
            rows: Arc::clone(&rows),
        });
        let factory = writer_factory(&writer_node(execution));
        let mut operator = factory.create_operator(1, 0);
        let profiles = OperatorProfiles::new(crate::runtime::profile::RuntimeProfile::new(
            "slow-writer-metrics",
        ));
        operator.set_profiles(profiles.clone());
        let state = test_runtime_state();
        operator.prepare().expect("prepare");
        operator.bind_runtime_state(&state).expect("bind");

        ProcessorOperator::push_chunk(&mut operator, &state, input_chunk(vec![1]))
            .expect("first page");
        assert!(poll_until(
            || append_started.load(Ordering::Acquire),
            Duration::from_secs(5)
        ));
        for value in 2..=4 {
            ProcessorOperator::push_chunk(&mut operator, &state, input_chunk(vec![value]))
                .expect("page fits the bounded writer queue");
        }
        assert!(
            !ProcessorOperator::need_input(&operator),
            "the slow writer's full queue must block composite input"
        );
        std::thread::sleep(Duration::from_millis(1));

        gate.notify_one();
        assert!(poll_until(
            || ProcessorOperator::need_input(&operator),
            Duration::from_secs(5)
        ));
        ProcessorOperator::push_chunk(&mut operator, &state, input_chunk(vec![5]))
            .expect("input resumes only after writer capacity is released");
        ProcessorOperator::set_finishing(&mut operator, &state).expect("finish");
        let mut boxed: Box<dyn Operator> = Box::new(operator);
        let outputs = wait_for_output(&mut boxed, &state).expect("writer output");
        let prefix = TableWriteRelationColumns::try_from_chunk(&outputs[0]).expect("prefix");
        assert_eq!(prefix.row_counts.value(0), 5);
        assert_eq!(rows.load(Ordering::Relaxed), 5);
        assert_eq!(
            profiles.common.counter_value("WriterQueueBlockedIntervals"),
            Some(1)
        );
        assert!(
            profiles
                .common
                .counter_value("WriterQueueBlockedTime")
                .is_some_and(|elapsed| elapsed > 0)
        );
    }

    #[test]
    fn fragment_cancel_waits_for_writer_abort_and_releases_queued_memory() {
        struct GatedAbortExecution {
            catalog_handle: CatalogHandle,
            append_started: Arc<AtomicBool>,
            abort_started: Arc<AtomicBool>,
            append_gate: Arc<Notify>,
            abort_gate: Arc<Notify>,
        }

        #[async_trait::async_trait]
        impl ConnectorWriteExecution for GatedAbortExecution {
            fn catalog_handle(&self) -> &CatalogHandle {
                &self.catalog_handle
            }

            async fn open_writer(
                &self,
                _request: ConnectorOpenWriterRequest,
            ) -> Result<Box<dyn ConnectorBatchWriter>, ConnectorError> {
                Ok(Box::new(GatedAbortWriter {
                    append_started: Arc::clone(&self.append_started),
                    abort_started: Arc::clone(&self.abort_started),
                    append_gate: Arc::clone(&self.append_gate),
                    abort_gate: Arc::clone(&self.abort_gate),
                }))
            }
        }

        struct GatedAbortWriter {
            append_started: Arc<AtomicBool>,
            abort_started: Arc<AtomicBool>,
            append_gate: Arc<Notify>,
            abort_gate: Arc<Notify>,
        }

        #[async_trait::async_trait]
        impl ConnectorBatchWriter for GatedAbortWriter {
            async fn append(&mut self, _batch: RecordBatch) -> Result<(), ConnectorError> {
                self.append_started.store(true, Ordering::Release);
                self.append_gate.notified().await;
                Ok(())
            }

            async fn finish(&mut self) -> Result<Vec<ConnectorCommitFragment>, ConnectorError> {
                panic!("a cancelled writer must not finish")
            }

            async fn abort(&mut self) -> Result<(), ConnectorError> {
                self.abort_started.store(true, Ordering::Release);
                self.abort_gate.notified().await;
                Ok(())
            }
        }

        let append_started = Arc::new(AtomicBool::new(false));
        let abort_started = Arc::new(AtomicBool::new(false));
        let append_gate = Arc::new(Notify::new());
        let abort_gate = Arc::new(Notify::new());
        let execution = Arc::new(GatedAbortExecution {
            catalog_handle: catalog_handle(),
            append_started: Arc::clone(&append_started),
            abort_started: Arc::clone(&abort_started),
            append_gate,
            abort_gate: Arc::clone(&abort_gate),
        });
        let memory = MemTracker::new_root("table-writer-cancel-test");
        let runtime_state = Arc::new(test_runtime_state_with_mem(Some(Arc::clone(&memory))));
        let factory = writer_factory(&writer_node(execution));
        let mut writer = factory.create(1, 0);
        bind(&mut writer, runtime_state.as_ref());
        let mut driver = PipelineDriver::new(
            0,
            vec![
                Box::new(OneChunkSource {
                    chunk: Some(input_chunk(vec![1, 2, 3])),
                    finished: false,
                }),
                writer,
                Box::new(CollectSink {
                    chunks: Arc::new(std::sync::Mutex::new(Vec::new())),
                    finished: false,
                }),
            ],
            None,
            Vec::new(),
            Arc::clone(&runtime_state),
            None,
        );
        let deadline = Instant::now() + Duration::from_secs(5);
        while !append_started.load(Ordering::Acquire) {
            match driver.process(Duration::from_millis(10)) {
                DriverState::Failed(error) => panic!("driver failed before cancellation: {error}"),
                state if Instant::now() >= deadline => {
                    panic!("writer append did not start before timeout: {state:?}")
                }
                _ => std::thread::sleep(Duration::from_millis(2)),
            }
        }
        assert!(memory.current() > 0, "queued Arrow input must be charged");

        let fragment = Arc::new(FragmentContext::new(
            None,
            Arc::clone(&runtime_state),
            None,
            None,
            None,
            None,
        ));
        let completion = FragmentCompletion::new(1);
        assert!(completion.fail("injected fragment cancellation".to_string()));
        let task = DriverTask::new(
            driver,
            Arc::clone(&completion),
            fragment,
            Duration::from_millis(10),
        );
        let task = task
            .finish_due_to_abort()
            .expect("driver must remain pending while writer abort is in flight");
        assert!(poll_until(
            || abort_started.load(Ordering::Acquire),
            Duration::from_secs(5)
        ));

        let (tx, rx) = std::sync::mpsc::channel();
        let waiting = Arc::clone(&completion);
        let waiter = std::thread::spawn(move || tx.send(waiting.wait()).expect("send result"));
        assert!(
            rx.recv_timeout(Duration::from_millis(50)).is_err(),
            "fragment completion must wait for the writer actor's bounded abort"
        );
        assert_eq!(
            memory.current(),
            0,
            "cancel must release queued Arrow input"
        );

        abort_gate.notify_one();
        assert!(poll_until(|| task.check_is_ready(), Duration::from_secs(5)));
        assert!(
            task.finish_due_to_abort().is_none(),
            "driver must become terminal after the writer actor joins"
        );
        let error = rx
            .recv_timeout(Duration::from_secs(1))
            .expect("fragment completion after writer abort")
            .expect_err("the injected cancellation remains first-wins");
        assert_eq!(error, "injected fragment cancellation");
        waiter.join().expect("completion waiter");
        assert_eq!(memory.current(), 0);
    }

    #[test]
    fn a_table_writer_is_never_the_pipeline_sink() {
        let stats = Arc::new(WriteExecutionStats::default());
        let execution = Arc::new(TestWriteExecution::new(stats));
        let factory = writer_factory(&writer_node(execution));
        assert!(!factory.is_sink());
        assert!(!factory.is_source());
    }
}
