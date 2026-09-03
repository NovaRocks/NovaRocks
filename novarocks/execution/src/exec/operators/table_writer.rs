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
//! On a successful finish the operator emits exactly one ROW_COUNT row followed
//! by zero or more COMMIT_FRAGMENT rows. Canonical bytes are produced by the
//! encoder port, never by this layer, and each fragment is charged against the
//! frozen single-fragment budget at egress.

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BinaryArray, BinaryBuilder, Int8Array, Int32Array, Int64Array,
};
use arrow::record_batch::RecordBatch;

use novarocks_spi::connector::ConnectorRequestContext;
use novarocks_spi::connector::write_stack::{
    ConnectorOpenWriterRequest, ConnectorWriteExecution, ConnectorWriterHandle,
    MAX_CONNECTOR_COMMIT_FRAGMENT_BYTES, WriteTargetOrdinal, WriterRowKind, row_count_to_wire,
    target_ordinal_to_wire,
};

use crate::exec::chunk::{Chunk, record_batch_bytes};
use crate::exec::node::table_write_relation::{
    ConnectorCommitFragmentEncoder, writer_relation_chunk_schema, writer_relation_schema,
};
use crate::exec::node::table_writer::{
    TableWriterInputProjection, TableWriterNode, TableWriterPhysicalContextTemplate,
};
use crate::exec::pipeline::async_writer::{AsyncWriterOwner, AsyncWriterQueueConfig};
use crate::exec::pipeline::operator::{Operator, ProcessorOperator};
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::exec::pipeline::schedule::observer::Observable;
use crate::runtime::mem_tracker::MemTracker;
use crate::runtime::runtime_state::RuntimeState;

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
    pub fn new(node: &TableWriterNode) -> Self {
        let name = if node.node_id >= 0 {
            format!("TABLE_WRITER (id={})", node.node_id)
        } else {
            "TABLE_WRITER".to_string()
        };
        Self {
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
            }),
        }
    }
}

impl OperatorFactory for TableWriterOperatorFactory {
    fn name(&self) -> &str {
        &self.name
    }

    fn create(&self, _dop: i32, driver_id: i32) -> Box<dyn Operator> {
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
        let writer = AsyncWriterOwner::new(
            Arc::clone(&plan.execution),
            request,
            AsyncWriterQueueConfig::default(),
            Box::new(move |accepted_rows, fragments| {
                build_output(target, fragment_encoder.as_ref(), accepted_rows, &fragments)
            }),
        );
        Box::new(TableWriterOperator {
            name: self.name.clone(),
            projection: plan.projection.clone(),
            writer,
            finishing: false,
            finished: false,
        })
    }

    /// A table writer has output, so it is never the pipeline's terminal sink.
    fn is_sink(&self) -> bool {
        false
    }
}

struct TableWriterOperator {
    name: String,
    projection: TableWriterInputProjection,
    writer: AsyncWriterOwner<Chunk>,
    finishing: bool,
    finished: bool,
}

fn build_output(
    target: WriteTargetOrdinal,
    fragment_encoder: &dyn ConnectorCommitFragmentEncoder,
    accepted_rows: u64,
    fragments: &[novarocks_spi::connector::write_stack::ConnectorCommitFragment],
) -> Result<Chunk, String> {
    let mut encoded = Vec::with_capacity(fragments.len());
    for fragment in fragments {
        let bytes = fragment_encoder
            .encode(target, fragment)
            .map_err(|error| format!("encode table writer commit fragment: {error}"))?;
        if bytes.len() > MAX_CONNECTOR_COMMIT_FRAGMENT_BYTES {
            return Err(format!(
                "table writer commit fragment of {} bytes exceeds the frozen single-fragment budget of {} bytes",
                bytes.len(),
                MAX_CONNECTOR_COMMIT_FRAGMENT_BYTES
            ));
        }
        encoded.push(bytes);
    }
    let rows = encoded.len() + 1;
    let mut kinds = Vec::with_capacity(rows);
    let mut ordinals = Vec::with_capacity(rows);
    let mut row_counts: Vec<Option<i64>> = Vec::with_capacity(rows);
    let mut payloads = BinaryBuilder::new();

    // The relation's carriers are signed because these columns cross the
    // FE/BE plan boundary; narrowing fails loudly instead of wrapping.
    let target = target_ordinal_to_wire(target)
        .map_err(|error| format!("table writer target ordinal: {error}"))?;
    let accepted_rows = row_count_to_wire(accepted_rows)
        .map_err(|error| format!("table writer row count: {error}"))?;

    kinds.push(WriterRowKind::RowCount.to_wire());
    ordinals.push(target);
    row_counts.push(Some(accepted_rows));
    payloads.append_null();

    for fragment in &encoded {
        kinds.push(WriterRowKind::CommitFragment.to_wire());
        ordinals.push(target);
        row_counts.push(None);
        payloads.append_value(fragment);
    }

    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int8Array::from(kinds)),
        Arc::new(Int32Array::from(ordinals)),
        Arc::new(Int64Array::from(row_counts)),
        Arc::new(payloads.finish()) as ArrayRef,
    ];
    let batch = RecordBatch::try_new(writer_relation_schema(), columns)
        .map_err(|error| format!("build table writer output batch: {error}"))?;
    Chunk::try_new_with_chunk_schema(batch, writer_relation_chunk_schema())
}

impl Operator for TableWriterOperator {
    fn name(&self) -> &str {
        &self.name
    }

    fn set_mem_tracker(&mut self, tracker: Arc<MemTracker>) {
        self.writer.set_mem_tracker(tracker);
    }

    fn bind_runtime_state(&mut self, state: &RuntimeState) -> Result<(), String> {
        self.writer
            .bind(state.sink_io_executor()?, state.error_state())
    }

    fn cancel(&mut self) {
        self.writer.request_abort();
        self.finishing = true;
    }

    fn on_driver_failure(&mut self) {
        self.cancel();
    }

    fn is_finished(&self) -> bool {
        self.finished || (self.finishing && self.writer.is_done() && !self.writer.has_output())
    }

    fn pending_finish(&self) -> bool {
        self.finishing && !self.writer.is_done()
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
        !self.finishing && !self.finished && self.writer.can_accept()
    }

    fn has_output(&self) -> bool {
        self.writer.has_output()
    }

    fn push_chunk(&mut self, _state: &RuntimeState, chunk: Chunk) -> Result<(), String> {
        if chunk.is_empty() {
            return Ok(());
        }
        let batch = self.projection.project(&chunk)?;
        let retained_bytes = record_batch_bytes(&batch);
        self.writer.enqueue(batch, retained_bytes)
    }

    fn pull_chunk(&mut self, _state: &RuntimeState) -> Result<Option<Chunk>, String> {
        let out = self.writer.take_output();
        if out.is_some() && self.finishing {
            self.finished = true;
        }
        Ok(out)
    }

    fn set_finishing(&mut self, _state: &RuntimeState) -> Result<(), String> {
        if self.finishing {
            return Ok(());
        }
        self.finishing = true;
        self.writer.request_finish()
    }

    fn sink_observable(&self) -> Option<Arc<Observable>> {
        Some(self.writer.observable())
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
    };
    use novarocks_spi::connector::{
        CatalogHandle, CatalogVersion, ConnectorCancellation, ConnectorError,
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

    fn test_runtime_state() -> RuntimeState {
        test_runtime_state_with_mem(None)
    }

    fn test_runtime_state_with_mem(mem_tracker: Option<Arc<MemTracker>>) -> RuntimeState {
        let runtime = Arc::new(
            crate::runtime::ExecutionRuntime::new(crate::runtime::ExecutionRuntimeConfig {
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
                exchange_max_transmit_batched_bytes: 1024,
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
            })
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
        let factory = TableWriterOperatorFactory::new(&writer_node(execution.clone()));

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
    fn table_writer_emits_one_row_count_row_and_one_row_per_fragment() {
        for fragment_count in [0usize, 1, 3] {
            let stats = Arc::new(WriteExecutionStats::default());
            let execution = Arc::new(
                TestWriteExecution::new(Arc::clone(&stats)).with_fragments(fragment_count, 4),
            );
            let factory = TableWriterOperatorFactory::new(&writer_node(execution));
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
            assert_eq!(chunks.len(), 1);
            let chunk = &chunks[0];
            assert_eq!(chunk.len(), fragment_count + 1);
            let columns = TableWriteRelationColumns::try_from_chunk(chunk).expect("columns");

            assert_eq!(columns.kinds.value(0), WriterRowKind::RowCount.to_wire());
            assert_eq!(columns.ordinals.value(0), 0);
            assert_eq!(columns.row_counts.value(0), 5);
            assert!(columns.fragments.is_null(0));

            for row in 1..=fragment_count {
                assert_eq!(
                    columns.kinds.value(row),
                    WriterRowKind::CommitFragment.to_wire()
                );
                assert_eq!(columns.ordinals.value(row), 0);
                assert!(columns.row_counts.is_null(row));
                assert_eq!(columns.fragments.value(row).len(), 4);
            }
            assert!(operator.is_finished());
        }
    }

    #[test]
    fn table_writer_reports_zero_rows_when_it_wrote_nothing() {
        let stats = Arc::new(WriteExecutionStats::default());
        let execution = Arc::new(TestWriteExecution::new(Arc::clone(&stats)).with_fragments(0, 0));
        let factory = TableWriterOperatorFactory::new(&writer_node(execution));
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
        let factory = TableWriterOperatorFactory::new(&writer_node(execution));
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
        let factory = TableWriterOperatorFactory::new(&writer_node(execution));
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
        let factory = TableWriterOperatorFactory::new(&writer_node(execution));
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
        let factory = TableWriterOperatorFactory::new(&writer_node(execution));
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
    fn driver_resumes_after_async_writer_finish_to_deliver_its_output() {
        let stats = Arc::new(WriteExecutionStats::default());
        let execution = Arc::new(TestWriteExecution::new(Arc::clone(&stats)));
        let factory = TableWriterOperatorFactory::new(&writer_node(execution));
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
        assert_eq!(chunks.len(), 1);
        let columns = TableWriteRelationColumns::try_from_chunk(&chunks[0]).expect("columns");
        assert_eq!(columns.row_counts.value(0), 3);
        assert_eq!(stats.finished.load(Ordering::Relaxed), 1);
        assert_eq!(stats.aborted.load(Ordering::Relaxed), 0);
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
        let factory = TableWriterOperatorFactory::new(&writer_node(execution));
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
        let factory = TableWriterOperatorFactory::new(&writer_node(execution));
        assert!(!factory.is_sink());
        assert!(!factory.is_source());
    }
}
