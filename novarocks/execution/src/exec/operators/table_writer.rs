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
//! `activate` starts a task-local actor which opens the driver's own
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
//! Design: ADR-0136 (docs/adr/ADR-0136-ordinary-aggregate-statistics-dataflow.md)

use crate::exec::pipeline::operator::OperatorLifecycle;
#[cfg(test)]
use crate::exec::pipeline::shape::TransformStart;
use crate::exec::pipeline::writer_failure::WriterFailure;
use crate::runtime::fragment::{ExecutionFailure, ExecutionResult};
fn writer_contract(detail: impl std::fmt::Display) -> ExecutionFailure {
    WriterFailure::FrozenContract {
        detail: novarocks_failure_contract::BoundedDetail::truncating_display(detail),
    }
    .into()
}
fn writer_connector(source: novarocks_spi::connector::ConnectorError) -> ExecutionFailure {
    use crate::runtime::fragment::connector_failure::{ConnectorCallFailure, ConnectorOperation};
    ConnectorCallFailure::new(ConnectorOperation::ValidateWriteRelation, source).into()
}
fn writer_arrow(source: arrow::error::ArrowError) -> ExecutionFailure {
    ExecutionFailure::from_domain(WriterArrowFailure(source))
}
#[derive(Debug)]
struct WriterArrowFailure(arrow::error::ArrowError);
impl std::fmt::Display for WriterArrowFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "table writer output: {}", self.0)
    }
}
impl std::error::Error for WriterArrowFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.0)
    }
}
impl novarocks_failure_contract::FailureProjection for WriterArrowFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use crate::runtime::fragment::ExecutionReason;
        use novarocks_failure_contract::{
            FailureClass, FailureFact, RepresentationBoundary, RepresentationFacts,
            RepresentationLayout, domain,
        };
        let (class, reason) = match self.0 {
            arrow::error::ArrowError::MemoryError(_) => {
                (FailureClass::Resource, ExecutionReason::WriterArrowMemory)
            }
            arrow::error::ArrowError::OffsetOverflowError(required) => {
                return FailureFact::new(
                    FailureClass::Representation,
                    ExecutionReason::WriterArrowOffset.reason(),
                )
                .with_representation(RepresentationFacts {
                    boundary: RepresentationBoundary::SingleOutput,
                    layout: RepresentationLayout {
                        domain: domain::EXECUTION,
                        code: std::num::NonZeroU16::new(7).unwrap(),
                    },
                    required: u64::try_from(required).ok(),
                    extent_limit: None,
                    representable_prefix: None,
                });
            }
            arrow::error::ArrowError::IoError(..) | arrow::error::ArrowError::ExternalError(_) => (
                FailureClass::Unexpected,
                ExecutionReason::WriterArrowUnexpected,
            ),
            _ => (FailureClass::Invariant, ExecutionReason::WriterArrow),
        };
        FailureFact::new(class, reason.reason())
    }
}

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

use crate::exec::chunk::{Chunk, record_batch_additional_bytes, record_batch_bytes};
use crate::exec::node::table_write_relation::{
    ConnectorCommitFragmentEncoder, WriterMultiplexRelationTemplate, WriterRelationPreparation,
};
#[cfg(novarocks_dev_controls)]
use crate::exec::node::table_write_relation::{
    TableWriteAggregateBoundary, TableWriteAggregateGuard,
};
use crate::exec::node::table_writer::{
    TableWriterPhysicalContextTemplate, TableWriterRuntimeBinding,
};
use crate::exec::operators::blocked_duration::BlockedDuration;
use crate::exec::pipeline::async_writer::{AsyncWriterOwner, AsyncWriterQueueConfig};
use crate::exec::pipeline::operator::{FinishWatch, InputEncodingPolicy, Operator};
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::exec::pipeline::schedule::observer::Observable;
use crate::exec::pipeline::{
    input::{PreparedInput, RawInput},
    shape::{ControlStep, HydrateInput, Transform, TransformKernel, child::ChildStep},
    step::{AdmissionNext, NextStep, StepResult, WaitFact, WaitReason},
    step_context::StepContext,
};
use crate::runtime::mem_tracker::{MemTracker, TrackedBytes};
use crate::runtime::observable::ObserverSubscription;
use crate::runtime::profile::OperatorProfiles;
use crate::runtime::runtime_state::RuntimeState;

/// Hard ceiling for one indivisible writer-relation row. The runtime exchange
/// batching setting is only a flush target: just like `DataStreamSink`, a
/// single row may exceed that target, but it must remain safely below the
/// 64 MiB native gRPC message limit.
const MAX_WRITER_MULTIPLEX_ROW_BYTES: usize = 16 * 1024 * 1024;

#[derive(Clone)]
#[cfg(test)]
struct LocalPreparedWriterPartial {
    calls: novarocks_local_program::PreparedWriterAggregateCalls,
    input_layout: novarocks_local_program::StaticLayout,
    output_layout: novarocks_local_program::StaticLayout,
    error: Arc<crate::runtime::runtime_state::RuntimeErrorState>,
}
/// The immutable per-node facts every driver copies when it opens its writer.
struct TableWriterPlan {
    handle: ConnectorWriterHandle,
    target: WriteTargetOrdinal,
    execution: Arc<dyn ConnectorWriteExecution>,
    expected_schema: arrow::datatypes::SchemaRef,
    projection: Arc<dyn WriterProjectionFactory>,
    physical_template: TableWriterPhysicalContextTemplate,
    request_context: ConnectorRequestContext,
    fragment_encoder: Arc<dyn ConnectorCommitFragmentEncoder>,
    writer_multiplex_schema: WriterRelationPreparation,
    /// Each driver's partial aggregate over its projected pages, emitting one
    /// state row whose auxiliary channels the writer packs as sparse
    /// `AGGREGATE_PARTIAL` rows, through its genuine compiled prepared calls.
    partial_aggregate_factory: Option<Arc<dyn OperatorFactory>>,
    #[cfg(test)]
    local_partial_source: Option<LocalPreparedWriterPartial>,
    #[cfg(novarocks_dev_controls)]
    aggregate_guard: Arc<dyn TableWriteAggregateGuard>,
}

/// How one driver projects an input page onto the exact provider schema its
/// writer and every embedded aggregate share.
pub(crate) trait WriterPageProjection: Send {
    fn prepare(
        &mut self,
        factory: Option<&Arc<dyn crate::exec::pipeline::operator::InputConstructionFactory>>,
    ) -> ExecutionResult<()>;
    fn set_mem_tracker(&mut self, _tracker: Arc<MemTracker>) {}
    fn bind_runtime_state(&mut self, _state: &RuntimeState) -> ExecutionResult<()> {
        Ok(())
    }
    fn project(&mut self, chunk: &Chunk) -> ExecutionResult<Chunk>;
}

/// Creates each driver's own page projection; it holds no per-driver state.
pub(crate) trait WriterProjectionFactory: Send + Sync {
    fn create(&self) -> Box<dyn WriterPageProjection>;
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
    /// A compiled writer: its projection evaluates the program's
    /// WriterProjection roots onto the exact provider schema, and its
    /// statistics, if any, run the program's prepared partial calls. It reads
    /// no function set. The sparse packer, guard and row limits are shared.
    pub(crate) fn try_new_compiled(
        node_id: i32,
        target: WriteTargetOrdinal,
        expected_schema: arrow::datatypes::SchemaRef,
        projection: Arc<dyn WriterProjectionFactory>,
        writer_multiplex_schema: WriterMultiplexRelationTemplate,
        partial_aggregate_factory: Option<Arc<dyn OperatorFactory>>,
        binding: &TableWriterRuntimeBinding,
    ) -> ExecutionResult<Self> {
        if binding.execution.catalog_handle() != binding.handle.binding().catalog_handle() {
            return Err(writer_contract(
                "table writer catalog handle does not match its query-leased write execution",
            ));
        }
        let name = if node_id >= 0 {
            format!("TABLE_WRITER (id={node_id})")
        } else {
            "TABLE_WRITER".to_string()
        };
        Ok(Self {
            name,
            plan: Arc::new(TableWriterPlan {
                handle: binding.handle.clone(),
                target,
                execution: Arc::clone(&binding.execution),
                expected_schema,
                projection,
                physical_template: binding.physical_template,
                request_context: binding.request_context.clone(),
                fragment_encoder: Arc::clone(&binding.fragment_encoder),
                writer_multiplex_schema: WriterRelationPreparation::compiled(
                    writer_multiplex_schema,
                ),
                partial_aggregate_factory,
                #[cfg(test)]
                local_partial_source: None,
                #[cfg(novarocks_dev_controls)]
                aggregate_guard: Arc::clone(&binding.aggregate_guard),
            }),
        })
    }

    /// Fixture-only composition through the actual compiled projection and
    /// immutable prepared COUNT handles; no erased aggregate or arena bridge.
    #[cfg(test)]
    fn for_test_fixture(
        node: &tests::WriterFixture,
        state: &RuntimeState,
    ) -> ExecutionResult<Self> {
        Self::for_test_fixture_with_projection_layout(node, state).map(|(factory, _)| factory)
    }

    #[cfg(test)]
    fn for_test_fixture_with_projection_layout(
        node: &tests::WriterFixture,
        state: &RuntimeState,
    ) -> ExecutionResult<(Self, novarocks_local_program::StaticLayout)> {
        let (program, projection, _) = tests::compiled_projection(
            node.projection.ty.clone(),
            node.expected_schema.clone(),
            state.error_state(),
        );
        let relation_layout = tests::relation_layout(node.writer_multiplex_schema.chunk_schema());
        let relation = WriterMultiplexRelationTemplate::try_new(&relation_layout)?;
        let mut factory = Self::try_new_compiled(
            node.node_id,
            node.target,
            node.expected_schema.clone(),
            projection,
            relation,
            None,
            &node.binding,
        )?;
        let novarocks_local_program::ProgramNodeKind::TableWriter { projection, .. } =
            program.graph().nodes()[program.graph().root().index()].kind()
        else {
            unreachable!()
        };
        let projection_layout = projection.layout.clone();
        if node.partial_count != 0 {
            Arc::get_mut(&mut factory.plan)
                .expect("unpublished fixture plan")
                .local_partial_source = Some(tests::prepare_count_source(
                node.partial_count,
                projection.layout.clone(),
                relation_layout,
                state,
            ));
        }
        Ok((factory, projection_layout))
    }
}

impl TableWriterOperatorFactory {
    fn create_operator(&self, dop: i32, driver_id: i32) -> TableWriterStep {
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
        let writer_subscription = forward_observable(&writer.observable(), &readiness_observable);
        let partial_aggregate =
            partial_aggregate.map(|partial| ChildStep::new(partial, readiness_observable.clone()));
        Transform::hydrating(TableWriterOperator {
            name: self.name.clone(),
            projection: plan.projection.create(),
            writer,
            target,
            relation: plan.writer_multiplex_schema.clone(),
            construction: None,
            runtime_error: None,
            partial_aggregate,
            #[cfg(test)]
            local_partial_source: plan.local_partial_source.clone(),
            #[cfg(novarocks_dev_controls)]
            aggregate_guard: Arc::clone(&plan.aggregate_guard),
            state: TableWriterState::Open,
            physical_rows: 0,
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
            _writer_subscription: writer_subscription,
            input: None,
            pending_append: None,
            pending_rows: None,
            writer_finish_requested: false,
        })
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

fn forward_observable(source: &Arc<Observable>, target: &Arc<Observable>) -> ObserverSubscription {
    let target = Arc::downgrade(target);
    source.subscribe(Arc::new(move || {
        if let Some(target) = target.upgrade() {
            target.notify_observers();
        }
    }))
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

type TableWriterStep = Transform<TableWriterOperator, HydrateInput>;

struct PendingAppend {
    batch: crate::exec::pipeline::async_writer::WriterAppendBatch,
    reservation: crate::exec::pipeline::async_writer::AsyncWriterReservation<WriterCompletion>,
    rows: u64,
}

struct TableWriterOperator {
    name: String,
    projection: Box<dyn WriterPageProjection>,
    writer: AsyncWriterOwner<WriterCompletion>,
    target: WriteTargetOrdinal,
    relation: WriterRelationPreparation,
    construction: Option<Arc<dyn crate::exec::pipeline::operator::InputConstructionFactory>>,
    runtime_error: Option<Arc<crate::runtime::runtime_state::RuntimeErrorState>>,
    partial_aggregate: Option<ChildStep>,
    #[cfg(test)]
    local_partial_source: Option<LocalPreparedWriterPartial>,
    #[cfg(novarocks_dev_controls)]
    aggregate_guard: Arc<dyn TableWriteAggregateGuard>,
    state: TableWriterState,
    physical_rows: u64,
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
    _writer_subscription: ObserverSubscription,
    input: Option<Chunk>,
    pending_append: Option<PendingAppend>,
    pending_rows: Option<u64>,
    writer_finish_requested: bool,
}

fn encode_writer_completion(
    target: WriteTargetOrdinal,
    fragment_encoder: &dyn ConnectorCommitFragmentEncoder,
    accepted_rows: u64,
    fragments: &[novarocks_spi::connector::write_stack::ConnectorCommitFragment],
    tracker: Option<Arc<MemTracker>>,
) -> ExecutionResult<WriterCompletion> {
    let mut encoded = VecDeque::with_capacity(fragments.len());
    for fragment in fragments {
        let bytes = fragment_encoder
            .encode(target, fragment)
            .map_err(|error| crate::runtime::fragment::connector_failure::ConnectorCallFailure::new(
                crate::runtime::fragment::connector_failure::ConnectorOperation::EncodeCommitFragment, error))?;
        if bytes.len() > MAX_CONNECTOR_COMMIT_FRAGMENT_BYTES {
            return Err(
                crate::exec::pipeline::writer_failure::WriterFailure::CommitFragmentLimit {
                    requested: bytes.len(),
                    limit: MAX_CONNECTOR_COMMIT_FRAGMENT_BYTES,
                }
                .into(),
            );
        }
        let accounting = tracker
            .as_ref()
            .map(|tracker| TrackedBytes::try_new(bytes.capacity(), Arc::clone(tracker)))
            .transpose()?;
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
        .transpose()?;
    Ok(WriterCompletion {
        accepted_rows,
        fragments: encoded,
        _index_accounting: index_accounting,
    })
}

impl OperatorLifecycle for TableWriterOperator {
    fn creates_typed_storage(&self) -> bool {
        self.relation.is_compiled()
            || self
                .partial_aggregate
                .as_ref()
                .is_some_and(|partial| partial.creates_typed_storage())
    }
    fn set_construction_factory(
        &mut self,
        factory: Arc<dyn crate::exec::pipeline::operator::InputConstructionFactory>,
    ) {
        if let Some(partial) = self.partial_aggregate.as_mut() {
            partial.set_construction_factory(Arc::clone(&factory));
        }
        self.construction = Some(factory);
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn set_mem_tracker(&mut self, tracker: Arc<MemTracker>) {
        self.projection.set_mem_tracker(Arc::clone(&tracker));
        self.writer.set_mem_tracker(Arc::clone(&tracker));
        let result_tracker = MemTracker::new_child("ConnectorWriterResult", &tracker);
        *self
            .result_tracker
            .lock()
            .expect("table writer result tracker lock") = Some(result_tracker);
        self.pending_output_tracker =
            Some(MemTracker::new_child("WriterMultiplexOutput", &tracker));
        let has_partials = self.partial_aggregate.is_some();
        #[cfg(test)]
        let has_partials = has_partials || self.local_partial_source.is_some();
        if has_partials {
            let aggregate_tracker = MemTracker::new_child("WriterPartialAggregate", &tracker);
            if let Some(partial) = self.partial_aggregate.as_mut() {
                partial.set_mem_tracker(aggregate_tracker.clone());
            }
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

    fn prepare(&mut self) -> ExecutionResult<()> {
        self.projection.prepare(self.construction.as_ref())?;
        if let Err(error) = self.relation.prepare(self.construction.as_ref()) {
            self.state = TableWriterState::Failed;
            return Err(error);
        }
        #[cfg(test)]
        if let Some(recipe) = self.local_partial_source.as_ref() {
            let deferred = (|| -> ExecutionResult<Box<dyn Operator>> {
                let factory = self
                    .construction
                    .as_ref()
                    .ok_or(crate::exec::pipeline::operator::ConstructionFailure::MissingOwner)?;
                let owner = factory.begin_compiled_layout_construction(&recipe.output_layout)?;
                let mut partial = super::compiled_writer_statistics::CompiledWriterPartialFactory::create_prepared_in(
                    recipe.calls.clone(), recipe.input_layout.clone(), recipe.output_layout.clone(),
                    recipe.error.clone(), owner.as_ref(),
                )?;
                partial.set_construction_factory(factory.clone());
                if let Some(tracker) = self.aggregate_tracker.as_ref() {
                    partial.set_mem_tracker(tracker.clone());
                }
                if let Some(profiles) = self.profiles.as_ref() {
                    partial.set_profiles(profiles.clone());
                }
                Ok(partial)
            })();
            match deferred {
                Ok(partial) => {
                    let mut child = ChildStep::new(partial, self.readiness_observable.clone());
                    if let Some(control) = &self.runtime_error {
                        child.bind_failure_port(control.clone())?;
                    }
                    self.partial_aggregate = Some(child);
                    self.local_partial_source = None;
                }
                Err(error) => {
                    self.state = TableWriterState::Failed;
                    return Err(error);
                }
            }
        }
        if let Some(partial) = self.partial_aggregate.as_mut()
            && let Err(error) = partial.prepare()
        {
            self.state = TableWriterState::Failed;
            return Err(error);
        }
        Ok(())
    }

    fn bind_runtime_state(&mut self, state: &RuntimeState) -> ExecutionResult<()> {
        let incoming = state.error_state();
        if self
            .runtime_error
            .as_ref()
            .is_some_and(|old| !Arc::ptr_eq(old, &incoming))
        {
            return Err(WriterFailure::AlreadyBound.into());
        }
        self.runtime_error = Some(incoming);
        if let Err(error) = self.projection.bind_runtime_state(state) {
            self.state = TableWriterState::Failed;
            return Err(error);
        }
        self.target_multiplex_batch_bytes = state
            .execution_runtime()
            .map(|runtime| runtime.config().exchange_max_transmit_batched_bytes)
            .unwrap_or(MAX_WRITER_MULTIPLEX_ROW_BYTES)
            .clamp(1, MAX_WRITER_MULTIPLEX_ROW_BYTES);
        if let Some(partial) = self.partial_aggregate.as_mut()
            && let Err(error) = partial.bind_runtime_state(state)
        {
            self.state = TableWriterState::Failed;
            return Err(error);
        }
        Ok(())
    }

    fn activate(&mut self, state: &RuntimeState) -> ExecutionResult<()> {
        if let Some(partial) = self.partial_aggregate.as_mut() {
            partial.activate(state)?;
        }
        let result = state
            .sink_io_executor()
            .map_err(crate::runtime::fragment::ExecutionFailure::from)
            .and_then(|executor| self.writer.bind(executor, state.error_state()));
        if result.is_err() {
            self.state = TableWriterState::Failed;
        }
        result
    }

    fn begin_exit(&mut self) -> ExecutionResult<()> {
        // An early downstream finish may end the driver while this receiver
        // still owns an open actor. Start that exit before observing its join.
        if !matches!(self.state, TableWriterState::Finished) {
            self.stop_writer(false);
        }
        if let Some(partial) = self.partial_aggregate.as_mut() {
            partial.begin_exit()?;
        }
        Ok(())
    }
    fn observe_exit(&mut self) -> crate::exec::pipeline::operator::ExitObservation {
        use crate::exec::pipeline::operator::ExitObservation;
        let observing_exit = match self.state {
            TableWriterState::Draining => {
                self.writer_finish_requested
                    && self.partial_child_finished()
                    && !self.partial_output_available()
            }
            TableWriterState::Aborting | TableWriterState::Failed | TableWriterState::Finished => {
                true
            }
            // The actor is serving this computation, not exiting. The step
            // protocol advances it; it must not park active input dataflow.
            TableWriterState::Open | TableWriterState::Producing => false,
        };
        if observing_exit {
            self.writer.observe_actor_exit()
        } else {
            ExitObservation::exited()
        }
    }
    fn visit_exits(
        &mut self,
        visit: &mut dyn FnMut(crate::exec::pipeline::operator::ExitObservation),
    ) {
        visit(self.observe_exit());
        if let Some(partial) = self.partial_aggregate.as_mut() {
            partial.visit_exits(visit);
        }
    }
    fn close(&mut self) -> ExecutionResult<()> {
        self.finish_blocked_intervals();
        self.sync_metrics();
        if let Some(partial) = self.partial_aggregate.as_mut() {
            partial.close()?;
        }
        Ok(())
    }

    fn cancel(&mut self) {
        self.stop_writer(false);
    }
    fn on_driver_failure(&mut self) {
        self.stop_writer(true);
    }

    fn pending_finish(&self) -> Option<FinishWatch> {
        let pending = match self.state {
            TableWriterState::Draining => {
                self.writer_finish_requested
                    && self.partial_child_finished()
                    && !self.partial_output_available()
                    && (!self.writer.is_done() || !self.writer.actor_exited())
            }
            TableWriterState::Aborting => !self.writer.is_done() || !self.writer.actor_exited(),
            TableWriterState::Failed | TableWriterState::Finished => !self.writer.actor_exited(),
            TableWriterState::Open | TableWriterState::Producing => false,
        };
        if !pending {
            None
        } else if self.writer.is_done() && !self.writer.actor_exited() {
            // Terminal publication can precede the executor wrapper's exit,
            // which has no completion observable of its own.
            Some(FinishWatch::RecheckAfter(std::time::Duration::from_millis(
                1,
            )))
        } else {
            Some(FinishWatch::Notify(Arc::clone(&self.readiness_observable)))
        }
    }
}

impl InputEncodingPolicy for TableWriterOperator {}
impl TransformKernel for TableWriterOperator {
    fn control_observable(&self) -> Option<Arc<Observable>> {
        Some(self.readiness_observable.clone())
    }
    fn poll_control(&mut self, ctx: &mut StepContext<'_>) -> ExecutionResult<ControlStep> {
        if let Some(error) = self.writer.error() {
            return Err(error);
        }
        if let Some(child) = self.partial_aggregate.as_mut() {
            let control = child.poll_control(ctx)?;
            if child.is_complete() && self.state == TableWriterState::Open {
                return Err(
                    crate::exec::pipeline::writer_failure::WriterFailure::PartialDemandClosed
                        .into(),
                );
            }
            if matches!(control, ControlStep::Runnable) || child.runnable() {
                return Ok(ControlStep::Runnable);
            }
        }
        Ok(ControlStep::Active)
    }
    fn admission(
        &self,
        _: &mut StepContext<'_>,
        _: &RawInput,
    ) -> ExecutionResult<Option<AdmissionNext>> {
        let generation = self.readiness_observable.generation();
        if let Some(error) = self.writer.error() {
            return Err(error);
        }
        if self.state == TableWriterState::Open
            && self
                .partial_aggregate
                .as_ref()
                .is_some_and(ChildStep::is_complete)
        {
            return Err(
                crate::exec::pipeline::writer_failure::WriterFailure::PartialDemandClosed.into(),
            );
        }
        let ready = self.state == TableWriterState::Open
            && self.input.is_none()
            && self.pending_append.is_none()
            && self.aggregate_output.is_none()
            && self
                .partial_aggregate
                .as_ref()
                .is_none_or(ChildStep::can_accept)
            && self.writer.can_accept();
        self.writer_queue_blocked_time
            .observe(!self.writer.can_accept());
        self.composite_blocked_time.observe(!ready);
        if ready {
            Ok(None)
        } else {
            self.blocked_checks.fetch_add(1, Ordering::Relaxed);
            Ok(Some(AdmissionNext::Wait(self.wait(generation))))
        }
    }
    fn apply(
        &mut self,
        ctx: &mut StepContext<'_>,
        input: PreparedInput,
    ) -> ExecutionResult<StepResult> {
        self.input = Some(input.into_uncovered_chunk()?);
        self.drive(ctx)
    }
    fn drive(&mut self, ctx: &mut StepContext<'_>) -> ExecutionResult<StepResult> {
        let generation = self.readiness_observable.generation();
        let result = self.advance(ctx, generation);
        match result {
            Ok(result) => Ok(result),
            Err(error) => {
                ctx.report_failure(error.clone());
                self.fail_attempt();
                Err(error)
            }
        }
    }
    fn finish(&mut self, ctx: &mut StepContext<'_>) -> ExecutionResult<StepResult> {
        if self.state == TableWriterState::Open {
            self.state = TableWriterState::Draining;
            self.finish_blocked_intervals();
            #[cfg(novarocks_dev_controls)]
            if self.partial_aggregate.is_some()
                && let Err(error) = self
                    .aggregate_guard
                    .check(TableWriteAggregateBoundary::PartialFinalize)
            {
                let error: crate::runtime::fragment::ExecutionFailure = crate::runtime::fragment::connector_failure::ConnectorCallFailure::new(crate::runtime::fragment::connector_failure::ConnectorOperation::CheckPartialAggregateFinalize,error).into();
                ctx.report_failure(error.clone());
                self.fail_attempt();
                return Err(error);
            }
            if let Some(child) = self.partial_aggregate.as_mut() {
                child.finish_input();
            }
        }
        self.drive(ctx)
    }
}

impl TableWriterOperator {
    fn wait(&self, generation: u64) -> WaitFact {
        WaitFact {
            reason: if self.state == TableWriterState::Open {
                WaitReason::Output
            } else {
                WaitReason::Finish
            },
            observable: self.readiness_observable.clone(),
            observed_generation: generation,
            deadline: self
                .partial_aggregate
                .as_ref()
                .and_then(|child| child.wait())
                .and_then(|wait| wait.deadline),
        }
    }
    fn next_work(&self, generation: u64) -> NextStep {
        if self.input.is_some()
            || self.aggregate_output.is_some()
            || self.pending_append.as_ref().is_some_and(|_| {
                self.partial_aggregate
                    .as_ref()
                    .is_none_or(|child| !child.input_pending())
            })
            || self
                .partial_aggregate
                .as_ref()
                .is_some_and(ChildStep::runnable)
        {
            return NextStep::Runnable;
        }
        match self.state {
            TableWriterState::Open
                if self.pending_append.is_none()
                    && self
                        .partial_aggregate
                        .as_ref()
                        .is_none_or(ChildStep::can_accept) =>
            {
                NextStep::NeedInput
            }
            TableWriterState::Draining
                if self.partial_child_finished()
                    && (!self.writer_finish_requested
                        || self.writer.has_output()
                        || self.writer.is_done()) =>
            {
                NextStep::Runnable
            }
            TableWriterState::Producing => NextStep::Runnable,
            TableWriterState::Finished | TableWriterState::Failed | TableWriterState::Aborting => {
                NextStep::Finished
            }
            _ => NextStep::Wait(self.wait(generation)),
        }
    }
    fn advance(
        &mut self,
        ctx: &mut StepContext<'_>,
        generation: u64,
    ) -> ExecutionResult<StepResult> {
        if let Some(error) = self.writer.error() {
            return Err(error);
        }
        if !ctx.work().try_consume(1) {
            return Ok(StepResult::new(None, NextStep::Yield, 0));
        }
        if let Some(mut chunk) = self.input.take()
            && !chunk.is_empty()
        {
            let rows = u64::try_from(chunk.len()).expect("supported row count fits u64");
            let next_rows = self.physical_rows.checked_add(rows).ok_or_else(|| {
                crate::runtime::fragment::ExecutionFailure::from(
                    crate::exec::pipeline::writer_failure::WriterFailure::AcceptedRowsOverflow {
                        current: self.physical_rows,
                        incoming: chunk.len(),
                    },
                )
            })?;
            let projected = self.projection.project(&chunk)?;
            let batch = projected.record_batch();
            let reservation = self.writer.try_reserve_input(
                batch.num_rows(),
                record_batch_bytes(batch),
                chunk.take_memory_lease(),
                crate::exec::chunk::record_batch_shared_owner_bytes(batch, chunk.record_batch()),
                record_batch_additional_bytes(batch, chunk.record_batch()),
            )?;
            let append = if projected.chunk_schema().is_typed() {
                let factory = self
                    .construction
                    .as_ref()
                    .ok_or(crate::exec::pipeline::operator::ConstructionFailure::MissingOwner)?;
                let owner = factory.begin_typed_construction(projected.chunk_schema())?;
                crate::exec::pipeline::async_writer::WriterAppendBatch::try_new(
                    projected.batch_projection(),
                    owner.as_ref(),
                )?
            } else {
                #[cfg(test)]
                {
                    crate::exec::pipeline::async_writer::WriterAppendBatch::from_fixture_projection(
                        projected.batch_projection(),
                    )
                }
                #[cfg(not(test))]
                return Err(
                    crate::exec::pipeline::operator::ConstructionFailure::InvalidSource(
                        "writer append requires its complete execution bindings".into(),
                    )
                    .into(),
                );
            };
            drop(chunk);
            if let Some(child) = self.partial_aggregate.as_mut() {
                #[cfg(novarocks_dev_controls)]
                    self.aggregate_guard
                        .check(TableWriteAggregateBoundary::PartialUpdate)
                        .map_err(|source| ExecutionFailure::from(crate::runtime::fragment::connector_failure::ConnectorCallFailure::new(crate::runtime::fragment::connector_failure::ConnectorOperation::CheckPartialAggregateUpdate,source)))?;
                child.offer(projected)?;
                self.pending_append = Some(PendingAppend {
                    batch: append,
                    reservation,
                    rows: next_rows,
                });
            } else {
                reservation.send(append)?;
                self.physical_rows = next_rows;
                self.pending_rows = Some(rows);
            }
        }
        if self.aggregate_output.is_none()
            && let Some(child) = self.partial_aggregate.as_mut()
            && child.runnable()
            && let Some(mut output) = child.advance(ctx)?
        {
            if let Some(tracker) = self.aggregate_tracker.as_ref() {
                output
                    .try_transfer_to(tracker)
                    .map_err(ExecutionFailure::from_domain)?;
            }
            self.aggregate_output = Some(output);
            self.aggregate_output_row = 0;
            self.next_auxiliary = 0;
        }
        if self.pending_append.is_some()
            && self
                .partial_aggregate
                .as_ref()
                .is_none_or(|child| !child.input_pending())
        {
            let PendingAppend {
                batch,
                reservation,
                rows,
            } = self.pending_append.take().unwrap();
            reservation.send(batch)?;
            self.pending_rows = Some(rows - self.physical_rows);
            self.physical_rows = rows;
        }
        if self.aggregate_output.is_some() {
            let output = self.take_partial_output()?;
            self.sync_metrics();
            return Ok(StepResult::new(
                output,
                self.next_work(generation),
                self.pending_rows.take().unwrap_or(0),
            ));
        }
        if self.state == TableWriterState::Draining && self.partial_child_finished() {
            if !self.writer_finish_requested {
                self.writer.request_finish()?;
                self.writer_finish_requested = true;
            }
            if let std::task::Poll::Ready(completion) = self.writer.poll_output()? {
                if completion.accepted_rows != self.physical_rows {
                    return Err(WriterFailure::RowCountDrift {
                        expected: self.physical_rows,
                        actual: completion.accepted_rows,
                    }
                    .into());
                }
                self.writer_completion = Some(completion);
                self.state = TableWriterState::Producing;
            }
        }
        let output = if self.state == TableWriterState::Producing {
            let output = if !self.emitted_row_count {
                self.emitted_row_count = true;
                let output = self.build_prefix_output(Some(self.physical_rows), &[])?;
                let bytes = self.output_size(&output)?;
                if bytes > MAX_WRITER_MULTIPLEX_ROW_BYTES {
                    return Err(WriterFailure::OutputBytes {
                        bytes,
                        limit: MAX_WRITER_MULTIPLEX_ROW_BYTES,
                    }
                    .into());
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
                self.writer_completion = None;
                self.state = TableWriterState::Finished;
            }
            output
        } else {
            None
        };
        self.sync_metrics();
        Ok(StepResult::new(
            output,
            self.next_work(generation),
            self.pending_rows.take().unwrap_or(0),
        ))
    }
}

impl TableWriterOperator {
    fn stop_writer(&mut self, failed: bool) {
        if matches!(
            self.state,
            TableWriterState::Finished | TableWriterState::Aborting
        ) {
            return;
        }
        self.state = TableWriterState::Aborting;
        self.input = None;
        self.pending_append = None;
        let child = self.partial_aggregate.as_mut().map(|partial| {
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                if failed {
                    partial.on_driver_failure();
                } else {
                    partial.cancel();
                }
            }))
        });
        self.writer.request_abort();
        self.aggregate_output = None;
        self.writer_completion = None;
        self.finish_blocked_intervals();
        self.sync_metrics();
        if let Some(Err(payload)) = child {
            std::panic::resume_unwind(payload);
        }
    }

    #[cfg(test)]
    fn replace_partial_aggregate_for_test(&mut self, partial: Box<dyn Operator>) {
        // A replacement is the actual child under test. Do not let prepare
        // silently overwrite it with the deferred compiled COUNT fixture.
        self.local_partial_source = None;
        let mut child = ChildStep::new(partial, self.readiness_observable.clone());
        if let Some(control) = &self.runtime_error {
            child
                .bind_failure_port(control.clone())
                .expect("same writer failure port");
        }
        self.partial_aggregate = Some(child);
    }
    fn partial_child_finished(&self) -> bool {
        self.partial_aggregate
            .as_ref()
            .is_none_or(ChildStep::is_complete)
    }
    fn partial_output_available(&self) -> bool {
        self.aggregate_output.is_some()
            || self
                .partial_aggregate
                .as_ref()
                .is_some_and(ChildStep::runnable)
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
            .schema()
            .ok()?
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
    ) -> ExecutionResult<Chunk> {
        let target = target_ordinal_to_wire(self.target);
        let rows = usize::from(row_count.is_some()) + fragments.len();
        let mut kinds = Vec::with_capacity(rows);
        let mut ordinals = Vec::with_capacity(rows);
        let mut row_counts = Vec::with_capacity(rows);
        let mut payloads = BinaryBuilder::new();
        if let Some(row_count) = row_count {
            kinds.push(WriterRowKind::RowCount.to_wire());
            ordinals.push(target);
            row_counts.push(Some(
                row_count_to_wire(row_count).map_err(writer_connector)?,
            ));
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
                .schema()?
                .contract()
                .auxiliary_channels()
                .iter()
                .map(|channel| new_null_array(channel.data_type(), rows)),
        );
        self.track_output(
            RecordBatch::try_new(
                self.relation.schema()?.chunk_schema().arrow_schema_ref(),
                columns,
            )
            .map_err(writer_arrow)?,
        )
    }

    fn take_fragment_output(&mut self) -> ExecutionResult<Chunk> {
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
                return Err(WriterFailure::OutputBytes {
                    bytes,
                    limit: MAX_WRITER_MULTIPLEX_ROW_BYTES,
                }
                .into());
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

    fn take_partial_output(&mut self) -> ExecutionResult<Option<Chunk>> {
        loop {
            if self.aggregate_output.is_none() {
                return Ok(None);
            }
            let Some(first) = self.next_non_null_auxiliary() else {
                self.advance_partial_row();
                continue;
            };
            let candidates = self
                .relation
                .schema()?
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
                    return Err(WriterFailure::OutputBytes {
                        bytes,
                        limit: MAX_WRITER_MULTIPLEX_ROW_BYTES,
                    }
                    .into());
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
            let sparse_rows = usize::from(
                count
                    < self
                        .relation
                        .schema()?
                        .contract()
                        .auxiliary_channels()
                        .len(),
            );
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

    fn build_partial_output(&self, selected: &[usize]) -> ExecutionResult<Chunk> {
        let aggregate = self
            .aggregate_output
            .as_ref()
            .ok_or_else(|| writer_contract("writer partial aggregate output is missing"))?;
        let target = target_ordinal_to_wire(self.target);
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
            .schema()?
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
            RecordBatch::try_new(
                self.relation.schema()?.chunk_schema().arrow_schema_ref(),
                columns,
            )
            .map_err(writer_arrow)?,
        )
    }

    fn track_output(&self, batch: RecordBatch) -> ExecutionResult<Chunk> {
        let mut chunk = Chunk::try_new_with_chunk_schema(
            batch,
            Arc::clone(self.relation.schema()?.chunk_schema()),
        )?;
        if let Some(tracker) = self.pending_output_tracker.as_ref() {
            chunk
                .try_transfer_to(tracker)
                .map_err(ExecutionFailure::from_domain)?;
        }
        Ok(chunk)
    }

    fn output_size(&self, chunk: &Chunk) -> ExecutionResult<usize> {
        let mut encoded = Vec::new();
        {
            let mut writer =
                StreamWriter::try_new(&mut encoded, &chunk.schema()).map_err(writer_arrow)?;
            writer.write(chunk.record_batch()).map_err(writer_arrow)?;
            writer.finish().map_err(writer_arrow)?;
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
        self.stop_writer(true);
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
    /// Read the fixed prefix at its SPI relation positions. The caller has
    /// already proved the chunk is exactly its frozen relation, whose columns
    /// may be carried by any slots (a compiled program allocates its own).
    pub fn try_from_relation(chunk: &'chunk Chunk) -> ExecutionResult<Self> {
        use novarocks_spi::connector::write_stack::{
            WRITE_RELATION_FRAGMENT_INDEX, WRITE_RELATION_KIND_INDEX,
            WRITE_RELATION_ROW_COUNT_INDEX, WRITE_RELATION_TARGET_INDEX,
        };
        Self::from_positions(
            chunk,
            WRITE_RELATION_KIND_INDEX,
            WRITE_RELATION_TARGET_INDEX,
            WRITE_RELATION_ROW_COUNT_INDEX,
            WRITE_RELATION_FRAGMENT_INDEX,
        )
    }

    fn from_positions(
        chunk: &'chunk Chunk,
        kind_index: usize,
        ordinal_index: usize,
        row_count_index: usize,
        fragment_index: usize,
    ) -> ExecutionResult<Self> {
        use crate::exec::node::table_write_relation::{WriteRelationViolation, relation_failure};
        let column = |position: usize, name: &str| {
            chunk.columns().get(position).ok_or_else(|| {
                relation_failure(
                    WriteRelationViolation::Column,
                    format_args!("table write relation column {name} is outside its record batch"),
                )
            })
        };
        let kinds = column(kind_index, "kind")?
            .as_any()
            .downcast_ref::<Int8Array>()
            .ok_or_else(|| {
                relation_failure(
                    WriteRelationViolation::Column,
                    "table write relation kind column is not Int8",
                )
            })?;
        let ordinals = column(ordinal_index, "write_target_ordinal")?
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or_else(|| {
                relation_failure(
                    WriteRelationViolation::Column,
                    "table write relation write_target_ordinal column is not Int32",
                )
            })?;
        let row_counts = column(row_count_index, "row_count")?
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| {
                relation_failure(
                    WriteRelationViolation::Column,
                    "table write relation row_count column is not Int64",
                )
            })?;
        let fragments = column(fragment_index, "commit_fragment")?
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| {
                relation_failure(
                    WriteRelationViolation::Column,
                    "table write relation commit_fragment column is not Binary",
                )
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
    use crate::exec::pipeline::shape::StepOperator;
    use crate::runtime::fragment::ExecutionFailure;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::time::{Duration, Instant};

    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use novarocks_spi::connector::write_stack::{
        ConnectorBatchWriter, ConnectorCommitFragment, ProviderWriteRuntime, WriteRuntimeAdapter,
        WriterAuxiliaryChannel, WriterMultiplexSchema,
    };
    use novarocks_spi::connector::{
        CatalogHandle, CatalogVersion, ConnectorError, ConnectorErrorKind,
        ConnectorInstanceDescriptor, ConnectorInstanceId, ConnectorProviderId,
        ConnectorRequestContext, ConnectorStopOwner, MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
        MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
    };
    use novarocks_types::SlotId;
    use tokio::sync::Notify;

    use super::*;
    use crate::exec::chunk::ChunkSchema;
    use crate::exec::node::table_write_relation::WriterMultiplexRelationSchema;
    use crate::exec::pipeline::driver::{DriverState, PipelineDriver};
    use crate::exec::pipeline::fragment_context::FragmentContext;
    use crate::exec::pipeline::global_driver_executor::{DriverTask, FragmentCompletion};
    use crate::runtime::mem_tracker::MemTracker;

    fn poll_until<F: FnMut() -> bool>(mut pred: F, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            if pred() {
                return true;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        pred()
    }

    #[cfg(novarocks_dev_controls)]
    struct RejectAggregateBoundary(TableWriteAggregateBoundary);

    #[cfg(novarocks_dev_controls)]
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
                    exchange_wait_ms: 120_000,
                    exchange_io_threads: 1,
                    exchange_io_max_inflight_bytes: 1024,
                    exchange_max_transmit_batched_bytes: max_packet_bytes,
                    operator_buffer_chunks: 1,
                    local_exchange_buffer_mem_limit_per_driver: 1024,
                    local_exchange_max_buffered_rows: 1024,
                    runtime_filter_scan_wait_time_ms_override: None,
                    runtime_filter_wait_timeout_ms_override: None,
                    sink_io_worker_threads: 1,
                    sink_io_max_blocking_threads: 1,
                },
                crate::runtime::execution_runtime::test_execution_function_set(),
                crate::runtime::execution_runtime::test_memory_authority(),
            )
            .expect("test execution runtime"),
        );
        RuntimeState::for_local_execution(None, None, None, None, None, mem_tracker, Some(runtime))
    }

    fn install_construction(operator: &mut dyn Operator, state: &RuntimeState) {
        let tracker = state
            .mem_tracker()
            .unwrap_or_else(|| MemTracker::new_root("writer input construction fixture"));
        // Bind the actual allocation host used by compiled finite-input
        // custody, on the same tracker as this operation's construction owner.
        operator.set_mem_tracker(tracker.clone());
        let construction = crate::exec::pipeline::operator::input_construction::try_runtime_input_construction_factory(
            crate::exec::pipeline::pipeline::representation_construction_profile().expect("actual runtime construction profile"),
            tracker, state.error_state(),
        ).expect("actual construction factory");
        operator.set_construction_factory(construction);
    }

    fn bind(operator: &mut Box<dyn Operator>, state: &RuntimeState) {
        install_construction(operator.as_mut(), state);
        operator.prepare().expect("prepare");
        operator
            .bind_runtime_state(state)
            .expect("bind writer state");
        operator.activate(state).expect("activate writer actor");
    }

    fn with_step<T>(state: &RuntimeState, action: impl FnOnce(&mut StepContext<'_>) -> T) -> T {
        let error = state.error_state();
        let mut turn =
            crate::exec::pipeline::step_context::TurnWork::new(std::num::NonZeroU64::MIN, 64);
        action(&mut StepContext::new(
            &error,
            &mut turn,
            &mut novarocks_value_arrays::decode::ValueConversionContext::default(),
        ))
    }
    fn complete_native_input(
        operator: &mut dyn Operator,
        state: &RuntimeState,
        chunk: Chunk,
    ) -> ExecutionResult<()> {
        use crate::exec::pipeline::step::InputResult;
        with_step(state, |ctx| {
            let step = operator;
            match step.try_push(ctx, RawInput::for_test(chunk)) {
                InputResult::Accepted(result) => {
                    assert!(result?.output.is_none());
                }
                InputResult::Rejected { failure, .. } => return Err(failure),
                InputResult::Deferred { .. } => panic!("fixture input must be admissible"),
            }
            for _ in 0..64 {
                let result = step.drive(ctx)?;
                assert!(
                    result.output.is_none(),
                    "fixture has no running partial output"
                );
                if matches!(result.next, NextStep::NeedInput) {
                    return Ok(());
                }
                assert!(
                    matches!(result.next, NextStep::Runnable | NextStep::Yield),
                    "fixture must not need an external input-completion event"
                );
            }
            panic!("fixture input failed to complete within its finite continuation");
        })
    }
    fn native_failure(
        operator: &mut dyn Operator,
        state: &RuntimeState,
        operation: crate::runtime::fragment::PipelineOperation,
    ) -> ExecutionFailure {
        use crate::runtime::fragment::PipelineOperation;
        if operation == PipelineOperation::Push {
            return complete_native_input(operator, state, input_chunk(vec![1])).unwrap_err();
        }
        with_step(state, |ctx| {
            let step = operator;
            if operation == PipelineOperation::Finishing {
                // StepOperator EOS is a no-payload control signal. Any
                // finalization output/failure is observed by admitted drive.
                if let Err(error) = step.finish(ctx) {
                    return error;
                }
            }
            for _ in 0..64 {
                match step.drive(ctx) {
                    Err(error) => return error,
                    Ok(result) => {
                        assert!(result.output.is_none());
                    }
                }
            }
            panic!("injected child error must become observable");
        })
    }
    fn wait_for_output(
        operator: &mut Box<dyn Operator>,
        state: &RuntimeState,
    ) -> ExecutionResult<Vec<Chunk>> {
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut outputs = Vec::new();
        let mut next = NextStep::Runnable;
        loop {
            if let Some(error) = state.error() {
                return Err(error);
            }
            assert!(Instant::now() < deadline, "writer completion deadline");
            if let NextStep::Wait(wait) = &next
                && !wait.is_ready()
            {
                std::thread::sleep(Duration::from_millis(1));
                continue;
            }
            let result = operator.drive_for_test(state)?;
            if let Some(output) = result.output {
                outputs.push(output);
            }
            next = result.next;
            if matches!(next, NextStep::Finished) {
                return Ok(outputs);
            }
            assert!(
                !matches!(next, NextStep::NeedInput),
                "EOS continuation must not request another page"
            );
        }
    }
    fn accept_native_input(
        operator: &mut dyn Operator,
        state: &RuntimeState,
        input: RawInput,
    ) -> ExecutionResult<()> {
        use crate::exec::pipeline::step::InputResult;
        match operator.try_push_for_test(state, input) {
            InputResult::Accepted(result) => {
                assert!(result?.output.is_none());
                Ok(())
            }
            InputResult::Rejected { failure, .. } => Err(failure),
            InputResult::Deferred { .. } => panic!("fixture input must be admissible"),
        }
    }
    fn next_running_output(operator: &mut dyn Operator, state: &RuntimeState) -> StepResult {
        for _ in 0..64 {
            let result = operator.drive_for_test(state).expect("running output step");
            if result.output.is_some() {
                return result;
            }
            assert!(
                matches!(result.next, NextStep::Runnable | NextStep::Yield),
                "an expected prefix must still be runnable"
            );
        }
        panic!("finite partial fixture must emit its next prefix");
    }
    fn exits_ready(operator: &mut dyn Operator) -> bool {
        let mut ready = true;
        operator.visit_exits(&mut |exit| {
            assert!(
                exit.failure.is_none(),
                "writer owner has no unconsumed required failure"
            );
            assert!(
                exit.unresolved_exit.is_none(),
                "writer owner has no unknown exit incident"
            );
            match exit.progress {
                crate::exec::pipeline::operator::ExitProgress::Pending(_)
                | crate::exec::pipeline::operator::ExitProgress::Unresolved(_) => ready = false,
                crate::exec::pipeline::operator::ExitProgress::Exited => {}
            }
        });
        ready
    }

    struct OneChunkSource {
        chunk: Option<Chunk>,
    }
    impl OperatorLifecycle for OneChunkSource {
        fn name(&self) -> &str {
            "one_chunk_source"
        }
    }
    impl crate::exec::pipeline::shape::SourceKernel for OneChunkSource {
        fn poll_output(&mut self, _: &mut StepContext<'_>) -> ExecutionResult<StepResult> {
            Ok(StepResult::new(self.chunk.take(), NextStep::Finished, 0))
        }
    }
    struct CollectSink {
        chunks: Arc<std::sync::Mutex<Vec<Chunk>>>,
    }

    struct ProjectedInputObserver {
        value_slot: SlotId,
        values: Arc<std::sync::Mutex<Vec<i64>>>,
        cancelled: bool,
    }

    impl OperatorLifecycle for ProjectedInputObserver {
        fn name(&self) -> &str {
            "projected_input_observer"
        }

        fn cancel(&mut self) {
            self.cancelled = true;
        }
    }

    impl InputEncodingPolicy for ProjectedInputObserver {}
    impl TransformKernel for ProjectedInputObserver {
        fn apply(
            &mut self,
            _: &mut StepContext<'_>,
            chunk: PreparedInput,
        ) -> ExecutionResult<StepResult> {
            let chunk = chunk.into_uncovered_chunk()?;
            let values = chunk
                .column_by_slot_id(self.value_slot)?
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| {
                    writer_contract("embedded aggregate did not receive projected Int64")
                })?
                .values()
                .to_vec();
            self.values.lock().expect("observed values").extend(values);
            Ok(StepResult::new(None, NextStep::NeedInput, 0))
        }
    }

    impl OperatorLifecycle for CollectSink {
        fn name(&self) -> &str {
            "collect_sink"
        }
    }
    impl InputEncodingPolicy for CollectSink {}
    impl crate::exec::pipeline::shape::SinkKernel for CollectSink {
        fn consume(
            &mut self,
            _: &mut StepContext<'_>,
            input: PreparedInput,
        ) -> ExecutionResult<crate::exec::pipeline::shape::SinkStep> {
            let chunk = input.into_uncovered_chunk()?;
            let rows = chunk.len() as u64;
            self.chunks.lock().expect("collected chunks").push(chunk);
            Ok(crate::exec::pipeline::shape::SinkStep::new(
                NextStep::NeedInput,
                rows,
            ))
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

    impl OperatorLifecycle for StreamingPartial {
        fn name(&self) -> &str {
            "streaming_partial"
        }

        fn cancel(&mut self) {
            self.cancelled = true;
            self.outputs.clear();
            self.observable.defer_notify().arm();
        }
    }

    impl InputEncodingPolicy for StreamingPartial {}
    impl TransformKernel for StreamingPartial {
        fn apply(
            &mut self,
            ctx: &mut StepContext<'_>,
            input: PreparedInput,
        ) -> ExecutionResult<StepResult> {
            let chunk = input.into_uncovered_chunk()?;
            self.accepted_rows.fetch_add(chunk.len(), Ordering::Relaxed);
            self.outputs.push_back(self.output(10));
            self.outputs.push_back(self.output(20));
            self.drive(ctx)
        }
        fn drive(&mut self, _: &mut StepContext<'_>) -> ExecutionResult<StepResult> {
            let output = self.outputs.pop_front();
            let next = if !self.outputs.is_empty() {
                NextStep::Runnable
            } else if self.finishing {
                NextStep::Finished
            } else {
                NextStep::NeedInput
            };
            Ok(StepResult::new(output, next, 0))
        }
        fn finish(&mut self, ctx: &mut StepContext<'_>) -> ExecutionResult<StepResult> {
            self.finishing = true;
            self.drive(ctx)
        }
        fn control_observable(&self) -> Option<Arc<Observable>> {
            Some(self.observable.clone())
        }
    }

    struct TypedFailurePartial {
        error: ExecutionFailure,
        operation: crate::runtime::fragment::PipelineOperation,
        cancelled: bool,
    }
    impl OperatorLifecycle for TypedFailurePartial {
        fn name(&self) -> &str {
            "TypedFailurePartial"
        }
        fn cancel(&mut self) {
            self.cancelled = true;
        }
    }
    impl InputEncodingPolicy for TypedFailurePartial {}
    impl TransformKernel for TypedFailurePartial {
        fn start(&self) -> TransformStart {
            if self.operation == crate::runtime::fragment::PipelineOperation::Pull {
                TransformStart::Ready
            } else {
                TransformStart::NeedInput
            }
        }
        fn apply(
            &mut self,
            _: &mut StepContext<'_>,
            _: PreparedInput,
        ) -> ExecutionResult<StepResult> {
            Err(self.error.clone())
        }
        fn drive(&mut self, _: &mut StepContext<'_>) -> ExecutionResult<StepResult> {
            Err(self.error.clone())
        }
        fn finish(&mut self, _: &mut StepContext<'_>) -> ExecutionResult<StepResult> {
            Err(self.error.clone())
        }
    }
    #[test]
    fn typed_failure_composite_writer_preserves_partial_cause_and_aborts_actual_writer() {
        use crate::runtime::fragment::PipelineOperation;
        use novarocks_functions::{KernelDiagnostic, KernelFailure};
        for cause in [
            KernelFailure::ResourceExhausted,
            KernelFailure::Cancelled,
            KernelFailure::DeadlineExceeded,
            KernelFailure::InstanceFailed,
            KernelFailure::InvalidProgram(KernelDiagnostic::new("invalid")),
            KernelFailure::Internal(KernelDiagnostic::new("internal")),
            KernelFailure::Operational(KernelDiagnostic::new("operational")),
        ] {
            for operation in [
                PipelineOperation::Push,
                PipelineOperation::Pull,
                PipelineOperation::Finishing,
            ] {
                let stats = Arc::new(WriteExecutionStats::default());
                let execution = Arc::new(TestWriteExecution::new(Arc::clone(&stats)));
                let node = writer_fixture_with_count_partials(execution, 1);
                let state = test_runtime_state();
                let factory = writer_factory(&node, &state);
                let mut operator = factory.create_operator(1, 0);
                let original: ExecutionFailure = cause.clone().into();
                operator
                    .kernel_for_test_mut()
                    .replace_partial_aggregate_for_test(Box::new(Transform::hydrating(
                        TypedFailurePartial {
                            error: original.clone(),
                            operation,
                            cancelled: false,
                        },
                    )));

                install_construction(&mut operator, &state);
                operator.prepare().unwrap();
                operator.bind_runtime_state(&state).unwrap();
                operator.activate(&state).unwrap();
                assert!(poll_until(
                    || stats.opened.load(Ordering::Relaxed) == 1,
                    Duration::from_secs(5)
                ));
                let error = native_failure(&mut operator, &state, operation);
                assert_eq!(error, original);
                assert!(poll_until(
                    || operator.pending_finish().is_none(),
                    Duration::from_secs(5)
                ));
                assert_eq!(stats.aborted.load(Ordering::Relaxed), 1);
                assert_eq!(stats.finished.load(Ordering::Relaxed), 0);
                operator.close().unwrap();
            }
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

    impl OperatorLifecycle for OneShotPartial {
        fn name(&self) -> &str {
            "one_shot_partial"
        }

        fn cancel(&mut self) {
            self.cancelled = true;
            self.output = None;
            self.observable.defer_notify().arm();
        }
    }

    impl InputEncodingPolicy for OneShotPartial {}
    impl TransformKernel for OneShotPartial {
        fn apply(
            &mut self,
            _: &mut StepContext<'_>,
            _: PreparedInput,
        ) -> ExecutionResult<StepResult> {
            Ok(StepResult::new(
                self.output_on_push.take(),
                NextStep::NeedInput,
                0,
            ))
        }
        fn finish(&mut self, _: &mut StepContext<'_>) -> ExecutionResult<StepResult> {
            self.finishing = true;
            Ok(StepResult::new(None, NextStep::Finished, 0))
        }
        fn control_observable(&self) -> Option<Arc<Observable>> {
            Some(self.observable.clone())
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

    pub(crate) fn request_context() -> ConnectorRequestContext {
        ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(60),
            ConnectorStopOwner::new().view(),
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
        pub appended_rows: AtomicUsize,
        pub finished: AtomicUsize,
        pub finish_started: AtomicUsize,
        pub aborted: AtomicUsize,
    }

    pub(crate) struct TestWriteExecution {
        catalog_handle: CatalogHandle,
        stats: Arc<WriteExecutionStats>,
        fragments_per_writer: usize,
        fragment_bytes: usize,
        finish_gate: Option<Arc<tokio::sync::Semaphore>>,
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
                finish_gate: None,
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
                finish_gate: self.finish_gate.clone(),
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
        finish_gate: Option<Arc<tokio::sync::Semaphore>>,
    }

    #[async_trait::async_trait]
    impl ConnectorBatchWriter for TestBatchWriter {
        async fn append(&mut self, batch: RecordBatch) -> Result<(), ConnectorError> {
            self.rows += batch.num_rows();
            self.stats
                .appended_rows
                .fetch_add(batch.num_rows(), Ordering::Release);
            Ok(())
        }

        async fn finish(&mut self) -> Result<Vec<ConnectorCommitFragment>, ConnectorError> {
            self.stats.finish_started.fetch_add(1, Ordering::Release);
            if let Some(gate) = &self.finish_gate {
                gate.acquire().await.expect("finish gate").forget();
            }
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

    #[derive(Clone)]
    pub(crate) struct ProjectionRecipe {
        pub(super) ty: novarocks_type_contract::FunctionValueType,
        schema: arrow::datatypes::SchemaRef,
    }
    pub(crate) struct WriterFixture {
        pub(super) node_id: i32,
        pub(super) binding: TableWriterRuntimeBinding,
        pub(super) target: WriteTargetOrdinal,
        pub(super) expected_schema: arrow::datatypes::SchemaRef,
        pub(super) projection: ProjectionRecipe,
        pub(super) writer_multiplex_schema: WriterMultiplexRelationSchema,
        pub(super) partial_count: usize,
    }
    impl std::fmt::Debug for WriterFixture {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("WriterFixture")
                .field("node_id", &self.node_id)
                .finish_non_exhaustive()
        }
    }
    impl WriterFixture {
        pub(crate) fn try_new(
            node_id: i32,
            handle: ConnectorWriterHandle,
            target: WriteTargetOrdinal,
            execution: Arc<dyn ConnectorWriteExecution>,
            expected_schema: arrow::datatypes::SchemaRef,
            projection: ProjectionRecipe,
            physical: TableWriterPhysicalContextTemplate,
            request: ConnectorRequestContext,
            encoder: Arc<dyn ConnectorCommitFragmentEncoder>,
        ) -> Result<Self, crate::exec::fragment::error::ExecPlanBuildError> {
            Self::try_new_with_relation(
                node_id,
                handle,
                target,
                execution,
                expected_schema,
                projection,
                physical,
                request,
                encoder,
                WriterMultiplexRelationSchema::empty(),
                0,
            )
        }
        #[expect(
            clippy::too_many_arguments,
            reason = "independent writer fixture declarations and runtime capability"
        )]
        pub(crate) fn try_new_with_relation(
            node_id: i32,
            handle: ConnectorWriterHandle,
            target: WriteTargetOrdinal,
            execution: Arc<dyn ConnectorWriteExecution>,
            expected_schema: arrow::datatypes::SchemaRef,
            projection: ProjectionRecipe,
            physical: TableWriterPhysicalContextTemplate,
            request: ConnectorRequestContext,
            encoder: Arc<dyn ConnectorCommitFragmentEncoder>,
            relation: WriterMultiplexRelationSchema,
            partial_count: usize,
        ) -> Result<Self, crate::exec::fragment::error::ExecPlanBuildError> {
            let binding =
                TableWriterRuntimeBinding::try_new(handle, execution, physical, request, encoder)?;
            if projection.schema.as_ref() != expected_schema.as_ref() {
                return Err(crate::exec::fragment::error::ExecPlanBuildError::new(
                    crate::exec::fragment::error::ExecPlanInvariant::Schema,
                    "table writer input projection does not produce the expected writer schema",
                ));
            }
            Ok(Self {
                node_id,
                binding,
                target,
                expected_schema,
                projection,
                writer_multiplex_schema: relation,
                partial_count,
            })
        }
        #[cfg(novarocks_dev_controls)]
        fn with_aggregate_guard(mut self, guard: Arc<dyn TableWriteAggregateGuard>) -> Self {
            self.binding = self.binding.with_aggregate_guard(guard);
            self
        }
        fn writer_multiplex_schema(&self) -> &WriterMultiplexRelationSchema {
            &self.writer_multiplex_schema
        }
    }
    pub(crate) fn identity_projection() -> ProjectionRecipe {
        ProjectionRecipe {
            ty: novarocks_type_contract::FunctionValueType::new(DataType::Int32, false),
            schema: writer_input_schema(),
        }
    }
    fn typed_writer_input_schema() -> crate::exec::chunk::ChunkSchemaRef {
        let (_, _, layout) = compiled_projection(
            identity_projection().ty,
            writer_input_schema(),
            Arc::new(crate::runtime::runtime_state::RuntimeErrorState::default()),
        );
        let value = novarocks_type_contract::FunctionValueType::new(DataType::Int32, false);
        let slot = crate::exec::chunk::ChunkSlotSchema::try_new_typed(
            SlotId::new(1),
            layout.schema().fields()[0].clone(),
            value.typed_binding().clone(),
            None,
            novarocks_type_contract::ValueTypeStorageLifetime::pure(),
        )
        .unwrap();
        Arc::new(
            ChunkSchema::try_new_typed(
                vec![slot],
                novarocks_type_contract::ValueTypeStorageLifetime::pure(),
            )
            .unwrap(),
        )
    }
    pub(crate) fn input_chunk(values: Vec<i32>) -> Chunk {
        let schema = typed_writer_input_schema();
        let batch = RecordBatch::try_new(
            schema.arrow_schema_ref(),
            vec![Arc::new(Int32Array::from(values)) as ArrayRef],
        )
        .unwrap();
        crate::exec::chunk::typed_fixture::chunk(
            batch,
            &[SlotId::new(1)],
            &[novarocks_type_contract::FunctionValueType::new(
                DataType::Int32,
                false,
            )],
        )
    }
    pub(crate) fn writer_fixture(execution: Arc<dyn ConnectorWriteExecution>) -> WriterFixture {
        WriterFixture::try_new(
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
        .unwrap()
    }
    fn writer_factory(node: &WriterFixture, state: &RuntimeState) -> TableWriterOperatorFactory {
        TableWriterOperatorFactory::for_test_fixture(node, state)
            .expect("actual compiled table writer factory")
    }
    pub(crate) fn writer_fixture_with_count_partials(
        execution: Arc<dyn ConnectorWriteExecution>,
        count: usize,
    ) -> WriterFixture {
        let channels = (0..count)
            .map(|index| {
                WriterAuxiliaryChannel::try_new(
                    10_000 + index as u32,
                    format!("partial_{index}"),
                    novarocks_type_contract::FunctionValueType::new(DataType::Int64, true),
                )
                .unwrap()
            })
            .collect::<Vec<_>>();
        let relation = WriterMultiplexRelationSchema::try_new(
            WriterMultiplexSchema::try_new(channels).unwrap(),
        )
        .unwrap();
        WriterFixture::try_new_with_relation(
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
            count,
        )
        .unwrap()
    }

    struct FixtureCompileControl;
    impl novarocks_type_contract::PureCompileControl for FixtureCompileControl {
        fn checkpoint(
            &self,
            _: novarocks_type_contract::CompilePhase,
            _: u32,
        ) -> Result<(), novarocks_type_contract::CompileControlError> {
            Ok(())
        }
    }

    /// Compile the real one-column writer slot projection with exact provider
    /// facts. The stream cut is authored explicitly; no actor or plan-tree
    /// evaluator is constructed. Provider recipe validation and LocalCompiler
    /// remain the actual authors of the projection's checked frozen port.
    pub(crate) fn compiled_projection(
        ty: novarocks_type_contract::FunctionValueType,
        provider_schema: arrow::datatypes::SchemaRef,
        errors: Arc<crate::runtime::runtime_state::RuntimeErrorState>,
    ) -> (
        Arc<novarocks_local_program::LocalProgram>,
        Arc<dyn WriterProjectionFactory>,
        novarocks_local_program::StaticLayout,
    ) {
        use novarocks_connector_contract::{
            ConnectorCodecCategory, ConnectorCodecRevision, ConnectorEncodedPayload,
            ConnectorEnvelopeHeader, ConnectorWriteFieldBinding, ConnectorWriteFieldToken,
            ConnectorWriteInputShape, ConnectorWriteRecipeCompiler, ConnectorWriteRecipeDraft,
            PureProviderCompileError, PureProviderManifestEntry, PureProviderProgramCatalog,
            PureProviderProgramDefinition,
        };
        use novarocks_physical_plan::*;
        use novarocks_type_contract::{
            ControlShape, EvaluationDomainId, ExpressionControlFlow, ExpressionEffectContext,
            ExpressionEvaluationDomain, ExpressionInvocation, ExpressionUseId, FunctionValueType,
            SemanticParameters,
        };
        use std::collections::BTreeMap;
        let control = FixtureCompileControl;
        assert_eq!(provider_schema.fields().len(), 1);
        let binding = adapter().binding().clone();
        let payload = ConnectorEncodedPayload::new(
            ConnectorEnvelopeHeader::new(
                binding.descriptor().provider_id.clone(),
                binding.catalog_handle().clone(),
                ConnectorCodecCategory::WriteHandle,
                ConnectorCodecRevision::try_new(1).unwrap(),
            ),
            bytes::Bytes::from_static(b"writer-handle"),
        );
        let token = ConnectorWriteFieldToken::from_bytes([1; 32]);
        let draft = ConnectorWriteRecipeDraft::try_new(
            binding,
            payload,
            ConnectorWriteInputShape::Data {
                fields: vec![
                    ConnectorWriteFieldBinding::try_new_typed(
                        token,
                        provider_schema.fields()[0].as_ref().clone(),
                        ty.clone(),
                    )
                    .unwrap(),
                ],
            },
        )
        .unwrap();
        let ordinal = novarocks_physical_plan::WriteTargetOrdinal::try_new(0).unwrap();
        let mut builder = FragmentBuilder::new(FragmentId::new(71));
        let source = NodeId::new(0);
        let input = NodeId::new(1);
        let writer = NodeId::new(2);
        let value = builder
            .add_value(
                ty.clone(),
                ValueOrigin::NodeOutput {
                    node: source,
                    output_ordinal: 0,
                },
            )
            .unwrap();
        // The physical Int64 literal vocabulary cannot stand in for Int32
        // or a nominal JSON source. Admit the exact declared carrier/type as
        // a checked pool, preserving the real WriterProjection slot author.
        let array: ArrayRef = match ty.data_type() {
            DataType::Boolean => Arc::new(arrow::array::BooleanArray::from(vec![true])),
            DataType::Int32 => Arc::new(Int32Array::from(vec![1])),
            DataType::Int64 => Arc::new(Int64Array::from(vec![1])),
            DataType::Utf8 => Arc::new(arrow::array::StringArray::from(vec!["{}"])),
            _ => panic!(
                "the writer lifecycle fixture declares only its original scalar source types"
            ),
        };
        let pool_id = ConstantPoolId::new(0);
        let pool = novarocks_functions::ConstantPool::try_new(
            Arc::new(ty.try_to_field("writer_fixture_cell").unwrap()),
            ty.clone(),
            array.to_data(),
            crate::exec::expr::test_support::constant_policy(),
            novarocks_type_contract::CompilePhase::Validate,
            &control,
        )
        .unwrap();
        let mut constants = ConstantPools::empty();
        constants.insert(pool_id, pool).unwrap();
        let cell = builder
            .add_expression(
                source,
                ty.clone(),
                ExprKind::Constant(ConstantReference {
                    pool: pool_id,
                    ordinal: 0,
                }),
            )
            .unwrap();
        builder
            .add_values(source, Box::from([Box::from([cell])]), Box::from([value]))
            .unwrap();
        let read = builder
            .add_expression(input, ty.clone(), ExprKind::Value(value))
            .unwrap();
        let projected = builder
            .add_value(
                ty.clone(),
                ValueOrigin::Expr {
                    node: input,
                    expr: read,
                },
            )
            .unwrap();
        builder
            .add_project(
                input,
                source,
                Box::from([(read, projected)]),
                Box::from([projected]),
            )
            .unwrap();
        let fields = novarocks_spi::connector::write_stack::writer_output_schema()
            .fields()
            .iter()
            .enumerate()
            .map(|(index, field)| {
                let (role, kind) = match index {
                    0 => (
                        WriterRelationFieldRole::Kind,
                        WriterDerivedKind::RelationKind,
                    ),
                    1 => (
                        WriterRelationFieldRole::TargetOrdinal,
                        WriterDerivedKind::WriteTargetOrdinal,
                    ),
                    2 => (
                        WriterRelationFieldRole::RowCount,
                        WriterDerivedKind::AffectedRows,
                    ),
                    3 => (
                        WriterRelationFieldRole::CommitFragment,
                        WriterDerivedKind::CommitFragment,
                    ),
                    _ => unreachable!(),
                };
                let ty = FunctionValueType::new(field.data_type().clone(), field.is_nullable());
                let value = builder
                    .add_value(
                        ty.clone(),
                        ValueOrigin::WriterDerived {
                            writer_node: writer,
                            kind,
                        },
                    )
                    .unwrap();
                WriterRelationField {
                    value,
                    name: field.name().as_str().into(),
                    ty,
                    role,
                }
            })
            .collect::<Vec<_>>();
        let property = |distribution| PhysicalProperties {
            distribution,
            row_multiplicity: RowMultiplicity::SingleCopy,
            ordering: Box::default(),
        };
        builder
            .insert_node_unchecked(PhysicalNode {
                id: writer,
                inputs: Box::from([input]),
                required_inputs: Box::from([property(Distribution::Singleton)]),
                output_properties: property(Distribution::Unconstrained),
                output: OutputPort {
                    node: writer,
                    columns: fields.iter().map(|f| f.value).collect(),
                },
                kind: NodeKind::TableWriter {
                    target: WriterTarget {
                        handle: draft.payload().clone(),
                        write_target_ordinal: ordinal,
                        input: Box::from([projected]),
                        required_distribution: Distribution::Singleton,
                        target_fields: Box::from([WriterTargetField {
                            token,
                            provider_name: provider_schema.fields()[0].name().clone().into(),
                            input: projected,
                            ty: ty.clone(),
                            hidden: false,
                        }]),
                        output_schema: WriterRelationSchema {
                            revision: WRITER_MULTIPLEX_SCHEMA_REVISION,
                            fields: fields.clone().into_boxed_slice(),
                        },
                        partial_aggregates: Box::default(),
                    },
                },
            })
            .unwrap();
        let edge = EdgeId::new(9);
        let fragment = builder
            .finish_definition(
                writer,
                FragmentSink::Stream { edge },
                PipelineDopDomain {
                    min: 1,
                    max: 1,
                    requires_power_of_two: false,
                },
                novarocks_physical_plan::StagePlacementPolicy::Singleton,
            )
            .unwrap();
        let roots = PhysicalExpressionRoots::try_new(&fragment, &control).unwrap();
        let domain = EvaluationDomainId::new(0);
        let mut invocations = vec![];
        let mut bindings = vec![];
        for (index, (site, root)) in roots.sites().iter().enumerate() {
            let use_id = ExpressionUseId::new(index as u32);
            invocations.push(ExpressionInvocation {
                context: ExpressionEffectContext {
                    use_id,
                    domain,
                    demand: root.demand,
                },
                definition: root.expr,
                control: ControlShape::Eager,
                arguments: Box::default(),
            });
            bindings.push((*site, use_id));
        }
        let flow = ExpressionControlFlow::try_new(
            vec![ExpressionEvaluationDomain {
                id: domain,
                parent: None,
                guard: None,
            }],
            invocations,
            fragment.expressions(),
            novarocks_type_contract::CompilePhase::Validate,
            &control,
        )
        .unwrap();
        let uses = PhysicalRootUses::try_new(&fragment, flow, bindings, &control).unwrap();
        let calls = FrozenFragmentCalls::try_new(&fragment, &uses, vec![], &control).unwrap();
        let cut = OutboundFragmentCut {
            edge,
            kind: EdgeKind::Stream,
            destination_fragment: FragmentId::new(72),
            destination_node: NodeId::new(0),
            projection: fields
                .iter()
                .map(|f| CutValue {
                    value: f.value,
                    ty: f.ty.clone(),
                })
                .collect(),
            destination_imports: fields
                .iter()
                .enumerate()
                .map(|(i, f)| CutImport {
                    source: CutValue {
                        value: f.value,
                        ty: f.ty.clone(),
                    },
                    destination: ValueId::new(i as u32),
                })
                .collect(),
            partitioning: EdgePartitioning {
                source: Distribution::Singleton,
                source_multiplicity: RowMultiplicity::SingleCopy,
                destination: Distribution::Singleton,
                destination_multiplicity: RowMultiplicity::SingleCopy,
            },
            source_port: novarocks_physical_plan::FragmentOutputPort::Root,
            delivery: novarocks_physical_plan::DeliveryRequirement::MustDrain,
            payload: novarocks_physical_plan::ExchangePayloadContract::WriterResults(
                WriterResultCut {
                    write_target_ordinal: ordinal,
                    schema_revision: WRITER_MULTIPLEX_SCHEMA_REVISION,
                    fields: fields
                        .iter()
                        .enumerate()
                        .map(|(i, f)| WriterResultCutField {
                            source: f.value,
                            destination: ValueId::new(i as u32),
                            name: f.name.clone(),
                            ty: f.ty.clone(),
                            role: f.role,
                        })
                        .collect(),
                },
            ),
        };
        let package = FragmentPackage::try_new(
            FragmentPackageInput {
                constants,
                version: PlanVersionId::try_new([71; 16]).unwrap(),
                required: RequiredContracts::default(),
                expression_uses: uses,
                calls,
                pruning: FrozenFragmentPruning::try_new(fragment.id(), vec![], &control).unwrap(),
                fragment,
                cuts: FragmentCuts {
                    outbound: Box::from([cut]),
                    ..Default::default()
                },
                result: None,
                parameters: SemanticParameters::try_new([]).unwrap(),
                scans: BTreeMap::new(),
                writes: BTreeMap::from([(writer, draft)]),
                annotations: Box::default(),
            },
            FragmentPackageAdmission {
                plan_limits: PlanLimits::FROZEN,
                source_retained_bytes: 64 * 1024 * 1024,
                property_projection_limits: PropertyProofProjectionLimits {
                    max_request_bytes: 16 * 1024 * 1024,
                    max_coexisting_bytes: 256 * 1024 * 1024,
                    max_projection_work: 16 * 1024 * 1024,
                },
            },
            &control,
        )
        .unwrap();
        struct KeepDraft;
        impl ConnectorWriteRecipeCompiler for KeepDraft {
            type Error = ConnectorError;
            fn compile_private(
                &self,
                draft: &ConnectorWriteRecipeDraft,
                _: &dyn novarocks_type_contract::PureCompileControl,
            ) -> Result<ConnectorWriteRecipeDraft, PureProviderCompileError<ConnectorError>>
            {
                Ok(draft.clone())
            }
        }
        let provider = adapter().binding().descriptor().provider_id.clone();
        let providers = PureProviderProgramCatalog::try_new(
            &[PureProviderManifestEntry::new(
                provider.clone(),
                false,
                true,
            )],
            vec![PureProviderProgramDefinition::new(
                provider,
                None,
                Some(Arc::new(KeepDraft)
                    as Arc<
                        dyn ConnectorWriteRecipeCompiler<Error = ConnectorError>,
                    >),
            )],
            &control,
        )
        .unwrap();
        let validated = novarocks_local_compiler::validate_fragment_providers(
            Arc::new(package),
            &providers,
            &control,
        )
        .unwrap();
        let program = Arc::new(
            novarocks_local_compiler::compile_fragment(
                validated,
                &crate::exec::expr::compiled_program::tests::rng_subset(),
                novarocks_local_compiler::LocalCompileOptions {
                    pipeline_dop: std::num::NonZeroUsize::MIN,
                    root_sink_dop: None,
                    kernel_abi: novarocks_local_program::KernelAbiVersion::CURRENT,
                    constants: crate::exec::expr::test_support::constant_policy(),
                    exchange_wait: Duration::from_secs(120),
                },
                &control,
            )
            .unwrap(),
        );
        let id = program.graph().root();
        let novarocks_local_program::ProgramNodeKind::TableWriter { input, .. } =
            program.graph().nodes()[id.index()].kind()
        else {
            panic!("actual compiled writer")
        };
        let input_layout = program.graph().nodes()[input.index()]
            .output_layout()
            .clone();
        assert_eq!(input_layout.slots(), &[SlotId::new(1)]);
        let projection = Arc::new(
            super::super::compiled_writer::CompiledWriterProjection::try_new(
                program.clone(),
                id,
                errors,
            )
            .unwrap(),
        ) as Arc<dyn WriterProjectionFactory>;
        (program, projection, input_layout)
    }

    #[derive(Debug)]
    enum CountPreparationFailure {
        Creator {
            _cause: crate::exec::pipeline::operator::ConstructionFailure,
        },
        Layout {
            _cause: novarocks_type_contract::owned_resources::layout::LayoutResourceError,
        },
        Calls {
            _cause: novarocks_local_program::PreparedWriterCallsError,
        },
    }
    impl From<novarocks_type_contract::owned_resources::layout::LayoutResourceError>
        for CountPreparationFailure
    {
        fn from(
            cause: novarocks_type_contract::owned_resources::layout::LayoutResourceError,
        ) -> Self {
            Self::Layout { _cause: cause }
        }
    }
    impl From<novarocks_local_program::PreparedWriterCallsError> for CountPreparationFailure {
        fn from(cause: novarocks_local_program::PreparedWriterCallsError) -> Self {
            Self::Calls { _cause: cause }
        }
    }
    pub(super) fn relation_layout(
        schema: &crate::exec::chunk::ChunkSchemaRef,
    ) -> novarocks_local_program::StaticLayout {
        // This borrows complete typed relation declarations, never infers a
        // semantic type from a row value or an observed first batch.
        let types = schema
            .slots()
            .iter()
            .map(|slot| {
                novarocks_type_contract::FunctionValueType::try_from_typed_binding(
                    slot.typed_binding().clone(),
                )
                .unwrap()
            })
            .collect::<Vec<_>>();
        crate::exec::chunk::typed_fixture::layout(
            schema.arrow_schema_ref(),
            schema.slot_ids(),
            &types,
        )
    }
    pub(super) fn prepare_count_source(
        count: usize,
        input: novarocks_local_program::StaticLayout,
        output: novarocks_local_program::StaticLayout,
        state: &RuntimeState,
    ) -> LocalPreparedWriterPartial {
        use novarocks_functions::*;
        use novarocks_type_contract::{
            CallProofScope, DecimalOverflowPolicy, EvaluationDemand, EvaluationDomainId,
            ExpressionEffectContext, ExpressionUseId, SemanticParameters,
        };
        let original =
            novarocks_functions::builtin::catalogue::build_builtin_engine_function_catalog()
                .unwrap();
        let mut builder = EngineFunctionCatalogBuilder::new();
        builder
            .register(
                original
                    .definition("count", FunctionKind::Aggregate)
                    .unwrap()
                    .clone(),
            )
            .unwrap();
        let catalog = builder
            .seal_pure([InstalledPureKernel {
                function: FunctionId::try_new("builtin.aggregate/count/v1").unwrap(),
                kind: FunctionKind::Aggregate,
                implementation: PureImplementationDeclaration {
                    overload: FunctionOverloadId::try_new("builtin.aggregate/count/derived-v1")
                        .unwrap(),
                    implementation: PureImplementationId::try_new(
                        "builtin.aggregate/count/selected-v1",
                    )
                    .unwrap(),
                    abi: PureKernelAbi::AggregateWindowV1,
                },
                aggregate_state_format: Some(
                    AggregateStateFormatIdentity::try_new("novarocks/count/state-v1").unwrap(),
                ),
            }])
            .unwrap();
        let declared = FunctionValueType::new(DataType::Int32, false);
        let args = [FunctionArgument::Value {
            value_type: declared.clone(),
            constant: None,
        }];
        let request = || FunctionBindingRequest {
            arguments: &args,
            logical_argument_count: 1,
            expected_result_type: None,
        };
        let bound = catalog
            .metadata()
            .resolve_bound_trusted(
                "count",
                FunctionKind::Aggregate,
                request(),
                &FixtureCompileControl,
            )
            .unwrap();
        let selected = Arc::new(bound.selected);
        let factory=crate::exec::pipeline::operator::input_construction::try_runtime_input_construction_factory(
            crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(),MemTracker::new_root("writer real prepared COUNT source"),state.error_state(),
        ).unwrap();
        let owner = factory.begin_compiled_layout_construction(&output).unwrap();
        owner
            .request(
                std::alloc::Layout::array::<novarocks_local_program::PreparedWriterAggregateCall>(
                    count,
                )
                .unwrap(),
            )
            .unwrap();
        let mut calls = Vec::with_capacity(count);
        let parameters = SemanticParameters::try_new([]).unwrap();
        for index in 0..count {
            let context = ExpressionEffectContext {
                use_id: ExpressionUseId::new(index as u32),
                domain: EvaluationDomainId::new(index as u32),
                demand: EvaluationDemand::Value,
            };
            let input_use = [Some(ExpressionUseId::new(count as u32 + index as u32))];
            let token = catalog
                .prepare_fresh(
                    CallEffectInput {
                        context,
                        argument_uses: CallArgumentUses::SelectedChannels(&input_use),
                        function_id: &bound.function_id,
                        kind: FunctionKind::Aggregate,
                        selected: &selected,
                        request: request(),
                        environment: &[],
                        parameters: &parameters,
                        decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
                        proof_scope: CallProofScope::Domain(context.domain),
                    },
                    selected.clone(),
                    PureCallPreparation::Aggregate {
                        arguments: ScopedExpressionEffects::pure_value(context),
                        options: AggregatePreparationOptions {
                            state_interpretation: None,
                            phase: AggregateKernelPhase::Partial,
                            distinct: false,
                            order_keys: Arc::from([]),
                            state_input_type: None,
                        },
                    },
                    &FixtureCompileControl,
                )
                .unwrap();
            let PreparedPureKernel::Aggregate(handle) = token.prepared() else {
                panic!("actual COUNT aggregate handle")
            };
            let output_position = index + 4;
            calls.push(novarocks_local_program::PreparedWriterAggregateCall::new(
                handle.clone(),
                input.slots()[0],
                output.slots()[output_position],
                output_position,
                owner.storage_lifetime(),
            ));
        }
        if calls.capacity() != calls.len() {
            owner.request(std::alloc::Layout::array::<novarocks_local_program::PreparedWriterAggregateCall>(calls.len()).unwrap()).unwrap();
        }
        let calls = novarocks_local_program::PreparedWriterAggregateCalls::try_new_observed::<
            CountPreparationFailure,
        >(
            calls.into_boxed_slice(),
            AggregateKernelPhase::Partial,
            owner.storage_lifetime(),
            |layout| {
                owner
                    .request(layout)
                    .map_err(|_cause| CountPreparationFailure::Creator { _cause })
            },
            || {
                owner
                    .observe(
                        0,
                        &[],
                        crate::exec::chunk::ArrayNormalizationStep::InspectConstructionWork {
                            units: 1,
                        },
                    )
                    .map_err(|_cause| CountPreparationFailure::Creator { _cause })
            },
        )
        .unwrap();
        LocalPreparedWriterPartial {
            calls,
            input_layout: input,
            output_layout: output,
            error: state.error_state(),
        }
    }

    #[test]
    fn table_writer_binding_rejects_a_foreign_catalog_generation() {
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
        let error = WriterFixture::try_new(
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
    fn prepared_writer_does_not_open_provider_until_activation() {
        let stats = Arc::new(WriteExecutionStats::default());
        let execution = Arc::new(TestWriteExecution::new(Arc::clone(&stats)));
        let state = test_runtime_state();
        let factory = writer_factory(&writer_fixture(execution), &state);
        let mut operator = factory.create(1, 0);

        install_construction(operator.as_mut(), &state);
        operator.prepare().expect("prepare writer");
        operator.bind_runtime_state(&state).expect("bind writer");
        assert_eq!(stats.opened.load(Ordering::Acquire), 0);

        operator.activate(&state).expect("activate writer");
        assert!(poll_until(
            || stats.opened.load(Ordering::Acquire) == 1,
            Duration::from_secs(5)
        ));
        operator.cancel();
        assert!(poll_until(
            || operator.pending_finish().is_none(),
            Duration::from_secs(5)
        ));
    }

    #[test]
    fn prepared_writer_abort_does_not_wait_for_an_unstarted_actor() {
        let stats = Arc::new(WriteExecutionStats::default());
        let execution = Arc::new(TestWriteExecution::new(Arc::clone(&stats)));
        let state = test_runtime_state();
        let factory = writer_factory(&writer_fixture(execution), &state);
        let mut operator = factory.create(1, 0);

        install_construction(operator.as_mut(), &state);
        operator.prepare().expect("prepare writer");
        operator.bind_runtime_state(&state).expect("bind writer");
        operator.cancel();

        assert_eq!(stats.opened.load(Ordering::Acquire), 0);
        assert!(matches!(
            operator.drive_for_test(&state).unwrap().next,
            NextStep::Finished
        ));
        assert!(operator.pending_finish().is_none());
    }

    #[test]
    fn table_writer_opens_one_independent_writer_per_driver() {
        let stats = Arc::new(WriteExecutionStats::default());
        let execution = Arc::new(TestWriteExecution::new(Arc::clone(&stats)));
        let writer_rows = Arc::clone(&execution.writer_rows);
        let state = test_runtime_state();
        let factory = writer_factory(&writer_fixture(execution.clone()), &state);

        let dop = 4;
        let mut operators: Vec<Box<dyn Operator>> =
            (0..dop).map(|driver| factory.create(dop, driver)).collect();

        for (driver, operator) in operators.iter_mut().enumerate() {
            bind(operator, &state);
            complete_native_input(
                operator.as_mut(),
                &state,
                input_chunk(vec![driver as i32; driver + 1]),
            )
            .expect("append");
            operator.finish_for_test(&state).expect("signal EOS");
            wait_for_output(operator, &state)
                .expect("drive this independent writer's finalization");
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
        let projection = ProjectionRecipe {
            ty: novarocks_type_contract::FunctionValueType::new(DataType::Int64, false),
            schema: expected_schema.clone(),
        };
        let node = WriterFixture::try_new(
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
        let state = test_runtime_state();
        let (factory, projection_layout) =
            TableWriterOperatorFactory::for_test_fixture_with_projection_layout(&node, &state)
                .unwrap();
        let mut operator = factory.create_operator(1, 0);
        let observed = Arc::new(std::sync::Mutex::new(Vec::new()));
        operator
            .kernel_for_test_mut()
            .replace_partial_aggregate_for_test(Box::new(Transform::hydrating(
                ProjectedInputObserver {
                    value_slot: projection_layout.slots()[0],
                    values: Arc::clone(&observed),
                    cancelled: false,
                },
            )));

        install_construction(&mut operator, &state);
        operator.prepare().expect("prepare");
        operator.bind_runtime_state(&state).expect("bind");
        operator.activate(&state).expect("activate");
        complete_native_input(&mut operator, &state, {
            let (_, _, layout) = compiled_projection(
                node.projection.ty.clone(),
                node.expected_schema.clone(),
                state.error_state(),
            );
            let batch = RecordBatch::try_new(
                layout.schema().clone(),
                vec![Arc::new(Int64Array::from(vec![7_i64, 11])) as ArrayRef],
            )
            .unwrap();
            crate::exec::chunk::typed_fixture::chunk(
                batch,
                layout.slots(),
                std::slice::from_ref(&node.projection.ty),
            )
        })
        .expect("push projected page");
        assert_eq!(
            observed.lock().expect("observed values").as_slice(),
            &[7_i64, 11_i64]
        );
        operator.finish_for_test(&state).expect("finish");
        let mut boxed: Box<dyn Operator> = Box::new(operator);
        wait_for_output(&mut boxed, &state).expect("writer output");
        assert_eq!(
            writer_rows.lock().expect("writer rows").as_slice(),
            &[(0, 2)]
        );
    }

    #[cfg(novarocks_dev_controls)]
    #[test]
    fn partial_aggregate_guard_rejects_update_before_either_consumer_accepts_the_page() {
        let stats = Arc::new(WriteExecutionStats::default());
        let execution = Arc::new(TestWriteExecution::new(Arc::clone(&stats)).with_fragments(0, 0));
        let writer_rows = Arc::clone(&execution.writer_rows);
        let node = writer_fixture_with_count_partials(execution, 1);
        let node = node.with_aggregate_guard(Arc::new(RejectAggregateBoundary(
            TableWriteAggregateBoundary::PartialUpdate,
        )));
        let state = test_runtime_state();
        let mut operator = writer_factory(&node, &state).create(1, 0);

        bind(&mut operator, &state);
        let error = native_failure(
            operator.as_mut(),
            &state,
            crate::runtime::fragment::PipelineOperation::Push,
        );
        assert!(error.detail().contains("PartialUpdate"), "{error}");
        assert!(
            writer_rows.lock().expect("writer rows").is_empty(),
            "the reserved page must not reach the provider writer"
        );
        assert!(
            operator
                .drive_for_test(&state)
                .unwrap_err()
                .same_occurrence(&error)
        );
    }

    #[cfg(novarocks_dev_controls)]
    #[test]
    fn partial_aggregate_guard_rejects_finalize_without_success_output() {
        let stats = Arc::new(WriteExecutionStats::default());
        let execution = Arc::new(TestWriteExecution::new(Arc::clone(&stats)).with_fragments(0, 0));
        let node = writer_fixture_with_count_partials(execution, 1);
        let node = node.with_aggregate_guard(Arc::new(RejectAggregateBoundary(
            TableWriteAggregateBoundary::PartialFinalize,
        )));
        let state = test_runtime_state();
        let mut operator = writer_factory(&node, &state).create(1, 0);

        bind(&mut operator, &state);
        complete_native_input(operator.as_mut(), &state, input_chunk(vec![1, 2, 3]))
            .expect("input before finalize fault");
        let error = native_failure(
            operator.as_mut(),
            &state,
            crate::runtime::fragment::PipelineOperation::Finishing,
        );
        assert!(error.detail().contains("PartialFinalize"), "{error}");
        assert!(
            operator
                .drive_for_test(&state)
                .unwrap_err()
                .same_occurrence(&error)
        );
    }

    #[test]
    fn table_writer_emits_one_row_count_row_and_one_row_per_fragment() {
        for fragment_count in [0usize, 1, 3] {
            let stats = Arc::new(WriteExecutionStats::default());
            let execution = Arc::new(
                TestWriteExecution::new(Arc::clone(&stats)).with_fragments(fragment_count, 4),
            );
            let state = test_runtime_state();
            let factory = writer_factory(&writer_fixture(execution), &state);
            let mut operator = factory.create(1, 0);

            bind(&mut operator, &state);
            complete_native_input(operator.as_mut(), &state, input_chunk(vec![1, 2, 3, 4, 5]))
                .expect("append without premature output");
            operator.finish_for_test(&state).expect("signal EOS");

            let chunks = wait_for_output(&mut operator, &state).expect("writer output");
            assert_eq!(
                chunks.iter().map(Chunk::len).sum::<usize>(),
                fragment_count + 1
            );
            let mut row = 0usize;
            for chunk in &chunks {
                let columns = TableWriteRelationColumns::try_from_relation(chunk).expect("columns");
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
            assert!(matches!(
                operator.drive_for_test(&state).unwrap().next,
                NextStep::Finished
            ));
        }
    }

    #[test]
    fn composite_writer_counts_every_page_on_both_sides_and_restores_1024_channels() {
        let stats = Arc::new(WriteExecutionStats::default());
        let execution = Arc::new(TestWriteExecution::new(Arc::clone(&stats)).with_fragments(0, 0));
        let writer_rows = Arc::clone(&execution.writer_rows);
        let node = writer_fixture_with_count_partials(execution, 1_024);
        let state = test_runtime_state();
        let factory = writer_factory(&node, &state);
        let mut operator = factory.create(1, 0);

        bind(&mut operator, &state);
        complete_native_input(operator.as_mut(), &state, input_chunk(vec![1, 2]))
            .expect("first page");
        complete_native_input(operator.as_mut(), &state, input_chunk(vec![3, 4, 5]))
            .expect("second page");
        operator.finish_for_test(&state).expect("finish");

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
                node.writer_multiplex_schema().contract().arrow_schema(),
            );
            let prefix = TableWriteRelationColumns::try_from_relation(chunk).expect("prefix");
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
        assert!(matches!(
            operator.drive_for_test(&state).unwrap().next,
            NextStep::Finished
        ));
    }

    #[test]
    fn streaming_partial_outputs_block_input_then_resume_and_continue_while_draining() {
        let stats = Arc::new(WriteExecutionStats::default());
        let execution = Arc::new(TestWriteExecution::new(Arc::clone(&stats)).with_fragments(0, 0));
        let writer_rows = Arc::clone(&execution.writer_rows);
        let node = writer_fixture_with_count_partials(execution, 1);
        let state = test_runtime_state();
        let factory = writer_factory(&node, &state);
        let slot_id = SlotId::new(
            node.writer_multiplex_schema()
                .contract()
                .auxiliary_channels()[0]
                .slot_id(),
        );
        let aggregate_rows = Arc::new(AtomicUsize::new(0));
        let mut operator = factory.create_operator(1, 0);
        operator
            .kernel_for_test_mut()
            .replace_partial_aggregate_for_test(Box::new(Transform::hydrating(
                StreamingPartial::new(slot_id, Arc::clone(&aggregate_rows)),
            )));

        install_construction(&mut operator, &state);
        operator.prepare().expect("prepare");
        operator.bind_runtime_state(&state).expect("bind");
        operator.activate(&state).expect("activate");

        accept_native_input(
            &mut operator,
            &state,
            RawInput::for_test(input_chunk(vec![1, 2])),
        )
        .expect("first input");
        let first = next_running_output(&mut operator, &state);
        assert!(!matches!(first.next, NextStep::NeedInput));
        let raw = RawInput::for_test(input_chunk(vec![3]));
        let original = raw.original_for_test().columns()[0].clone();
        let raw = match operator.try_push_for_test(&state, raw) {
            crate::exec::pipeline::step::InputResult::Deferred {
                input,
                next: AdmissionNext::ResumeExisting,
            } => input,
            _ => panic!("running partial output must defer the next original input"),
        };
        assert!(Arc::ptr_eq(
            &original,
            &raw.original_for_test().columns()[0]
        ));
        let second = next_running_output(&mut operator, &state);
        if !matches!(second.next, NextStep::NeedInput) {
            let mut completed = false;
            for _ in 0..64 {
                let step = operator
                    .drive_for_test(&state)
                    .expect("complete first input");
                assert!(
                    step.output.is_none(),
                    "first input emits exactly two partials"
                );
                if matches!(step.next, NextStep::NeedInput) {
                    completed = true;
                    break;
                }
                assert!(matches!(step.next, NextStep::Runnable | NextStep::Yield));
            }
            assert!(
                completed,
                "finite first input completes before accepting another"
            );
        }
        accept_native_input(&mut operator, &state, raw).expect("the same second raw page resumes");
        // EOS only marks input complete; the subsequent admitted drive
        // drains both accepted partial output and finalization output.
        operator
            .finish_for_test(&state)
            .expect("signal EOS once with accepted work outstanding");
        assert!(
            operator.pending_finish().is_none(),
            "partial continuation remains dataflow work"
        );
        let mut outputs = vec![first.output.unwrap(), second.output.unwrap()];
        let mut boxed: Box<dyn Operator> = Box::new(operator);
        outputs.extend(
            wait_for_output(&mut boxed, &state).expect("drain accepted input and finalization"),
        );
        assert_eq!(aggregate_rows.load(Ordering::Relaxed), 3);
        assert_eq!(
            writer_rows.lock().expect("writer rows").as_slice(),
            &[(0, 3)]
        );
        let mut partial_values = Vec::new();
        let mut row_count = None;
        for output in &outputs {
            let prefix = TableWriteRelationColumns::try_from_relation(output).expect("prefix");
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
        let channel = WriterAuxiliaryChannel::try_new(
            20_000,
            "huge",
            novarocks_type_contract::FunctionValueType::new(DataType::Binary, true),
        )
        .expect("binary auxiliary channel");
        let relation = WriterMultiplexRelationSchema::try_new(
            WriterMultiplexSchema::try_new(vec![channel.clone()]).expect("multiplex contract"),
        )
        .expect("multiplex relation");
        let node = WriterFixture::try_new_with_relation(
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
            0,
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

        let state = test_runtime_state_with_packet_budget(PACKET_BYTES, None);
        let factory = writer_factory(&node, &state);
        let mut operator = factory.create_operator(1, 0);
        operator
            .kernel_for_test_mut()
            .replace_partial_aggregate_for_test(Box::new(Transform::hydrating(
                OneShotPartial::new(partial),
            )));

        install_construction(&mut operator, &state);
        operator.prepare().expect("prepare");
        operator.bind_runtime_state(&state).expect("bind");
        operator.activate(&state).expect("activate");
        assert!(poll_until(
            || stats.opened.load(Ordering::Relaxed) == 1,
            Duration::from_secs(5)
        ));
        let error = native_failure(
            &mut operator,
            &state,
            crate::runtime::fragment::PipelineOperation::Push,
        );
        use novarocks_failure_contract::{FailureClass, FailureProjection, ResourceDimension};
        let fact = error.failure_fact();
        assert_eq!(fact.class(), FailureClass::Resource);
        assert_eq!(
            fact.reason(),
            crate::runtime::fragment::ExecutionReason::WriterOutputBytes.reason()
        );
        let resource = fact.resource().expect("original output budget facts");
        assert_eq!(resource.dimension, ResourceDimension::Bytes);
        assert_eq!(resource.limit, Some(MAX_WRITER_MULTIPLEX_ROW_BYTES as u64));
        assert!(resource.requested.unwrap() > MAX_WRITER_MULTIPLEX_ROW_BYTES as u64);
        assert_eq!(resource.committed, None);
        assert!(
            operator
                .drive_for_test(&state)
                .unwrap_err()
                .same_occurrence(&error),
            "a failed continuation can never emit a success prefix"
        );
        assert!(poll_until(
            || stats.aborted.load(Ordering::Relaxed) == 1,
            Duration::from_secs(5)
        ));
    }

    #[test]
    fn sparse_packer_splits_1024_typed_channels_across_multiple_batches() {
        assert_sparse_packer_splits_1024_channels(20 * 1024, false);
    }

    #[test]
    fn sparse_packer_splits_144_byte_channels_without_single_channel_exception() {
        // Exact retained n=16 Theta v3 states carry 16 + 8*16 = 144 bytes.
        assert_sparse_packer_splits_1024_channels(144, true);
    }

    fn assert_sparse_packer_splits_1024_channels(
        value_bytes: usize,
        require_multiple_channels_per_batch: bool,
    ) {
        const CHANNELS: usize = 1_024;
        const PACKET_BYTES: usize = 320 * 1024;

        let stats = Arc::new(WriteExecutionStats::default());
        let execution = Arc::new(TestWriteExecution::new(Arc::clone(&stats)).with_fragments(0, 0));
        let channels = (0..CHANNELS)
            .map(|index| {
                WriterAuxiliaryChannel::try_new(
                    30_000 + u32::try_from(index).expect("channel index"),
                    format!("binary_partial_{index}"),
                    novarocks_type_contract::FunctionValueType::new(DataType::Binary, true),
                )
                .expect("binary channel")
            })
            .collect::<Vec<_>>();
        let relation = WriterMultiplexRelationSchema::try_new(
            WriterMultiplexSchema::try_new(channels.clone()).expect("multiplex contract"),
        )
        .expect("multiplex relation");
        let node = WriterFixture::try_new_with_relation(
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
            0,
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
                let value = vec![u8::try_from(index % 251).expect("byte"); value_bytes];
                Arc::new(BinaryArray::from(vec![Some(value.as_slice())])) as ArrayRef
            })
            .collect::<Vec<_>>();
        let schema = Arc::new(Schema::new(fields));
        let batch = RecordBatch::try_new(Arc::clone(&schema), arrays).expect("wide partial batch");
        let chunk_schema =
            ChunkSchema::try_ref_from_schema_and_slot_ids(schema.as_ref(), &slot_ids)
                .expect("wide partial schema");
        let partial = Chunk::new_with_chunk_schema(batch, chunk_schema);

        let state = test_runtime_state_with_packet_budget(PACKET_BYTES, None);
        let factory = writer_factory(&node, &state);
        let mut operator = factory.create_operator(1, 0);
        operator
            .kernel_for_test_mut()
            .replace_partial_aggregate_for_test(Box::new(Transform::hydrating(
                OneShotPartial::new(partial),
            )));

        install_construction(&mut operator, &state);
        operator.prepare().expect("prepare");
        operator.bind_runtime_state(&state).expect("bind");
        operator.activate(&state).expect("activate");
        assert!(poll_until(
            || stats.opened.load(Ordering::Relaxed) == 1,
            Duration::from_secs(5)
        ));
        accept_native_input(
            &mut operator,
            &state,
            RawInput::for_test(input_chunk(vec![1])),
        )
        .expect("input reaches both children");

        let mut output_batches = 0usize;
        let mut seen = vec![false; CHANNELS];
        let mut next = NextStep::Runnable;
        while !matches!(next, NextStep::NeedInput) {
            let step = operator.drive_for_test(&state).expect("sparse output step");
            next = step.next;
            let Some(output) = step.output else {
                assert!(matches!(
                    next,
                    NextStep::NeedInput | NextStep::Runnable | NextStep::Yield
                ));
                continue;
            };
            output_batches += 1;
            assert_eq!(output.len(), 1);
            assert!(
                operator
                    .kernel_for_test()
                    .output_size(&output)
                    .expect("encoded output size")
                    <= PACKET_BYTES,
                "every sparse batch must honor the runtime packet budget"
            );
            let prefix = TableWriteRelationColumns::try_from_relation(&output).expect("prefix");
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
                    assert_eq!(value.len(), value_bytes);
                    assert_eq!(value[0], u8::try_from(index % 251).expect("byte"));
                    assert!(
                        value
                            .iter()
                            .all(|byte| *byte == u8::try_from(index % 251).expect("byte")),
                        "channel {index} changed its complete payload"
                    );
                    seen[index] = true;
                }
            }
            assert!(populated > 0 && populated < CHANNELS);
            if require_multiple_channels_per_batch {
                assert!(
                    populated > 1,
                    "the 144-byte fixture must not rely on the single-channel size exception"
                );
            }
        }
        assert!(output_batches > 1, "the wide row must be split by bytes");
        assert_eq!(operator.kernel_for_test().partial_batches, output_batches);
        assert_eq!(
            operator.kernel_for_test().sparse_partial_rows,
            output_batches
        );
        assert_eq!(operator.kernel_for_test().partial_channels, CHANNELS);
        assert!(seen.into_iter().all(|channel| channel));
        assert!(matches!(next, NextStep::NeedInput));
        operator.cancel();
        assert!(poll_until(
            || exits_ready(&mut operator),
            Duration::from_secs(5)
        ));
    }

    #[test]
    fn table_writer_reports_zero_rows_when_it_wrote_nothing() {
        let stats = Arc::new(WriteExecutionStats::default());
        let execution = Arc::new(TestWriteExecution::new(Arc::clone(&stats)).with_fragments(0, 0));
        let state = test_runtime_state();
        let factory = writer_factory(&writer_fixture(execution), &state);
        let mut operator = factory.create(1, 0);

        bind(&mut operator, &state);
        operator.finish_for_test(&state).expect("finish");
        let chunks = wait_for_output(&mut operator, &state).expect("writer output");
        let columns = TableWriteRelationColumns::try_from_relation(&chunks[0]).expect("columns");
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
        let state = test_runtime_state();
        let factory = writer_factory(&writer_fixture(execution), &state);
        let mut operator = factory.create(1, 0);

        bind(&mut operator, &state);
        operator
            .finish_for_test(&state)
            .expect("the exact single-fragment budget is legal");
        wait_for_output(&mut operator, &state).expect("exact-budget output");

        // One byte more is a typed rejection, and the writer is aborted.
        let stats = Arc::new(WriteExecutionStats::default());
        let execution = Arc::new(
            TestWriteExecution::new(Arc::clone(&stats))
                .with_fragments(1, MAX_CONNECTOR_COMMIT_FRAGMENT_BYTES + 1),
        );
        let state = test_runtime_state();
        let factory = writer_factory(&writer_fixture(execution), &state);
        let mut operator = factory.create(1, 0);
        bind(&mut operator, &state);
        operator
            .finish_for_test(&state)
            .expect("finish request is asynchronous");
        let error =
            wait_for_output(&mut operator, &state).expect_err("over the single-fragment budget");
        let fact = novarocks_failure_contract::FailureProjection::failure_fact(&error);
        assert_eq!(
            fact.class(),
            novarocks_failure_contract::FailureClass::Resource
        );
        let resource = fact
            .resource()
            .expect("the producing writer reports its exact bound");
        assert_eq!(
            resource.dimension,
            novarocks_failure_contract::ResourceDimension::Bytes
        );
        assert_eq!(
            resource.requested,
            Some((MAX_CONNECTOR_COMMIT_FRAGMENT_BYTES + 1) as u64)
        );
        assert_eq!(
            resource.limit,
            Some(MAX_CONNECTOR_COMMIT_FRAGMENT_BYTES as u64)
        );
        assert!(
            error
                .detail()
                .contains("exceeds the frozen single-fragment budget")
        );
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
        let state = test_runtime_state();
        let factory = writer_factory(&writer_fixture(execution), &state);
        let mut first = factory.create(2, 0);
        let mut second = factory.create(2, 1);

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
            .finish_for_test(&state)
            .expect("second driver finishes independently");
        wait_for_output(&mut second, &state).expect("drive independent sibling finalization");
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
        let state = test_runtime_state();
        let factory = writer_factory(&writer_fixture(execution), &state);
        let mut operator = factory.create(1, 0);

        bind(&mut operator, &state);
        assert!(poll_until(
            || state.error().is_some(),
            Duration::from_secs(5)
        ));
        let error = state
            .error()
            .expect("a failed writer open must fail its driver");
        assert!(error.detail().contains("open connector writer"));
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
        let state = test_runtime_state();
        let factory = writer_factory(&writer_fixture(execution), &state);
        let mut operator = factory.create_operator(1, 0);

        install_construction(&mut operator, &state);
        operator.prepare().expect("prepare");
        operator.bind_runtime_state(&state).expect("bind");
        operator.activate(&state).expect("activate");
        operator.finish_for_test(&state).expect("request finish");

        // EOS only marks the input boundary. This worker call starts the
        // real writer finalizer; the actor reports its error independently.
        let step = operator.drive_for_test(&state).expect("start finalization");
        assert!(step.output.is_none());
        assert!(matches!(step.next, NextStep::Runnable | NextStep::Wait(_)));

        let error = state
            .error()
            .or_else(|| {
                assert!(poll_until(
                    || state.error().is_some(),
                    Duration::from_secs(5)
                ));
                state.error()
            })
            .expect("actor reports its originating failure before data observes stop");
        assert!(
            error.detail().contains("finish connector writer"),
            "{error}"
        );
        operator.on_driver_failure();
        assert!(
            poll_until(
                || {
                    let mut pending = false;
                    operator.visit_exits(&mut |exit| {
                        if matches!(
                            exit.report_to(&state.error_state()),
                            crate::exec::pipeline::operator::ExitProgress::Pending(_)
                                | crate::exec::pipeline::operator::ExitProgress::Unresolved(_)
                        ) {
                            pending = true;
                        }
                    });
                    !pending
                },
                Duration::from_secs(5)
            ),
            "actual actor join completes"
        );
        assert_eq!(aborted.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn driver_resumes_after_mandatory_writer_finish_to_deliver_its_output() {
        let stats = Arc::new(WriteExecutionStats::default());
        let finish_gate = Arc::new(tokio::sync::Semaphore::new(0));
        let mut execution = TestWriteExecution::new(Arc::clone(&stats));
        execution.finish_gate = Some(finish_gate.clone());
        let execution = Arc::new(execution);
        let runtime_state = Arc::new(test_runtime_state());
        let factory = writer_factory(&writer_fixture(execution), runtime_state.as_ref());

        let mut writer = factory.create(1, 0);
        bind(&mut writer, runtime_state.as_ref());
        let collected = Arc::new(std::sync::Mutex::new(Vec::new()));
        let mut driver = PipelineDriver::new(
            0,
            vec![
                Box::new(crate::exec::pipeline::shape::Source::new(OneChunkSource {
                    chunk: Some(input_chunk(vec![1, 2, 3])),
                })),
                writer,
                Box::new(crate::exec::pipeline::shape::Transform::hydrating(
                    CollectSink {
                        chunks: Arc::clone(&collected),
                    },
                )),
            ],
            None,
            Vec::new(),
            Arc::clone(&runtime_state),
            None,
        );

        let deadline = Instant::now() + Duration::from_secs(5);
        while stats.finish_started.load(Ordering::Acquire) == 0 {
            let state = driver.process(Duration::from_millis(10));
            assert!(
                !matches!(state, DriverState::Finished | DriverState::Failed(_)),
                "writer finish is still gated: {state:?}"
            );
            assert!(Instant::now() < deadline, "writer finish was not entered");
            std::thread::yield_now();
        }
        assert_eq!(
            driver.process(Duration::from_millis(10)),
            DriverState::Blocked(crate::exec::pipeline::operator::BlockedReason::OutputFull)
        );
        assert_eq!(stats.finished.load(Ordering::Acquire), 0);
        finish_gate.add_permits(1);
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            match driver.process(Duration::from_millis(10)) {
                DriverState::Finished => break,
                DriverState::Failed(error) => panic!("driver failed: {error}"),
                _ => {}
            }
            assert!(
                Instant::now() < deadline,
                "driver did not observe writer completion"
            );
            std::thread::yield_now();
        }
        assert_eq!(stats.finish_started.load(Ordering::Acquire), 1);
        assert_eq!(stats.appended_rows.load(Ordering::Acquire), 3);
        let chunks = collected.lock().expect("collected chunks");
        assert_eq!(chunks.len(), 2);
        let columns = TableWriteRelationColumns::try_from_relation(&chunks[0]).expect("columns");
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
        let state = test_runtime_state();
        let factory = writer_factory(&writer_fixture(execution), &state);
        let mut operator = factory.create_operator(1, 0);
        let profiles = OperatorProfiles::new(crate::runtime::profile::RuntimeProfile::new(
            "slow-writer-metrics",
        ));
        operator.set_profiles(profiles.clone());

        install_construction(&mut operator, &state);
        operator.prepare().expect("prepare");
        operator.bind_runtime_state(&state).expect("bind");
        operator.activate(&state).expect("activate");

        complete_native_input(&mut operator, &state, input_chunk(vec![1])).expect("first page");
        assert!(poll_until(
            || append_started.load(Ordering::Acquire),
            Duration::from_secs(5)
        ));
        for value in 2..=4 {
            complete_native_input(&mut operator, &state, input_chunk(vec![value]))
                .expect("page fits the bounded writer queue");
        }
        use crate::exec::pipeline::{shape::StepOperator, step::InputResult};
        let raw = RawInput::for_test(input_chunk(vec![5]));
        let original = raw.original_for_test().columns()[0].clone();
        let (raw, wait) = with_step(&state, |ctx| match operator.try_push(ctx, raw) {
            InputResult::Deferred {
                input,
                next: AdmissionNext::Wait(wait),
            } => (input, wait),
            _ => panic!("full writer must defer the original raw input"),
        });
        assert!(Arc::ptr_eq(
            &original,
            &raw.original_for_test().columns()[0]
        ));
        assert!(!wait.is_ready());
        std::thread::sleep(Duration::from_millis(1));
        gate.notify_one();
        assert!(poll_until(|| wait.is_ready(), Duration::from_secs(5)));
        with_step(&state, |ctx| match operator.try_push(ctx, raw) {
            InputResult::Accepted(result) => {
                result.unwrap();
            }
            _ => panic!("the same raw input resumes after queue capacity is released"),
        });
        with_step(&state, |ctx| {
            let result = operator.drive(ctx).unwrap();
            assert!(matches!(result.next, NextStep::NeedInput));
        });
        operator.finish_for_test(&state).expect("finish");
        let mut boxed: Box<dyn Operator> = Box::new(operator);
        let outputs = wait_for_output(&mut boxed, &state).expect("writer output");
        let prefix = TableWriteRelationColumns::try_from_relation(&outputs[0]).expect("prefix");
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
            append_gate: Arc::clone(&append_gate),
            abort_gate: Arc::clone(&abort_gate),
        });
        let memory = MemTracker::new_root("table-writer-cancel-test");
        let runtime_state = Arc::new(test_runtime_state_with_mem(Some(Arc::clone(&memory))));
        let factory = writer_factory(&writer_fixture(execution), runtime_state.as_ref());
        let mut writer = factory.create(1, 0);
        bind(&mut writer, runtime_state.as_ref());
        let mut driver = PipelineDriver::new(
            0,
            vec![
                Box::new(crate::exec::pipeline::shape::Source::new(OneChunkSource {
                    chunk: Some(input_chunk(vec![1, 2, 3])),
                })),
                writer,
                Box::new(crate::exec::pipeline::shape::Transform::hydrating(
                    CollectSink {
                        chunks: Arc::new(std::sync::Mutex::new(Vec::new())),
                    },
                )),
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
        // Find the actor's actual installed queue owner after PipelineDriver
        // has rebound the operator trackers. Compiled schemas and prepared
        // creators remain live until driver retirement and are not queue data.
        fn collect_writer_queues(root: &Arc<MemTracker>, queues: &mut Vec<Arc<MemTracker>>) {
            if root.label() == "ConnectorWriterQueue" {
                queues.push(root.clone());
            }
            for child in root.children() {
                collect_writer_queues(&child, queues);
            }
        }
        let mut queues = Vec::new();
        collect_writer_queues(&memory, &mut queues);
        assert_eq!(
            queues.len(),
            1,
            "one actual actor queue belongs to this driver"
        );
        let queued_input = queues.pop().unwrap();
        assert!(
            queued_input.current() > 0,
            "actual queue owns the in-flight Arrow input"
        );

        let fragment = Arc::new(FragmentContext::new(
            None,
            Arc::clone(&runtime_state),
            None,
            None,
            None,
            None,
        ));
        let completion = FragmentCompletion::new(1, runtime_state.error_state());
        let cancellation = ExecutionFailure::from(novarocks_functions::KernelFailure::Cancelled);
        let submitted_failure: crate::runtime::fragment::ExecutionFailure = cancellation.clone();
        completion.fail(submitted_failure.clone());
        assert!(
            completion
                .conclusion()
                .unwrap()
                .unwrap_err()
                .same_occurrence(&submitted_failure)
        );
        let task = DriverTask::new(
            driver,
            Arc::clone(&completion),
            fragment,
            Duration::from_millis(10),
        );
        let mut task = task
            .finish_due_to_abort()
            .expect("driver must remain pending while writer abort is in flight");
        assert!(!abort_started.load(Ordering::Acquire));
        assert!(task.has_pending_finish());
        assert!(memory.current() > 0, "in-flight append retains its input");
        assert!(
            queued_input.current() > 0,
            "the queue keeps its input until append exits"
        );
        append_gate.notify_one();
        assert!(poll_until(
            || abort_started.load(Ordering::Acquire),
            Duration::from_secs(5)
        ));

        let (tx, rx) = std::sync::mpsc::channel();
        let waiting = Arc::clone(&completion);
        let waiter = std::thread::spawn(move || tx.send(waiting.wait()).expect("send result"));
        assert!(
            rx.recv_timeout(Duration::from_millis(50)).is_err(),
            "fragment completion must wait for the writer's actual abort exit"
        );
        assert_eq!(
            queued_input.current(),
            0,
            "cancel must release queued Arrow input"
        );
        assert!(
            memory.current() > 0,
            "live driver retains its compiled schema/creator owners while abort is pending"
        );

        abort_gate.notify_one();
        assert!(poll_until(
            || !task.has_pending_finish(),
            Duration::from_secs(5)
        ));
        assert!(
            task.finish_due_to_abort().is_none(),
            "driver must become terminal after the writer actor joins"
        );
        let error = rx
            .recv_timeout(Duration::from_secs(1))
            .expect("fragment completion after writer abort")
            .expect_err("the injected cancellation remains first-wins");
        assert_eq!(error, cancellation);
        waiter.join().expect("completion waiter");
        assert_eq!(memory.current(), 0);
    }

    struct ClosedPartial;
    impl OperatorLifecycle for ClosedPartial {
        fn name(&self) -> &str {
            "ClosedPartial"
        }
    }
    impl InputEncodingPolicy for ClosedPartial {}
    impl TransformKernel for ClosedPartial {
        fn poll_control(&mut self, _: &mut StepContext<'_>) -> ExecutionResult<ControlStep> {
            Ok(ControlStep::Withdrawn)
        }
        fn apply(
            &mut self,
            _: &mut StepContext<'_>,
            _: PreparedInput,
        ) -> ExecutionResult<StepResult> {
            panic!("closed partial never receives data")
        }
    }
    #[test]
    fn required_partial_withdrawal_is_an_invariant_failure_and_aborts_its_actor() {
        use crate::exec::pipeline::shape::StepOperator;
        use crate::exec::pipeline::writer_failure::WriterFailure;
        use crate::runtime::fragment::ExecutionFailureCause;
        let stats = Arc::new(WriteExecutionStats::default());
        let execution = Arc::new(TestWriteExecution::new(stats.clone()));
        let state = test_runtime_state();
        let mut operator = writer_factory(&writer_fixture(execution), &state).create_operator(1, 0);
        operator
            .kernel_for_test_mut()
            .replace_partial_aggregate_for_test(Box::new(Transform::hydrating(ClosedPartial)));

        install_construction(&mut operator, &state);
        operator.prepare().unwrap();
        operator.bind_runtime_state(&state).unwrap();
        operator.activate(&state).unwrap();
        assert!(poll_until(
            || stats.opened.load(Ordering::Acquire) == 1,
            Duration::from_secs(5)
        ));
        let failure = with_step(&state, |ctx| operator.poll_control(ctx)).unwrap_err();
        assert!(
            matches!(failure.cause(),ExecutionFailureCause::Writer(cause) if cause.cause()==&WriterFailure::PartialDemandClosed)
        );
        assert!(state.error().unwrap().same_occurrence(&failure));
        operator.on_driver_failure();
        assert!(poll_until(
            || {
                let mut pending = false;
                operator.visit_exits(&mut |exit| match exit.report_to(&state.error_state()) {
                    crate::exec::pipeline::operator::ExitProgress::Pending(_)
                    | crate::exec::pipeline::operator::ExitProgress::Unresolved(_) => {
                        pending = true
                    }
                    crate::exec::pipeline::operator::ExitProgress::Exited => {}
                });
                !pending
            },
            Duration::from_secs(5)
        ));
        assert_eq!(stats.aborted.load(Ordering::Acquire), 1);
        assert_eq!(stats.finished.load(Ordering::Acquire), 0);
        assert_eq!(stats.appended_rows.load(Ordering::Acquire), 0);
        operator.close().unwrap();
    }

    #[test]
    fn native_writer_holds_append_until_child_accepts_and_finish_until_child_completes() {
        use crate::exec::pipeline::shape::fanout::test_support::{Probe, sink};
        use crate::exec::pipeline::{shape::StepOperator, step::InputResult};
        let stats = Arc::new(WriteExecutionStats::default());
        let execution = Arc::new(TestWriteExecution::new(stats.clone()).with_fragments(0, 0));
        let memory = MemTracker::new_root("native writer child handoff");
        let state = test_runtime_state_with_mem(Some(memory.clone()));
        let (factory, projection_layout) =
            TableWriterOperatorFactory::for_test_fixture_with_projection_layout(
                &writer_fixture(execution),
                &state,
            )
            .unwrap();
        let mut operator = factory.create_operator(1, 0);
        let child = Probe::with_slot(false, projection_layout.slots()[0]);
        child.finish_ready.store(false, Ordering::Release);
        operator
            .kernel_for_test_mut()
            .replace_partial_aggregate_for_test(sink(child.clone()));

        operator.set_mem_tracker(memory.clone());

        install_construction(&mut operator, &state);
        operator.prepare().unwrap();
        operator.bind_runtime_state(&state).unwrap();
        operator.activate(&state).unwrap();
        assert!(poll_until(
            || stats.opened.load(Ordering::Acquire) == 1,
            Duration::from_secs(5)
        ));
        with_step(&state, |ctx| {
            assert!(matches!(
                operator.try_push(ctx, RawInput::for_test(input_chunk(vec![7, 11]))),
                InputResult::Accepted(Ok(_))
            ));
            let result = operator.drive(ctx).unwrap();
            assert!(result.output.is_none());
            let NextStep::Wait(wait) = result.next else {
                panic!("child admission must park the owning parent");
            };
            assert!(!wait.is_ready());
            for _ in 0..2 {
                assert!(matches!(
                    operator.drive(ctx).unwrap().next,
                    NextStep::Wait(_)
                ));
            }
            assert_eq!(child.tries.load(Ordering::Acquire), 1);
            assert_eq!(child.drives.load(Ordering::Acquire), 0);
            assert_eq!(
                stats.appended_rows.load(Ordering::Acquire),
                0,
                "reservation must not publish before the required child accepts"
            );
            assert!(operator.kernel_for_test().pending_append.is_some());
            child.ready.store(true, Ordering::Release);
            child.event.notify_observers();
            assert!(wait.is_ready());
            assert!(matches!(
                operator.drive(ctx).unwrap().next,
                NextStep::Runnable
            ));
            assert!(operator.kernel_for_test().pending_append.is_none());
            assert_eq!(
                child.tries.load(Ordering::Acquire),
                2,
                "shared probe checks the exact same child raw identity"
            );
            assert!(matches!(
                operator.drive(ctx).unwrap().next,
                NextStep::NeedInput
            ));
            assert_eq!(*child.rows.lock().unwrap(), [7, 11]);
            operator.finish(ctx).unwrap();
            let parked = loop {
                let result = operator.drive(ctx).unwrap();
                assert!(result.output.is_none());
                if let NextStep::Wait(wait) = result.next {
                    let partial = operator
                        .kernel_for_test()
                        .partial_aggregate
                        .as_ref()
                        .unwrap();
                    let child_wait = partial
                        .wait()
                        .expect("child finalization must remain blocked");
                    assert!(Arc::ptr_eq(&child_wait.observable, &child.event));
                    assert!(!child_wait.is_ready());
                    assert!(!operator.kernel_for_test().writer_finish_requested);
                    // Queue release can wake the parent while its child remains blocked.
                    if !wait.is_ready() {
                        break wait;
                    }
                    continue;
                }
                assert!(matches!(result.next, NextStep::Runnable));
            };
            // Exercise the writer subscription after parking without releasing the child.
            operator
                .kernel_for_test()
                .writer
                .observable()
                .notify_observers();
            assert!(parked.is_ready());
            let result = operator.drive(ctx).unwrap();
            assert!(result.output.is_none());
            assert!(matches!(result.next, NextStep::Wait(_)));
            let partial = operator
                .kernel_for_test()
                .partial_aggregate
                .as_ref()
                .unwrap();
            assert!(!partial.wait().unwrap().is_ready());
            assert!(!operator.kernel_for_test().writer_finish_requested);
        });
        assert!(poll_until(
            || stats.appended_rows.load(Ordering::Acquire) == 2,
            Duration::from_secs(5)
        ));
        assert_eq!(
            stats.finished.load(Ordering::Acquire),
            0,
            "provider finish cannot overtake child finalization"
        );
        assert!(!operator.kernel_for_test().writer_finish_requested);
        assert!(
            operator.pending_finish().is_none(),
            "mandatory child work must remain dataflow-drivable"
        );
        assert_eq!(child.finishes.load(Ordering::Acquire), 1);
        child.finish_ready.store(true, Ordering::Release);
        child.event.notify_observers();
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut outputs = Vec::new();
        loop {
            let result = with_step(&state, |ctx| operator.drive(ctx)).unwrap();
            if let Some(output) = result.output {
                outputs.push(output);
            }
            match result.next {
                NextStep::Finished => break,
                NextStep::Wait(wait) if !wait.is_ready() => {
                    std::thread::sleep(Duration::from_millis(1))
                }
                _ => {}
            }
            assert!(
                Instant::now() < deadline,
                "native writer completes after the exact child event"
            );
        }
        assert_eq!(outputs.len(), 1);
        let columns = TableWriteRelationColumns::try_from_relation(&outputs[0]).unwrap();
        assert_eq!(columns.kinds.value(0), WriterRowKind::RowCount.to_wire());
        assert_eq!(columns.row_counts.value(0), 2);
        assert_eq!(stats.finished.load(Ordering::Acquire), 1);
        assert_eq!(stats.aborted.load(Ordering::Acquire), 0);
        assert_eq!(child.seals.load(Ordering::Acquire), 1);
        operator.begin_exit().unwrap();
        assert!(poll_until(
            || {
                let mut pending = false;
                operator.visit_exits(&mut |exit| match exit.report_to(&state.error_state()) {
                    crate::exec::pipeline::operator::ExitProgress::Pending(_)
                    | crate::exec::pipeline::operator::ExitProgress::Unresolved(_) => {
                        pending = true
                    }
                    crate::exec::pipeline::operator::ExitProgress::Exited => {}
                });
                !pending
            },
            Duration::from_secs(5)
        ));
        operator.close().unwrap();
        drop(outputs);
        drop(operator);
        assert_eq!(memory.current(), 0);
        drop(child.event.subscribe(Arc::new(|| {})));
    }

    #[test]
    fn terminal_writer_keeps_pending_finish_until_actor_handle_exits() {
        let stats = Arc::new(WriteExecutionStats::default());
        let execution = Arc::new(TestWriteExecution::new(stats));
        let runtime_state = test_runtime_state_with_mem(None);
        let mut writer =
            writer_factory(&writer_fixture(execution), &runtime_state).create_operator(1, 0);

        let gate = Arc::new(tokio::sync::Semaphore::new(0));
        let actor_gate = Arc::clone(&gate);
        let actor = runtime_state
            .sink_io_executor()
            .expect("sink-I/O executor")
            .spawn(async move {
                let permit = actor_gate.acquire().await.expect("actor exit gate");
                permit.forget();
            });
        writer
            .kernel_for_test_mut()
            .writer
            .install_terminal_actor_for_test(actor);
        writer.kernel_for_test_mut().state = TableWriterState::Finished;

        use crate::exec::pipeline::shape::StepOperator;
        with_step(&runtime_state, |ctx| {
            assert!(matches!(
                writer.drive(ctx).unwrap().next,
                NextStep::Finished
            ));
        });
        assert!(
            writer.pending_finish().is_some(),
            "live actor prevents actual stop"
        );
        gate.add_permits(1);
        assert!(poll_until(
            || matches!(
                writer
                    .kernel_for_test_mut()
                    .writer
                    .observe_actor_exit()
                    .progress,
                crate::exec::pipeline::operator::ExitProgress::Exited
            ),
            Duration::from_secs(5)
        ));
    }

    #[test]
    fn a_table_writer_is_never_the_pipeline_sink() {
        let stats = Arc::new(WriteExecutionStats::default());
        let execution = Arc::new(TestWriteExecution::new(stats));
        let state = test_runtime_state();
        let factory = writer_factory(&writer_fixture(execution), &state);
        assert!(!factory.is_sink());
        assert!(!factory.is_source());
    }
}
