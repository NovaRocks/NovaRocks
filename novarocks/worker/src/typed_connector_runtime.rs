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

//! The typed connector scan source and its scan operation.
//!
//! Responsibilities:
//! - Turns one validated `ConnectorTableScanSource` plus one installed typed
//!   provider into an execution `ScanSource`/`ScanOp` pair.
//! - Hands the scan's driver one stream over the task's runtime split queue:
//!   one split becomes one provider page stream, whose pages become `Chunk`s
//!   through the execution-owned page converter (see [`stream`]).
//! - Owns terminal cleanup: terminating the scan stops its successor window,
//!   closes its queue and seals its task source, so the stream's close only
//!   observes the exit of what already runs.
//!
//! Key exported interfaces:
//! - Types: `TypedConnectorScanSource`, `TypedConnectorScanOp`.
//!
//! Current limitations:
//! - The provider receives an unconstrained filter unless a runtime owner
//!   supplies recorded consumer contracts. Compiled expression filtering
//!   remains with its pipeline owner.
//!
//! Provider neutrality: this file holds protocol-validated carriers and trait
//! objects only. It never matches a provider variant and never downcasts, so it
//! compiles with no provider crate in the dependency graph.

use novarocks_execution::runtime::task::connector_failure::{
    ConnectorCallFailure, ConnectorOperation,
};
use novarocks_execution::runtime::task::{ExecutionFailure, ExecutionResult};
use std::collections::BTreeMap;
use std::sync::Arc;

use crate::RuntimeFilterSessionResolver;
use crate::ScanStreamHost;
use crate::TypedConnectorReadDescriptor;
use crate::connector_batch_transform::ConnectorBatchTransform;
use crate::read_attempt::ReceivedReadSplit;
use crate::typed_preparation_flow::StreamPreparationFlow;
use crate::typed_scan_filter::TypedScanLiveDynamicFilterFactory;
use novarocks_execution::connector::{SplitQueue, SplitQueues};
use novarocks_execution::exec::chunk::{Chunk, ChunkSchemaRef};
use novarocks_execution::exec::node::scan::{
    BoundScanRanges, ScanOp, ScanSource, ScanStreamSource,
};
use novarocks_execution::runtime_filter::RuntimeFilterConsumerContract;
use novarocks_spi::connector::ConnectorRequestContext;
use novarocks_spi::connector::read_stack::{
    ConnectorReadDynamicFilter, ConnectorReadPageSourceProvider, ConnectorSession,
};
use novarocks_types::SlotId;

mod stream;

type PageProviderFactory =
    Arc<dyn Fn() -> ExecutionResult<Arc<dyn ConnectorReadPageSourceProvider>> + Send + Sync>;

/// Decode retains only construction work. Admission installs the real tracker
/// before `ScanSource::bind` calls this factory and opens a provider.
enum ReadProvider<P: ?Sized> {
    Ready(Arc<P>),
    Deferred(Arc<dyn Fn() -> ExecutionResult<Arc<P>> + Send + Sync>),
}

impl<P: ?Sized> Clone for ReadProvider<P> {
    fn clone(&self) -> Self {
        match self {
            Self::Ready(provider) => Self::Ready(Arc::clone(provider)),
            Self::Deferred(factory) => Self::Deferred(Arc::clone(factory)),
        }
    }
}

impl<P: ?Sized> ReadProvider<P> {
    fn resolve(&self) -> ExecutionResult<Arc<P>> {
        match self {
            Self::Ready(provider) => Ok(Arc::clone(provider)),
            Self::Deferred(factory) => factory(),
        }
    }
}

/// Everything one typed scan needs, shared by the source, the op, and the
/// stream the op hands out.
struct TypedConnectorScanShared {
    /// Immutable SPI facts decoded at the native edge. It names the relation,
    /// assignments, and initial complete filter without retaining a carrier.
    descriptor: TypedConnectorReadDescriptor,
    provider: ReadProvider<dyn ConnectorReadPageSourceProvider>,
    session: ConnectorSession,
    /// Resolves the attempt's runtime-filter session at the moment it is
    /// needed. Held as a resolver rather than a session because a fragment
    /// does not hold its admission permit while its plan is decoded, and the
    /// lifecycle refuses a session without one.
    runtime_filter: RuntimeFilterSessionResolver,
    live_dynamic_filter_factory: Arc<dyn TypedScanLiveDynamicFilterFactory>,
    #[cfg(novarocks_dev_controls)]
    test_observation: Option<Arc<dyn novarocks_execution::runtime::exchange::TestEventObserver>>,
    /// This fragment's runtime-filter consumer contracts, by the filter id the
    /// scan carrier binds. Empty when the scan consumes no runtime filter.
    runtime_filter_contracts: BTreeMap<u32, RuntimeFilterConsumerContract>,
    /// Deadline and cancellation: checked before every open and on every
    /// driver turn.
    request: ConnectorRequestContext,
    plan_node_id: i32,
    /// Ordered read slot ids. `slot_ids[i]` names page channel `i`. These are
    /// the columns the connector itself produces, which is not necessarily the
    /// node's whole output.
    slot_ids: Vec<SlotId>,
    output_schema: ChunkSchemaRef,
    dynamic_filter: Arc<ConnectorReadDynamicFilter>,
    /// Builds the columns the connector does not read, and the output schema
    /// the result must have.
    ///
    /// Absent when no derived column exists. The frozen output schema is
    /// still bound by the page converter for ordinary columns.
    output_materialization: Option<OutputMaterialization>,
    stream_host: ScanStreamHost,
}

/// How one scan turns the connector's read columns into the node's output.
struct OutputMaterialization {
    transform: Arc<dyn ConnectorBatchTransform>,
}

impl TypedConnectorScanShared {
    /// Turn one read chunk into the node's output chunk.
    ///
    /// Ordinary output was already checked and retagged by its page converter.
    fn materialize_output(
        &self,
        chunk: Chunk,
        factory: &dyn novarocks_execution::exec::pipeline::operator::InputConstructionFactory,
    ) -> ExecutionResult<Chunk> {
        materialize_output(self.output_materialization.as_ref(), chunk, factory)
    }

    /// Fail fast on a cancelled or expired attempt, before any provider call.
    fn check_liveness(&self, operation: ConnectorOperation) -> ExecutionResult<()> {
        use novarocks_spi::connector::ConnectorOperationControl;
        self.request
            .check_active()
            .map_err(|cause| ConnectorCallFailure::new(operation, cause).into())
    }
}

/// Turn one read chunk into the node's output chunk.
///
/// Ordinary output was already checked and retagged by its page converter.
fn materialize_output(
    materialization: Option<&OutputMaterialization>,
    chunk: Chunk,
    factory: &dyn novarocks_execution::exec::pipeline::operator::InputConstructionFactory,
) -> ExecutionResult<Chunk> {
    let Some(materialization) = materialization else {
        return Ok(chunk);
    };
    materialization.transform.transform(chunk, factory)
}

/// Emit one connector-reader evidence marker, behind the shared test gate.
///
/// It prints scheduling identity and nothing else: a marker must never carry a
/// credential, a key metadata blob, or any part of a data value.
#[cfg(novarocks_dev_controls)]
fn emit_page_source_marker(
    observation: Option<&dyn novarocks_execution::runtime::exchange::TestEventObserver>,
    marker: &str,
    plan_node_id: i32,
    sequence_id: Option<u64>,
) {
    if let Some(observer) = observation {
        let event = match sequence_id {
            Some(sequence_id) => {
                format!("{marker} plan_node={plan_node_id} sequence={sequence_id}")
            }
            None => format!("{marker} plan_node={plan_node_id}"),
        };
        observer.observe(&event);
    }
}

/// A physical source for one typed connector scan node of one task attempt.
///
/// The source carries no split: splits arrive at runtime through the task's
/// per-plan-node queue, so binding it never waits for enumeration.
pub struct TypedConnectorScanSource {
    shared: Arc<TypedConnectorScanShared>,
    queues: Arc<SplitQueues<ReceivedReadSplit>>,
}

impl TypedConnectorScanSource {
    #[expect(
        clippy::too_many_arguments,
        reason = "The public BE scan constructors keep frozen provider, queue, output, filter, and stream capabilities explicit; a new carrier would duplicate the cross-crate input schema."
    )]
    pub fn new(
        descriptor: TypedConnectorReadDescriptor,
        provider: Arc<dyn ConnectorReadPageSourceProvider>,
        session: ConnectorSession,
        request: ConnectorRequestContext,
        queues: Arc<SplitQueues<ReceivedReadSplit>>,
        plan_node_id: i32,
        slot_ids: Vec<SlotId>,
        output_schema: ChunkSchemaRef,
        runtime_filter: RuntimeFilterSessionResolver,
        live_dynamic_filter_factory: Arc<dyn TypedScanLiveDynamicFilterFactory>,
        #[cfg(novarocks_dev_controls)] test_observation: Option<
            Arc<dyn novarocks_execution::runtime::exchange::TestEventObserver>,
        >,
        stream_host: ScanStreamHost,
    ) -> Self {
        Self::from_provider(
            descriptor,
            ReadProvider::Ready(provider),
            session,
            request,
            queues,
            plan_node_id,
            slot_ids,
            output_schema,
            runtime_filter,
            live_dynamic_filter_factory,
            #[cfg(novarocks_dev_controls)]
            test_observation,
            stream_host,
        )
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "The private shared constructor preserves exactly the independent frozen facts accepted by both ready and deferred public constructors; a second aggregate carrier adds no distinct contract."
    )]
    fn from_provider(
        descriptor: TypedConnectorReadDescriptor,
        provider: ReadProvider<dyn ConnectorReadPageSourceProvider>,
        session: ConnectorSession,
        request: ConnectorRequestContext,
        queues: Arc<SplitQueues<ReceivedReadSplit>>,
        plan_node_id: i32,
        slot_ids: Vec<SlotId>,
        output_schema: ChunkSchemaRef,
        runtime_filter: RuntimeFilterSessionResolver,
        live_dynamic_filter_factory: Arc<dyn TypedScanLiveDynamicFilterFactory>,
        #[cfg(novarocks_dev_controls)] test_observation: Option<
            Arc<dyn novarocks_execution::runtime::exchange::TestEventObserver>,
        >,
        stream_host: ScanStreamHost,
    ) -> Self {
        Self {
            shared: Arc::new(TypedConnectorScanShared {
                dynamic_filter: descriptor.complete_dynamic_filter(),
                descriptor,
                provider,
                session,
                runtime_filter,
                live_dynamic_filter_factory,
                #[cfg(novarocks_dev_controls)]
                test_observation,
                runtime_filter_contracts: BTreeMap::new(),
                request,
                plan_node_id,
                slot_ids,
                output_schema,
                output_materialization: None,
                stream_host,
            }),
            queues,
        }
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "The public BE scan constructors keep frozen provider, queue, output, filter, and stream capabilities explicit; a new carrier would duplicate the cross-crate input schema."
    )]
    pub fn new_deferred(
        descriptor: TypedConnectorReadDescriptor,
        provider_factory: PageProviderFactory,
        session: ConnectorSession,
        request: ConnectorRequestContext,
        queues: Arc<SplitQueues<ReceivedReadSplit>>,
        plan_node_id: i32,
        slot_ids: Vec<SlotId>,
        output_schema: ChunkSchemaRef,
        runtime_filter: RuntimeFilterSessionResolver,
        live_dynamic_filter_factory: Arc<dyn TypedScanLiveDynamicFilterFactory>,
        #[cfg(novarocks_dev_controls)] test_observation: Option<
            Arc<dyn novarocks_execution::runtime::exchange::TestEventObserver>,
        >,
        stream_host: ScanStreamHost,
    ) -> Self {
        Self::from_provider(
            descriptor,
            ReadProvider::Deferred(provider_factory),
            session,
            request,
            queues,
            plan_node_id,
            slot_ids,
            output_schema,
            runtime_filter,
            live_dynamic_filter_factory,
            #[cfg(novarocks_dev_controls)]
            test_observation,
            stream_host,
        )
    }

    /// Build the node's output columns from what the connector read.
    ///
    /// A scan whose output carries derived columns — VARIANT path columns are
    /// the case that exists — reads only the physical ones and materializes
    /// the rest here, so exactly one place produces the node's output schema.
    pub fn with_output_materialization(
        mut self,
        transform: Arc<dyn ConnectorBatchTransform>,
    ) -> Self {
        let shared = Arc::get_mut(&mut self.shared)
            .expect("typed connector scan source is not shared before it is bound");
        shared.output_materialization = Some(OutputMaterialization { transform });
        self
    }

    /// The single seam for a live, backend-driven dynamic filter.
    ///
    /// Runtime-filter production and its wait policy belong to the backend
    /// runtime-filter owner, not to this scan: when that owner can hand over a
    /// filter, it is substituted here and nothing else in this file changes. No
    /// other code path may synthesize a filter.
    pub fn with_backend_dynamic_filter(
        mut self,
        dynamic_filter: Arc<ConnectorReadDynamicFilter>,
    ) -> Self {
        let shared = Arc::get_mut(&mut self.shared)
            .expect("typed connector scan source is not shared before it is bound");
        shared.dynamic_filter = dynamic_filter;
        self
    }

    /// Subscribe to the live filter, now that the attempt will hand out its
    /// runtime-filter session.
    ///
    /// Absent session or absent contract both mean this scan receives no
    /// feedback, and it keeps the truthful unconstrained filter rather than
    /// claiming one that could never narrow.
    fn live_dynamic_filter(&self) -> ExecutionResult<Option<Arc<ConnectorReadDynamicFilter>>> {
        if self.shared.runtime_filter_contracts.is_empty() {
            return Ok(None);
        }
        let session = (self.shared.runtime_filter)().map_err(ExecutionFailure::from_domain)?;
        self.shared
            .live_dynamic_filter_factory
            .build(session.as_ref(), &self.shared.runtime_filter_contracts)
            .map(Some)
            .map_err(Into::into)
    }

    fn with_substituted_filter(&self, dynamic_filter: Arc<ConnectorReadDynamicFilter>) -> Self {
        Self {
            shared: Arc::new(TypedConnectorScanShared {
                descriptor: self.shared.descriptor.clone(),
                provider: self.shared.provider.clone(),
                session: self.shared.session.clone(),
                runtime_filter: Arc::clone(&self.shared.runtime_filter),
                live_dynamic_filter_factory: Arc::clone(&self.shared.live_dynamic_filter_factory),
                #[cfg(novarocks_dev_controls)]
                test_observation: self.shared.test_observation.clone(),
                runtime_filter_contracts: self.shared.runtime_filter_contracts.clone(),
                request: self.shared.request.clone(),
                plan_node_id: self.shared.plan_node_id,
                slot_ids: self.shared.slot_ids.clone(),
                output_schema: Arc::clone(&self.shared.output_schema),
                dynamic_filter,
                stream_host: self.shared.stream_host.clone(),
                output_materialization: self.shared.output_materialization.as_ref().map(
                    |materialization| OutputMaterialization {
                        transform: Arc::clone(&materialization.transform),
                    },
                ),
            }),
            queues: Arc::clone(&self.queues),
        }
    }
}

impl ScanSource for TypedConnectorScanSource {
    fn bind(&self, ranges: BoundScanRanges) -> ExecutionResult<Arc<dyn ScanOp>> {
        match ranges {
            // Typed scans carry no frozen range: their work arrives as splits.
            BoundScanRanges::None => {}
            BoundScanRanges::SchemaSelection { .. } => {
                return Err(TypedScanBindingFailure::WrongRangeKind.into());
            }
        }
        if self.shared.output_materialization.is_none()
            && self.shared.slot_ids.as_slice() != self.shared.output_schema.slot_ids()
        {
            return Err(TypedScanBindingFailure::OutputSlots.into());
        }
        self.shared
            .check_liveness(ConnectorOperation::PrepareReader)?;
        // Created empty on first use, and born closed when the attempt is
        // already terminal, so a late bind observes termination instead of
        // parking on a queue nobody will ever serve.
        let queue = self.queues.queue(self.shared.plan_node_id);
        // Subscribing here rather than at decode: this is the first moment the
        // attempt will hand out its runtime-filter session.
        let mut shared = match self.live_dynamic_filter()? {
            Some(dynamic_filter) => self.with_substituted_filter(dynamic_filter).shared,
            None => {
                self.with_substituted_filter(Arc::clone(&self.shared.dynamic_filter))
                    .shared
            }
        };
        Arc::get_mut(&mut shared)
            .expect("bound scan source has one owner")
            .provider = ReadProvider::Ready(self.shared.provider.resolve()?);
        let flow = StreamPreparationFlow::new(
            shared.stream_host.preparation(),
            Arc::clone(shared.stream_host.timer()),
        );
        let stream = stream::TypedScanStreamSource::new(
            Arc::clone(&shared),
            Arc::clone(&queue),
            Arc::clone(&flow),
        );
        Ok(Arc::new(TypedConnectorScanOp {
            flow,
            stream,
            shared,
            queue,
        }))
    }

    fn profile_name(&self) -> Option<String> {
        Some("TypedConnectorScan".to_string())
    }
}

/// One bound typed connector scan.
pub struct TypedConnectorScanOp {
    shared: Arc<TypedConnectorScanShared>,
    queue: Arc<SplitQueue<ReceivedReadSplit>>,
    flow: Arc<StreamPreparationFlow>,
    stream: Arc<stream::TypedScanStreamSource>,
}

impl ScanOp for TypedConnectorScanOp {
    fn stream_source(&self) -> Arc<dyn ScanStreamSource> {
        Arc::clone(&self.stream) as Arc<dyn ScanStreamSource>
    }

    fn on_output_backpressure(&self, paused: bool) {
        self.flow.on_backpressure(paused);
    }

    fn on_nonempty_chunk_consumed(&self) {
        self.flow.on_nonempty_chunk_consumed();
    }

    /// Stops every operation of the scan without waiting for it: the
    /// scan's stream observes their exit when it is closed.
    fn terminate(&self) -> ExecutionResult<()> {
        self.flow.stop();
        // Idempotent, drops anything still queued, and wakes a stream parked
        // on the queue through its observer.
        self.queue.close();
        self.stream.wake();
        // Sealing the task source stops the reads of the open split, whose
        // operations are admitted to children of it.
        if let Some(operations) = self.shared.request.source_operations() {
            operations.seal();
        }
        Ok(())
    }

    fn profile_name(&self) -> Option<String> {
        Some("TypedConnectorScan".to_string())
    }
}

#[derive(Debug)]
enum TypedScanBindingFailure {
    WrongRangeKind,
    OutputSlots,
    ReclaimedStream,
    MissingExecutionSource,
}
impl std::fmt::Display for TypedScanBindingFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::WrongRangeKind => "typed scan source requires an empty range binding",
            Self::OutputSlots => "typed scan read slots do not match the frozen output slots",
            Self::ReclaimedStream => "typed scan stream is already claimed",
            Self::MissingExecutionSource => {
                "typed runtime scan requires its exact Task execution source"
            }
        })
    }
}
impl std::error::Error for TypedScanBindingFailure {}
impl novarocks_execution_contract::failure::FailureProjection for TypedScanBindingFailure {
    fn failure_fact(&self) -> novarocks_execution_contract::failure::FailureFact {
        use crate::failure_reason::WorkerFailureReason as R;
        use novarocks_execution_contract::failure::{FailureClass, FailureFact};
        let reason = match self {
            Self::WrongRangeKind => R::ScanWrongRangeKind,
            Self::OutputSlots => R::ScanOutputSlots,
            Self::ReclaimedStream => R::ScanStreamReclaimed,
            Self::MissingExecutionSource => R::ScanMissingExecutionSource,
        };
        FailureFact::new(FailureClass::Invariant, reason.reason())
    }
}
impl From<TypedScanBindingFailure> for ExecutionFailure {
    fn from(cause: TypedScanBindingFailure) -> Self {
        Self::from_domain(cause)
    }
}
