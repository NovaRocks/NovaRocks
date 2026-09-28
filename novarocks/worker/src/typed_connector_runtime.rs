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
//! - The dynamic filter handed to the provider is the truthful unconstrained
//!   one until this fragment's runtime-filter consumer contracts are decoded.
//!   `ScanSource::with_runtime_filter_contracts` is the one seam a live,
//!   backend-driven filter is substituted through.
//!
//! Provider neutrality: this file holds protocol-validated carriers and trait
//! objects only. It never matches a provider variant and never downcasts, so it
//! compiles with no provider crate in the dependency graph.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Instant;

use crate::RuntimeFilterSessionResolver;
use crate::ScanStreamHost;
use crate::TypedConnectorReadDescriptor;
use crate::connector_batch_transform::ConnectorBatchTransform;
use crate::read_attempt::ReceivedReadSplit;
use crate::typed_preparation_flow::StreamPreparationFlow;
use crate::typed_scan_filter::TypedScanLiveDynamicFilterFactory;
use novarocks_execution::connector::{SplitQueue, TaskAttemptSplitQueues};
use novarocks_execution::exec::chunk::{Chunk, ChunkSchemaRef};
use novarocks_execution::exec::node::runtime_filter::RuntimeFilterConsumerBinding;
use novarocks_execution::exec::node::scan::{
    BoundScanRanges, ScanOp, ScanSource, ScanStreamSource,
};
use novarocks_execution::runtime_filter::RuntimeFilterConsumerContract;
use novarocks_spi::connector::ConnectorRequestContext;
use novarocks_spi::connector::read_stack::{
    ConnectorReadDynamicFilter, ConnectorReadPageSourceProvider, ConnectorReadSystemTableProvider,
    ConnectorSession,
};
use novarocks_types::SlotId;

mod stream;

type PageProviderFactory =
    Arc<dyn Fn() -> Result<Arc<dyn ConnectorReadPageSourceProvider>, String> + Send + Sync>;
type SystemProviderFactory =
    Arc<dyn Fn() -> Result<Arc<dyn ConnectorReadSystemTableProvider>, String> + Send + Sync>;

/// Decode retains only construction work. Admission installs the real tracker
/// before `ScanSource::bind` calls this factory and opens a provider.
enum ReadProvider<P: ?Sized> {
    Ready(Arc<P>),
    Deferred(Arc<dyn Fn() -> Result<Arc<P>, String> + Send + Sync>),
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
    fn resolve(&self) -> Result<Arc<P>, String> {
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
    emit_reader_markers: bool,
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
    dynamic_filter: Arc<ConnectorReadDynamicFilter>,
    /// Builds the columns the connector does not read, and the output schema
    /// the result must have.
    ///
    /// Absent when the node's output is exactly what the connector reads,
    /// which is every scan that projects no derived column.
    output_materialization: Option<OutputMaterialization>,
    stream_host: ScanStreamHost,
}

/// How one scan turns the connector's read columns into the node's output.
struct OutputMaterialization {
    transform: Arc<dyn ConnectorBatchTransform>,
    chunk_schema: ChunkSchemaRef,
}

impl TypedConnectorScanShared {
    /// Turn one read chunk into the node's output chunk.
    ///
    /// Without a materialization the connector already read the whole output,
    /// so the chunk passes through untouched and no schema is rebuilt.
    fn materialize_output(&self, chunk: Chunk) -> Result<Chunk, String> {
        materialize_output(self.output_materialization.as_ref(), chunk)
    }

    /// Fail fast on a cancelled or expired attempt, before any provider call.
    fn check_liveness(&self, action: &str) -> Result<(), String> {
        if self.request.is_cancelled() {
            return Err(format!("typed connector scan {action} was cancelled"));
        }
        if Instant::now() >= self.request.deadline() {
            return Err(format!("typed connector scan {action} deadline elapsed"));
        }
        Ok(())
    }
}

/// Turn one read chunk into the node's output chunk.
///
/// Without a materialization the connector already read the whole output, so
/// the chunk passes through untouched and no schema is rebuilt.
fn materialize_output(
    materialization: Option<&OutputMaterialization>,
    chunk: Chunk,
) -> Result<Chunk, String> {
    let Some(materialization) = materialization else {
        return Ok(chunk);
    };
    let batch = materialization.transform.transform(chunk.batch)?;
    Chunk::try_new_with_chunk_schema(batch, Arc::clone(&materialization.chunk_schema))
        .map_err(|error| error.to_string())
}

/// Emit one connector-reader evidence marker, behind the shared test gate.
///
/// It prints scheduling identity and nothing else: a marker must never carry a
/// credential, a key metadata blob, or any part of a data value.
fn emit_page_source_marker(
    enabled: bool,
    marker: &str,
    plan_node_id: i32,
    sequence_id: Option<u64>,
) {
    if !enabled {
        return;
    }
    match sequence_id {
        Some(sequence_id) => {
            println!("{marker} plan_node={plan_node_id} sequence={sequence_id}");
        }
        None => println!("{marker} plan_node={plan_node_id}"),
    }
    let _ = std::io::Write::flush(&mut std::io::stdout());
}

/// A physical source for one typed connector scan node of one task attempt.
///
/// The source carries no split: splits arrive at runtime through the task's
/// per-plan-node queue, so binding it never waits for enumeration.
pub struct TypedConnectorScanSource {
    shared: Arc<TypedConnectorScanShared>,
    queues: Arc<TaskAttemptSplitQueues<ReceivedReadSplit>>,
}

impl TypedConnectorScanSource {
    pub fn new(
        descriptor: TypedConnectorReadDescriptor,
        provider: Arc<dyn ConnectorReadPageSourceProvider>,
        session: ConnectorSession,
        request: ConnectorRequestContext,
        queues: Arc<TaskAttemptSplitQueues<ReceivedReadSplit>>,
        plan_node_id: i32,
        slot_ids: Vec<SlotId>,
        runtime_filter: RuntimeFilterSessionResolver,
        live_dynamic_filter_factory: Arc<dyn TypedScanLiveDynamicFilterFactory>,
        emit_reader_markers: bool,
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
            runtime_filter,
            live_dynamic_filter_factory,
            emit_reader_markers,
            stream_host,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn from_provider(
        descriptor: TypedConnectorReadDescriptor,
        provider: ReadProvider<dyn ConnectorReadPageSourceProvider>,
        session: ConnectorSession,
        request: ConnectorRequestContext,
        queues: Arc<TaskAttemptSplitQueues<ReceivedReadSplit>>,
        plan_node_id: i32,
        slot_ids: Vec<SlotId>,
        runtime_filter: RuntimeFilterSessionResolver,
        live_dynamic_filter_factory: Arc<dyn TypedScanLiveDynamicFilterFactory>,
        emit_reader_markers: bool,
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
                emit_reader_markers,
                runtime_filter_contracts: BTreeMap::new(),
                request,
                plan_node_id,
                slot_ids,
                output_materialization: None,
                stream_host,
            }),
            queues,
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new_deferred(
        descriptor: TypedConnectorReadDescriptor,
        provider_factory: PageProviderFactory,
        session: ConnectorSession,
        request: ConnectorRequestContext,
        queues: Arc<TaskAttemptSplitQueues<ReceivedReadSplit>>,
        plan_node_id: i32,
        slot_ids: Vec<SlotId>,
        runtime_filter: RuntimeFilterSessionResolver,
        live_dynamic_filter_factory: Arc<dyn TypedScanLiveDynamicFilterFactory>,
        emit_reader_markers: bool,
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
            runtime_filter,
            live_dynamic_filter_factory,
            emit_reader_markers,
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
        chunk_schema: ChunkSchemaRef,
    ) -> Self {
        let shared = Arc::get_mut(&mut self.shared)
            .expect("typed connector scan source is not shared before it is bound");
        shared.output_materialization = Some(OutputMaterialization {
            transform,
            chunk_schema,
        });
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

    /// Rebuild this source around one live dynamic filter.
    ///
    /// Every field but the filter is carried over: the scan carrier, the
    /// installed provider, the session, and the request all belong to this
    /// fragment instance and are unaffected by which filter the page streams
    /// consult.
    /// Carry this fragment's consumer contracts without subscribing yet.
    fn with_recorded_contracts(
        &self,
        contracts: BTreeMap<u32, RuntimeFilterConsumerContract>,
    ) -> Self {
        let mut rebuilt = self.with_substituted_filter(Arc::clone(&self.shared.dynamic_filter));
        Arc::get_mut(&mut rebuilt.shared)
            .expect("a freshly rebuilt typed scan source is not shared")
            .runtime_filter_contracts = contracts;
        rebuilt
    }

    /// Subscribe to the live filter, now that the attempt will hand out its
    /// runtime-filter session.
    ///
    /// Absent session or absent contract both mean this scan receives no
    /// feedback, and it keeps the truthful unconstrained filter rather than
    /// claiming one that could never narrow.
    fn live_dynamic_filter(&self) -> Result<Option<Arc<ConnectorReadDynamicFilter>>, String> {
        if self.shared.runtime_filter_contracts.is_empty() {
            return Ok(None);
        }
        let session = (self.shared.runtime_filter)()?;
        self.shared
            .live_dynamic_filter_factory
            .build(session.as_ref(), &self.shared.runtime_filter_contracts)
            .map(Some)
            .map_err(|error| format!("create typed scan live runtime filter: {error}"))
    }

    fn with_substituted_filter(&self, dynamic_filter: Arc<ConnectorReadDynamicFilter>) -> Self {
        Self {
            shared: Arc::new(TypedConnectorScanShared {
                descriptor: self.shared.descriptor.clone(),
                provider: self.shared.provider.clone(),
                session: self.shared.session.clone(),
                runtime_filter: Arc::clone(&self.shared.runtime_filter),
                live_dynamic_filter_factory: Arc::clone(&self.shared.live_dynamic_filter_factory),
                emit_reader_markers: self.shared.emit_reader_markers,
                runtime_filter_contracts: self.shared.runtime_filter_contracts.clone(),
                request: self.shared.request.clone(),
                plan_node_id: self.shared.plan_node_id,
                slot_ids: self.shared.slot_ids.clone(),
                dynamic_filter,
                stream_host: self.shared.stream_host.clone(),
                output_materialization: self.shared.output_materialization.as_ref().map(
                    |materialization| OutputMaterialization {
                        transform: Arc::clone(&materialization.transform),
                        chunk_schema: Arc::clone(&materialization.chunk_schema),
                    },
                ),
            }),
            queues: Arc::clone(&self.queues),
        }
    }
}

impl ScanSource for TypedConnectorScanSource {
    fn bind(&self, ranges: BoundScanRanges) -> Result<Arc<dyn ScanOp>, String> {
        match ranges {
            // Typed scans carry no frozen range: their work arrives as splits.
            BoundScanRanges::None => {}
            BoundScanRanges::SchemaSelection { .. } => {
                return Err(
                    "typed connector scan source requires an empty range binding".to_string(),
                );
            }
        }
        self.shared.check_liveness("provider open")?;
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

    /// Subscribe to the live filter this fragment's consumer contracts describe.
    ///
    /// `DynamicFilterBinding.filter_id` on the scan carrier is the runtime
    /// filter's binding id, so a contract is matched to a binding by that id
    /// alone. With no session or no contract this scan keeps the truthful
    /// unconstrained filter it was built with rather than claiming feedback it
    /// never receives.
    fn with_runtime_filter_contracts(
        &self,
        contracts: &[RuntimeFilterConsumerBinding],
    ) -> Result<Option<Arc<dyn ScanSource>>, String> {
        let by_filter_id: BTreeMap<u32, RuntimeFilterConsumerContract> = contracts
            .iter()
            .map(|binding| {
                (
                    binding.contract.binding_id().get(),
                    binding.contract.clone(),
                )
            })
            .collect();
        if by_filter_id.is_empty() {
            return Ok(None);
        }
        // Recorded, not subscribed. Subscribing needs the attempt's
        // runtime-filter session, which the lifecycle will not hand out while
        // this fragment is still being decoded, so the subscription happens
        // when the scan binds.
        Ok(Some(Arc::new(self.with_recorded_contracts(by_filter_id))))
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
    fn terminate(&self) -> Result<(), String> {
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

/// A system relation whose rows come from one immutable metadata file.
///
/// It has no split and never touches the task-update queue: the coordinator
/// resolved it to exactly one backend, and synthesizing a split would invent
/// scheduling identity for work that has none. A scan bound to this source
/// therefore reads its one stream and then finishes, instead of parking on a
/// queue nobody will ever serve.
///
/// Reading it on more than one instance would duplicate every row, so the
/// coordinator is the only thing that keeps this to a single task; this source
/// does not and cannot check that.
pub struct TypedConnectorSystemTableScanSource {
    shared: Arc<TypedSystemTableScanShared>,
}

struct TypedSystemTableScanShared {
    descriptor: TypedConnectorReadDescriptor,
    provider: ReadProvider<dyn ConnectorReadSystemTableProvider>,
    session: ConnectorSession,
    request: ConnectorRequestContext,
    plan_node_id: i32,
    emit_reader_markers: bool,
    /// Ordered read slot ids. `slot_ids[i]` names page channel `i`.
    slot_ids: Vec<SlotId>,
    /// Builds the columns the connector does not read, exactly as an ordinary
    /// typed scan does. A system relation is not exempt: its output can carry
    /// derived columns too, and refusing them here rather than materializing
    /// them would be a second policy for the same fact.
    output_materialization: Option<OutputMaterialization>,
    /// Entered whenever the scan's stream is polled or closed.
    stream_runtime: tokio::runtime::Handle,
}

impl TypedSystemTableScanShared {
    fn materialize_output(&self, chunk: Chunk) -> Result<Chunk, String> {
        materialize_output(self.output_materialization.as_ref(), chunk)
    }

    /// Fail fast on a cancelled or expired attempt, before any provider call.
    fn check_liveness(&self, action: &str) -> Result<(), String> {
        if self.request.is_cancelled() {
            return Err(format!("typed system relation scan {action} was cancelled"));
        }
        if Instant::now() >= self.request.deadline() {
            return Err(format!(
                "typed system relation scan {action} deadline elapsed"
            ));
        }
        Ok(())
    }
}

impl TypedConnectorSystemTableScanSource {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        descriptor: TypedConnectorReadDescriptor,
        provider: Arc<dyn ConnectorReadSystemTableProvider>,
        session: ConnectorSession,
        request: ConnectorRequestContext,
        plan_node_id: i32,
        slot_ids: Vec<SlotId>,
        emit_reader_markers: bool,
        stream_runtime: tokio::runtime::Handle,
    ) -> Self {
        Self::from_provider(
            descriptor,
            ReadProvider::Ready(provider),
            session,
            request,
            plan_node_id,
            slot_ids,
            emit_reader_markers,
            stream_runtime,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn from_provider(
        descriptor: TypedConnectorReadDescriptor,
        provider: ReadProvider<dyn ConnectorReadSystemTableProvider>,
        session: ConnectorSession,
        request: ConnectorRequestContext,
        plan_node_id: i32,
        slot_ids: Vec<SlotId>,
        emit_reader_markers: bool,
        stream_runtime: tokio::runtime::Handle,
    ) -> Self {
        Self {
            shared: Arc::new(TypedSystemTableScanShared {
                descriptor,
                provider,
                session,
                request,
                plan_node_id,
                emit_reader_markers,
                slot_ids,
                output_materialization: None,
                stream_runtime,
            }),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new_deferred(
        descriptor: TypedConnectorReadDescriptor,
        provider_factory: SystemProviderFactory,
        session: ConnectorSession,
        request: ConnectorRequestContext,
        plan_node_id: i32,
        slot_ids: Vec<SlotId>,
        emit_reader_markers: bool,
        stream_runtime: tokio::runtime::Handle,
    ) -> Self {
        Self::from_provider(
            descriptor,
            ReadProvider::Deferred(provider_factory),
            session,
            request,
            plan_node_id,
            slot_ids,
            emit_reader_markers,
            stream_runtime,
        )
    }

    /// Build the node's output columns from what the connector read.
    ///
    /// The same seam an ordinary typed scan has, for the same reason: exactly
    /// one place produces the node's output schema.
    pub fn with_output_materialization(
        mut self,
        transform: Arc<dyn ConnectorBatchTransform>,
        chunk_schema: ChunkSchemaRef,
    ) -> Self {
        let shared = Arc::get_mut(&mut self.shared)
            .expect("typed system relation scan source is not shared before it is bound");
        shared.output_materialization = Some(OutputMaterialization {
            transform,
            chunk_schema,
        });
        self
    }
}

impl ScanSource for TypedConnectorSystemTableScanSource {
    fn bind(&self, ranges: BoundScanRanges) -> Result<Arc<dyn ScanOp>, String> {
        match ranges {
            BoundScanRanges::None => {}
            BoundScanRanges::SchemaSelection { .. } => {
                return Err(
                    "typed system relation scan source requires an empty range binding".to_string(),
                );
            }
        }
        self.shared.check_liveness("provider open")?;
        let mut shared = Arc::new(TypedSystemTableScanShared {
            descriptor: self.shared.descriptor.clone(),
            provider: self.shared.provider.clone(),
            session: self.shared.session.clone(),
            request: self.shared.request.clone(),
            plan_node_id: self.shared.plan_node_id,
            emit_reader_markers: self.shared.emit_reader_markers,
            slot_ids: self.shared.slot_ids.clone(),
            output_materialization: self.shared.output_materialization.as_ref().map(|value| {
                OutputMaterialization {
                    transform: Arc::clone(&value.transform),
                    chunk_schema: Arc::clone(&value.chunk_schema),
                }
            }),
            stream_runtime: self.shared.stream_runtime.clone(),
        });
        Arc::get_mut(&mut shared)
            .expect("bound system scan source has one owner")
            .provider = ReadProvider::Ready(self.shared.provider.resolve()?);
        Ok(Arc::new(TypedConnectorSystemTableScanOp {
            stream: stream::TypedSystemTableStreamSource::new(Arc::clone(&shared)),
            shared,
        }))
    }

    fn profile_name(&self) -> Option<String> {
        Some("TypedConnectorSystemTableScan".to_string())
    }
}

/// One bound system relation scan.
pub struct TypedConnectorSystemTableScanOp {
    shared: Arc<TypedSystemTableScanShared>,
    stream: Arc<stream::TypedSystemTableStreamSource>,
}

impl ScanOp for TypedConnectorSystemTableScanOp {
    fn stream_source(&self) -> Arc<dyn ScanStreamSource> {
        Arc::clone(&self.stream) as Arc<dyn ScanStreamSource>
    }

    /// Seals the task source, which stops the relation's reads; the stream
    /// observes their exit when it is closed.
    fn terminate(&self) -> Result<(), String> {
        if let Some(operations) = self.shared.request.source_operations() {
            operations.seal();
        }
        Ok(())
    }
}
