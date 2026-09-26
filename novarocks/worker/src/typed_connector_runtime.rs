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
//! - Drives the runtime split stream: one split becomes one page source, whose
//!   pages become `Chunk`s through the execution-owned page adapter.
//! - Owns terminal cleanup for the page sources it opened, mirroring the
//!   the same explicit reader-group discipline as the retired opaque carrier.
//!
//! Key exported interfaces:
//! - Types: `TypedConnectorScanSource`, `TypedConnectorScanOp`.
//!
//! Current limitations:
//! - The scan produces no morsel of its own yet. `build_morsels` is empty with
//!   `has_more` still true, which is exactly "this scan may still receive
//!   work"; the queue-driven morsel that schedules that work is a later task.
//! - The dynamic filter handed to the provider is the truthful unconstrained
//!   one until this fragment's runtime-filter consumer contracts are decoded.
//!   `ScanSource::with_runtime_filter_contracts` is the one seam a live,
//!   backend-driven filter is substituted through.
//!
//! Provider neutrality: this file holds protocol-validated carriers and trait
//! objects only. It never matches a provider variant and never downcasts, so it
//! compiles with no provider crate in the dependency graph.

use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use crate::RuntimeFilterSessionResolver;
use crate::ScanPreparationConfig;
use crate::ScanPreparationTimer;
use crate::TypedConnectorReadDescriptor;
use crate::connector_batch_transform::ConnectorBatchTransform;
use crate::read_attempt::ReceivedReadSplit;
use crate::typed_page_source::{
    RegisteredPageSource, TypedConnectorReaderMarker, TypedPageSourceGroup,
};
use crate::typed_preparation_flow::StreamPreparationFlow;
use crate::typed_scan_filter::TypedScanLiveDynamicFilterFactory;
use novarocks_execution::connector::{
    ConnectorPageAdapter, PageConversion, ScheduledSplitFacts, SplitPoll, SplitQueue,
    TaskAttemptSplitQueues,
};
use novarocks_execution::exec::chunk::{Chunk, ChunkSchemaRef};
use novarocks_execution::exec::node::runtime_filter::RuntimeFilterConsumerBinding;
use novarocks_execution::exec::node::scan::{
    BoundScanRanges, IncrementalScanRange, RuntimeFilterContext, ScanMorsel, ScanMorsels, ScanOp,
    ScanSource,
};
use novarocks_execution::exec::node::{BoxedExecIter, ExecResult};
use novarocks_execution::runtime::profile::{ProfileUnit, RuntimeProfile};
use novarocks_execution::runtime_filter::RuntimeFilterConsumerContract;
use novarocks_spi::connector::ConnectorError;
use novarocks_spi::connector::ConnectorRequestContext;
use novarocks_spi::connector::read_stack::{
    ConnectorPageSource, ConnectorPreparationControl, ConnectorPreparationProgress,
    ConnectorPreparationStart, ConnectorPreparedPageSource, ConnectorReadDynamicFilter,
    ConnectorReadPageSourceProvider, ConnectorReadSystemTableProvider, ConnectorSession,
    PageSourceMetrics, SourcePage,
};
use novarocks_types::SlotId;

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

/// How long a driver parks on an empty, non-terminal split queue before it
/// re-checks cancellation, the deadline, and the terminal latch.
///
/// A wake always arrives through the queue's observable; this bound exists so a
/// cancelled or expired attempt is noticed even if a wake is lost, never as the
/// primary way progress is made.
const SPLIT_WAIT_POLL_INTERVAL: Duration = Duration::from_millis(20);

/// How long a driver waits on a page source that reports itself blocked.
///
/// The page contract has no wake-up, so a blocked source can only be polled
/// again. Sleeping briefly keeps an idle turn from becoming a spin without
/// turning "nothing right now" into end of stream.
const BLOCKED_PAGE_SOURCE_BACKOFF: Duration = Duration::from_millis(1);

/// Everything one typed scan needs, shared by the source, the op, and every
/// iterator the op hands out.
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
    /// Deadline and cancellation, exactly as the opaque connector path uses
    /// them: checked before every open and on every driver turn.
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
    preparation_config: ScanPreparationConfig,
    preparation_timer: Arc<ScanPreparationTimer>,
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

/// Keep the page-source evidence with the source that the registered slot closes.
/// The iterator can outlive terminal group cleanup, so its Drop is not the
/// resource-close boundary.
struct ObservedPageSource {
    source: Box<dyn ConnectorPageSource>,
    emit_marker: bool,
    plan_node_id: i32,
    closed: bool,
}

impl ObservedPageSource {
    fn open(
        source: Box<dyn ConnectorPageSource>,
        emit_marker: bool,
        plan_node_id: i32,
        sequence_id: Option<u64>,
    ) -> Box<dyn ConnectorPageSource> {
        emit_page_source_marker(
            emit_marker,
            "NOVAROCKS_CONNECTOR_PAGE_SOURCE_OPEN",
            plan_node_id,
            sequence_id,
        );
        Box::new(Self {
            source,
            emit_marker,
            plan_node_id,
            closed: false,
        })
    }
}

impl ConnectorPageSource for ObservedPageSource {
    fn next_source_page(&mut self) -> Result<Option<SourcePage>, ConnectorError> {
        self.source.next_source_page()
    }

    fn is_finished(&self) -> bool {
        self.source.is_finished()
    }

    fn is_blocked(&self) -> bool {
        self.source.is_blocked()
    }

    fn advance_successor_preparation(
        &mut self,
        remaining_input_bytes: u64,
        remaining_candidates: usize,
    ) -> Result<ConnectorPreparationProgress, ConnectorError> {
        self.source
            .advance_successor_preparation(remaining_input_bytes, remaining_candidates)
    }

    fn successor_preparation_input_bytes(&self) -> u64 {
        self.source.successor_preparation_input_bytes()
    }

    fn successor_preparation_candidate_count(&self) -> usize {
        self.source.successor_preparation_candidate_count()
    }

    fn successor_preparation_control(&self) -> Option<Arc<dyn ConnectorPreparationControl>> {
        self.source.successor_preparation_control()
    }

    fn metrics(&self) -> PageSourceMetrics {
        self.source.metrics()
    }

    fn memory_usage_bytes(&self) -> u64 {
        self.source.memory_usage_bytes()
    }

    fn close(&mut self) -> Result<(), ConnectorError> {
        if self.closed {
            return Ok(());
        }
        self.closed = true;
        let result = self.source.close();
        emit_page_source_marker(
            self.emit_marker,
            "NOVAROCKS_CONNECTOR_PAGE_SOURCE_CLOSE",
            self.plan_node_id,
            None,
        );
        result
    }
}

impl Drop for ObservedPageSource {
    fn drop(&mut self) {
        let _ = self.close();
    }
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
        preparation_config: ScanPreparationConfig,
        preparation_timer: Arc<ScanPreparationTimer>,
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
            preparation_config,
            preparation_timer,
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
        preparation_config: ScanPreparationConfig,
        preparation_timer: Arc<ScanPreparationTimer>,
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
                preparation_config,
                preparation_timer,
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
        preparation_config: ScanPreparationConfig,
        preparation_timer: Arc<ScanPreparationTimer>,
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
            preparation_config,
            preparation_timer,
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
    /// fragment instance and are unaffected by which filter the page sources
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
                preparation_config: self.shared.preparation_config,
                preparation_timer: Arc::clone(&self.shared.preparation_timer),
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
        let waiter = Arc::new(SplitWaiter::default());
        // Weak, so dropping the op leaves an inert observer rather than keeping
        // this op's state alive for as long as the attempt's queue lives.
        let woken = Arc::downgrade(&waiter);
        queue.observable().add_observer(Arc::new(move || {
            if let Some(waiter) = woken.upgrade() {
                waiter.wake();
            }
        }));
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
        Ok(Arc::new(TypedConnectorScanOp {
            flow: StreamPreparationFlow::new(
                shared.preparation_config,
                Arc::clone(&shared.preparation_timer),
            ),
            shared,
            queue,
            waiter,
            sources: Arc::new(TypedPageSourceGroup::default()),
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
    waiter: Arc<SplitWaiter>,
    sources: Arc<TypedPageSourceGroup>,
    flow: Arc<StreamPreparationFlow>,
}

impl ScanOp for TypedConnectorScanOp {
    fn on_output_backpressure(&self, paused: bool) {
        self.flow.on_backpressure(paused);
    }

    fn on_nonempty_chunk_consumed(&self) {
        self.flow.on_nonempty_chunk_consumed();
    }

    fn terminate(&self) -> Result<(), String> {
        self.flow.stop_and_drain();
        // Page sources first: stop the I/O this scan started before waking the
        // drivers that would otherwise start more.
        let closed = self.sources.terminate();
        // Idempotent, drops anything still queued, and wakes every waiter once.
        self.queue.close();
        // A queue close notifies through the observable, but a driver parked
        // between two polls must also be woken directly.
        self.waiter.wake();
        closed
    }

    fn build_morsels(&self) -> Result<ScanMorsels, String> {
        // Exactly one, and never zero. A typed scan's work is not a morsel set
        // at all — it is the task's split queue, which the morsel's driver
        // drains until the queue says no split can ever follow. Reporting no
        // morsel would leave nobody to drain it: the splits would arrive, be
        // enqueued, and never be read, and the query would return zero rows
        // while every part of it reported success.
        //
        // `has_more` is false for the same reason: the set does not grow, the
        // queue does. A scan that starts before its first split arrives, or
        // receives none at all, is expressed by the driver parking on the
        // queue, not by an empty morsel set.
        Ok(ScanMorsels::new(vec![ScanMorsel::OperatorDriven], false))
    }

    fn supports_incremental_scan_ranges(&self) -> bool {
        // Growth arrives as splits on the task-update queue, never as a legacy
        // incremental scan range, so the morsel set itself is final.
        false
    }

    fn build_incremental_morsels(
        &self,
        _scan_ranges: &[IncrementalScanRange],
    ) -> Result<ScanMorsels, String> {
        Err(
            "typed connector scan receives splits through its task-update split queue, \
             not through incremental scan ranges"
                .to_string(),
        )
    }

    fn execute_iter(
        &self,
        morsel: ScanMorsel,
        profile: Option<RuntimeProfile>,
        _runtime_filters: Option<&RuntimeFilterContext>,
    ) -> Result<BoxedExecIter, String> {
        // The execution-layer runtime filters are not the connector's dynamic
        // filter: the connector consults the one this source was built with,
        // through `with_backend_dynamic_filter`. Applying an execution filter
        // here would push a predicate the provider never agreed to.
        match morsel {
            // This scan's work unit is its split queue, so the morsel carries
            // no scheduling identity of its own.
            ScanMorsel::OperatorDriven => {}
            ScanMorsel::Empty => {
                return Err(
                    "typed connector scan received an empty morsel, which would read none of \
                     the splits delivered to its task"
                        .to_string(),
                );
            }
            ScanMorsel::FileRange { .. } => {
                return Err(
                    "typed connector scan received a file-range morsel it does not own".to_string(),
                );
            }
            ScanMorsel::ConnectorScanUnit { .. } => {
                return Err(
                    "typed connector scan received an opaque prepared-unit morsel".to_string(),
                );
            }
            ScanMorsel::Schema { .. } => {
                return Err("typed connector scan received a schema morsel".to_string());
            }
        }
        Ok(Box::new(TypedConnectorSplitIter {
            shared: Arc::clone(&self.shared),
            queue: Arc::clone(&self.queue),
            waiter: Arc::clone(&self.waiter),
            sources: Arc::clone(&self.sources),
            current: None,
            current_successor_control_id: None,
            claims: VecDeque::new(),
            flow: Arc::clone(&self.flow),
            profile,
            finished: false,
        }))
    }

    fn profile_name(&self) -> Option<String> {
        Some("TypedConnectorScan".to_string())
    }
}

/// The chunk stream of one driver over this scan's split queue.
struct TypedConnectorSplitIter {
    shared: Arc<TypedConnectorScanShared>,
    queue: Arc<SplitQueue<ReceivedReadSplit>>,
    waiter: Arc<SplitWaiter>,
    sources: Arc<TypedPageSourceGroup>,
    /// The split currently being read. `None` between two splits.
    current: Option<RegisteredPageSource>,
    current_successor_control_id: Option<u64>,
    claims: VecDeque<PreparedClaim>,
    flow: Arc<StreamPreparationFlow>,
    profile: Option<RuntimeProfile>,
    finished: bool,
}

struct PreparedClaim {
    split: ReceivedReadSplit,
    candidate: ClaimCandidate,
    control_id: Option<u64>,
}

enum ClaimCandidate {
    Unprepared,
    Prepared(Box<dyn ConnectorPreparedPageSource>),
    Failed(String, Option<Box<dyn ConnectorPreparedPageSource>>),
}

impl TypedConnectorSplitIter {
    fn open_page_source(&mut self, split: &ReceivedReadSplit) -> Result<(), String> {
        self.shared.check_liveness("page source open")?;
        let page_source = self
            .shared
            .provider
            .resolve()?
            .create_page_source(
                &self.shared.session,
                self.shared.descriptor.table(),
                split.split(),
                split.sequence_id(),
                self.shared.descriptor.assignments(),
                &self.shared.dynamic_filter,
            )
            .map_err(|error| {
                format!(
                    "create typed connector page source for sequence {}: {error}",
                    split.sequence_id()
                )
            })?;
        self.install_page_source(split, page_source)
    }

    fn install_page_source(
        &mut self,
        split: &ReceivedReadSplit,
        page_source: Box<dyn ConnectorPageSource>,
    ) -> Result<(), String> {
        let page_source = ObservedPageSource::open(
            page_source,
            self.shared.emit_reader_markers,
            self.shared.plan_node_id,
            Some(split.sequence_id()),
        );
        let adapter = ConnectorPageAdapter::new(self.shared.slot_ids.clone(), page_source);
        let marker = TypedConnectorReaderMarker::for_split(split, self.shared.emit_reader_markers);
        self.current = Some(
            self.sources
                .register(adapter, marker, self.profile.clone())?,
        );
        if let Some(profile) = self.profile.as_ref() {
            profile.counter_add("TypedConnectorPageSourcesOpened", ProfileUnit::Unit, 1);
        }
        Ok(())
    }

    fn promote_claim(&mut self, claim: PreparedClaim) -> Result<(), String> {
        let PreparedClaim {
            split,
            candidate,
            control_id,
        } = claim;
        match candidate {
            ClaimCandidate::Unprepared => self.open_page_source(&split),
            ClaimCandidate::Failed(error, prepared) => {
                if let Some(prepared) = prepared {
                    let control = prepared.control();
                    control.request_stop();
                    futures::executor::block_on(control.wait_drained());
                }
                if let Some(id) = control_id {
                    self.flow.unregister(id);
                }
                Err(error)
            }
            ClaimCandidate::Prepared(prepared) => {
                self.shared
                    .check_liveness("prepared page source promotion")?;
                let page_source =
                    prepared
                        .promote(&self.shared.dynamic_filter)
                        .map_err(|error| {
                            format!(
                                "promote typed connector page source for sequence {}: {error}",
                                split.sequence_id()
                            )
                        })?;
                if let Some(id) = control_id {
                    self.flow.retire(id);
                }
                self.install_page_source(&split, page_source)
            }
        }
    }

    // Design: ADR-0158 (docs/adr/ADR-0158-bounded-parquet-range-preparation.md)
    fn advance_preparation(&mut self) {
        if !self.flow.may_prepare() {
            return;
        }
        if let Some(current) = self.current.as_ref() {
            if self.current_successor_control_id.is_none()
                && let Some(control) = current.successor_preparation_control()
            {
                self.current_successor_control_id = Some(self.flow.register(control));
            }
            let (_, current_candidates) = current.successor_preparation_footprint();
            let used = self
                .claims
                .len()
                .saturating_add(current_candidates)
                .saturating_add(self.flow.retired_count());
            if used < self.shared.preparation_config.max_candidates {
                let remaining_bytes = self
                    .shared
                    .preparation_config
                    .input_bytes_per_stream
                    .saturating_sub(
                        usize::try_from(self.flow.retained_input_bytes()).unwrap_or(usize::MAX),
                    );
                let _ = current.advance_successor_preparation(
                    remaining_bytes as u64,
                    self.shared.preparation_config.max_candidates - used,
                );
            }
        }
        for claim in &mut self.claims {
            if let ClaimCandidate::Prepared(prepared) = &mut claim.candidate {
                let remaining_bytes = self
                    .shared
                    .preparation_config
                    .input_bytes_per_stream
                    .saturating_sub(
                        usize::try_from(self.flow.retained_input_bytes()).unwrap_or(usize::MAX),
                    );
                if let Err(error) = prepared.advance(remaining_bytes as u64) {
                    let failed =
                        std::mem::replace(&mut claim.candidate, ClaimCandidate::Unprepared);
                    if let ClaimCandidate::Prepared(prepared) = &failed {
                        prepared.control().request_stop();
                    }
                    claim.candidate = ClaimCandidate::Failed(
                        format!(
                            "prepare typed connector page source for sequence {}: {error}",
                            claim.split.sequence_id()
                        ),
                        match failed {
                            ClaimCandidate::Prepared(prepared) => Some(prepared),
                            _ => None,
                        },
                    );
                }
            }
        }
        if self.current.is_none() {
            return;
        }
        let (_, current_candidates) = self
            .current
            .as_ref()
            .map(RegisteredPageSource::successor_preparation_footprint)
            .unwrap_or((0, 0));
        while self
            .claims
            .len()
            .saturating_add(current_candidates)
            .saturating_add(self.flow.retired_count())
            < self.shared.preparation_config.max_candidates
        {
            let SplitPoll::Ready(split) = self.queue.poll() else {
                break;
            };
            let candidate = match self.shared.provider.resolve().and_then(|provider| {
                provider
                    .prepare_page_source(
                        &self.shared.session,
                        self.shared.descriptor.table(),
                        split.split(),
                        split.sequence_id(),
                        self.shared.descriptor.assignments(),
                        &self.shared.dynamic_filter,
                    )
                    .map_err(|error| error.to_string())
            }) {
                Ok(ConnectorPreparationStart::Unsupported) => ClaimCandidate::Unprepared,
                Ok(ConnectorPreparationStart::Prepared(prepared)) => {
                    ClaimCandidate::Prepared(prepared)
                }
                Err(error) => ClaimCandidate::Failed(
                    format!(
                        "prepare typed connector page source for sequence {}: {error}",
                        split.sequence_id()
                    ),
                    None,
                ),
            };
            let control_id = match &candidate {
                ClaimCandidate::Prepared(prepared) => Some(self.flow.register(prepared.control())),
                _ => None,
            };
            let unsupported = matches!(candidate, ClaimCandidate::Unprepared);
            self.claims.push_back(PreparedClaim {
                split,
                candidate,
                control_id,
            });
            if unsupported {
                break;
            }
        }
    }

    fn close_current(&mut self) -> Result<(), String> {
        if let Some(id) = self.current_successor_control_id.take() {
            self.flow.unregister(id);
        }
        match self.current.take() {
            Some(source) => source.close(),
            None => Ok(()),
        }
    }

    /// End this stream on a primary failure, still releasing the open source.
    fn fail(&mut self, primary: String) -> ExecResult {
        self.finished = true;
        self.flow.stop_and_drain();
        match self.close_current() {
            Ok(()) => Err(primary),
            Err(cleanup) => Err(format!("{primary} (cleanup: {cleanup})")),
        }
    }
}

impl Iterator for TypedConnectorSplitIter {
    type Item = ExecResult;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.finished {
                return None;
            }
            if let Err(error) = self.shared.check_liveness("split driver") {
                return Some(self.fail(error));
            }
            if self.sources.is_terminal() {
                // `terminate` already closed everything and no further I/O may
                // start, so this driver ends without touching the provider.
                self.finished = true;
                return None;
            }

            if self.current.is_some() {
                self.advance_preparation();
            }
            if let Some(source) = self.current.as_ref() {
                match source.pull() {
                    Err(error) => return Some(self.fail(error)),
                    Ok(PageConversion::Chunk(chunk)) => {
                        return Some(match self.shared.materialize_output(chunk) {
                            Ok(chunk) => Ok(chunk),
                            Err(error) => self.fail(error),
                        });
                    }
                    Ok(PageConversion::Idle) => {
                        // Nothing right now, which is not end of stream. Only a
                        // source that says it is waiting earns a sleep.
                        if source.is_blocked() {
                            std::thread::sleep(BLOCKED_PAGE_SOURCE_BACKOFF);
                        }
                        continue;
                    }
                    Ok(PageConversion::Finished) => {
                        if let Err(error) = self.close_current() {
                            return Some(self.fail(error));
                        }
                        if let Some(profile) = self.profile.as_ref() {
                            profile.counter_add("TypedConnectorSplitsRead", ProfileUnit::Unit, 1);
                        }
                        continue;
                    }
                }
            }

            if let Some(claim) = self.claims.pop_front() {
                if let Err(error) = self.promote_claim(claim) {
                    return Some(self.fail(error));
                }
                continue;
            }

            // Read the wake generation before polling, so a split that arrives
            // between the poll and the park is never slept through.
            let generation = self.waiter.generation();
            match self.queue.poll() {
                SplitPoll::Ready(split) => {
                    if let Err(error) = self.open_page_source(&split) {
                        return Some(self.fail(error));
                    }
                    continue;
                }
                SplitPoll::Blocked => {
                    self.waiter.wait(generation, SPLIT_WAIT_POLL_INTERVAL);
                    continue;
                }
                // The only end of stream: drained after the terminal marker, or
                // closed.
                SplitPoll::Exhausted => {
                    self.finished = true;
                    return self.close_current().err().map(Err);
                }
            }
        }
    }
}

impl Drop for TypedConnectorSplitIter {
    fn drop(&mut self) {
        self.flow.stop_and_drain();
        // A dropped driver must still release its page source. Terminal
        // cleanup normally does it; this is the final safety net.
        let _ = self.close_current();
    }
}

/// A wake latch for drivers parked on an empty, non-terminal split queue.
///
/// The queue publishes state changes through observers, which cannot park a
/// thread by themselves; this turns one into a wake.
#[derive(Default)]
struct SplitWaiter {
    generation: Mutex<u64>,
    signal: Condvar,
}

impl SplitWaiter {
    fn wake(&self) {
        let mut generation = self.generation.lock().expect("split waiter lock");
        *generation = generation.wrapping_add(1);
        drop(generation);
        self.signal.notify_all();
    }

    fn generation(&self) -> u64 {
        *self.generation.lock().expect("split waiter lock")
    }

    /// Park until the generation moves past `seen`, or until `timeout`.
    fn wait(&self, seen: u64, timeout: Duration) {
        let generation = self.generation.lock().expect("split waiter lock");
        if *generation != seen {
            return;
        }
        let _unused = self
            .signal
            .wait_timeout(generation, timeout)
            .expect("split waiter lock");
    }
}

// ---------------------------------------------------------------------------
// System relations read by exactly one backend
// ---------------------------------------------------------------------------

/// A system relation whose rows come from one immutable metadata file.
///
/// It has no split and never touches the task-update queue: the coordinator
/// resolved it to exactly one backend, and synthesizing a split would invent
/// scheduling identity for work that has none. A scan bound to this source
/// therefore does its whole job in one morsel and then finishes, instead of
/// parking on a queue nobody will ever serve.
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
    pub fn new(
        descriptor: TypedConnectorReadDescriptor,
        provider: Arc<dyn ConnectorReadSystemTableProvider>,
        session: ConnectorSession,
        request: ConnectorRequestContext,
        plan_node_id: i32,
        slot_ids: Vec<SlotId>,
        emit_reader_markers: bool,
    ) -> Self {
        Self::from_provider(
            descriptor,
            ReadProvider::Ready(provider),
            session,
            request,
            plan_node_id,
            slot_ids,
            emit_reader_markers,
        )
    }

    fn from_provider(
        descriptor: TypedConnectorReadDescriptor,
        provider: ReadProvider<dyn ConnectorReadSystemTableProvider>,
        session: ConnectorSession,
        request: ConnectorRequestContext,
        plan_node_id: i32,
        slot_ids: Vec<SlotId>,
        emit_reader_markers: bool,
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
            }),
        }
    }

    pub fn new_deferred(
        descriptor: TypedConnectorReadDescriptor,
        provider_factory: SystemProviderFactory,
        session: ConnectorSession,
        request: ConnectorRequestContext,
        plan_node_id: i32,
        slot_ids: Vec<SlotId>,
        emit_reader_markers: bool,
    ) -> Self {
        Self::from_provider(
            descriptor,
            ReadProvider::Deferred(provider_factory),
            session,
            request,
            plan_node_id,
            slot_ids,
            emit_reader_markers,
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
        });
        Arc::get_mut(&mut shared)
            .expect("bound system scan source has one owner")
            .provider = ReadProvider::Ready(self.shared.provider.resolve()?);
        Ok(Arc::new(TypedConnectorSystemTableScanOp {
            shared,
            sources: Arc::new(TypedPageSourceGroup::default()),
        }))
    }

    fn profile_name(&self) -> Option<String> {
        Some("TypedConnectorSystemTableScan".to_string())
    }
}

/// One bound system relation scan.
pub struct TypedConnectorSystemTableScanOp {
    shared: Arc<TypedSystemTableScanShared>,
    sources: Arc<TypedPageSourceGroup>,
}

impl ScanOp for TypedConnectorSystemTableScanOp {
    fn terminate(&self) -> Result<(), String> {
        self.sources.terminate()
    }

    fn build_morsels(&self) -> Result<ScanMorsels, String> {
        // Exactly one unit of work, known before execution starts: the whole
        // relation is one metadata file. `has_more` is false because nothing
        // can add work to this scan later.
        Ok(ScanMorsels::new(vec![ScanMorsel::OperatorDriven], false))
    }

    fn execute_iter(
        &self,
        morsel: ScanMorsel,
        profile: Option<RuntimeProfile>,
        _runtime_filters: Option<&RuntimeFilterContext>,
    ) -> Result<BoxedExecIter, String> {
        match morsel {
            ScanMorsel::OperatorDriven => {}
            ScanMorsel::Empty => {
                return Err(
                    "typed system relation scan received an empty morsel, which would read \
                     none of the relation"
                        .to_string(),
                );
            }
            ScanMorsel::FileRange { .. } => {
                return Err(
                    "typed system relation scan received a file-range morsel it does not own"
                        .to_string(),
                );
            }
            ScanMorsel::ConnectorScanUnit { .. } => {
                return Err(
                    "typed system relation scan received an opaque prepared-unit morsel"
                        .to_string(),
                );
            }
            ScanMorsel::Schema { .. } => {
                return Err("typed system relation scan received a schema morsel".to_string());
            }
        }
        Ok(Box::new(TypedSystemTableIter {
            shared: Arc::clone(&self.shared),
            sources: Arc::clone(&self.sources),
            current: None,
            opened: false,
            finished: false,
            profile,
        }))
    }
}

/// Drains one system relation's page source to end of stream.
struct TypedSystemTableIter {
    shared: Arc<TypedSystemTableScanShared>,
    sources: Arc<TypedPageSourceGroup>,
    current: Option<RegisteredPageSource>,
    opened: bool,
    finished: bool,
    profile: Option<RuntimeProfile>,
}

impl TypedSystemTableIter {
    fn open(&mut self) -> Result<(), String> {
        self.shared.check_liveness("open")?;
        let page_source = self
            .shared
            .provider
            .resolve()?
            .create_system_page_source(
                &self.shared.session,
                self.shared.descriptor.table(),
                self.shared.descriptor.assignments(),
            )
            .map_err(|error| format!("create typed system relation page source: {error}"))?;
        let page_source = ObservedPageSource::open(
            page_source,
            self.shared.emit_reader_markers,
            self.shared.plan_node_id,
            None,
        );
        let adapter = ConnectorPageAdapter::new(self.shared.slot_ids.clone(), page_source);
        self.current = Some(self.sources.register(adapter, None, self.profile.clone())?);
        if let Some(profile) = self.profile.as_ref() {
            profile.counter_add("TypedSystemTablePageSourcesOpened", ProfileUnit::Unit, 1);
        }
        Ok(())
    }

    fn close_current(&mut self) -> Result<(), String> {
        match self.current.take() {
            Some(source) => source.close(),
            None => Ok(()),
        }
    }

    fn fail(&mut self, primary: String) -> ExecResult {
        self.finished = true;
        let _ = self.close_current();
        Err(primary)
    }
}

impl Iterator for TypedSystemTableIter {
    type Item = ExecResult;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.finished {
                return None;
            }
            if self.sources.is_terminal() {
                self.finished = true;
                return None;
            }
            if !self.opened {
                self.opened = true;
                if let Err(error) = self.open() {
                    return Some(self.fail(error));
                }
            }
            let Some(source) = self.current.as_ref() else {
                self.finished = true;
                return None;
            };
            match source.pull() {
                Err(error) => return Some(self.fail(error)),
                Ok(PageConversion::Chunk(chunk)) => {
                    return Some(match self.shared.materialize_output(chunk) {
                        Ok(chunk) => Ok(chunk),
                        Err(error) => self.fail(error),
                    });
                }
                Ok(PageConversion::Idle) => {
                    // A metadata reader that is waiting is still waiting on its
                    // own I/O, not on scheduling, so this yields rather than
                    // sleeping on a wake that has no producer.
                    if source.is_blocked() {
                        std::thread::sleep(BLOCKED_PAGE_SOURCE_BACKOFF);
                    }
                    continue;
                }
                Ok(PageConversion::Finished) => {
                    self.finished = true;
                    return self.close_current().err().map(Err);
                }
            }
        }
    }
}

impl Drop for TypedSystemTableIter {
    fn drop(&mut self) {
        let _ = self.close_current();
    }
}

#[cfg(test)]
mod page_source_evidence_tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    struct CountingPageSource(Arc<AtomicUsize>);

    impl ConnectorPageSource for CountingPageSource {
        fn next_source_page(&mut self) -> Result<Option<SourcePage>, ConnectorError> {
            Ok(None)
        }

        fn is_finished(&self) -> bool {
            false
        }

        fn metrics(&self) -> PageSourceMetrics {
            PageSourceMetrics::default()
        }

        fn memory_usage_bytes(&self) -> u64 {
            0
        }

        fn close(&mut self) -> Result<(), ConnectorError> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    #[test]
    fn terminal_group_close_releases_page_source_before_registered_handle_drop() {
        let closes = Arc::new(AtomicUsize::new(0));
        let observed = ObservedPageSource::open(
            Box::new(CountingPageSource(Arc::clone(&closes))),
            false,
            7,
            Some(1),
        );
        let group = Arc::new(TypedPageSourceGroup::default());
        let registered = group
            .register(ConnectorPageAdapter::new(Vec::new(), observed), None, None)
            .expect("register page source");

        group.terminate().expect("terminate page sources");
        assert_eq!(closes.load(Ordering::SeqCst), 1);
        group.terminate().expect("repeat termination");
        registered.close().expect("late iterator close");
        assert_eq!(closes.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn dropped_observed_source_closes_once() {
        let closes = Arc::new(AtomicUsize::new(0));
        let mut observed = ObservedPageSource::open(
            Box::new(CountingPageSource(Arc::clone(&closes))),
            false,
            7,
            None,
        );
        observed.close().expect("explicit close");
        observed.close().expect("repeat close");
        drop(observed);
        assert_eq!(closes.load(Ordering::SeqCst), 1);
    }
}
