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

use std::collections::BTreeMap;
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

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
use novarocks_spi::connector::ConnectorRequestContext;
#[cfg(test)]
use novarocks_spi::connector::read_stack::CompleteAllDynamicFilter;
use novarocks_spi::connector::read_stack::{
    ConnectorReadDynamicFilter, ConnectorReadPageSourceProvider, ConnectorReadSystemTableProvider,
    ConnectorSession,
};
use novarocks_types::SlotId;
use novarocks_worker::RuntimeFilterSessionResolver;
use novarocks_worker::TypedConnectorReadDescriptor;
use novarocks_worker::connector_batch_transform::ConnectorBatchTransform;
use novarocks_worker::read_attempt::ReceivedReadSplit;
use novarocks_worker::typed_page_source::{
    RegisteredPageSource, TypedConnectorReaderMarker, TypedPageSourceGroup,
};
use novarocks_worker::typed_scan_filter::TypedScanLiveDynamicFilterFactory;

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
    provider: Arc<dyn ConnectorReadPageSourceProvider>,
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
        if self.request.cancellation().is_cancelled() {
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
    pub(crate) fn new(
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
            }),
            queues,
        }
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
                provider: Arc::clone(&self.shared.provider),
                session: self.shared.session.clone(),
                runtime_filter: Arc::clone(&self.shared.runtime_filter),
                live_dynamic_filter_factory: Arc::clone(&self.shared.live_dynamic_filter_factory),
                emit_reader_markers: self.shared.emit_reader_markers,
                runtime_filter_contracts: self.shared.runtime_filter_contracts.clone(),
                request: self.shared.request.clone(),
                plan_node_id: self.shared.plan_node_id,
                slot_ids: self.shared.slot_ids.clone(),
                dynamic_filter,
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
        let shared = match self.live_dynamic_filter()? {
            Some(dynamic_filter) => self.with_substituted_filter(dynamic_filter).shared,
            None => Arc::clone(&self.shared),
        };
        Ok(Arc::new(TypedConnectorScanOp {
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
}

impl ScanOp for TypedConnectorScanOp {
    fn terminate(&self) -> Result<(), String> {
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
    profile: Option<RuntimeProfile>,
    finished: bool,
}

impl TypedConnectorSplitIter {
    fn open_page_source(&mut self, split: &ReceivedReadSplit) -> Result<(), String> {
        self.shared.check_liveness("page source open")?;
        let page_source = self
            .shared
            .provider
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
        let adapter = ConnectorPageAdapter::new(self.shared.slot_ids.clone(), page_source);
        let marker = TypedConnectorReaderMarker::for_split(split, self.shared.emit_reader_markers);
        self.current = Some(
            self.sources
                .register(adapter, marker, self.profile.clone())?,
        );
        // Acceptance evidence: a distributed run proves a page source was
        // opened on this backend for this exact scheduled split, which a
        // result-only assertion cannot show.
        emit_page_source_marker(
            self.shared.emit_reader_markers,
            "NOVAROCKS_CONNECTOR_PAGE_SOURCE_OPEN",
            self.shared.plan_node_id,
            Some(split.sequence_id()),
        );
        if let Some(profile) = self.profile.as_ref() {
            profile.counter_add("TypedConnectorPageSourcesOpened", ProfileUnit::Unit, 1);
        }
        Ok(())
    }

    fn close_current(&mut self) -> Result<(), String> {
        match self.current.take() {
            Some(source) => {
                let closed = source.close();
                emit_page_source_marker(
                    self.shared.emit_reader_markers,
                    "NOVAROCKS_CONNECTOR_PAGE_SOURCE_CLOSE",
                    self.shared.plan_node_id,
                    None,
                );
                closed
            }
            None => Ok(()),
        }
    }

    /// End this stream on a primary failure, still releasing the open source.
    fn fail(&mut self, primary: String) -> ExecResult {
        self.finished = true;
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

/// Wire fixtures shared by this module's tests and the typed scan decoder's.
///
/// They live here because the carrier they build is this module's input; the
/// decoder tests assert what the decoder does with exactly that carrier.
#[cfg(test)]
pub(crate) mod test_support {
    use novarocks_proto_codec::connector_common::encode_connector_payload_message;
    use novarocks_proto_codec::connector_read::{ConnectorReadDecoder, encode_value_type};
    use novarocks_proto_models::connector_read as dto;
    use novarocks_spi::connector::ConnectorExecutionReadBinding;
    use novarocks_spi::connector::read_stack::ConnectorValueType;
    use novarocks_worker::read_attempt::TypedReadAttemptContext;

    pub(crate) fn encoded_payload(
        category: novarocks_spi::connector::ConnectorCodecCategory,
        value: impl Into<bytes::Bytes>,
    ) -> novarocks_proto_models::connector_common::ConnectorEncodedPayload {
        encode_connector_payload_message(&novarocks_spi::connector::ConnectorEncodedPayload::new(
            novarocks_spi::connector::ConnectorEnvelopeHeader::new(
                novarocks_spi::connector::ConnectorProviderId::parse("fixture")
                    .expect("provider id"),
                novarocks_spi::connector::CatalogHandle::new(
                    novarocks_spi::connector::ConnectorInstanceId::try_from_canonical("test.typed")
                        .expect("instance id"),
                    novarocks_spi::connector::CatalogVersion::from_bytes([1; 32]),
                ),
                category,
                novarocks_spi::connector::ConnectorCodecRevision::try_new(1)
                    .expect("codec revision"),
            ),
            value.into(),
        ))
    }

    pub(crate) fn unconstrained() -> dto::TupleDomain {
        dto::TupleDomain {
            none: false,
            column_domains: Vec::new(),
        }
    }

    pub(crate) fn column_handle(field_id: i32) -> dto::ColumnHandle {
        dto::ColumnHandle {
            provider_payload: Some(encoded_payload(
                novarocks_spi::connector::ConnectorCodecCategory::ReadColumn,
                bytes::Bytes::from(format!("column-{field_id}")),
            )),
        }
    }

    pub(crate) fn catalog_table_handle() -> dto::CatalogTableHandle {
        dto::CatalogTableHandle {
            catalog_handle: Some(novarocks_proto_models::catalog::CatalogHandle {
                catalog_name: "test.typed".to_owned(),
                version: vec![1; 32],
            }),
            transaction: Some(dto::ConnectorTransactionHandle {
                provider_payload: Some(encoded_payload(
                    novarocks_spi::connector::ConnectorCodecCategory::ReadView,
                    bytes::Bytes::from_static(b"transaction"),
                )),
            }),
            relation: Some(dto::catalog_table_handle::Relation::Table(
                dto::ConnectorTableHandle {
                    provider_payload: Some(encoded_payload(
                        novarocks_spi::connector::ConnectorCodecCategory::ReadTable,
                        bytes::Bytes::from_static(b"table"),
                    )),
                },
            )),
        }
    }

    #[derive(Clone, Debug)]
    struct FixtureTable;
    #[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
    struct FixtureColumn(i32);
    #[derive(Clone, Debug)]
    struct FixtureSplit;

    impl novarocks_spi::connector::read_stack::ColumnHandle for FixtureColumn {}
    impl novarocks_spi::connector::read_stack::ConnectorSplit for FixtureSplit {
        fn retained_size_in_bytes(&self) -> u64 {
            64
        }
    }

    struct FixtureRuntime {
        descriptor: novarocks_spi::connector::ConnectorInstanceDescriptor,
        catalog_handle: novarocks_spi::connector::CatalogHandle,
    }

    impl FixtureRuntime {
        fn new() -> Self {
            let descriptor = novarocks_spi::connector::ConnectorInstanceDescriptor {
                provider_id: novarocks_spi::connector::ConnectorProviderId::parse("fixture")
                    .expect("provider id"),
                instance_id: novarocks_spi::connector::ConnectorInstanceId::try_from_canonical(
                    "test.typed",
                )
                .expect("instance id"),
            };
            Self {
                catalog_handle: novarocks_spi::connector::CatalogHandle::new(
                    descriptor.instance_id.clone(),
                    novarocks_spi::connector::CatalogVersion::from_bytes([1; 32]),
                ),
                descriptor,
            }
        }
    }

    impl novarocks_spi::connector::read_stack::adapter::ProviderReadRuntime for FixtureRuntime {
        type Table = FixtureTable;
        type Column = FixtureColumn;
        type Transaction = ();
        type Split = FixtureSplit;

        fn descriptor(&self) -> &novarocks_spi::connector::ConnectorInstanceDescriptor {
            &self.descriptor
        }
        fn catalog_handle(&self) -> &novarocks_spi::connector::CatalogHandle {
            &self.catalog_handle
        }
        fn transaction(&self) -> Self::Transaction {}
    }

    #[derive(Clone)]
    struct FixtureCodec {
        adapter: novarocks_spi::connector::read_stack::adapter::ReadRuntimeAdapter<FixtureRuntime>,
    }

    fn fixture_codec() -> FixtureCodec {
        FixtureCodec {
            adapter: novarocks_spi::connector::read_stack::adapter::ReadRuntimeAdapter::new(
                std::sync::Arc::new(FixtureRuntime::new()),
            ),
        }
    }

    pub(crate) fn installed_read_execution() -> ConnectorExecutionReadBinding {
        ConnectorExecutionReadBinding::new(
            std::sync::Arc::new(FixtureFactory),
            std::sync::Arc::new(fixture_codec()),
        )
    }

    impl novarocks_spi::connector::ConnectorReadWireDecoder for FixtureCodec {
        fn owner(&self) -> &str {
            "fixture"
        }
        fn decode_relation_payload(
            &self,
            _relation: &novarocks_spi::connector::ConnectorReadRelationPayload,
        ) -> Result<
            novarocks_spi::connector::read_stack::ConnectorReadRelation,
            novarocks_spi::connector::ConnectorCodecError,
        > {
            let table = self.adapter.wrap_table(FixtureTable);
            self.adapter
                .relation(
                    novarocks_spi::connector::read_stack::ConnectorReadRelationKind::Table,
                    table,
                )
                .map_err(|error| {
                    novarocks_spi::connector::ConnectorCodecError::new(
                        novarocks_spi::connector::ConnectorFieldPath::root("table"),
                        novarocks_spi::connector::ConnectorCodecErrorKind::InvalidValue,
                        error.to_string(),
                    )
                })
        }
        fn decode_column_payload(
            &self,
            _: &novarocks_spi::connector::ConnectorEncodedPayload,
        ) -> Result<
            novarocks_spi::connector::read_stack::ConnectorReadColumnHandle,
            novarocks_spi::connector::ConnectorCodecError,
        > {
            Ok(self.adapter.wrap_column(FixtureColumn(1)))
        }
        fn decode_transaction_payload(
            &self,
            _: &novarocks_spi::connector::ConnectorEncodedPayload,
        ) -> Result<
            novarocks_spi::connector::read_stack::ConnectorReadTransactionHandle,
            novarocks_spi::connector::ConnectorCodecError,
        > {
            Ok(self.adapter.wrap_transaction(()))
        }
        fn decode_split_payload(
            &self,
            _: &novarocks_spi::connector::ConnectorReadSplitPayload,
            _: &novarocks_spi::connector::read_stack::ConnectorReadSplitFacts,
        ) -> Result<
            novarocks_spi::connector::read_stack::ConnectorReadSplit,
            novarocks_spi::connector::ConnectorCodecError,
        > {
            Ok(self.adapter.wrap_split(FixtureSplit))
        }
    }

    struct InertPageProvider;
    impl novarocks_spi::connector::read_stack::ConnectorReadPageSourceProvider for InertPageProvider {
        fn create_page_source(
            &self,
            _: &novarocks_spi::connector::read_stack::ConnectorSession,
            _: &novarocks_spi::connector::read_stack::ConnectorReadTableHandle,
            _: &novarocks_spi::connector::read_stack::ConnectorReadSplit,
            _: u64,
            _: &[novarocks_spi::connector::read_stack::Assignment<
                novarocks_spi::connector::read_stack::ConnectorReadColumnHandle,
            >],
            _: &std::sync::Arc<novarocks_spi::connector::read_stack::ConnectorReadDynamicFilter>,
        ) -> Result<
            Box<dyn novarocks_spi::connector::read_stack::ConnectorPageSource>,
            novarocks_spi::connector::ConnectorError,
        > {
            Err(novarocks_spi::connector::ConnectorError::new(
                novarocks_spi::connector::ConnectorErrorKind::Internal,
                "fixture page provider is never read",
            ))
        }
    }
    struct InertSystemProvider;
    impl novarocks_spi::connector::read_stack::ConnectorReadSystemTableProvider
        for InertSystemProvider
    {
        fn create_system_page_source(
            &self,
            _: &novarocks_spi::connector::read_stack::ConnectorSession,
            _: &novarocks_spi::connector::read_stack::ConnectorReadTableHandle,
            _: &[novarocks_spi::connector::read_stack::Assignment<
                novarocks_spi::connector::read_stack::ConnectorReadColumnHandle,
            >],
        ) -> Result<
            Box<dyn novarocks_spi::connector::read_stack::ConnectorPageSource>,
            novarocks_spi::connector::ConnectorError,
        > {
            Err(novarocks_spi::connector::ConnectorError::new(
                novarocks_spi::connector::ConnectorErrorKind::Internal,
                "fixture system provider is never read",
            ))
        }
    }
    struct FixtureFactory;
    impl novarocks_spi::connector::read_stack::ConnectorReadProviderFactory for FixtureFactory {
        fn create_page_source_provider(
            &self,
            _: &novarocks_spi::connector::ConnectorRequestContext,
            _: novarocks_spi::connector::read_stack::ConnectorPageSourceProviderOptions,
        ) -> Result<
            std::sync::Arc<
                dyn novarocks_spi::connector::read_stack::ConnectorReadPageSourceProvider,
            >,
            novarocks_spi::connector::ConnectorError,
        > {
            Ok(std::sync::Arc::new(InertPageProvider))
        }
        fn create_system_table_provider(
            &self,
            _: &novarocks_spi::connector::ConnectorRequestContext,
        ) -> Result<
            std::sync::Arc<
                dyn novarocks_spi::connector::read_stack::ConnectorReadSystemTableProvider,
            >,
            novarocks_spi::connector::ConnectorError,
        > {
            Ok(std::sync::Arc::new(InertSystemProvider))
        }
    }

    pub(crate) fn decoded_scan() -> novarocks_proto_codec::connector_read::DecodedConnectorReadScan
    {
        let raw = novarocks_proto_codec::connector_read::ConnectorTableScanSource::parse(
            scan_source_proto(),
            novarocks_proto_codec::FieldPath::root("scan"),
        )
        .expect("scan");
        novarocks_proto_codec::connector_read::DecodedConnectorReadScan::decode(
            &fixture_codec(),
            &raw,
        )
        .expect("decoded scan")
    }

    pub(crate) fn decoded_scheduled_split(
        plan_node_id: i32,
        sequence_id: u64,
    ) -> novarocks_proto_codec::connector_read::DecodedScheduledReadSplit {
        let raw = novarocks_proto_codec::connector_read::ScheduledSplit::parse(
            split_proto(plan_node_id, sequence_id),
            novarocks_proto_codec::FieldPath::root("split"),
        )
        .expect("split");
        fixture_codec()
            .decode_scheduled_split(&raw)
            .expect("decoded split")
    }

    /// The runtime bundle a typed decode needs, wired to the same binding
    /// generation `catalog_table_handle` names.
    pub(crate) fn typed_scan_runtime() -> novarocks_worker::TypedScanRuntime {
        struct NoVendedStorageResolver;

        impl novarocks_spi::connector::ConnectorStorageResolver for NoVendedStorageResolver {
            fn resolve_vended_s3(
                &self,
                _: &novarocks_spi::connector::StorageAccessRequest,
            ) -> Result<
                novarocks_spi::connector::ResolvedVendedS3Access,
                novarocks_spi::connector::ConnectorError,
            > {
                Err(novarocks_spi::connector::ConnectorError::new(
                    novarocks_spi::connector::ConnectorErrorKind::InvalidRequest,
                    "test fixture has no vended storage lease",
                ))
            }
        }

        use novarocks_types::{AttemptId, QueryId};
        let catalog_handle = novarocks_spi::connector::CatalogHandle::new(
            novarocks_spi::connector::ConnectorInstanceId::try_from_canonical("test.typed")
                .expect("canonical instance id"),
            novarocks_spi::connector::CatalogVersion::from_bytes([1; 32]),
        );
        let execution = installed_read_execution();
        let execution_id = novarocks_proto_codec::lifecycle::QueryExecutionId::new(
            QueryId::new(1, 2),
            AttemptId::new(1).expect("attempt"),
        )
        .expect("execution id");
        let queues = novarocks_execution::connector::SplitQueueRegistry::new().open_attempt(
            novarocks_execution::connector::TaskAttemptKey::new(
                execution_id,
                novarocks_types::UniqueId::new(9, 1),
            ),
            novarocks_execution::connector::SplitQueueConfig::default(),
        );
        let session = novarocks_spi::connector::read_stack::ConnectorSession::try_new(
            "q1",
            "novarocks",
            "UTC",
            "en_US",
            std::time::SystemTime::UNIX_EPOCH,
        )
        .expect("session");
        novarocks_worker::TypedScanRuntime::new(
            execution_id,
            std::sync::Arc::new(move |handle| {
                if handle == &catalog_handle {
                    Ok(execution.clone())
                } else {
                    Err("no query-leased test catalog runtime".to_owned())
                }
            }),
            std::sync::Arc::new(|_| Err("no query-leased test writer runtime".to_owned())),
            queues,
            session,
            std::sync::Arc::new(|| Ok(None)),
            std::sync::Arc::new(TypedReadAttemptContext::new()),
            std::sync::Arc::new(NoVendedStorageResolver),
        )
    }

    pub(crate) fn scan_source_proto() -> dto::ConnectorTableScanSource {
        dto::ConnectorTableScanSource {
            table: Some(catalog_table_handle()),
            assignments: vec![dto::ScanAssignment {
                variable: "v0".to_owned(),
                column: Some(column_handle(1)),
                value_type: Some(encode_value_type(ConnectorValueType::BigInt)),
            }],
            enforced_predicate: Some(unconstrained()),
            unenforced_predicate: Some(unconstrained()),
            remaining_expression: None,
            dynamic_filters: Vec::new(),
            max_batch_rows: 1024,
            max_batch_bytes: 1 << 20,
            work_source: dto::ScanWorkSource::RuntimeSplits as i32,
        }
    }

    pub(crate) fn split_proto(plan_node_id: i32, sequence_id: u64) -> dto::ScheduledSplit {
        dto::ScheduledSplit {
            sequence_id,
            plan_node_id,
            split: Some(dto::ConnectorSplit {
                split_weight_raw: 100,
                remotely_accessible: true,
                addresses: Vec::new(),
                affinity_key: None,
                retained_size_in_bytes: 64,
                category: Some(dto::connector_split::Category::Data(dto::DataSplit {
                    provider_payload: Some(encoded_payload(
                        novarocks_spi::connector::ConnectorCodecCategory::ReadSplit,
                        bytes::Bytes::from(format!("split-{sequence_id}")),
                    )),
                })),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use novarocks_spi::connector::read_stack::PageSourceFileMetrics;
    use novarocks_worker::typed_page_source::flush_page_source_file_metrics;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::SystemTime;

    use arrow::array::{ArrayRef, Int64Array};
    use novarocks_execution::connector::{SplitQueueConfig, SplitQueueRegistry, TaskAttemptKey};
    use novarocks_proto_codec::FieldPath;
    use novarocks_proto_codec::connector_read::ConnectorTableScanSource;
    use novarocks_proto_models::connector_read as dto;
    use novarocks_spi::connector::read_stack::{
        ConnectorPageSource, ConnectorReadDynamicFilter, ConnectorReadPageSourceProvider,
        ConnectorReadSystemTableProvider, PageSourceMetrics, SourcePage,
    };
    use novarocks_spi::connector::{ConnectorCancellation, ConnectorError, ConnectorErrorKind};
    use novarocks_types::{AttemptId, QueryExecutionId, QueryId, UniqueId};

    use super::*;

    fn scan_source() -> ConnectorTableScanSource {
        ConnectorTableScanSource::parse(
            test_support::scan_source_proto(),
            FieldPath::root("typed_connector_read"),
        )
        .expect("valid typed scan source")
    }

    const NODE: i32 = 7;

    struct NeverCancelled;

    impl ConnectorCancellation for NeverCancelled {
        fn is_cancelled(&self) -> bool {
            false
        }
    }

    struct AlwaysCancelled;

    impl ConnectorCancellation for AlwaysCancelled {
        fn is_cancelled(&self) -> bool {
            true
        }
    }

    /// A page source scripted turn by turn. A `None` entry is an idle turn, not
    /// termination: only running out of script finishes it.
    struct ScriptedPageSource {
        pages: Vec<Option<SourcePage>>,
        cursor: usize,
        finished: bool,
        closes: Arc<AtomicUsize>,
    }

    impl ConnectorPageSource for ScriptedPageSource {
        fn next_source_page(&mut self) -> Result<Option<SourcePage>, ConnectorError> {
            if self.cursor >= self.pages.len() {
                self.finished = true;
                return Ok(None);
            }
            let page = self.pages[self.cursor].take();
            self.cursor += 1;
            Ok(page)
        }

        fn is_finished(&self) -> bool {
            self.finished
        }

        fn metrics(&self) -> PageSourceMetrics {
            PageSourceMetrics::default()
        }

        fn memory_usage_bytes(&self) -> u64 {
            0
        }

        fn close(&mut self) -> Result<(), ConnectorError> {
            self.closes.fetch_add(1, Ordering::AcqRel);
            Ok(())
        }
    }

    #[test]
    fn typed_page_source_file_metrics_are_projected_as_deltas() {
        let profile = RuntimeProfile::new("typed-file-read");
        let mut last = PageSourceFileMetrics::default();
        let first = PageSourceFileMetrics {
            bytes_read: 25,
            page_index_attempts: 2,
            page_index_rows_considered: 16,
            page_index_rows_pruned: 12,
            ..Default::default()
        };
        flush_page_source_file_metrics(Some(&profile), &mut last, first);
        // Re-observing one cumulative snapshot must not count it twice.
        flush_page_source_file_metrics(Some(&profile), &mut last, first);
        flush_page_source_file_metrics(
            Some(&profile),
            &mut last,
            PageSourceFileMetrics {
                bytes_read: 40,
                page_index_attempts: 3,
                page_index_rows_considered: 24,
                page_index_rows_pruned: 18,
                ..Default::default()
            },
        );

        assert_eq!(profile.counter_value("ConnectorFileBytesRead"), Some(40));
        assert_eq!(
            profile.counter_value("ConnectorFilePageIndexAttempts"),
            Some(3)
        );
        assert_eq!(
            profile.counter_value("ConnectorFilePageIndexRowsConsidered"),
            Some(24)
        );
        assert_eq!(
            profile.counter_value("ConnectorFilePageIndexRowsPruned"),
            Some(18)
        );
    }

    /// A page-source provider that hands out one scripted source per split.
    struct ScriptedProvider {
        script: Mutex<Vec<Vec<Option<SourcePage>>>>,
        opens: Arc<AtomicUsize>,
        closes: Arc<AtomicUsize>,
        fail_open: bool,
    }

    impl ScriptedProvider {
        fn new(script: Vec<Vec<Option<SourcePage>>>) -> Arc<Self> {
            Arc::new(Self {
                script: Mutex::new(script),
                opens: Arc::new(AtomicUsize::new(0)),
                closes: Arc::new(AtomicUsize::new(0)),
                fail_open: false,
            })
        }
    }

    impl ConnectorReadPageSourceProvider for ScriptedProvider {
        fn create_page_source(
            &self,
            _session: &ConnectorSession,
            _table: &novarocks_spi::connector::read_stack::ConnectorReadTableHandle,
            _split: &novarocks_spi::connector::read_stack::ConnectorReadSplit,
            _scheduled_split_sequence_id: u64,
            _columns: &[novarocks_spi::connector::read_stack::Assignment<
                novarocks_spi::connector::read_stack::ConnectorReadColumnHandle,
            >],
            _dynamic_filter: &Arc<ConnectorReadDynamicFilter>,
        ) -> Result<Box<dyn ConnectorPageSource>, ConnectorError> {
            if self.fail_open {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::Unavailable,
                    "scripted open failure",
                ));
            }
            self.opens.fetch_add(1, Ordering::AcqRel);
            let mut script = self.script.lock().expect("script lock");
            let pages = if script.is_empty() {
                Vec::new()
            } else {
                script.remove(0)
            };
            Ok(Box::new(ScriptedPageSource {
                pages,
                cursor: 0,
                finished: false,
                closes: Arc::clone(&self.closes),
            }))
        }
    }

    /// Records how many columns the dynamic filter it was handed covers.
    struct FilterRecordingProvider {
        observed: Arc<Mutex<Option<usize>>>,
    }

    impl ConnectorReadPageSourceProvider for FilterRecordingProvider {
        fn create_page_source(
            &self,
            _session: &ConnectorSession,
            _table: &novarocks_spi::connector::read_stack::ConnectorReadTableHandle,
            _split: &novarocks_spi::connector::read_stack::ConnectorReadSplit,
            _scheduled_split_sequence_id: u64,
            _columns: &[novarocks_spi::connector::read_stack::Assignment<
                novarocks_spi::connector::read_stack::ConnectorReadColumnHandle,
            >],
            dynamic_filter: &Arc<ConnectorReadDynamicFilter>,
        ) -> Result<Box<dyn ConnectorPageSource>, ConnectorError> {
            *self.observed.lock().expect("observed lock") =
                Some(dynamic_filter.columns_covered().len());
            Ok(Box::new(ScriptedPageSource {
                pages: Vec::new(),
                cursor: 0,
                finished: false,
                closes: Arc::new(AtomicUsize::new(0)),
            }))
        }
    }

    /// A scan whose attempt installed no runtime filter.
    fn no_runtime_filter() -> RuntimeFilterSessionResolver {
        Arc::new(|| Ok(None))
    }

    fn descriptor() -> TypedConnectorReadDescriptor {
        let wire_scan = scan_source();
        let decoded_scan = test_support::decoded_scan();
        TypedConnectorReadDescriptor::new(
            decoded_scan.relation().table().clone(),
            decoded_scan.assignments().to_vec(),
            novarocks_native_adapter::runtime_filter_typed_scan::complete_all_scan_dynamic_filter(
                &wire_scan,
                &decoded_scan,
            ),
        )
    }

    fn live_dynamic_filter_factory() -> Arc<dyn TypedScanLiveDynamicFilterFactory> {
        novarocks_native_adapter::runtime_filter_typed_scan::typed_scan_live_dynamic_filter_factory(
            scan_source(),
            test_support::decoded_scan(),
        )
    }

    fn int_page(values: Vec<i64>) -> SourcePage {
        let positions = values.len();
        let column: ArrayRef = Arc::new(Int64Array::from(values));
        SourcePage::try_new(positions, vec![column]).expect("valid page")
    }

    fn scheduled_split(sequence_id: u64) -> ReceivedReadSplit {
        let (evidence, split) =
            test_support::decoded_scheduled_split(NODE, sequence_id).into_parts();
        ReceivedReadSplit::new(evidence.sequence_id(), evidence.plan_node_id(), split)
    }

    fn attempt_queues() -> Arc<TaskAttemptSplitQueues<ReceivedReadSplit>> {
        let registry = SplitQueueRegistry::new();
        registry.open_attempt(
            TaskAttemptKey::new(
                QueryExecutionId::new(QueryId::new(1, 2), AttemptId::new(1).expect("attempt"))
                    .expect("execution id"),
                UniqueId::new(3, 4),
            ),
            SplitQueueConfig::default(),
        )
    }

    fn request(cancellation: Arc<dyn ConnectorCancellation>) -> ConnectorRequestContext {
        ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(60),
            cancellation,
            1 << 20,
            1 << 22,
        )
        .expect("request context")
    }

    fn session() -> ConnectorSession {
        ConnectorSession::try_new("q-1", "test", "UTC", "en_US", SystemTime::UNIX_EPOCH)
            .expect("session")
    }

    fn source_with(
        provider: Arc<ScriptedProvider>,
        queues: Arc<TaskAttemptSplitQueues<ReceivedReadSplit>>,
        cancellation: Arc<dyn ConnectorCancellation>,
    ) -> TypedConnectorScanSource {
        TypedConnectorScanSource::new(
            descriptor(),
            provider,
            session(),
            request(cancellation),
            queues,
            NODE,
            vec![SlotId::new(1)],
            no_runtime_filter(),
            live_dynamic_filter_factory(),
            false,
        )
    }

    fn bind(source: &TypedConnectorScanSource) -> Arc<dyn ScanOp> {
        source
            .bind(BoundScanRanges::None)
            .expect("bind typed connector scan")
    }

    #[test]
    fn typed_scan_starts_with_zero_splits_and_does_not_end_the_stream() {
        let queues = attempt_queues();
        let source = source_with(
            ScriptedProvider::new(Vec::new()),
            Arc::clone(&queues),
            Arc::new(NeverCancelled),
        );
        let op = bind(&source);

        // Exactly one morsel, before any split exists: it is the driver that
        // will drain the queue. Reporting none would leave the splits enqueued
        // and unread, and the query would return zero rows while reporting
        // success everywhere.
        let morsels = op.build_morsels().expect("build morsels");
        assert_eq!(morsels.morsels.len(), 1);
        // The morsel set is final; it is the queue that grows.
        assert!(!morsels.has_more);
        assert!(!op.supports_incremental_scan_ranges());

        // The terminal marker alone is a clean, empty end of stream.
        queues
            .queue(NODE)
            .offer_splits(NODE, Vec::new(), true)
            .expect("terminal marker");
        let rows = op
            .execute_iter(ScanMorsel::OperatorDriven, None, None)
            .expect("driver")
            .collect::<Result<Vec<_>, _>>()
            .expect("drive an empty typed scan");
        assert!(rows.is_empty());
    }

    /// A system relation resolved to one backend has no split at all, so its
    /// scan must do its whole job in one morsel. A source that waited on a
    /// split queue here would park forever.
    struct ScriptedSystemTables {
        script: Mutex<Vec<Option<SourcePage>>>,
        opens: Arc<AtomicUsize>,
        closes: Arc<AtomicUsize>,
    }

    impl ConnectorReadSystemTableProvider for ScriptedSystemTables {
        fn create_system_page_source(
            &self,
            _session: &ConnectorSession,
            _table: &novarocks_spi::connector::read_stack::ConnectorReadTableHandle,
            _columns: &[novarocks_spi::connector::read_stack::Assignment<
                novarocks_spi::connector::read_stack::ConnectorReadColumnHandle,
            >],
        ) -> Result<Box<dyn ConnectorPageSource>, ConnectorError> {
            self.opens.fetch_add(1, Ordering::AcqRel);
            let pages = std::mem::take(&mut *self.script.lock().expect("script lock"));
            Ok(Box::new(ScriptedPageSource {
                pages,
                cursor: 0,
                finished: false,
                closes: Arc::clone(&self.closes),
            }))
        }
    }

    fn system_table_source(
        provider: Arc<ScriptedSystemTables>,
    ) -> TypedConnectorSystemTableScanSource {
        TypedConnectorSystemTableScanSource::new(
            descriptor(),
            provider,
            session(),
            request(Arc::new(NeverCancelled)),
            NODE,
            vec![SlotId::new(1)],
            false,
        )
    }

    #[test]
    fn a_system_relation_scan_reads_its_metadata_file_without_any_split() {
        let opens = Arc::new(AtomicUsize::new(0));
        let closes = Arc::new(AtomicUsize::new(0));
        let provider = Arc::new(ScriptedSystemTables {
            script: Mutex::new(vec![Some(int_page(vec![7, 8]))]),
            opens: Arc::clone(&opens),
            closes: Arc::clone(&closes),
        });
        let source = system_table_source(provider);
        let op = source
            .bind(BoundScanRanges::None)
            .expect("a system relation binds with no range");

        // One unit of work, known before execution: nothing can add more.
        let morsels = op.build_morsels().expect("build morsels");
        assert_eq!(morsels.morsels.len(), 1);
        assert!(!morsels.has_more);

        let rows = op
            .execute_iter(ScanMorsel::OperatorDriven, None, None)
            .expect("driver")
            .collect::<Result<Vec<_>, _>>()
            .expect("drain the metadata file");
        assert_eq!(
            rows.iter()
                .map(|chunk| chunk.batch.num_rows())
                .sum::<usize>(),
            2
        );
        assert_eq!(opens.load(Ordering::Acquire), 1, "opened exactly once");
        assert_eq!(closes.load(Ordering::Acquire), 1, "closed exactly once");
    }

    /// Its work unit is the relation itself, so a morsel that names a physical
    /// range belongs to some other scan and must not be silently accepted.
    #[test]
    fn a_system_relation_scan_refuses_a_morsel_it_does_not_own() {
        let provider = Arc::new(ScriptedSystemTables {
            script: Mutex::new(Vec::new()),
            opens: Arc::new(AtomicUsize::new(0)),
            closes: Arc::new(AtomicUsize::new(0)),
        });
        let source = system_table_source(provider);
        let op = source.bind(BoundScanRanges::None).expect("bind");
        let outcome = op.execute_iter(
            ScanMorsel::FileRange {
                path: "s3://bucket/f.parquet".to_string(),
                offset: 0,
                length: 1,
                file_len: 1,
                scan_range_id: 0,
                external_datacache: None,
            },
            None,
            None,
        );
        let error = match outcome {
            Ok(_) => panic!("a file-range morsel is not this scan's work unit"),
            Err(error) => error,
        };
        assert!(
            error.contains("file-range morsel"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn typed_scan_reads_a_split_that_arrives_after_the_driver_started() {
        let queues = attempt_queues();
        let provider = ScriptedProvider::new(vec![vec![Some(int_page(vec![1, 2, 3]))]]);
        let source = source_with(
            Arc::clone(&provider),
            Arc::clone(&queues),
            Arc::new(NeverCancelled),
        );
        let op = bind(&source);

        let mut iter = op
            .execute_iter(ScanMorsel::OperatorDriven, None, None)
            .expect("driver");
        // The driver parks on an empty queue; the split arrives only now.
        let offering = {
            let queues = Arc::clone(&queues);
            std::thread::spawn(move || {
                let queue = queues.queue(NODE);
                queue
                    .offer_splits(NODE, vec![scheduled_split(1)], true)
                    .expect("late split");
            })
        };

        let chunk = iter
            .next()
            .expect("the late split produces a chunk")
            .expect("chunk");
        assert_eq!(chunk.len(), 3);
        assert!(iter.next().is_none());
        offering.join().expect("offering thread");
        assert_eq!(provider.opens.load(Ordering::Acquire), 1);
        // One split, one page source, closed when the split finished.
        assert_eq!(provider.closes.load(Ordering::Acquire), 1);
    }

    #[test]
    fn typed_scan_treats_an_idle_page_as_not_end_of_stream() {
        let queues = attempt_queues();
        let provider = ScriptedProvider::new(vec![vec![
            None,
            Some(int_page(vec![10])),
            None,
            Some(int_page(vec![20, 30])),
        ]]);
        let source = source_with(
            Arc::clone(&provider),
            Arc::clone(&queues),
            Arc::new(NeverCancelled),
        );
        let op = bind(&source);
        queues
            .queue(NODE)
            .offer_splits(NODE, vec![scheduled_split(1)], true)
            .expect("one split");

        let rows = op
            .execute_iter(ScanMorsel::OperatorDriven, None, None)
            .expect("driver")
            .collect::<Result<Vec<_>, _>>()
            .expect("drive across idle turns");
        // Both pages arrive: the idle turns between them ended nothing.
        assert_eq!(
            rows.iter()
                .map(novarocks_execution::exec::chunk::Chunk::len)
                .sum::<usize>(),
            3
        );
    }

    #[test]
    fn typed_scan_reads_every_queued_split_before_exhaustion_ends_it() {
        let queues = attempt_queues();
        let provider = ScriptedProvider::new(vec![
            vec![Some(int_page(vec![1]))],
            vec![Some(int_page(vec![2, 3]))],
        ]);
        let source = source_with(
            Arc::clone(&provider),
            Arc::clone(&queues),
            Arc::new(NeverCancelled),
        );
        let op = bind(&source);
        queues
            .queue(NODE)
            .offer_splits(NODE, vec![scheduled_split(1), scheduled_split(2)], true)
            .expect("two splits");

        let rows = op
            .execute_iter(ScanMorsel::OperatorDriven, None, None)
            .expect("driver")
            .collect::<Result<Vec<_>, _>>()
            .expect("drive both splits");
        assert_eq!(rows.len(), 2);
        assert_eq!(provider.opens.load(Ordering::Acquire), 2);
        assert_eq!(provider.closes.load(Ordering::Acquire), 2);
        assert!(queues.queue(NODE).is_exhausted());
    }

    #[test]
    fn typed_scan_terminate_closes_the_page_source_and_the_queue_exactly_once() {
        let queues = attempt_queues();
        let provider =
            ScriptedProvider::new(vec![vec![Some(int_page(vec![1])), Some(int_page(vec![2]))]]);
        let source = source_with(
            Arc::clone(&provider),
            Arc::clone(&queues),
            Arc::new(NeverCancelled),
        );
        let op = bind(&source);
        let queue = queues.queue(NODE);
        let closes = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&closes);
        queue.observable().add_observer(Arc::new(move || {
            observed.fetch_add(1, Ordering::AcqRel);
        }));
        queue
            .offer_splits(NODE, vec![scheduled_split(1)], false)
            .expect("one split");
        let woken_by_offer = closes.load(Ordering::Acquire);

        let mut iter = op
            .execute_iter(ScanMorsel::OperatorDriven, None, None)
            .expect("driver");
        iter.next()
            .expect("first page")
            .expect("first page is a chunk");
        assert_eq!(provider.opens.load(Ordering::Acquire), 1);

        op.terminate().expect("terminate");
        op.terminate().expect("terminate is idempotent");
        op.terminate().expect("terminate is idempotent");
        assert_eq!(provider.closes.load(Ordering::Acquire), 1);
        assert_eq!(closes.load(Ordering::Acquire), woken_by_offer + 1);
        assert!(queue.is_closed());

        // After terminal the driver ends without another provider call.
        assert!(iter.next().is_none());
        assert_eq!(provider.opens.load(Ordering::Acquire), 1);
    }

    #[test]
    fn typed_scan_after_terminate_opens_no_new_page_source() {
        let queues = attempt_queues();
        let provider = ScriptedProvider::new(vec![vec![Some(int_page(vec![1]))]]);
        let source = source_with(
            Arc::clone(&provider),
            Arc::clone(&queues),
            Arc::new(NeverCancelled),
        );
        let op = bind(&source);
        queues
            .queue(NODE)
            .offer_splits(NODE, vec![scheduled_split(1)], true)
            .expect("one split");
        op.terminate().expect("terminate before any read");

        let rows = op
            .execute_iter(ScanMorsel::OperatorDriven, None, None)
            .expect("driver")
            .collect::<Result<Vec<_>, _>>()
            .expect("a terminated scan yields nothing");
        assert!(rows.is_empty());
        assert_eq!(provider.opens.load(Ordering::Acquire), 0);
    }

    #[test]
    fn typed_scan_fails_fast_on_a_cancelled_attempt() {
        let queues = attempt_queues();
        let provider = ScriptedProvider::new(vec![vec![Some(int_page(vec![1]))]]);
        let source = source_with(
            Arc::clone(&provider),
            Arc::clone(&queues),
            Arc::new(AlwaysCancelled),
        );
        let op = bind(&source);
        queues
            .queue(NODE)
            .offer_splits(NODE, vec![scheduled_split(1)], true)
            .expect("one split");

        let error = op
            .execute_iter(ScanMorsel::OperatorDriven, None, None)
            .expect("driver")
            .collect::<Result<Vec<_>, _>>()
            .expect_err("a cancelled attempt must not read");
        assert!(error.contains("cancelled"), "unexpected error: {error}");
        assert_eq!(provider.opens.load(Ordering::Acquire), 0);
    }

    #[test]
    fn typed_scan_rejects_a_morsel_it_does_not_own() {
        let source = source_with(
            ScriptedProvider::new(Vec::new()),
            attempt_queues(),
            Arc::new(NeverCancelled),
        );
        let op = bind(&source);
        assert!(
            op.execute_iter(ScanMorsel::ConnectorScanUnit { index: 0 }, None, None,)
                .is_err()
        );
        assert!(
            op.build_incremental_morsels(&[IncrementalScanRange::Empty { has_more: None }])
                .is_err()
        );
    }

    #[test]
    fn typed_scan_hands_the_substituted_dynamic_filter_to_the_provider() {
        let queues = attempt_queues();
        let observed = Arc::new(Mutex::new(None));
        let provider = Arc::new(FilterRecordingProvider {
            observed: Arc::clone(&observed),
        });
        // The seam: a backend-driven filter replaces the default one, and the
        // provider is handed exactly what was substituted.
        let covered = BTreeSet::from([test_support::decoded_scan().assignments()[0]
            .column()
            .clone()]);
        let source = TypedConnectorScanSource::new(
            descriptor(),
            provider,
            session(),
            request(Arc::new(NeverCancelled)),
            Arc::clone(&queues),
            NODE,
            vec![SlotId::new(1)],
            no_runtime_filter(),
            live_dynamic_filter_factory(),
            false,
        )
        .with_backend_dynamic_filter(Arc::new(CompleteAllDynamicFilter::new(covered)));
        let op = bind(&source);
        queues
            .queue(NODE)
            .offer_splits(NODE, vec![scheduled_split(1)], true)
            .expect("one split");

        let _ = op
            .execute_iter(ScanMorsel::OperatorDriven, None, None)
            .expect("driver")
            .collect::<Result<Vec<_>, _>>()
            .expect("drive the scan");
        assert_eq!(
            *observed.lock().expect("observed lock"),
            Some(1),
            "the provider must see the substituted filter's covered columns"
        );
    }

    #[test]
    fn typed_scan_dynamic_filter_covers_only_the_scans_bound_columns() {
        let mut proto = test_support::scan_source_proto();
        proto.dynamic_filters = vec![dto::DynamicFilterBinding {
            filter_id: 3,
            variable: "v0".to_owned(),
        }];
        let scan = ConnectorTableScanSource::parse(proto, FieldPath::root("scan"))
            .expect("valid typed scan source");
        let filter =
            novarocks_native_adapter::runtime_filter_typed_scan::complete_all_scan_dynamic_filter(
                &scan,
                &test_support::decoded_scan(),
            );
        assert_eq!(filter.columns_covered().len(), 1);
        // Truthful and unconstrained: never blocked, never awaitable.
        assert!(filter.current_predicate().is_all());
        assert!(filter.is_complete());
        assert!(!filter.is_awaitable());
        assert!(!filter.is_blocked());

        // A scan with no binding covers nothing at all.
        assert!(
            novarocks_native_adapter::runtime_filter_typed_scan::complete_all_scan_dynamic_filter(
                &scan_source(),
                &test_support::decoded_scan(),
            )
            .columns_covered()
            .is_empty()
        );
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
    provider: Arc<dyn ConnectorReadSystemTableProvider>,
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
        if self.request.cancellation().is_cancelled() {
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
        Ok(Arc::new(TypedConnectorSystemTableScanOp {
            shared: Arc::clone(&self.shared),
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
            .create_system_page_source(
                &self.shared.session,
                self.shared.descriptor.table(),
                self.shared.descriptor.assignments(),
            )
            .map_err(|error| format!("create typed system relation page source: {error}"))?;
        let adapter = ConnectorPageAdapter::new(self.shared.slot_ids.clone(), page_source);
        self.current = Some(self.sources.register(adapter, None, self.profile.clone())?);
        // No sequence: a system relation read has no split, and printing one
        // would be the first step toward asserting scheduling identity it does
        // not have.
        emit_page_source_marker(
            self.shared.emit_reader_markers,
            "NOVAROCKS_CONNECTOR_PAGE_SOURCE_OPEN",
            self.shared.plan_node_id,
            None,
        );
        if let Some(profile) = self.profile.as_ref() {
            profile.counter_add("TypedSystemTablePageSourcesOpened", ProfileUnit::Unit, 1);
        }
        Ok(())
    }

    fn close_current(&mut self) -> Result<(), String> {
        match self.current.take() {
            Some(source) => {
                let closed = source.close();
                emit_page_source_marker(
                    self.shared.emit_reader_markers,
                    "NOVAROCKS_CONNECTOR_PAGE_SOURCE_CLOSE",
                    self.shared.plan_node_id,
                    None,
                );
                closed
            }
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
