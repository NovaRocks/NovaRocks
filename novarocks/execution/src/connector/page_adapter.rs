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

//! The connector `SourcePage` to execution `Chunk` boundary.
//!
//! Responsibilities:
//! - Owns one `ConnectorPageSource` and turns each page it produces into a
//!   `Chunk` bound to the scan's ordered output slot ids.
//! - Keeps the two distinctions the page contract makes and a naive adapter
//!   would lose: `None` is "nothing right now", not end of stream, and a page
//!   with zero channels and a positive position count is a real result.
//!
//! Key exported interfaces:
//! - Types: `ConnectorPageAdapter`, `PageConversion`, `PageAdapterError`,
//!   `PageAdapterErrorKind`.
//! - Functions: `source_page_to_chunk`.
//!
//! Current limitations:
//! - The Arrow field of each output column is derived from the array the
//!   connector materialized. This adapter carries no independent type
//!   declaration to check it against, so a provider that changes a column's
//!   Arrow type between pages produces chunks whose schema changes with it.
//!
//! Provider neutrality: nothing here names a provider or inspects a provider
//! variant, so this file compiles with no provider crate in the dependency
//! graph.

use std::sync::Arc;

use arrow::array::{ArrayRef, RecordBatch, RecordBatchOptions};
use arrow::datatypes::Field;

use novarocks_spi::connector::ConnectorError;
use novarocks_spi::connector::read_stack::{ConnectorPageSource, PageSourceMetrics, SourcePage};
use novarocks_types::SlotId;

use crate::exec::chunk::{Chunk, ChunkSchema, ChunkSchemaRef, ChunkSlotSchema};
use crate::runtime::mem_tracker::MemTracker;

/// Why one page could not become a chunk.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum PageAdapterErrorKind {
    /// The page carries fewer channels than the output binds.
    ChannelMismatch,
    /// A materialized channel does not agree with the page's position count.
    PositionMismatch,
    /// The connector failed while producing, materializing, or closing.
    Connector,
    /// The materialized columns could not form a chunk.
    Chunk,
}

impl std::fmt::Display for PageAdapterErrorKind {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::ChannelMismatch => "channel mismatch",
            Self::PositionMismatch => "position mismatch",
            Self::Connector => "connector failure",
            Self::Chunk => "chunk construction failure",
        })
    }
}

/// A typed page-conversion failure.
///
/// A connector failure keeps its original [`ConnectorError`], because the
/// caller's fail-fast policy depends on its kind: a cancellation and corrupt
/// data are not the same event.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PageAdapterError {
    kind: PageAdapterErrorKind,
    detail: String,
    connector_error: Option<ConnectorError>,
}

impl PageAdapterError {
    pub fn new(kind: PageAdapterErrorKind, detail: impl Into<String>) -> Self {
        Self {
            kind,
            detail: detail.into(),
            connector_error: None,
        }
    }

    pub fn from_connector(error: ConnectorError, context: &str) -> Self {
        Self {
            kind: PageAdapterErrorKind::Connector,
            detail: format!("{context}: {error}"),
            connector_error: Some(error),
        }
    }

    pub const fn kind(&self) -> PageAdapterErrorKind {
        self.kind
    }

    pub fn detail(&self) -> &str {
        &self.detail
    }

    /// The underlying connector failure, when the adapter did not originate it.
    pub const fn connector_error(&self) -> Option<&ConnectorError> {
        self.connector_error.as_ref()
    }
}

impl std::fmt::Display for PageAdapterError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}: {}", self.kind, self.detail)
    }
}

impl std::error::Error for PageAdapterError {}

/// What one pull produced.
///
/// Three states, not an `Option`: a page source that has nothing right now is
/// not finished, and conflating the two would end a scan that is merely waiting
/// on I/O.
#[derive(Debug)]
pub enum PageConversion {
    /// One chunk. A chunk with zero columns and a positive row count is the
    /// legal result of a count-only or partition-only scan.
    Chunk(Chunk),
    /// No page right now. The source has not finished; the caller retries.
    Idle,
    /// The source reported termination and produced no further page.
    Finished,
}

/// Build one chunk from one page and the scan's ordered output slot ids.
///
/// `slot_ids[i]` names channel `i`. Channels beyond the bound prefix are the
/// provider's own working columns and are never materialized.
pub fn source_page_to_chunk(
    page: SourcePage,
    slot_ids: &[SlotId],
) -> Result<Chunk, PageAdapterError> {
    let mut schema: Option<ChunkSchemaRef> = None;
    convert_page(page, slot_ids, &mut schema)
}

/// Owns one connector page source and converts its pages into chunks.
pub struct ConnectorPageAdapter {
    slot_ids: Vec<SlotId>,
    /// `None` once closed, which is what makes closing idempotent.
    source: Option<Box<dyn ConnectorPageSource>>,
    /// Reused while the connector keeps producing the same Arrow types.
    schema: Option<ChunkSchemaRef>,
    mem_tracker: Option<Arc<MemTracker>>,
    metrics: PageSourceMetrics,
    source_memory_usage_bytes: u64,
}

impl ConnectorPageAdapter {
    pub fn new(slot_ids: Vec<SlotId>, source: Box<dyn ConnectorPageSource>) -> Self {
        Self {
            slot_ids,
            source: Some(source),
            schema: None,
            mem_tracker: None,
            metrics: PageSourceMetrics::default(),
            source_memory_usage_bytes: 0,
        }
    }

    /// Charge produced chunks to this tracker, matching how the scan async
    /// queue accounts for buffered chunks.
    pub fn attach_mem_tracker(&mut self, tracker: Arc<MemTracker>) {
        self.mem_tracker = Some(tracker);
    }

    pub fn slot_ids(&self) -> &[SlotId] {
        &self.slot_ids
    }

    /// Counters as of the last pull.
    pub const fn metrics(&self) -> PageSourceMetrics {
        self.metrics
    }

    /// Bytes the source held as of the last pull.
    pub const fn source_memory_usage_bytes(&self) -> u64 {
        self.source_memory_usage_bytes
    }

    /// Whether the source is waiting on external work. A closed adapter is not
    /// blocked; it is done.
    pub fn source_is_blocked(&self) -> bool {
        self.source
            .as_ref()
            .is_some_and(|source| source.is_blocked())
    }

    /// Whether the source reported termination. Only this is end of stream.
    pub fn source_is_finished(&self) -> bool {
        self.source
            .as_ref()
            .is_none_or(|source| source.is_finished())
    }

    /// Pull at most one page and convert it.
    pub fn pull(&mut self) -> Result<PageConversion, PageAdapterError> {
        let Some(source) = self.source.as_mut() else {
            return Ok(PageConversion::Finished);
        };
        let page = source.next_source_page();
        // Counters are refreshed on every pull, including an empty one: an idle
        // source still reports the time it spent waiting. Refresh before
        // propagating an error as well, because failed I/O may have advanced
        // physical reader counters.
        self.metrics = source.metrics();
        self.source_memory_usage_bytes = source.memory_usage_bytes();
        let finished = source.is_finished();
        let page =
            page.map_err(|error| PageAdapterError::from_connector(error, "connector page source"))?;

        match page {
            Some(page) => {
                let chunk = convert_page(page, &self.slot_ids, &mut self.schema)?;
                Ok(PageConversion::Chunk(self.track(chunk)))
            }
            // `None` is "nothing right now". Only `is_finished` terminates.
            None if finished => Ok(PageConversion::Finished),
            None => Ok(PageConversion::Idle),
        }
    }

    /// Convert a page the caller already holds, without pulling.
    pub fn convert(&mut self, page: SourcePage) -> Result<Chunk, PageAdapterError> {
        let chunk = convert_page(page, &self.slot_ids, &mut self.schema)?;
        Ok(self.track(chunk))
    }

    /// Close the source. Idempotent, and safe after an error or a cancellation.
    pub fn close(&mut self) -> Result<(), PageAdapterError> {
        let Some(mut source) = self.source.take() else {
            return Ok(());
        };
        let result = source.close();
        // Close may retire the final underlying file reader, so its terminal
        // snapshot is authoritative even when close itself reports an error.
        self.metrics = source.metrics();
        self.source_memory_usage_bytes = source.memory_usage_bytes();
        result
            .map_err(|error| PageAdapterError::from_connector(error, "connector page source close"))
    }

    fn track(&self, mut chunk: Chunk) -> Chunk {
        if let Some(tracker) = self.mem_tracker.as_ref() {
            chunk.transfer_to(tracker);
        }
        chunk
    }
}

impl Drop for ConnectorPageAdapter {
    fn drop(&mut self) {
        // A dropped adapter must still release the source's resources. The
        // close result has nowhere to go here, so it is deliberately ignored;
        // callers that need the error call `close` themselves.
        let _ = self.close();
    }
}

fn convert_page(
    mut page: SourcePage,
    slot_ids: &[SlotId],
    schema_cache: &mut Option<ChunkSchemaRef>,
) -> Result<Chunk, PageAdapterError> {
    let positions = page.position_count();
    let channel_count = page.channel_count();
    if slot_ids.len() > channel_count {
        return Err(PageAdapterError::new(
            PageAdapterErrorKind::ChannelMismatch,
            format!(
                "scan output binds {} channels but the page carries {channel_count}",
                slot_ids.len()
            ),
        ));
    }

    // Discard provider working channels before extracting the page so an
    // unbound lazy channel is never materialized. The accounted extraction
    // moves the provider reservation together with the visible Arrow buffers.
    page.truncate_channels(slot_ids.len()).map_err(|error| {
        PageAdapterError::from_connector(error, "connector page channel projection")
    })?;
    let (extracted_positions, columns, output_memory) =
        page.into_accounted_columns().map_err(|error| {
            PageAdapterError::from_connector(error, "connector page channel materialization")
        })?;
    debug_assert_eq!(positions, extracted_positions);
    for (index, column) in columns.iter().enumerate() {
        if column.len() != positions {
            return Err(PageAdapterError::new(
                PageAdapterErrorKind::PositionMismatch,
                format!(
                    "channel {index} produced {} values for a page of {positions} positions",
                    column.len()
                ),
            ));
        }
    }

    let schema = chunk_schema_for(slot_ids, &columns, schema_cache)?;
    let batch = if columns.is_empty() {
        // A page with no channels still reports rows. Arrow only keeps that row
        // count when it is stated explicitly, so a count-only scan would
        // silently become an empty chunk without this branch.
        let options = RecordBatchOptions::new().with_row_count(Some(positions));
        RecordBatch::try_new_with_options(schema.arrow_schema_ref(), Vec::new(), &options)
    } else {
        RecordBatch::try_new(schema.arrow_schema_ref(), columns)
    }
    .map_err(|error| {
        PageAdapterError::new(
            PageAdapterErrorKind::Chunk,
            format!("connector page record batch failed: {error}"),
        )
    })?;

    let output_memory = if let Some(mut output_memory) = output_memory {
        let visible_bytes = batch
            .columns()
            .iter()
            .map(|column| column.get_array_memory_size() as u64)
            .fold(0_u64, u64::saturating_add);
        output_memory.shrink_to(visible_bytes).map_err(|error| {
            PageAdapterError::from_connector(error, "connector page visible output accounting")
        })?;
        if visible_bytes > 0 {
            Some(output_memory)
        } else {
            None
        }
    } else {
        None
    };
    let mut chunk = Chunk::try_new_with_chunk_schema(batch, schema)
        .map_err(|error| PageAdapterError::new(PageAdapterErrorKind::Chunk, error))?;
    if let Some(output_memory) = output_memory {
        chunk
            .attach_connector_output_memory(output_memory)
            .map_err(|error| PageAdapterError::new(PageAdapterErrorKind::Chunk, error))?;
    }
    Ok(chunk)
}

fn chunk_schema_for(
    slot_ids: &[SlotId],
    columns: &[ArrayRef],
    cache: &mut Option<ChunkSchemaRef>,
) -> Result<ChunkSchemaRef, PageAdapterError> {
    if let Some(cached) = cache.as_ref()
        && cached.slots().len() == columns.len()
        && cached
            .slots()
            .iter()
            .zip(columns)
            .all(|(slot, column)| slot.data_type() == column.data_type())
    {
        return Ok(Arc::clone(cached));
    }

    let slots = slot_ids
        .iter()
        .zip(columns)
        .map(|(slot_id, column)| {
            // Nullable, because the page carries no nullability declaration:
            // a nullable Arrow field accepts an array with or without nulls,
            // while the reverse would reject a legal page outright.
            let field = Field::new(format!("slot_{slot_id}"), column.data_type().clone(), true);
            ChunkSlotSchema::try_new_with_field(*slot_id, field, None, None)
        })
        .collect::<Result<Vec<_>, String>>()
        .map_err(|error| PageAdapterError::new(PageAdapterErrorKind::Chunk, error))?;
    let schema = Arc::new(
        ChunkSchema::try_new(slots)
            .map_err(|error| PageAdapterError::new(PageAdapterErrorKind::Chunk, error))?,
    );
    *cache = Some(Arc::clone(&schema));
    Ok(schema)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

    use arrow::array::{Array, Int64Array};
    use novarocks_spi::connector::read_stack::LazyBlockLoader;
    use novarocks_spi::connector::{
        ConnectorError, ConnectorErrorKind, ConnectorRequestResources, ConnectorResourceCheckpoint,
        ConnectorResourceClass, ConnectorResourceLease, ConnectorResourceLedger,
    };

    use super::*;

    /// A lazy channel that records how often it was materialized.
    struct CountingLoader {
        loads: Arc<AtomicUsize>,
        values: Vec<i64>,
    }

    struct OutputLedger {
        retained: Arc<AtomicU64>,
    }

    impl ConnectorResourceLedger for OutputLedger {
        fn checkpoint(&self) -> Result<ConnectorResourceCheckpoint, ConnectorError> {
            Ok(ConnectorResourceCheckpoint::new(0))
        }

        fn try_reserve(
            &self,
            class: ConnectorResourceClass,
            bytes: u64,
        ) -> Result<Box<dyn ConnectorResourceLease>, ConnectorError> {
            assert_eq!(class, ConnectorResourceClass::ReaderOutput);
            self.retained.fetch_add(bytes, Ordering::AcqRel);
            Ok(Box::new(OutputLease {
                retained: Arc::clone(&self.retained),
                bytes,
            }))
        }
    }

    struct OutputLease {
        retained: Arc<AtomicU64>,
        bytes: u64,
    }

    impl ConnectorResourceLease for OutputLease {
        fn bytes(&self) -> u64 {
            self.bytes
        }

        fn try_grow(&mut self, additional: u64) -> Result<(), ConnectorError> {
            self.retained.fetch_add(additional, Ordering::AcqRel);
            self.bytes += additional;
            Ok(())
        }

        fn shrink_to(&mut self, bytes: u64) -> Result<(), ConnectorError> {
            assert!(bytes <= self.bytes);
            self.retained
                .fetch_sub(self.bytes - bytes, Ordering::AcqRel);
            self.bytes = bytes;
            Ok(())
        }
    }

    impl Drop for OutputLease {
        fn drop(&mut self) {
            self.retained.fetch_sub(self.bytes, Ordering::AcqRel);
        }
    }

    impl LazyBlockLoader for CountingLoader {
        fn load(&mut self) -> Result<ArrayRef, ConnectorError> {
            self.loads.fetch_add(1, Ordering::AcqRel);
            Ok(Arc::new(Int64Array::from(self.values.clone())))
        }

        fn retained_size_in_bytes(&self) -> u64 {
            (self.values.len() * size_of::<i64>()) as u64
        }
    }

    /// A scripted page source. `None` entries are idle turns, not termination.
    struct ScriptedSource {
        pages: Vec<Option<SourcePage>>,
        cursor: usize,
        finished: bool,
        blocked: bool,
        fail_next: Option<ConnectorError>,
        closes: Arc<AtomicUsize>,
        metrics: PageSourceMetrics,
        memory_usage_bytes: u64,
    }

    impl ScriptedSource {
        fn new(pages: Vec<Option<SourcePage>>, closes: Arc<AtomicUsize>) -> Self {
            Self {
                pages,
                cursor: 0,
                finished: false,
                blocked: false,
                fail_next: None,
                closes,
                metrics: PageSourceMetrics::default(),
                memory_usage_bytes: 0,
            }
        }
    }

    impl ConnectorPageSource for ScriptedSource {
        fn next_source_page(&mut self) -> Result<Option<SourcePage>, ConnectorError> {
            if let Some(error) = self.fail_next.take() {
                return Err(error);
            }
            if self.cursor >= self.pages.len() {
                self.finished = true;
                return Ok(None);
            }
            let page = self.pages[self.cursor].take();
            self.cursor += 1;
            self.metrics.completed_positions += page
                .as_ref()
                .map(|page| page.position_count() as u64)
                .unwrap_or(0);
            self.metrics.read_time_nanos += 1;
            self.memory_usage_bytes = 4096;
            if self.cursor >= self.pages.len() && page.is_some() {
                // The last scripted page still leaves the source unfinished, so
                // the caller must ask again to learn that it ended.
                self.finished = false;
            }
            Ok(page)
        }

        fn is_finished(&self) -> bool {
            self.finished
        }

        fn is_blocked(&self) -> bool {
            self.blocked
        }

        fn metrics(&self) -> PageSourceMetrics {
            self.metrics
        }

        fn memory_usage_bytes(&self) -> u64 {
            self.memory_usage_bytes
        }

        fn close(&mut self) -> Result<(), ConnectorError> {
            self.closes.fetch_add(1, Ordering::AcqRel);
            Ok(())
        }
    }

    fn int_page(values: Vec<i64>) -> SourcePage {
        let positions = values.len();
        let column: ArrayRef = Arc::new(Int64Array::from(values));
        SourcePage::try_new(positions, vec![column]).expect("valid page")
    }

    fn adapter(
        slot_ids: Vec<SlotId>,
        pages: Vec<Option<SourcePage>>,
    ) -> (ConnectorPageAdapter, Arc<AtomicUsize>) {
        let closes = Arc::new(AtomicUsize::new(0));
        let source = ScriptedSource::new(pages, Arc::clone(&closes));
        (
            ConnectorPageAdapter::new(slot_ids, Box::new(source)),
            closes,
        )
    }

    #[test]
    fn a_zero_channel_page_converts_to_a_row_counted_chunk() {
        let chunk = source_page_to_chunk(SourcePage::zero_channel(1024), &[]).expect("chunk");
        assert_eq!(chunk.len(), 1024);
        assert_eq!(chunk.columns().len(), 0);
        // It is a real result, never end of stream.
        assert!(!chunk.is_empty());
    }

    #[test]
    fn accounted_page_moves_one_existing_charge_to_the_chunk() {
        let retained = Arc::new(AtomicU64::new(0));
        let resources = ConnectorRequestResources::new(Arc::new(OutputLedger {
            retained: Arc::clone(&retained),
        }));
        let column: ArrayRef = Arc::new(Int64Array::from(vec![1_i64, 2, 3]));
        let bytes = column.get_array_memory_size() as u64;
        let output_memory = resources
            .try_reserve(ConnectorResourceClass::ReaderOutput, bytes)
            .expect("reserve provider output")
            .into_output(bytes)
            .expect("freeze provider output charge");
        let page = SourcePage::try_new_accounted(3, vec![column], output_memory)
            .expect("accounted source page");
        let (mut adapter, _) = adapter(vec![SlotId::new(1)], vec![Some(page)]);
        let tracker = MemTracker::new_root("converted connector chunk");
        adapter.attach_mem_tracker(Arc::clone(&tracker));

        let chunk = match adapter.pull().expect("convert accounted page") {
            PageConversion::Chunk(chunk) => chunk,
            PageConversion::Idle | PageConversion::Finished => panic!("expected a chunk"),
        };
        assert_eq!(retained.load(Ordering::Acquire), bytes);
        assert_eq!(tracker.current(), 0, "the page must not be charged twice");

        let shared = chunk.clone();
        drop(chunk);
        assert_eq!(retained.load(Ordering::Acquire), bytes);
        drop(shared);
        assert_eq!(retained.load(Ordering::Acquire), 0);
        assert_eq!(tracker.current(), 0);
    }

    #[test]
    fn accounted_page_releases_hidden_materialized_channels_before_chunk_handoff() {
        let retained = Arc::new(AtomicU64::new(0));
        let resources = ConnectorRequestResources::new(Arc::new(OutputLedger {
            retained: Arc::clone(&retained),
        }));
        let visible: ArrayRef = Arc::new(Int64Array::from(vec![1_i64, 2, 3]));
        let hidden: ArrayRef = Arc::new(Int64Array::from((0_i64..1024).collect::<Vec<_>>()));
        let hidden = hidden.slice(0, 3);
        let visible_bytes = visible.get_array_memory_size() as u64;
        let total_bytes = visible_bytes + hidden.get_array_memory_size() as u64;
        let output_memory = resources
            .try_reserve(ConnectorResourceClass::ReaderOutput, total_bytes)
            .expect("reserve visible and hidden provider output")
            .into_output(total_bytes)
            .expect("freeze full provider output charge");
        let page = SourcePage::try_new_accounted(3, vec![visible, hidden], output_memory)
            .expect("accounted source page");
        let (mut adapter, _) = adapter(vec![SlotId::new(1)], vec![Some(page)]);

        let chunk = match adapter.pull().expect("convert projected page") {
            PageConversion::Chunk(chunk) => chunk,
            PageConversion::Idle | PageConversion::Finished => panic!("expected a chunk"),
        };
        assert_eq!(retained.load(Ordering::Acquire), visible_bytes);
        drop(chunk);
        assert_eq!(retained.load(Ordering::Acquire), 0);
    }

    #[test]
    fn accounted_count_only_projection_releases_the_complete_page_charge() {
        let retained = Arc::new(AtomicU64::new(0));
        let resources = ConnectorRequestResources::new(Arc::new(OutputLedger {
            retained: Arc::clone(&retained),
        }));
        let hidden: ArrayRef = Arc::new(Int64Array::from(vec![1_i64, 2, 3]));
        let bytes = hidden.get_array_memory_size() as u64;
        let output_memory = resources
            .try_reserve(ConnectorResourceClass::ReaderOutput, bytes)
            .expect("reserve hidden provider output")
            .into_output(bytes)
            .expect("freeze hidden provider output charge");
        let page = SourcePage::try_new_accounted(3, vec![hidden], output_memory)
            .expect("accounted source page");
        let (mut adapter, _) = adapter(Vec::new(), vec![Some(page)]);

        let chunk = match adapter.pull().expect("convert count-only page") {
            PageConversion::Chunk(chunk) => chunk,
            PageConversion::Idle | PageConversion::Finished => panic!("expected a chunk"),
        };
        assert_eq!(chunk.len(), 3);
        assert_eq!(retained.load(Ordering::Acquire), 0);
    }

    #[test]
    fn a_none_page_before_termination_is_idle_not_end_of_stream() {
        let (mut adapter, _) = adapter(vec![SlotId::new(1)], vec![None, Some(int_page(vec![7]))]);
        assert!(matches!(
            adapter.pull().expect("idle"),
            PageConversion::Idle
        ));
        assert!(!adapter.source_is_finished());
        match adapter.pull().expect("chunk") {
            PageConversion::Chunk(chunk) => assert_eq!(chunk.len(), 1),
            PageConversion::Idle | PageConversion::Finished => {
                panic!("the scripted page must convert")
            }
        }
    }

    #[test]
    fn a_none_page_after_termination_is_finished() {
        let (mut adapter, _) = adapter(vec![SlotId::new(1)], vec![Some(int_page(vec![1, 2]))]);
        assert!(matches!(
            adapter.pull().expect("chunk"),
            PageConversion::Chunk(_)
        ));
        assert!(matches!(
            adapter.pull().expect("finished"),
            PageConversion::Finished
        ));
        assert!(adapter.source_is_finished());
    }

    #[test]
    fn a_bound_lazy_channel_materializes_exactly_once() {
        let loads = Arc::new(AtomicUsize::new(0));
        let mut page = SourcePage::zero_channel(3);
        page.push_lazy_channel(Box::new(CountingLoader {
            loads: Arc::clone(&loads),
            values: vec![10, 20, 30],
        }));

        let chunk = source_page_to_chunk(page, &[SlotId::new(4)]).expect("chunk");
        assert_eq!(loads.load(Ordering::Acquire), 1);
        assert_eq!(chunk.len(), 3);
        let column = chunk
            .column_by_slot_id(SlotId::new(4))
            .expect("column by slot");
        assert_eq!(column.len(), 3);
    }

    #[test]
    fn an_unbound_trailing_lazy_channel_is_never_materialized() {
        let loads = Arc::new(AtomicUsize::new(0));
        let visible: ArrayRef = Arc::new(Int64Array::from(vec![1_i64, 2]));
        let mut page = SourcePage::try_new(2, vec![visible]).expect("valid page");
        page.push_lazy_channel(Box::new(CountingLoader {
            loads: Arc::clone(&loads),
            values: vec![5, 6],
        }));

        let chunk = source_page_to_chunk(page, &[SlotId::new(1)]).expect("chunk");
        assert_eq!(chunk.columns().len(), 1);
        assert_eq!(loads.load(Ordering::Acquire), 0);
    }

    #[test]
    fn binding_more_channels_than_the_page_carries_is_rejected() {
        let error = source_page_to_chunk(SourcePage::zero_channel(4), &[SlotId::new(1)])
            .expect_err("channel mismatch");
        assert_eq!(error.kind(), PageAdapterErrorKind::ChannelMismatch);
    }

    #[test]
    fn the_derived_schema_is_reused_while_the_arrow_types_hold() {
        let (mut adapter, _) = adapter(
            vec![SlotId::new(1)],
            vec![Some(int_page(vec![1])), Some(int_page(vec![2, 3]))],
        );
        let first = match adapter.pull().expect("first chunk") {
            PageConversion::Chunk(chunk) => chunk.chunk_schema_ref(),
            PageConversion::Idle | PageConversion::Finished => panic!("expected a chunk"),
        };
        let second = match adapter.pull().expect("second chunk") {
            PageConversion::Chunk(chunk) => chunk.chunk_schema_ref(),
            PageConversion::Idle | PageConversion::Finished => panic!("expected a chunk"),
        };
        assert!(Arc::ptr_eq(&first, &second));
    }

    #[test]
    fn metrics_and_memory_usage_follow_the_source() {
        let (mut adapter, _) = adapter(vec![SlotId::new(1)], vec![Some(int_page(vec![1, 2, 3]))]);
        assert_eq!(adapter.metrics(), PageSourceMetrics::default());
        assert!(matches!(
            adapter.pull().expect("chunk"),
            PageConversion::Chunk(_)
        ));
        assert_eq!(adapter.metrics().completed_positions, 3);
        assert_eq!(adapter.source_memory_usage_bytes(), 4096);
        assert!(!adapter.source_is_blocked());
    }

    #[test]
    fn close_is_idempotent_and_safe_after_an_error() {
        let closes = Arc::new(AtomicUsize::new(0));
        let mut source = ScriptedSource::new(vec![Some(int_page(vec![1]))], Arc::clone(&closes));
        source.fail_next = Some(ConnectorError::new(
            ConnectorErrorKind::Unavailable,
            "object store unavailable",
        ));
        let mut adapter = ConnectorPageAdapter::new(vec![SlotId::new(1)], Box::new(source));

        let error = adapter.pull().expect_err("the scripted failure surfaces");
        assert_eq!(error.kind(), PageAdapterErrorKind::Connector);
        assert_eq!(
            error.connector_error().map(ConnectorError::kind),
            Some(ConnectorErrorKind::Unavailable)
        );

        adapter.close().expect("first close");
        adapter.close().expect("second close");
        adapter.close().expect("third close");
        assert_eq!(closes.load(Ordering::Acquire), 1);

        // A closed adapter is finished rather than idle, and stays pullable
        // without panicking.
        assert!(matches!(
            adapter.pull().expect("finished"),
            PageConversion::Finished
        ));
        assert!(adapter.source_is_finished());
    }

    #[test]
    fn dropping_the_adapter_closes_the_source() {
        let closes = Arc::new(AtomicUsize::new(0));
        {
            let source = ScriptedSource::new(Vec::new(), Arc::clone(&closes));
            let _adapter = ConnectorPageAdapter::new(Vec::new(), Box::new(source));
        }
        assert_eq!(closes.load(Ordering::Acquire), 1);
    }
}
