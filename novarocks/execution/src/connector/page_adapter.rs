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
//! - Turns each page a scan's page stream produces into a `Chunk` bound to
//!   the scan's ordered output slot ids.
//! - Keeps the distinction the page contract makes and a naive adapter would
//!   lose: a page with zero channels and a positive position count is a real
//!   result, never end of stream.
//!
//! Key exported interfaces:
//! - Types: `SourcePageConverter`, `PageAdapterError`, `PageAdapterErrorKind`.
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
use novarocks_spi::connector::read_stack::SourcePage;
use novarocks_types::SlotId;

use crate::exec::chunk::{Chunk, ChunkSchema, ChunkSchemaRef, ChunkSlotSchema};

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

/// Build one chunk from one page and the scan's ordered output slot ids.
///
/// `slot_ids[i]` names channel `i`. Channels beyond the bound prefix are the
/// provider's own working columns and are dropped.
pub fn source_page_to_chunk(
    page: SourcePage,
    slot_ids: &[SlotId],
) -> Result<Chunk, PageAdapterError> {
    let mut schema: Option<ChunkSchemaRef> = None;
    convert_page(page, slot_ids, &mut schema)
}

/// Converts the pages of one scan stream into chunks, reusing the chunk
/// schema of the last page while the page shape does not change.
///
/// `slot_ids[i]` names channel `i`. Channels beyond the bound prefix are the
/// provider's own working columns and are dropped. A page's accounted output
/// memory moves onto its chunk.
pub struct SourcePageConverter {
    slot_ids: Vec<SlotId>,
    schema: Option<ChunkSchemaRef>,
}

impl SourcePageConverter {
    pub fn new(slot_ids: Vec<SlotId>) -> Self {
        Self {
            slot_ids,
            schema: None,
        }
    }

    pub fn convert(&mut self, page: SourcePage) -> Result<Chunk, PageAdapterError> {
        convert_page(page, &self.slot_ids, &mut self.schema)
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

    // Discard provider working channels before extracting the page. The
    // accounted extraction moves the provider reservation together with the
    // visible Arrow buffers.
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
    use std::sync::atomic::{AtomicU64, Ordering};

    use arrow::array::{Array, Int64Array};
    use novarocks_spi::connector::{
        ConnectorError, ConnectorExecutionResources, ConnectorResourceCheckpoint,
        ConnectorResourceClass, ConnectorResourceLease, ConnectorResourceLedger,
    };

    use super::*;
    use crate::runtime::mem_tracker::MemTracker;

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

    fn int_page(values: Vec<i64>) -> SourcePage {
        let positions = values.len();
        let column: ArrayRef = Arc::new(Int64Array::from(values));
        SourcePage::try_new(positions, vec![column]).expect("valid page")
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
        let resources = ConnectorExecutionResources::from_admitted_ledger(Arc::new(OutputLedger {
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
        let tracker = MemTracker::new_root("converted connector chunk");
        let mut chunk = SourcePageConverter::new(vec![SlotId::new(1)])
            .convert(page)
            .expect("convert accounted page");
        chunk.transfer_to(&tracker);
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
        let resources = ConnectorExecutionResources::from_admitted_ledger(Arc::new(OutputLedger {
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
        let chunk = source_page_to_chunk(page, &[SlotId::new(1)]).expect("convert projected page");
        assert_eq!(retained.load(Ordering::Acquire), visible_bytes);
        drop(chunk);
        assert_eq!(retained.load(Ordering::Acquire), 0);
    }

    #[test]
    fn accounted_count_only_projection_releases_the_complete_page_charge() {
        let retained = Arc::new(AtomicU64::new(0));
        let resources = ConnectorExecutionResources::from_admitted_ledger(Arc::new(OutputLedger {
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
        let chunk = source_page_to_chunk(page, &[]).expect("convert count-only page");
        assert_eq!(chunk.len(), 3);
        assert_eq!(retained.load(Ordering::Acquire), 0);
    }

    #[test]
    fn an_unbound_trailing_channel_is_dropped() {
        let visible: ArrayRef = Arc::new(Int64Array::from(vec![1_i64, 2]));
        let hidden: ArrayRef = Arc::new(Int64Array::from(vec![5_i64, 6]));
        let page = SourcePage::try_new(2, vec![visible, hidden]).expect("valid page");

        let chunk = source_page_to_chunk(page, &[SlotId::new(1)]).expect("chunk");
        assert_eq!(chunk.columns().len(), 1);
        assert_eq!(chunk.len(), 2);
    }

    #[test]
    fn binding_more_channels_than_the_page_carries_is_rejected() {
        let error = source_page_to_chunk(SourcePage::zero_channel(4), &[SlotId::new(1)])
            .expect_err("channel mismatch");
        assert_eq!(error.kind(), PageAdapterErrorKind::ChannelMismatch);
    }

    #[test]
    fn the_derived_schema_is_reused_while_the_arrow_types_hold() {
        let mut converter = SourcePageConverter::new(vec![SlotId::new(1)]);
        let first = converter
            .convert(int_page(vec![1]))
            .expect("first chunk")
            .chunk_schema_ref();
        let second = converter
            .convert(int_page(vec![2, 3]))
            .expect("second chunk")
            .chunk_schema_ref();
        assert!(Arc::ptr_eq(&first, &second));
    }
}
