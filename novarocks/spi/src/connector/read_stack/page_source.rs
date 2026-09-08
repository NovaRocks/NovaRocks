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

//! Page production: `SourcePage`, `ConnectorPageSource`, and its provider.
//!
//! A page source reads exactly one split. The engine adapter converts a
//! `SourcePage` into an Arrow `Chunk` after the connector has produced it, so
//! slot layout never leaks into the connector.

use std::fmt::Debug;

use arrow::array::ArrayRef;

use crate::connector::{ConnectorError, ConnectorErrorKind, ConnectorOutputMemoryToken};

/// A column whose materialization can be deferred until it is read.
pub trait LazyBlockLoader: Send {
    fn load(&mut self) -> Result<ArrayRef, ConnectorError>;

    /// Best-effort retained size before materialization.
    fn retained_size_in_bytes(&self) -> u64 {
        0
    }
}

enum PageChannel {
    Materialized(ArrayRef),
    Lazy(Box<dyn LazyBlockLoader>),
}

impl Debug for PageChannel {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Materialized(array) => formatter
                .debug_struct("Materialized")
                .field("len", &array.len())
                .finish(),
            Self::Lazy(_) => formatter.write_str("Lazy"),
        }
    }
}

/// One page produced by a connector.
///
/// A page with zero channels and a positive position count is legal: it is how
/// a count-only or partition-only scan reports rows. It is never end of
/// stream.
#[derive(Debug)]
pub struct SourcePage {
    position_count: usize,
    channels: Vec<PageChannel>,
    output_memory: Option<ConnectorOutputMemoryToken>,
}

impl SourcePage {
    /// A page whose columns are already materialized.
    pub fn try_new(position_count: usize, columns: Vec<ArrayRef>) -> Result<Self, ConnectorError> {
        for column in &columns {
            if column.len() != position_count {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::CorruptData,
                    "connector source page column length differs from its position count",
                ));
            }
        }
        Ok(Self {
            position_count,
            channels: columns.into_iter().map(PageChannel::Materialized).collect(),
            output_memory: None,
        })
    }

    /// A page whose Arrow buffers carry their admitted host reservation.
    pub fn try_new_accounted(
        position_count: usize,
        columns: Vec<ArrayRef>,
        output_memory: ConnectorOutputMemoryToken,
    ) -> Result<Self, ConnectorError> {
        let mut page = Self::try_new(position_count, columns)?;
        page.output_memory = Some(output_memory);
        Ok(page)
    }

    /// A page that reports positions without producing any column.
    pub const fn zero_channel(position_count: usize) -> Self {
        Self {
            position_count,
            channels: Vec::new(),
            output_memory: None,
        }
    }

    /// Append a column that is materialized only when first read.
    pub fn push_lazy_channel(&mut self, loader: Box<dyn LazyBlockLoader>) {
        self.channels.push(PageChannel::Lazy(loader));
    }

    pub const fn position_count(&self) -> usize {
        self.position_count
    }

    pub fn channel_count(&self) -> usize {
        self.channels.len()
    }

    /// Materialize and borrow one channel.
    pub fn block(&mut self, channel: usize) -> Result<&ArrayRef, ConnectorError> {
        let position_count = self.position_count;
        let slot = self.channels.get_mut(channel).ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector source page channel index is out of range",
            )
        })?;
        if let PageChannel::Lazy(loader) = slot {
            let array = loader.load()?;
            if array.len() != position_count {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::CorruptData,
                    "connector lazily loaded column length differs from its position count",
                ));
            }
            *slot = PageChannel::Materialized(array);
        }
        match slot {
            PageChannel::Materialized(array) => Ok(array),
            PageChannel::Lazy(_) => unreachable!("the channel was just materialized"),
        }
    }

    /// Materialize every channel and hand back the columns in order.
    pub fn into_columns(mut self) -> Result<(usize, Vec<ArrayRef>), ConnectorError> {
        if self.output_memory.is_some() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "accounted connector page requires move-aware column extraction",
            ));
        }
        let mut columns = Vec::with_capacity(self.channels.len());
        for index in 0..self.channels.len() {
            columns.push(self.block(index)?.clone());
        }
        Ok((self.position_count, columns))
    }

    /// Materialize every channel and move its existing host charge to the
    /// engine adapter together with the Arrow buffers.
    pub fn into_accounted_columns(
        mut self,
    ) -> Result<(usize, Vec<ArrayRef>, Option<ConnectorOutputMemoryToken>), ConnectorError> {
        let mut columns = Vec::with_capacity(self.channels.len());
        for index in 0..self.channels.len() {
            columns.push(self.block(index)?.clone());
        }
        Ok((self.position_count, columns, self.output_memory.take()))
    }

    pub fn output_memory_bytes(&self) -> Option<u64> {
        self.output_memory.as_ref().map(|token| token.bytes())
    }

    /// Keep only a prefix of the channels.
    ///
    /// This is how a provider drops the hidden columns it added for delete
    /// evaluation after the deletes have been applied.
    pub fn truncate_channels(&mut self, keep: usize) -> Result<(), ConnectorError> {
        if keep > self.channels.len() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector source page prefix exceeds its channel count",
            ));
        }
        self.channels.truncate(keep);
        Ok(())
    }

    /// Keep only the listed positions, in the order given.
    pub fn select_positions(&mut self, positions: &[u32]) -> Result<(), ConnectorError> {
        let indices = arrow::array::UInt32Array::from(positions.to_vec());
        for index in 0..self.channels.len() {
            let selected = arrow::compute::take(self.block(index)?.as_ref(), &indices, None)
                .map_err(|error| {
                    ConnectorError::new(
                        ConnectorErrorKind::Internal,
                        format!("connector source page position selection failed: {error}"),
                    )
                })?;
            self.channels[index] = PageChannel::Materialized(selected);
        }
        self.position_count = positions.len();
        Ok(())
    }

    /// Bytes currently held by materialized channels.
    pub fn retained_size_in_bytes(&self) -> u64 {
        self.channels
            .iter()
            .map(|channel| match channel {
                PageChannel::Materialized(array) => array.get_array_memory_size() as u64,
                PageChannel::Lazy(loader) => loader.retained_size_in_bytes(),
            })
            .sum()
    }
}

/// Runtime counters a page source reports.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PageSourceMetrics {
    pub completed_bytes: u64,
    pub completed_positions: u64,
    pub read_time_nanos: u64,
    /// Provider-neutral physical file-reader counters, when this page source
    /// is backed by files. Non-file sources leave this snapshot empty.
    pub file: PageSourceFileMetrics,
}

/// Physical file-reader counters retained across the typed page-source
/// boundary.
///
/// These are execution facts rather than provider semantics: a provider may
/// obtain them from any file implementation, while the backend projects them
/// into the stable runtime-profile counter names.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PageSourceFileMetrics {
    pub bytes_read: u64,
    pub read_requests: u64,
    pub rows_decoded: u64,
    pub batches_delivered: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub io_time_ns: u64,
    pub decode_time_ns: u64,
    pub row_groups_read: u64,
    pub row_groups_pruned: u64,
    pub delayed_materialization_ranges: u64,
    pub page_index_attempts: u64,
    pub page_index_fallbacks: u64,
    pub page_index_rows_considered: u64,
    pub page_index_rows_pruned: u64,
}

impl PageSourceFileMetrics {
    pub fn saturating_delta_since(self, previous: Self) -> Self {
        Self {
            bytes_read: self.bytes_read.saturating_sub(previous.bytes_read),
            read_requests: self.read_requests.saturating_sub(previous.read_requests),
            rows_decoded: self.rows_decoded.saturating_sub(previous.rows_decoded),
            batches_delivered: self
                .batches_delivered
                .saturating_sub(previous.batches_delivered),
            cache_hits: self.cache_hits.saturating_sub(previous.cache_hits),
            cache_misses: self.cache_misses.saturating_sub(previous.cache_misses),
            io_time_ns: self.io_time_ns.saturating_sub(previous.io_time_ns),
            decode_time_ns: self.decode_time_ns.saturating_sub(previous.decode_time_ns),
            row_groups_read: self
                .row_groups_read
                .saturating_sub(previous.row_groups_read),
            row_groups_pruned: self
                .row_groups_pruned
                .saturating_sub(previous.row_groups_pruned),
            delayed_materialization_ranges: self
                .delayed_materialization_ranges
                .saturating_sub(previous.delayed_materialization_ranges),
            page_index_attempts: self
                .page_index_attempts
                .saturating_sub(previous.page_index_attempts),
            page_index_fallbacks: self
                .page_index_fallbacks
                .saturating_sub(previous.page_index_fallbacks),
            page_index_rows_considered: self
                .page_index_rows_considered
                .saturating_sub(previous.page_index_rows_considered),
            page_index_rows_pruned: self
                .page_index_rows_pruned
                .saturating_sub(previous.page_index_rows_pruned),
        }
    }
}

/// Query-scoped reader policy supplied when a worker creates a page-source
/// provider. It is separate from the provider's generation-scoped resources:
/// two queries using the same installed connector generation may choose
/// different physical reader behavior.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConnectorPageSourceProviderOptions {
    pub enable_parquet_reader_page_index: bool,
    /// Frozen query policy for external file reads. The connector maps these
    /// neutral values to its local cache implementation without accepting
    /// cache state or credentials from the coordinator.
    pub data_cache: ConnectorDataCacheOptions,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConnectorDataCacheOptions {
    pub enable_scan_datacache: bool,
    pub enable_populate_datacache: bool,
    pub enable_datacache_async_populate_mode: bool,
    pub enable_datacache_io_adaptor: bool,
    pub enable_cache_select: bool,
    pub datacache_evict_probability: i32,
    pub datacache_priority: i32,
    pub datacache_ttl_seconds: i64,
    pub datacache_sharing_work_period: Option<i64>,
}

impl Default for ConnectorDataCacheOptions {
    fn default() -> Self {
        Self {
            enable_scan_datacache: false,
            enable_populate_datacache: false,
            enable_datacache_async_populate_mode: false,
            enable_datacache_io_adaptor: false,
            enable_cache_select: false,
            datacache_evict_probability: 100,
            datacache_priority: 0,
            datacache_ttl_seconds: 0,
            datacache_sharing_work_period: None,
        }
    }
}

impl Default for ConnectorPageSourceProviderOptions {
    fn default() -> Self {
        Self {
            enable_parquet_reader_page_index: false,
            data_cache: ConnectorDataCacheOptions::default(),
        }
    }
}

/// A connector reader bound to exactly one split.
pub trait ConnectorPageSource: Send {
    /// `None` means no page is available right now. It is not end of stream:
    /// only [`Self::is_finished`] reports termination.
    fn next_source_page(&mut self) -> Result<Option<SourcePage>, ConnectorError>;

    fn is_finished(&self) -> bool;

    /// Whether the source is waiting on external work.
    fn is_blocked(&self) -> bool {
        false
    }

    fn metrics(&self) -> PageSourceMetrics;

    fn memory_usage_bytes(&self) -> u64;

    /// Idempotent; may be called after an error or a cancellation.
    fn close(&mut self) -> Result<(), ConnectorError>;
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Int64Array, UInt32Array};

    use super::*;

    struct CountingLoader {
        calls: usize,
    }

    impl LazyBlockLoader for CountingLoader {
        fn load(&mut self) -> Result<ArrayRef, ConnectorError> {
            self.calls += 1;
            Ok(Arc::new(Int64Array::from(vec![1_i64, 2, 3])))
        }
    }

    #[test]
    fn zero_channel_pages_still_report_positions() {
        let page = SourcePage::zero_channel(1024);
        assert_eq!(page.channel_count(), 0);
        assert_eq!(page.position_count(), 1024);
    }

    #[test]
    fn column_length_must_match_the_position_count() {
        let column: ArrayRef = Arc::new(Int64Array::from(vec![1_i64, 2]));
        assert!(SourcePage::try_new(3, vec![column]).is_err());
    }

    #[test]
    fn lazy_channels_materialize_once_on_first_read() {
        let mut page = SourcePage::zero_channel(3);
        page.push_lazy_channel(Box::new(CountingLoader { calls: 0 }));
        assert_eq!(page.channel_count(), 1);
        assert_eq!(page.block(0).expect("loads").len(), 3);
        assert_eq!(page.block(0).expect("cached").len(), 3);
    }

    #[test]
    fn truncating_channels_drops_the_hidden_delete_suffix() {
        let visible: ArrayRef = Arc::new(Int64Array::from(vec![1_i64, 2]));
        let hidden: ArrayRef = Arc::new(UInt32Array::from(vec![0_u32, 1]));
        let mut page = SourcePage::try_new(2, vec![visible, hidden]).expect("valid page");
        page.truncate_channels(1).expect("prefix");
        assert_eq!(page.channel_count(), 1);
        assert!(page.truncate_channels(2).is_err());
    }

    #[test]
    fn position_selection_rewrites_every_channel_consistently() {
        let column: ArrayRef = Arc::new(Int64Array::from(vec![10_i64, 20, 30]));
        let mut page = SourcePage::try_new(3, vec![column]).expect("valid page");
        page.select_positions(&[2, 0]).expect("selects");
        assert_eq!(page.position_count(), 2);
        let (positions, columns) = page.into_columns().expect("materializes");
        assert_eq!(positions, 2);
        let values = columns[0]
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("int64");
        assert_eq!(values.values(), &[30, 10]);
    }
}
