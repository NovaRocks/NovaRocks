// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License.  You may obtain a copy of the License
// at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Core-owned runtime adapters for connector-provided batches.
//!
//! Providers own handle codecs and `ConnectorBatchReader`; this module is the
//! sole conversion boundary into core's `Chunk` execution representation.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, RwLock, Weak};

use novarocks_spi::connector::{
    ConnectorBatchReader, ConnectorError, ConnectorErrorKind, ConnectorExecutionBinding,
    ConnectorOpenReaderRequest, ConnectorReaderMetricsSnapshot, ConnectorSplit,
};

use crate::exec::chunk::{Chunk, ChunkSchemaRef};
use crate::exec::node::ExecResult;
use crate::exec::node::scan::{
    BoundScanRanges, ConnectorRowPosition, IncrementalScanRange, RuntimeFilterContext, ScanMorsel,
    ScanMorsels, ScanOp, ScanSource,
};
use crate::runtime::profile::{ProfileUnit, RuntimeProfile};

pub(crate) struct ConnectorBatchReaderIter {
    reader: Option<Box<dyn ConnectorBatchReader>>,
    chunk_schema: ChunkSchemaRef,
    batch_transform: Option<Arc<dyn ConnectorBatchTransform>>,
    profile: Option<RuntimeProfile>,
    last_metrics: ConnectorReaderMetricsSnapshot,
    finished: bool,
}

/// Backend-supplied, execution-domain batch projection applied after a
/// connector reader yields a batch and before Core materializes it as a
/// `Chunk`. It intentionally exposes neither connector registry nor query
/// runtime state.
pub trait ConnectorBatchTransform: Send + Sync {
    fn transform(
        &self,
        batch: arrow::record_batch::RecordBatch,
    ) -> Result<arrow::record_batch::RecordBatch, String>;
}

/// Fragment-local ownership of readers opened by a connector scan source.
/// Terminal scan lifecycle closes these readers explicitly; iterator Drop is
/// only a final safety net.
#[derive(Default)]
struct ConnectorReaderGroup {
    state: Mutex<ConnectorReaderGroupState>,
}

#[derive(Default)]
struct ConnectorReaderGroupState {
    phase: ConnectorReaderGroupPhase,
    next_reader_id: usize,
    readers: BTreeMap<usize, RegisteredConnectorReader>,
}

struct RegisteredConnectorReader {
    reader: Arc<Mutex<Option<Box<dyn ConnectorBatchReader>>>>,
    marker: Option<ConnectorReaderMarker>,
}

#[derive(Clone)]
struct ConnectorReaderMarker {
    provider_id: String,
    instance_id: String,
    incarnation: String,
}

impl ConnectorReaderMarker {
    fn emit(&self, event: &str) {
        println!(
            "NOVAROCKS_CONNECTOR_READER_{event} provider={} instance={} incarnation={}",
            self.provider_id, self.instance_id, self.incarnation
        );
        let _ = std::io::Write::flush(&mut std::io::stdout());
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
enum ConnectorReaderGroupPhase {
    #[default]
    Open,
    Terminating,
    Closed,
}

impl ConnectorReaderGroup {
    fn register(
        self: &Arc<Self>,
        reader: Box<dyn ConnectorBatchReader>,
        marker: Option<ConnectorReaderMarker>,
    ) -> Result<Box<dyn ConnectorBatchReader>, String> {
        let reader = Arc::new(Mutex::new(Some(reader)));
        let reader_id = {
            let mut state = self
                .state
                .lock()
                .map_err(|_| "connector reader group lock poisoned".to_string())?;
            if state.phase != ConnectorReaderGroupPhase::Open {
                return Err(format!("connector reader group is {:?}", state.phase));
            }
            let reader_id = state.next_reader_id;
            state.next_reader_id = state.next_reader_id.saturating_add(1);
            state.readers.insert(
                reader_id,
                RegisteredConnectorReader {
                    reader: Arc::clone(&reader),
                    marker: marker.clone(),
                },
            );
            reader_id
        };
        if let Some(marker) = marker.as_ref() {
            marker.emit("OPEN");
        }
        Ok(Box::new(GroupedConnectorBatchReader {
            reader,
            group: Arc::downgrade(self),
            reader_id,
            marker,
        }))
    }

    fn unregister(&self, reader_id: usize) {
        if let Ok(mut state) = self.state.lock() {
            state.readers.remove(&reader_id);
        }
    }

    fn terminate(&self) -> Result<(), String> {
        let readers = {
            let mut state = self
                .state
                .lock()
                .map_err(|_| "connector reader group lock poisoned".to_string())?;
            if state.phase != ConnectorReaderGroupPhase::Open {
                return Ok(());
            }
            state.phase = ConnectorReaderGroupPhase::Terminating;
            std::mem::take(&mut state.readers)
                .into_values()
                .collect::<Vec<_>>()
        };
        let mut cleanup_errors = Vec::new();
        for registered in readers {
            let mut was_closed = false;
            let result = registered
                .reader
                .lock()
                .map_err(|_| "connector reader lock poisoned".to_string())
                .and_then(|mut reader| match reader.take() {
                    Some(mut reader) => {
                        was_closed = true;
                        reader.close().map_err(|error| error.to_string())
                    }
                    None => Ok(()),
                });
            if was_closed {
                if let Some(marker) = registered.marker.as_ref() {
                    marker.emit("CLOSE");
                }
            }
            if let Err(error) = result {
                cleanup_errors.push(error);
            }
        }
        let closed = self
            .state
            .lock()
            .map_err(|_| "connector reader group lock poisoned".to_string())
            .map(|mut state| {
                state.phase = ConnectorReaderGroupPhase::Closed;
            });
        if let Err(error) = closed {
            cleanup_errors.push(error);
        }
        if cleanup_errors.is_empty() {
            Ok(())
        } else {
            Err(format!(
                "connector reader group cleanup failed: {}",
                cleanup_errors.join("; ")
            ))
        }
    }
}

struct GroupedConnectorBatchReader {
    reader: Arc<Mutex<Option<Box<dyn ConnectorBatchReader>>>>,
    group: Weak<ConnectorReaderGroup>,
    reader_id: usize,
    marker: Option<ConnectorReaderMarker>,
}

impl GroupedConnectorBatchReader {
    fn unregister(&self) {
        if let Some(group) = self.group.upgrade() {
            group.unregister(self.reader_id);
        }
    }
}

impl ConnectorBatchReader for GroupedConnectorBatchReader {
    fn next_batch(
        &mut self,
    ) -> Result<Option<arrow::record_batch::RecordBatch>, novarocks_spi::connector::ConnectorError>
    {
        let mut reader = self.reader.lock().map_err(|_| {
            novarocks_spi::connector::ConnectorError::new(
                novarocks_spi::connector::ConnectorErrorKind::Internal,
                "connector reader lock poisoned",
            )
        })?;
        let Some(reader) = reader.as_mut() else {
            return Ok(None);
        };
        reader.next_batch()
    }

    fn close(&mut self) -> Result<(), novarocks_spi::connector::ConnectorError> {
        let mut was_closed = false;
        let result = self
            .reader
            .lock()
            .map_err(|_| {
                novarocks_spi::connector::ConnectorError::new(
                    novarocks_spi::connector::ConnectorErrorKind::Internal,
                    "connector reader lock poisoned",
                )
            })?
            .take()
            .map(|mut reader| {
                was_closed = true;
                reader.close()
            })
            .transpose();
        self.unregister();
        if was_closed {
            if let Some(marker) = self.marker.as_ref() {
                marker.emit("CLOSE");
            }
        }
        result.map(|_| ())
    }

    fn metrics_snapshot(&self) -> ConnectorReaderMetricsSnapshot {
        self.reader
            .lock()
            .ok()
            .and_then(|reader| reader.as_ref().map(|reader| reader.metrics_snapshot()))
            .unwrap_or_default()
    }
}

impl Drop for GroupedConnectorBatchReader {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

#[cfg(test)]
mod connector_reader_group_tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    struct RecordingReader {
        closes: Arc<AtomicUsize>,
    }

    impl ConnectorBatchReader for RecordingReader {
        fn next_batch(
            &mut self,
        ) -> Result<
            Option<arrow::record_batch::RecordBatch>,
            novarocks_spi::connector::ConnectorError,
        > {
            Ok(None)
        }

        fn close(&mut self) -> Result<(), novarocks_spi::connector::ConnectorError> {
            self.closes.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    #[test]
    fn terminal_group_closes_open_reader_once_and_rejects_new_reader() {
        let group = Arc::new(ConnectorReaderGroup::default());
        let closes = Arc::new(AtomicUsize::new(0));
        let mut reader = group
            .register(
                Box::new(RecordingReader {
                    closes: Arc::clone(&closes),
                }),
                None,
            )
            .expect("reader registration");

        group.terminate().expect("terminal cleanup");
        assert_eq!(closes.load(Ordering::SeqCst), 1);
        reader.close().expect("idempotent reader close");
        assert_eq!(closes.load(Ordering::SeqCst), 1);
        assert!(
            group
                .register(Box::new(RecordingReader { closes }), None)
                .is_err()
        );
    }
}

/// Provider-private conversion result for FE ranges that arrive after a scan
/// source has already been scheduled. The generic runtime never decodes the
/// range or split payload.
#[derive(Clone)]
pub(crate) enum ConnectorSplitAppend {
    Plain {
        splits: Vec<ConnectorSplit>,
        has_more: bool,
    },
    Scheduled {
        scheduled: Vec<ConnectorScheduledSplit>,
        has_more: bool,
    },
}

impl ConnectorSplitAppend {
    fn scheduled(&self) -> (Vec<ConnectorScheduledSplit>, bool) {
        match self {
            Self::Plain { splits, has_more } => (plain_scheduled(splits.clone()), *has_more),
            Self::Scheduled {
                scheduled,
                has_more,
            } => (scheduled.clone(), *has_more),
        }
    }

    pub(crate) fn scheduled_file_splits(&self) -> Option<&[ConnectorScheduledSplit]> {
        match self {
            Self::Plain { .. } => None,
            Self::Scheduled { scheduled, .. } => Some(scheduled),
        }
    }
}

/// A queued provider split plus provider-neutral core row-position identity.
#[derive(Clone)]
pub struct ConnectorScheduledSplit {
    split: ConnectorSplit,
    row_position: Option<ConnectorRowPosition>,
    storage_tablet_id: Option<i64>,
}

impl ConnectorScheduledSplit {
    pub fn plain(split: ConnectorSplit) -> Self {
        Self {
            split,
            row_position: None,
            storage_tablet_id: None,
        }
    }

    /// Attach a protocol-neutral storage tablet identity for execution paths
    /// that synthesize per-row positions. The provider payload stays opaque.
    pub fn storage_tablet(split: ConnectorSplit, tablet_id: i64) -> Self {
        Self {
            split,
            row_position: None,
            storage_tablet_id: Some(tablet_id),
        }
    }

    pub fn with_row_position(split: ConnectorSplit, row_position: ConnectorRowPosition) -> Self {
        Self {
            split,
            row_position: Some(row_position),
            storage_tablet_id: None,
        }
    }

    pub fn split(&self) -> &ConnectorSplit {
        &self.split
    }

    fn storage_tablet_id(&self) -> Option<i64> {
        self.storage_tablet_id
    }

    fn morsel(&self, index: usize) -> ScanMorsel {
        ScanMorsel::ConnectorSplit {
            index,
            row_position: self.row_position,
        }
    }
}

/// Core-internal adapter used only by compat/native transport adapters that
/// receive incremental ranges. It deliberately is not part of the SPI trait.
pub(crate) trait IncrementalConnectorSplitAdapter: Send + Sync {
    fn prepare_incremental_ranges(
        &self,
        ranges: &[IncrementalScanRange],
    ) -> Result<ConnectorSplitAppend, String>;

    /// Commits provider-private state only after the generic queue has
    /// validated every opaque split and its core sidecar. Implementations must
    /// reject a stale or malformed prepared append without partial mutation.
    fn commit_incremental_ranges(&self, _append: &ConnectorSplitAppend) -> Result<(), String> {
        Ok(())
    }
}

struct ConnectorSplitState {
    scheduled: Vec<ConnectorScheduledSplit>,
    split_ids: BTreeSet<String>,
    total_payload_bytes: usize,
    has_more: bool,
}

impl ConnectorSplitState {
    fn new(scheduled: Vec<ConnectorScheduledSplit>, has_more: bool) -> Self {
        let split_ids = scheduled
            .iter()
            .map(|scheduled| scheduled.split.split_id().to_string())
            .collect();
        let total_payload_bytes = scheduled
            .iter()
            .map(|scheduled| scheduled.split.payload().len())
            .sum();
        Self {
            scheduled,
            split_ids,
            total_payload_bytes,
            has_more,
        }
    }
}

fn plain_scheduled(splits: Vec<ConnectorSplit>) -> Vec<ConnectorScheduledSplit> {
    splits
        .into_iter()
        .map(ConnectorScheduledSplit::plain)
        .collect()
}

impl ConnectorBatchReaderIter {
    pub(crate) fn new(reader: Box<dyn ConnectorBatchReader>, chunk_schema: ChunkSchemaRef) -> Self {
        Self {
            reader: Some(reader),
            chunk_schema,
            batch_transform: None,
            profile: None,
            last_metrics: ConnectorReaderMetricsSnapshot::default(),
            finished: false,
        }
    }

    pub(crate) fn with_profile(
        reader: Box<dyn ConnectorBatchReader>,
        chunk_schema: ChunkSchemaRef,
        profile: Option<RuntimeProfile>,
    ) -> Self {
        let mut iter = Self::new(reader, chunk_schema);
        iter.profile = profile;
        iter
    }

    fn with_profile_and_transform(
        reader: Box<dyn ConnectorBatchReader>,
        chunk_schema: ChunkSchemaRef,
        profile: Option<RuntimeProfile>,
        batch_transform: Option<Arc<dyn ConnectorBatchTransform>>,
    ) -> Self {
        let mut iter = Self::with_profile(reader, chunk_schema, profile);
        iter.batch_transform = batch_transform;
        iter
    }

    fn flush_metrics_snapshot(&mut self, snapshot: ConnectorReaderMetricsSnapshot) {
        let delta = snapshot.saturating_delta_since(self.last_metrics);
        self.last_metrics = snapshot;
        let Some(profile) = self.profile.as_ref() else {
            return;
        };
        for (name, unit, value) in [
            (
                "ConnectorFileBytesRead",
                ProfileUnit::Bytes,
                delta.bytes_read,
            ),
            (
                "ConnectorFileReadRequests",
                ProfileUnit::Unit,
                delta.read_requests,
            ),
            (
                "ConnectorFileRowsDecoded",
                ProfileUnit::Unit,
                delta.rows_decoded,
            ),
            (
                "ConnectorFileBatchesDelivered",
                ProfileUnit::Unit,
                delta.batches_delivered,
            ),
            (
                "ConnectorFileCacheHits",
                ProfileUnit::Unit,
                delta.cache_hits,
            ),
            (
                "ConnectorFileCacheMisses",
                ProfileUnit::Unit,
                delta.cache_misses,
            ),
            ("ConnectorFileIoTime", ProfileUnit::TimeNs, delta.io_time_ns),
            (
                "ConnectorFileDecodeTime",
                ProfileUnit::TimeNs,
                delta.decode_time_ns,
            ),
            (
                "ConnectorFileRowGroupsRead",
                ProfileUnit::Unit,
                delta.row_groups_read,
            ),
            (
                "ConnectorFileRowGroupsPruned",
                ProfileUnit::Unit,
                delta.row_groups_pruned,
            ),
            (
                "ConnectorFileDelayedMaterializationRanges",
                ProfileUnit::Unit,
                delta.delayed_materialization_ranges,
            ),
        ] {
            if value > 0 {
                profile.counter_add(name, unit, value.min(i64::MAX as u64) as i64);
            }
        }
    }

    fn close(&mut self) -> Result<(), String> {
        let Some(mut reader) = self.reader.take() else {
            return Ok(());
        };
        let result = reader.close().map_err(|error| error.to_string());
        self.flush_metrics_snapshot(reader.metrics_snapshot());
        result
    }

    fn finish_with_primary_error(&mut self, primary: String) -> ExecResult {
        self.finished = true;
        match self.close() {
            Ok(()) => Err(primary),
            Err(cleanup) => Err(format!("{primary} (cleanup: {cleanup})")),
        }
    }
}

impl Iterator for ConnectorBatchReaderIter {
    type Item = ExecResult;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }
        let next_batch = self
            .reader
            .as_mut()
            .expect("connector reader must exist before end of stream")
            .next_batch();
        let metrics = self
            .reader
            .as_ref()
            .expect("connector reader must exist before end of stream")
            .metrics_snapshot();
        self.flush_metrics_snapshot(metrics);
        match next_batch {
            Ok(Some(batch)) => Some(
                (match self.batch_transform.as_ref() {
                    Some(transform) => transform.transform(batch),
                    None => Ok(batch),
                })
                .and_then(|batch| {
                    Chunk::try_new_with_chunk_schema(batch, self.chunk_schema.clone())
                        .map_err(|error| error.to_string())
                })
                .or_else(|error| self.finish_with_primary_error(error)),
            ),
            Ok(None) => {
                self.finished = true;
                self.close().err().map(Err)
            }
            Err(error) => Some(self.finish_with_primary_error(error.to_string())),
        }
    }
}

impl Drop for ConnectorBatchReaderIter {
    fn drop(&mut self) {
        if !self.finished {
            let _ = self.close();
            self.finished = true;
        }
    }
}

/// A generic physical source for one already-assigned SPI split.
///
/// The source owns no provider-specific type.  Wire decoders resolve the
/// opaque split to its typed host instance, while core owns scheduling and
/// adapts the returned Arrow batches into `Chunk`s.
pub struct ConnectorReadScanSource {
    binding: ConnectorReadBinding,
    splits: Arc<RwLock<ConnectorSplitState>>,
    request: ConnectorOpenReaderRequest,
    chunk_schema: ChunkSchemaRef,
    batch_transform: Option<Arc<dyn ConnectorBatchTransform>>,
    incremental: Option<Arc<dyn IncrementalConnectorSplitAdapter>>,
    reader_group: Arc<ConnectorReaderGroup>,
}

#[derive(Clone)]
enum ConnectorReadBinding {
    Execution(Arc<ConnectorExecutionBinding>),
}

impl ConnectorReadBinding {
    fn instance_id(&self) -> &novarocks_spi::connector::ConnectorInstanceId {
        match self {
            Self::Execution(binding) => &binding.key().instance_id,
        }
    }

    fn provider_id(&self) -> &str {
        match self {
            Self::Execution(binding) => binding.provider_id().as_str(),
        }
    }

    fn incarnation(&self) -> novarocks_spi::connector::ConnectorInstanceIncarnation {
        match self {
            Self::Execution(binding) => binding.key().incarnation,
        }
    }

    fn open_reader(
        &self,
        split: &ConnectorSplit,
        request: ConnectorOpenReaderRequest,
    ) -> Result<Box<dyn ConnectorBatchReader>, novarocks_spi::connector::ConnectorError> {
        match self {
            Self::Execution(binding) => binding
                .read()
                .ok_or_else(|| {
                    ConnectorError::new(
                        ConnectorErrorKind::Unsupported,
                        "connector execution binding has no read capability",
                    )
                })?
                .open_reader(split, request),
        }
    }
}

impl ConnectorReadScanSource {
    pub(crate) fn new(
        binding: Arc<ConnectorExecutionBinding>,
        splits: Vec<ConnectorSplit>,
        request: ConnectorOpenReaderRequest,
        chunk_schema: ChunkSchemaRef,
    ) -> Self {
        Self {
            binding: ConnectorReadBinding::Execution(binding),
            splits: Arc::new(RwLock::new(ConnectorSplitState::new(
                plain_scheduled(splits),
                false,
            ))),
            request,
            chunk_schema,
            batch_transform: None,
            incremental: None,
            reader_group: Arc::new(ConnectorReaderGroup::default()),
        }
    }

    pub(crate) fn new_with_incremental(
        binding: Arc<ConnectorExecutionBinding>,
        splits: Vec<ConnectorSplit>,
        request: ConnectorOpenReaderRequest,
        chunk_schema: ChunkSchemaRef,
        incremental: Arc<dyn IncrementalConnectorSplitAdapter>,
        has_more: bool,
    ) -> Self {
        Self {
            binding: ConnectorReadBinding::Execution(binding),
            splits: Arc::new(RwLock::new(ConnectorSplitState::new(
                plain_scheduled(splits),
                has_more,
            ))),
            request,
            chunk_schema,
            batch_transform: None,
            incremental: Some(incremental),
            reader_group: Arc::new(ConnectorReaderGroup::default()),
        }
    }

    pub(crate) fn new_scheduled(
        binding: Arc<ConnectorExecutionBinding>,
        scheduled: Vec<ConnectorScheduledSplit>,
        request: ConnectorOpenReaderRequest,
        chunk_schema: ChunkSchemaRef,
    ) -> Self {
        Self {
            binding: ConnectorReadBinding::Execution(binding),
            splits: Arc::new(RwLock::new(ConnectorSplitState::new(scheduled, false))),
            request,
            chunk_schema,
            batch_transform: None,
            incremental: None,
            reader_group: Arc::new(ConnectorReaderGroup::default()),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new_scheduled_with_incremental(
        binding: Arc<ConnectorExecutionBinding>,
        scheduled: Vec<ConnectorScheduledSplit>,
        request: ConnectorOpenReaderRequest,
        chunk_schema: ChunkSchemaRef,
        incremental: Option<Arc<dyn IncrementalConnectorSplitAdapter>>,
        has_more: bool,
    ) -> Self {
        Self {
            binding: ConnectorReadBinding::Execution(binding),
            splits: Arc::new(RwLock::new(ConnectorSplitState::new(scheduled, has_more))),
            request,
            chunk_schema,
            batch_transform: None,
            incremental,
            reader_group: Arc::new(ConnectorReaderGroup::default()),
        }
    }

    pub fn new_scheduled_execution(
        binding: Arc<ConnectorExecutionBinding>,
        scheduled: Vec<ConnectorScheduledSplit>,
        request: ConnectorOpenReaderRequest,
        chunk_schema: ChunkSchemaRef,
    ) -> Self {
        Self::new_scheduled_execution_with_batch_transform(
            binding,
            scheduled,
            request,
            chunk_schema,
            None,
        )
    }

    pub fn new_scheduled_execution_with_batch_transform(
        binding: Arc<ConnectorExecutionBinding>,
        scheduled: Vec<ConnectorScheduledSplit>,
        request: ConnectorOpenReaderRequest,
        chunk_schema: ChunkSchemaRef,
        batch_transform: Option<Arc<dyn ConnectorBatchTransform>>,
    ) -> Self {
        Self {
            binding: ConnectorReadBinding::Execution(binding),
            splits: Arc::new(RwLock::new(ConnectorSplitState::new(scheduled, false))),
            request,
            chunk_schema,
            batch_transform,
            incremental: None,
            reader_group: Arc::new(ConnectorReaderGroup::default()),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new_scheduled_execution_with_incremental(
        binding: Arc<ConnectorExecutionBinding>,
        scheduled: Vec<ConnectorScheduledSplit>,
        request: ConnectorOpenReaderRequest,
        chunk_schema: ChunkSchemaRef,
        incremental: Option<Arc<dyn IncrementalConnectorSplitAdapter>>,
        has_more: bool,
    ) -> Self {
        Self {
            binding: ConnectorReadBinding::Execution(binding),
            splits: Arc::new(RwLock::new(ConnectorSplitState::new(scheduled, has_more))),
            request,
            chunk_schema,
            batch_transform: None,
            incremental,
            reader_group: Arc::new(ConnectorReaderGroup::default()),
        }
    }
}

impl ScanSource for ConnectorReadScanSource {
    fn bind(&self, ranges: BoundScanRanges) -> Result<Arc<dyn ScanOp>, String> {
        if !matches!(ranges, BoundScanRanges::None) {
            return Err("SPI connector scan source requires an empty range binding".to_string());
        }
        Ok(Arc::new(ConnectorReadScanOp {
            binding: self.binding.clone(),
            splits: Arc::clone(&self.splits),
            request: self.request.clone(),
            chunk_schema: Arc::clone(&self.chunk_schema),
            batch_transform: self.batch_transform.clone(),
            incremental: self.incremental.clone(),
            reader_group: Arc::clone(&self.reader_group),
        }))
    }
}

struct ConnectorReadScanOp {
    binding: ConnectorReadBinding,
    splits: Arc<RwLock<ConnectorSplitState>>,
    request: ConnectorOpenReaderRequest,
    chunk_schema: ChunkSchemaRef,
    batch_transform: Option<Arc<dyn ConnectorBatchTransform>>,
    incremental: Option<Arc<dyn IncrementalConnectorSplitAdapter>>,
    reader_group: Arc<ConnectorReaderGroup>,
}

impl ScanOp for ConnectorReadScanOp {
    fn terminate(&self) -> Result<(), String> {
        self.reader_group.terminate()
    }

    fn execute_iter(
        &self,
        morsel: ScanMorsel,
        profile: Option<RuntimeProfile>,
        _runtime_filters: Option<&RuntimeFilterContext>,
    ) -> Result<crate::exec::node::BoxedExecIter, String> {
        let index = match morsel {
            ScanMorsel::ConnectorSplit { index, .. } => index,
            _ => {
                return Err("SPI connector scan received an unexpected morsel".to_string());
            }
        };
        let split = self
            .splits
            .read()
            .map_err(|_| "SPI connector split state lock poisoned".to_string())?
            .scheduled
            .get(index)
            .map(|scheduled| scheduled.split.clone())
            .ok_or_else(|| format!("SPI connector scan split index {index} is out of bounds"))?;
        let reader = self
            .binding
            .open_reader(&split, self.request.clone())
            .map_err(|error| error.to_string())?;
        let marker = crate::common::config::debug_emit_connector_reader_marker().then(|| {
            ConnectorReaderMarker {
                provider_id: self.binding.provider_id().to_string(),
                instance_id: self.binding.instance_id().as_str().to_string(),
                incarnation: hex::encode(self.binding.incarnation().to_bytes()),
            }
        });
        let reader = self.reader_group.register(reader, marker)?;
        Ok(Box::new(
            ConnectorBatchReaderIter::with_profile_and_transform(
                reader,
                Arc::clone(&self.chunk_schema),
                profile,
                self.batch_transform.clone(),
            ),
        ))
    }

    fn storage_tablet_id(&self, morsel: &ScanMorsel) -> Result<Option<i64>, String> {
        let ScanMorsel::ConnectorSplit { index, .. } = morsel else {
            return Ok(None);
        };
        self.splits
            .read()
            .map_err(|_| "SPI connector split state lock poisoned".to_string())?
            .scheduled
            .get(*index)
            .map(ConnectorScheduledSplit::storage_tablet_id)
            .ok_or_else(|| format!("SPI connector scan split index {index} is out of bounds"))
    }

    fn build_morsels(&self) -> Result<ScanMorsels, String> {
        let state = self
            .splits
            .read()
            .map_err(|_| "SPI connector split state lock poisoned".to_string())?;
        Ok(ScanMorsels::new(
            state
                .scheduled
                .iter()
                .enumerate()
                .map(|(index, scheduled)| scheduled.morsel(index))
                .collect(),
            state.has_more,
        ))
    }

    fn supports_incremental_scan_ranges(&self) -> bool {
        self.incremental.is_some()
    }

    fn build_incremental_morsels(
        &self,
        ranges: &[IncrementalScanRange],
    ) -> Result<ScanMorsels, String> {
        let adapter = self
            .incremental
            .as_ref()
            .ok_or_else(|| "SPI connector scan does not support incremental ranges".to_string())?;
        let mut state = self
            .splits
            .write()
            .map_err(|_| "SPI connector split state lock poisoned".to_string())?;
        if !state.has_more {
            return Err("SPI connector split queue is closed".to_string());
        }
        let append = adapter.prepare_incremental_ranges(ranges)?;
        let (appended, has_more) = append.scheduled();
        let expected_owner = self.binding.instance_id();
        let start = state.scheduled.len();
        let mut appended_ids = BTreeSet::new();
        let append_payload_bytes = appended.iter().try_fold(0usize, |total, scheduled| {
            let split = &scheduled.split;
            if split.owner() != expected_owner {
                return Err(
                    "incremental connector split owner does not match its instance".to_string(),
                );
            }
            if split.payload().len() > self.request.context.max_handle_payload_bytes() {
                return Err(
                    "incremental connector split payload exceeds its handle budget".to_string(),
                );
            }
            if state.split_ids.contains(split.split_id()) {
                return Err(format!(
                    "incremental connector split ID `{}` already exists",
                    split.split_id()
                ));
            }
            if !appended_ids.insert(split.split_id().to_string()) {
                return Err(format!(
                    "incremental connector split ID `{}` is duplicated in one append",
                    split.split_id()
                ));
            }
            total
                .checked_add(split.payload().len())
                .ok_or_else(|| "incremental connector split payload total overflowed".to_string())
        })?;
        let total_payload_bytes = state
            .total_payload_bytes
            .checked_add(append_payload_bytes)
            .ok_or_else(|| "incremental connector split payload total overflowed".to_string())?;
        if total_payload_bytes > self.request.context.max_total_payload_bytes() {
            return Err(
                "incremental connector split payloads exceed their total budget".to_string(),
            );
        }
        adapter.commit_incremental_ranges(&append)?;
        for scheduled in appended {
            state
                .split_ids
                .insert(scheduled.split.split_id().to_string());
            state.scheduled.push(scheduled);
        }
        state.total_payload_bytes = total_payload_bytes;
        state.has_more = has_more;
        let end = state.scheduled.len();
        Ok(ScanMorsels::new(
            (start..end)
                .map(|index| state.scheduled[index].morsel(index))
                .collect(),
            state.has_more,
        ))
    }
}
