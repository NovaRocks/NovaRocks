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

//! Provider-private, read-only shared-data storage kernel.
//!
//! This module deliberately models only immutable StarRocks storage facts and
//! an Arrow fixture reader.  An outer adapter is responsible for resolving
//! StarOS locations and opening files through a startup-owned filesystem
//! binding; neither credentials nor filesystem clients appear here.

#![allow(dead_code)] // Fixture-only domain variants document the read closure.

mod kernel;
mod model;
mod page;
mod remote;
mod segment;
mod wire;

pub use remote::StarRocksSharedDataStorageResolver;

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fmt;
use std::sync::Arc;
use std::time::Instant;

use arrow::array::UInt64Array;
use arrow::compute::{filter_record_batch, take};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use novarocks_spi::connector::{
    ConnectorBatchBudget, ConnectorBatchReader, ConnectorError, ConnectorErrorKind,
    ConnectorReaderMetricsSnapshot, ConnectorRequestContext,
};

const MAX_STORAGE_COLUMNS: usize = 16 * 1024;
const MAX_STORAGE_ROWSETS: usize = 16 * 1024;
const MAX_STORAGE_SEGMENTS: usize = 16 * 1024;
const MAX_STORAGE_TEXT_BYTES: usize = 16 * 1024;

/// The physical metadata layout selected for an immutable tablet version.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StarRocksStorageMetadataLayout {
    Standalone,
    Bundle,
}

/// Immutable tablet facts needed by the storage reader.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StarRocksStorageTablet {
    pub tablet_id: i64,
    pub partition_id: i64,
    pub tablet_version: i64,
    pub schema_version: Bytes,
    pub data_version: Bytes,
    pub layout: StarRocksStorageMetadataLayout,
}

impl StarRocksStorageTablet {
    pub fn try_new(
        tablet_id: i64,
        partition_id: i64,
        tablet_version: i64,
        schema_version: Bytes,
        data_version: Bytes,
        layout: StarRocksStorageMetadataLayout,
    ) -> Result<Self, ConnectorError> {
        if tablet_id <= 0
            || partition_id <= 0
            || tablet_version <= 0
            || schema_version.is_empty()
            || data_version.is_empty()
        {
            return Err(invalid("invalid immutable StarRocks storage tablet facts"));
        }
        Ok(Self {
            tablet_id,
            partition_id,
            tablet_version,
            schema_version,
            data_version,
            layout,
        })
    }
}

/// One schema field identified by StarRocks' stable column unique ID.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StarRocksStorageColumn {
    pub unique_id: i32,
    pub name: Arc<str>,
    pub data_type: DataType,
    pub nullable: bool,
}

impl StarRocksStorageColumn {
    pub fn try_new(
        unique_id: i32,
        name: impl Into<Arc<str>>,
        data_type: DataType,
        nullable: bool,
    ) -> Result<Self, ConnectorError> {
        let name = name.into();
        if unique_id <= 0 || name.is_empty() || name.len() > MAX_STORAGE_TEXT_BYTES {
            return Err(invalid("invalid StarRocks storage column"));
        }
        Ok(Self {
            unique_id,
            name,
            data_type,
            nullable,
        })
    }
}

/// A deterministic Arrow schema reconstructed from immutable storage metadata.
#[derive(Clone)]
pub struct StarRocksStorageSchema {
    columns: Vec<StarRocksStorageColumn>,
    arrow: SchemaRef,
}

impl StarRocksStorageSchema {
    pub fn try_new(columns: Vec<StarRocksStorageColumn>) -> Result<Self, ConnectorError> {
        if columns.is_empty() || columns.len() > MAX_STORAGE_COLUMNS {
            return Err(invalid(
                "StarRocks storage schema must contain bounded columns",
            ));
        }
        let mut ids = BTreeSet::new();
        let mut names = BTreeSet::new();
        for column in &columns {
            if !ids.insert(column.unique_id) || !names.insert(column.name.clone()) {
                return Err(invalid(
                    "StarRocks storage schema column IDs and names must be unique",
                ));
            }
        }
        let arrow = Arc::new(Schema::new(
            columns
                .iter()
                .map(|column| {
                    Field::new(
                        column.name.as_ref(),
                        column.data_type.clone(),
                        column.nullable,
                    )
                })
                .collect::<Vec<_>>(),
        ));
        Ok(Self { columns, arrow })
    }

    pub fn columns(&self) -> &[StarRocksStorageColumn] {
        &self.columns
    }

    pub fn arrow_schema(&self) -> &SchemaRef {
        &self.arrow
    }

    /// Reject a reader request whose exact names, types, nullability or field
    /// order differ from the immutable storage schema.
    pub fn validate_expected_schema(&self, expected: &SchemaRef) -> Result<(), ConnectorError> {
        if self.arrow.as_ref() != expected.as_ref() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "StarRocks storage schema does not match the requested output schema",
            ));
        }
        Ok(())
    }
}

impl fmt::Debug for StarRocksStorageSchema {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StarRocksStorageSchema")
            .field("columns", &self.columns)
            .finish()
    }
}

/// One immutable Arrow segment within a rowset.  It is a fixture substitute
/// for a future file-backed segment reader.
#[derive(Clone)]
pub struct StarRocksStorageSegment {
    pub segment_id: u32,
    pub batch: RecordBatch,
}

impl StarRocksStorageSegment {
    pub fn try_new(segment_id: u32, batch: RecordBatch) -> Result<Self, ConnectorError> {
        if batch.num_columns() == 0 {
            return Err(invalid("StarRocks storage segment must have columns"));
        }
        Ok(Self { segment_id, batch })
    }
}

impl fmt::Debug for StarRocksStorageSegment {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StarRocksStorageSegment")
            .field("segment_id", &self.segment_id)
            .field("rows", &self.batch.num_rows())
            .finish()
    }
}

/// Immutable rowset data selected by a tablet version.
#[derive(Clone, Debug)]
pub struct StarRocksStorageRowset {
    pub rowset_id: u64,
    pub version: u64,
    pub segments: Vec<StarRocksStorageSegment>,
}

impl StarRocksStorageRowset {
    pub fn try_new(
        rowset_id: u64,
        version: u64,
        segments: Vec<StarRocksStorageSegment>,
    ) -> Result<Self, ConnectorError> {
        if rowset_id == 0
            || version == 0
            || segments.is_empty()
            || segments.len() > MAX_STORAGE_SEGMENTS
        {
            return Err(invalid("invalid StarRocks storage rowset"));
        }
        let mut ids = BTreeSet::new();
        if segments
            .iter()
            .any(|segment| !ids.insert(segment.segment_id))
        {
            return Err(invalid(
                "StarRocks storage rowset segment IDs must be unique",
            ));
        }
        Ok(Self {
            rowset_id,
            version,
            segments,
        })
    }
}

/// Bundle manifest used only for bundle-layout tablet metadata.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StarRocksStorageBundle {
    pub bundle_id: Arc<str>,
    pub rowset_ids: Vec<u64>,
}

impl StarRocksStorageBundle {
    pub fn try_new(
        bundle_id: impl Into<Arc<str>>,
        rowset_ids: Vec<u64>,
    ) -> Result<Self, ConnectorError> {
        let bundle_id = bundle_id.into();
        if bundle_id.is_empty()
            || bundle_id.len() > MAX_STORAGE_TEXT_BYTES
            || rowset_ids.is_empty()
            || rowset_ids.contains(&0)
            || rowset_ids.iter().collect::<BTreeSet<_>>().len() != rowset_ids.len()
        {
            return Err(invalid("invalid StarRocks storage bundle"));
        }
        Ok(Self {
            bundle_id,
            rowset_ids,
        })
    }
}

/// Position deletes supplied by immutable delete metadata.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StarRocksStorageDelete {
    pub rowset_id: u64,
    pub segment_id: u32,
    pub deleted_positions: BTreeSet<u32>,
}

impl StarRocksStorageDelete {
    pub fn try_new(
        rowset_id: u64,
        segment_id: u32,
        deleted_positions: impl IntoIterator<Item = u32>,
    ) -> Result<Self, ConnectorError> {
        if rowset_id == 0 {
            return Err(invalid(
                "StarRocks storage delete rowset ID must be non-zero",
            ));
        }
        let deleted_positions = deleted_positions.into_iter().collect::<BTreeSet<_>>();
        if deleted_positions.is_empty() {
            return Err(invalid("StarRocks storage delete vector must not be empty"));
        }
        Ok(Self {
            rowset_id,
            segment_id,
            deleted_positions,
        })
    }
}

/// A segment-level delete vector. It composes with position deletes by set
/// union, never by a best-effort fallback.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StarRocksStorageDeleteVector {
    pub rowset_id: u64,
    pub segment_id: u32,
    pub deleted_positions: BTreeSet<u32>,
}

impl StarRocksStorageDeleteVector {
    pub fn try_new(
        rowset_id: u64,
        segment_id: u32,
        deleted_positions: impl IntoIterator<Item = u32>,
    ) -> Result<Self, ConnectorError> {
        let delete = StarRocksStorageDelete::try_new(rowset_id, segment_id, deleted_positions)?;
        Ok(Self {
            rowset_id: delete.rowset_id,
            segment_id: delete.segment_id,
            deleted_positions: delete.deleted_positions,
        })
    }
}

/// Fixture-oriented immutable storage input. It has the same validation
/// boundary a real metadata+filesystem implementation must satisfy.
#[derive(Clone)]
pub struct StarRocksStorageFixture {
    tablet: StarRocksStorageTablet,
    schema: StarRocksStorageSchema,
    rowsets: Vec<StarRocksStorageRowset>,
    bundle: Option<StarRocksStorageBundle>,
    deletes: Vec<StarRocksStorageDelete>,
    delete_vectors: Vec<StarRocksStorageDeleteVector>,
}

impl StarRocksStorageFixture {
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        tablet: StarRocksStorageTablet,
        schema: StarRocksStorageSchema,
        rowsets: Vec<StarRocksStorageRowset>,
        bundle: Option<StarRocksStorageBundle>,
        deletes: Vec<StarRocksStorageDelete>,
        delete_vectors: Vec<StarRocksStorageDeleteVector>,
    ) -> Result<Self, ConnectorError> {
        if rowsets.is_empty() || rowsets.len() > MAX_STORAGE_ROWSETS {
            return Err(invalid(
                "StarRocks storage fixture must contain bounded rowsets",
            ));
        }
        let mut rowset_ids = BTreeSet::new();
        let mut segments = BTreeMap::new();
        for rowset in &rowsets {
            if !rowset_ids.insert(rowset.rowset_id) {
                return Err(invalid("StarRocks storage rowset IDs must be unique"));
            }
            for segment in &rowset.segments {
                schema.validate_expected_schema(&segment.batch.schema())?;
                let key = (rowset.rowset_id, segment.segment_id);
                if segments.insert(key, segment.batch.num_rows()).is_some() {
                    return Err(invalid("StarRocks storage segment keys must be unique"));
                }
            }
        }
        match (tablet.layout, &bundle) {
            (StarRocksStorageMetadataLayout::Standalone, None) => {}
            (StarRocksStorageMetadataLayout::Bundle, Some(bundle))
                if bundle.rowset_ids.iter().copied().collect::<BTreeSet<_>>() == rowset_ids => {}
            (StarRocksStorageMetadataLayout::Standalone, Some(_)) => {
                return Err(invalid(
                    "standalone StarRocks tablet metadata must not carry a bundle",
                ));
            }
            (StarRocksStorageMetadataLayout::Bundle, _) => {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::CorruptData,
                    "StarRocks bundle metadata does not exactly cover the selected rowsets",
                ));
            }
        }
        validate_delete_positions(&segments, deletes.iter().map(delete_parts))?;
        validate_delete_positions(&segments, delete_vectors.iter().map(delete_vector_parts))?;
        Ok(Self {
            tablet,
            schema,
            rowsets,
            bundle,
            deletes,
            delete_vectors,
        })
    }

    pub fn tablet(&self) -> &StarRocksStorageTablet {
        &self.tablet
    }

    pub fn schema(&self) -> &StarRocksStorageSchema {
        &self.schema
    }

    pub fn bundle(&self) -> Option<&StarRocksStorageBundle> {
        self.bundle.as_ref()
    }

    pub fn open_reader(
        &self,
        expected_schema: SchemaRef,
        batch: ConnectorBatchBudget,
        context: ConnectorRequestContext,
    ) -> Result<StarRocksStorageFixtureReader, ConnectorError> {
        self.schema.validate_expected_schema(&expected_schema)?;
        ensure_active(&context)?;
        let deleted = self.deleted_positions();
        let mut chunks = VecDeque::new();
        let mut bytes_read = 0_u64;
        let mut read_requests = 0_u64;
        for rowset in &self.rowsets {
            for segment in &rowset.segments {
                ensure_active(&context)?;
                read_requests = read_requests.saturating_add(1);
                bytes_read =
                    bytes_read.saturating_add(segment.batch.get_array_memory_size() as u64);
                let filtered = apply_deletes(
                    &segment.batch,
                    deleted.get(&(rowset.rowset_id, segment.segment_id)),
                )?;
                chunks.extend(slice_batch(&filtered, batch)?);
            }
        }
        Ok(StarRocksStorageFixtureReader {
            chunks,
            context,
            closed: false,
            metrics: ConnectorReaderMetricsSnapshot {
                bytes_read,
                read_requests,
                ..ConnectorReaderMetricsSnapshot::default()
            },
        })
    }

    fn deleted_positions(&self) -> BTreeMap<(u64, u32), BTreeSet<u32>> {
        let mut deleted = BTreeMap::<(u64, u32), BTreeSet<u32>>::new();
        for delete in &self.deletes {
            deleted
                .entry((delete.rowset_id, delete.segment_id))
                .or_default()
                .extend(delete.deleted_positions.iter().copied());
        }
        for delete_vector in &self.delete_vectors {
            deleted
                .entry((delete_vector.rowset_id, delete_vector.segment_id))
                .or_default()
                .extend(delete_vector.deleted_positions.iter().copied());
        }
        deleted
    }
}

impl fmt::Debug for StarRocksStorageFixture {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StarRocksStorageFixture")
            .field("tablet", &self.tablet)
            .field("rowsets", &self.rowsets.len())
            .field("bundle", &self.bundle)
            .field("deletes", &self.deletes.len())
            .field("delete_vectors", &self.delete_vectors.len())
            .finish()
    }
}

/// Deterministic, fixture-only direct storage reader. The real file-backed
/// adapter must preserve its cancellation, close, metrics and batch semantics.
pub trait DirectStorageReader: Send {
    fn next_batch(&mut self) -> Result<Option<RecordBatch>, ConnectorError>;

    fn close(&mut self) -> Result<(), ConnectorError>;

    fn metrics_snapshot(&self) -> ConnectorReaderMetricsSnapshot {
        ConnectorReaderMetricsSnapshot::default()
    }
}

/// Adapts a provider-private storage reader to the generic SPI reader while
/// retaining cancellation/deadline checks at every batch boundary.
pub struct DirectStorageConnectorReader {
    reader: Box<dyn DirectStorageReader>,
    context: ConnectorRequestContext,
    closed: bool,
}

impl DirectStorageConnectorReader {
    pub fn new(reader: Box<dyn DirectStorageReader>, context: ConnectorRequestContext) -> Self {
        Self {
            reader,
            context,
            closed: false,
        }
    }
}

impl ConnectorBatchReader for DirectStorageConnectorReader {
    fn next_batch(&mut self) -> Result<Option<RecordBatch>, ConnectorError> {
        if self.closed {
            return Ok(None);
        }
        ensure_active(&self.context)?;
        self.reader.next_batch()
    }

    fn close(&mut self) -> Result<(), ConnectorError> {
        if !self.closed {
            self.closed = true;
            self.reader.close()?;
        }
        Ok(())
    }

    fn metrics_snapshot(&self) -> ConnectorReaderMetricsSnapshot {
        self.reader.metrics_snapshot()
    }
}

pub struct StarRocksStorageFixtureReader {
    chunks: VecDeque<RecordBatch>,
    context: ConnectorRequestContext,
    closed: bool,
    metrics: ConnectorReaderMetricsSnapshot,
}

impl fmt::Debug for StarRocksStorageFixtureReader {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StarRocksStorageFixtureReader")
            .field("queued_batches", &self.chunks.len())
            .field("closed", &self.closed)
            .finish()
    }
}

impl DirectStorageReader for StarRocksStorageFixtureReader {
    fn next_batch(&mut self) -> Result<Option<RecordBatch>, ConnectorError> {
        if self.closed {
            return Ok(None);
        }
        ensure_active(&self.context)?;
        let batch = self.chunks.pop_front();
        if let Some(batch) = &batch {
            self.metrics.batches_delivered = self.metrics.batches_delivered.saturating_add(1);
            self.metrics.rows_decoded = self
                .metrics
                .rows_decoded
                .saturating_add(batch.num_rows() as u64);
        }
        Ok(batch)
    }

    fn close(&mut self) -> Result<(), ConnectorError> {
        self.closed = true;
        self.chunks.clear();
        Ok(())
    }

    fn metrics_snapshot(&self) -> ConnectorReaderMetricsSnapshot {
        self.metrics
    }
}

fn delete_parts(delete: &StarRocksStorageDelete) -> ((u64, u32), &BTreeSet<u32>) {
    (
        (delete.rowset_id, delete.segment_id),
        &delete.deleted_positions,
    )
}

fn delete_vector_parts(delete: &StarRocksStorageDeleteVector) -> ((u64, u32), &BTreeSet<u32>) {
    (
        (delete.rowset_id, delete.segment_id),
        &delete.deleted_positions,
    )
}

fn validate_delete_positions<'a>(
    segments: &BTreeMap<(u64, u32), usize>,
    deletes: impl IntoIterator<Item = ((u64, u32), &'a BTreeSet<u32>)>,
) -> Result<(), ConnectorError> {
    for (key, positions) in deletes {
        let rows = segments.get(&key).ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "StarRocks delete metadata references an unknown storage segment",
            )
        })?;
        if positions.iter().any(|position| *position as usize >= *rows) {
            return Err(ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "StarRocks delete metadata references a row outside its storage segment",
            ));
        }
    }
    Ok(())
}

fn apply_deletes(
    batch: &RecordBatch,
    deleted: Option<&BTreeSet<u32>>,
) -> Result<RecordBatch, ConnectorError> {
    let Some(deleted) = deleted else {
        return Ok(batch.clone());
    };
    let keep = arrow::array::BooleanArray::from_iter(
        (0..batch.num_rows()).map(|position| Some(!deleted.contains(&(position as u32)))),
    );
    filter_record_batch(batch, &keep).map_err(|error| {
        ConnectorError::new(
            ConnectorErrorKind::CorruptData,
            format!("failed to apply StarRocks storage deletes: {error}"),
        )
    })
}

fn slice_batch(
    batch: &RecordBatch,
    budget: ConnectorBatchBudget,
) -> Result<Vec<RecordBatch>, ConnectorError> {
    if batch.num_rows() == 0 {
        return Ok(Vec::new());
    }
    let mut output = Vec::new();
    let mut offset = 0_usize;
    while offset < batch.num_rows() {
        let remaining = batch.num_rows() - offset;
        let row_cap = remaining.min(budget.max_rows.get());
        let take_rows = largest_fitting_prefix(batch, offset, row_cap, budget.max_bytes.get())?;
        output.push(compact_slice(batch, offset, take_rows)?);
        offset += take_rows;
    }
    Ok(output)
}

fn largest_fitting_prefix(
    batch: &RecordBatch,
    offset: usize,
    row_cap: usize,
    max_bytes: usize,
) -> Result<usize, ConnectorError> {
    if compact_slice(batch, offset, 1)?.get_array_memory_size() > max_bytes {
        return Err(ConnectorError::new(
            ConnectorErrorKind::ResourceExhausted,
            "one StarRocks direct row exceeds the requested byte budget",
        ));
    }
    let mut low = 1_usize;
    let mut high = row_cap;
    while low < high {
        let middle = low + (high - low).div_ceil(2);
        if compact_slice(batch, offset, middle)?.get_array_memory_size() <= max_bytes {
            low = middle;
        } else {
            high = middle - 1;
        }
    }
    Ok(low)
}

fn compact_slice(
    batch: &RecordBatch,
    offset: usize,
    length: usize,
) -> Result<RecordBatch, ConnectorError> {
    let indices = UInt64Array::from_iter_values(offset as u64..(offset + length) as u64);
    let columns = batch
        .columns()
        .iter()
        .map(|column| {
            take(column.as_ref(), &indices, None).map_err(|error| {
                ConnectorError::new(
                    ConnectorErrorKind::Internal,
                    format!("compact StarRocks direct batch to budget: {error}"),
                )
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    RecordBatch::try_new(batch.schema(), columns).map_err(|error| {
        ConnectorError::new(
            ConnectorErrorKind::Internal,
            format!("build compact StarRocks direct batch: {error}"),
        )
    })
}

fn ensure_active(context: &ConnectorRequestContext) -> Result<(), ConnectorError> {
    if context.cancellation().is_cancelled() {
        return Err(ConnectorError::new(
            ConnectorErrorKind::Cancelled,
            "StarRocks storage read was cancelled",
        ));
    }
    if Instant::now() >= context.deadline() {
        return Err(ConnectorError::new(
            ConnectorErrorKind::DeadlineExceeded,
            "StarRocks storage read deadline elapsed",
        ));
    }
    Ok(())
}

fn invalid(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::InvalidRequest, message)
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;
    use std::sync::Arc;
    use std::time::Duration;

    use arrow::array::Int64Array;
    use novarocks_spi::connector::ConnectorCancellation;

    use super::*;

    struct NeverCancelled;
    impl ConnectorCancellation for NeverCancelled {
        fn is_cancelled(&self) -> bool {
            false
        }
    }

    struct Cancelled;
    impl ConnectorCancellation for Cancelled {
        fn is_cancelled(&self) -> bool {
            true
        }
    }

    fn context() -> ConnectorRequestContext {
        ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(5),
            Arc::new(NeverCancelled),
            64 * 1024,
            128 * 1024,
        )
        .expect("context")
    }

    fn schema() -> StarRocksStorageSchema {
        StarRocksStorageSchema::try_new(vec![
            StarRocksStorageColumn::try_new(1, "id", DataType::Int64, false).unwrap(),
        ])
        .unwrap()
    }

    fn fixture() -> StarRocksStorageFixture {
        let schema = schema();
        let batch = RecordBatch::try_new(
            schema.arrow_schema().clone(),
            vec![Arc::new(Int64Array::from(vec![1_i64, 2, 3, 4, 5]))],
        )
        .unwrap();
        let rowset = StarRocksStorageRowset::try_new(
            7,
            1,
            vec![StarRocksStorageSegment::try_new(0, batch).unwrap()],
        )
        .unwrap();
        StarRocksStorageFixture::try_new(
            StarRocksStorageTablet::try_new(
                1,
                2,
                3,
                Bytes::from_static(b"schema-v1"),
                Bytes::from_static(b"data-v1"),
                StarRocksStorageMetadataLayout::Standalone,
            )
            .unwrap(),
            schema,
            vec![rowset],
            None,
            vec![StarRocksStorageDelete::try_new(7, 0, [1_u32]).unwrap()],
            vec![StarRocksStorageDeleteVector::try_new(7, 0, [3_u32]).unwrap()],
        )
        .unwrap()
    }

    fn budget(rows: usize, bytes: usize) -> ConnectorBatchBudget {
        ConnectorBatchBudget {
            max_rows: NonZeroUsize::new(rows).unwrap(),
            max_bytes: NonZeroUsize::new(bytes).unwrap(),
        }
    }

    #[test]
    fn fixture_applies_delete_and_delvec_then_slices_arrow_batches() {
        let fixture = fixture();
        let mut reader = fixture
            .open_reader(
                fixture.schema().arrow_schema().clone(),
                budget(1, 1024),
                context(),
            )
            .unwrap();
        let mut values = Vec::new();
        while let Some(batch) = reader.next_batch().unwrap() {
            assert_eq!(batch.num_rows(), 1);
            let values_array = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            values.push(values_array.value(0));
        }
        assert_eq!(values, vec![1, 3, 5]);
        assert_eq!(reader.metrics_snapshot().rows_decoded, 3);
        assert_eq!(reader.metrics_snapshot().batches_delivered, 3);
    }

    #[test]
    fn fixture_rejects_schema_layout_and_delete_metadata_mismatches() {
        let schema = schema();
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "other",
                DataType::Int64,
                false,
            )])),
            vec![Arc::new(Int64Array::from(vec![1_i64]))],
        )
        .unwrap();
        let rowset = StarRocksStorageRowset::try_new(
            7,
            1,
            vec![StarRocksStorageSegment::try_new(0, batch).unwrap()],
        )
        .unwrap();
        let result = StarRocksStorageFixture::try_new(
            StarRocksStorageTablet::try_new(
                1,
                2,
                3,
                Bytes::from_static(b"schema-v1"),
                Bytes::from_static(b"data-v1"),
                StarRocksStorageMetadataLayout::Standalone,
            )
            .unwrap(),
            schema,
            vec![rowset],
            None,
            vec![],
            vec![],
        );
        assert_eq!(result.unwrap_err().kind(), ConnectorErrorKind::CorruptData);

        let fixture = fixture();
        let bad_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        assert_eq!(
            fixture
                .open_reader(bad_schema, budget(1, 1024), context())
                .unwrap_err()
                .kind(),
            ConnectorErrorKind::CorruptData
        );
    }

    #[test]
    fn reader_checks_cancellation_and_close() {
        let fixture = fixture();
        let cancelled = ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(5),
            Arc::new(Cancelled),
            64 * 1024,
            128 * 1024,
        )
        .unwrap();
        assert_eq!(
            fixture
                .open_reader(
                    fixture.schema().arrow_schema().clone(),
                    budget(2, 1024),
                    cancelled
                )
                .unwrap_err()
                .kind(),
            ConnectorErrorKind::Cancelled
        );

        let mut reader = fixture
            .open_reader(
                fixture.schema().arrow_schema().clone(),
                budget(2, 1024),
                context(),
            )
            .unwrap();
        reader.close().unwrap();
        assert!(reader.next_batch().unwrap().is_none());
    }

    #[test]
    fn fixture_enforces_exact_arrow_byte_budget() {
        let fixture = fixture();
        let segment = &fixture.rowsets[0].segments[0].batch;
        let one_row_bytes = compact_slice(segment, 0, 1)
            .unwrap()
            .get_array_memory_size();
        let mut reader = fixture
            .open_reader(
                fixture.schema().arrow_schema().clone(),
                budget(3, one_row_bytes),
                context(),
            )
            .unwrap();
        while let Some(batch) = reader.next_batch().unwrap() {
            assert!(batch.get_array_memory_size() <= one_row_bytes);
        }
        assert_eq!(
            fixture
                .open_reader(
                    fixture.schema().arrow_schema().clone(),
                    budget(1, one_row_bytes.saturating_sub(1).max(1)),
                    context(),
                )
                .unwrap_err()
                .kind(),
            ConnectorErrorKind::ResourceExhausted
        );
    }
}
