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

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::Instant;

use arrow::array::UInt64Array;
use arrow::datatypes::SchemaRef;
use parquet::DecodeResult;
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions, RowSelection};
use parquet::arrow::push_decoder::{ParquetPushDecoder, ParquetPushDecoderBuilder};
use parquet::basic::{SortOrder, Type as ParquetType};
use parquet::file::FOOTER_SIZE;
use parquet::file::metadata::{
    FooterTail, PageIndexPolicy, ParquetMetaData, ParquetMetaDataPushDecoder,
    ParquetMetaDataReader, RowGroupMetaData,
};
use parquet::file::page_index::column_index::ColumnIndexMetaData;
use parquet::file::statistics::Statistics;

use super::chunk_reader::{BoundChunkReader, ReaderMetrics, SmallFileBuffer};
use super::range_io::{coalesce_ranges, read_decoder_ranges, read_decoder_ranges_async};
use crate::{
    BoundFile, DataCacheContext, FileBatch, FileBatchReader, FileError, FileErrorKind, FileFormat,
    FileIdentity, FileMetricsSnapshot, FileProjection, FileReadContext, FileReadRange,
    FileReadRequest, FileReaderOptions, FileResult, MinMaxPredicateValue, PreparedFileInput,
    ScanPredicate, ScanPredicateDomain,
};
use novarocks_spi::connector::StorageAccessDomainId;

/// Upper bounds for a footer inspection. They cap metadata retained before a
/// connector has chosen any scan units and deliberately do not depend on a
/// connector's own facts budget.
pub const MAX_PARQUET_INSPECTION_ROW_GROUPS: usize = 65_536;
pub const MAX_PARQUET_INSPECTION_PHYSICAL_COLUMNS: usize = 4_096;
pub const MAX_PARQUET_INSPECTION_STATISTIC_CELLS: usize = 1_048_576;
pub const MAX_PARQUET_INSPECTION_STATISTIC_VALUE_BYTES: usize = 64 * 1024;

/// Connector-neutral physical layout of one Parquet row group.
///
/// The descriptor intentionally exposes only facts from the immutable file
/// footer. Table formats decide whether and how those facts become scan work.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ParquetRowGroupLayout {
    pub ordinal: u32,
    pub compressed_bytes: u64,
    pub row_count: u64,
}

/// A primitive Parquet leaf from the immutable footer schema.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ParquetPhysicalColumn {
    ordinal: u32,
    path: Vec<String>,
    field_id: Option<i32>,
    physical_type: ParquetPhysicalType,
    sort_order: ParquetStatisticsSortOrder,
}

impl ParquetPhysicalColumn {
    pub fn ordinal(&self) -> u32 {
        self.ordinal
    }

    pub fn path(&self) -> &[String] {
        &self.path
    }

    pub fn field_id(&self) -> Option<i32> {
        self.field_id
    }

    pub fn physical_type(&self) -> ParquetPhysicalType {
        self.physical_type
    }

    pub fn sort_order(&self) -> ParquetStatisticsSortOrder {
        self.sort_order
    }
}

/// The primitive physical representation used by a Parquet leaf.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ParquetPhysicalType {
    Boolean,
    Int32,
    Int64,
    Int96,
    Float,
    Double,
    ByteArray,
    FixedLenByteArray,
}

/// The ordering declared by the Parquet schema for statistics aggregation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ParquetStatisticsSortOrder {
    Signed,
    Unsigned,
    Undefined,
}

/// A decoded value from a Parquet footer statistic. This deliberately retains
/// physical values only; connector-specific logical coercion belongs above FS.
#[derive(Clone, Debug, PartialEq)]
pub enum ParquetStatisticsValue {
    Boolean(bool),
    Int32(i32),
    Int64(i64),
    Int96([u32; 3]),
    Float(f32),
    Double(f64),
    ByteArray(Vec<u8>),
    FixedLenByteArray(Vec<u8>),
}

/// Raw footer statistics for one row-group/physical-column cell.
///
/// `min` and `max` remain absent when the writer did not provide usable
/// values. Exactness and ordering are surfaced separately so consumers cannot
/// silently treat truncated or legacy bounds as exact facts.
#[derive(Clone, Debug, PartialEq)]
pub struct ParquetColumnStatistics {
    null_count: Option<u64>,
    min: Option<ParquetStatisticsValue>,
    max: Option<ParquetStatisticsValue>,
    min_is_exact: bool,
    max_is_exact: bool,
    min_max_deprecated: bool,
    min_max_backwards_compatible: bool,
    sort_order: ParquetStatisticsSortOrder,
}

impl ParquetColumnStatistics {
    pub fn null_count(&self) -> Option<u64> {
        self.null_count
    }

    pub fn min(&self) -> Option<&ParquetStatisticsValue> {
        self.min.as_ref()
    }

    pub fn max(&self) -> Option<&ParquetStatisticsValue> {
        self.max.as_ref()
    }

    pub fn min_is_exact(&self) -> bool {
        self.min_is_exact
    }

    pub fn max_is_exact(&self) -> bool {
        self.max_is_exact
    }

    pub fn min_max_deprecated(&self) -> bool {
        self.min_max_deprecated
    }

    pub fn min_max_backwards_compatible(&self) -> bool {
        self.min_max_backwards_compatible
    }

    pub fn sort_order(&self) -> ParquetStatisticsSortOrder {
        self.sort_order
    }
}

/// Immutable, bounded snapshot of one authorized Parquet footer.
///
/// The private reader metadata keeps the exact footer snapshot alive. Public
/// accessors expose only copied schema/layout/statistics facts and never open
/// data pages or construct a decoder.
#[derive(Clone, Debug)]
pub struct ParquetMetadataInspection {
    footer: Arc<ArrowReaderMetadata>,
    access_domain: StorageAccessDomainId,
    identity: FileIdentity,
    /// The footer with its page indexes, loaded once for every reader of this
    /// inspection that needs them.
    indexed_footer: Arc<tokio::sync::OnceCell<ArrowReaderMetadata>>,
    small_file: SmallFileBuffer,
    schema: SchemaRef,
    physical_columns: Vec<ParquetPhysicalColumn>,
    row_groups: Vec<ParquetRowGroupLayout>,
    statistics: Vec<Vec<Option<ParquetColumnStatistics>>>,
}

impl ParquetMetadataInspection {
    pub fn access_domain(&self) -> StorageAccessDomainId {
        self.access_domain
    }

    pub fn identity(&self) -> &FileIdentity {
        &self.identity
    }

    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    pub fn physical_columns(&self) -> &[ParquetPhysicalColumn] {
        &self.physical_columns
    }

    pub fn row_groups(&self) -> &[ParquetRowGroupLayout] {
        &self.row_groups
    }

    /// Returns the footer statistics for a physical column in one row group.
    /// Unknown ordinals and cells without a statistics struct both return
    /// `None`; callers must therefore retain their requested identities.
    pub fn column_statistics(
        &self,
        row_group_ordinal: u32,
        physical_column_ordinal: u32,
    ) -> Option<&ParquetColumnStatistics> {
        self.statistics
            .get(usize::try_from(row_group_ordinal).ok()?)?
            .get(usize::try_from(physical_column_ordinal).ok()?)?
            .as_ref()
    }

    /// Internal readers use this only to ensure the inspection owns the same
    /// immutable footer snapshot for its full lifetime.
    #[allow(dead_code)]
    pub(crate) fn footer(&self) -> &ArrowReaderMetadata {
        &self.footer
    }

    fn metadata_for(
        &self,
        file: &BoundFile,
        chunk_reader: &BoundChunkReader,
        page_index_policy: PageIndexPolicy,
        context: &FileReadContext,
    ) -> FileResult<ArrowReaderMetadata> {
        if let Some(metadata) = self.ready_metadata(file, page_index_policy, context)? {
            return Ok(metadata);
        }
        let metadata = load_page_indexes_blocking(&self.footer, chunk_reader, page_index_policy)?;
        context.check_active()?;
        let _ = self.indexed_footer.set(metadata);
        Ok(self
            .indexed_footer
            .get()
            .expect("page indexes loaded")
            .clone())
    }

    /// Awaited [`Self::metadata_for`]; concurrent readers share one load.
    async fn metadata_for_async(
        &self,
        file: &BoundFile,
        chunk_reader: &BoundChunkReader,
        page_index_policy: PageIndexPolicy,
        context: &FileReadContext,
    ) -> FileResult<ArrowReaderMetadata> {
        if let Some(metadata) = self.ready_metadata(file, page_index_policy, context)? {
            return Ok(metadata);
        }
        let metadata = self
            .indexed_footer
            .get_or_try_init(|| {
                load_page_indexes(&self.footer, chunk_reader, page_index_policy, context)
            })
            .await?
            .clone();
        context.check_active()?;
        Ok(metadata)
    }

    /// The metadata a reader of `file` needs when it is already in hand.
    fn ready_metadata(
        &self,
        file: &BoundFile,
        page_index_policy: PageIndexPolicy,
        context: &FileReadContext,
    ) -> FileResult<Option<ArrowReaderMetadata>> {
        if self.access_domain != file.access_domain() || self.identity != *file.identity() {
            return Err(FileError::invalid(
                "Parquet inspection belongs to a different file identity or access domain",
            ));
        }
        context.check_active()?;
        if page_index_policy == PageIndexPolicy::Skip {
            return Ok(Some(self.footer.as_ref().clone()));
        }
        Ok(self.indexed_footer.get().cloned())
    }
}

fn load_page_indexes_blocking(
    footer: &ArrowReaderMetadata,
    chunk_reader: &BoundChunkReader,
    page_index_policy: PageIndexPolicy,
) -> FileResult<ArrowReaderMetadata> {
    let options = ArrowReaderOptions::new().with_page_index_policy(page_index_policy);
    let mut reader = ParquetMetaDataReader::new_with_metadata(footer.metadata().as_ref().clone())
        .with_page_index_policy(page_index_policy);
    reader
        .read_page_indexes(chunk_reader)
        .map_err(|error| parquet_error("load Parquet page indexes", error))?;
    ArrowReaderMetadata::try_new(
        Arc::new(
            reader
                .finish()
                .map_err(|error| parquet_error("finish Parquet page indexes", error))?,
        ),
        options,
    )
    .map_err(|error| parquet_error("bind Parquet page indexes", error))
}

/// Awaited [`load_page_indexes_blocking`]: the same single page-index range.
async fn load_page_indexes(
    footer: &ArrowReaderMetadata,
    chunk_reader: &BoundChunkReader,
    page_index_policy: PageIndexPolicy,
    context: &FileReadContext,
) -> FileResult<ArrowReaderMetadata> {
    let options = ArrowReaderOptions::new().with_page_index_policy(page_index_policy);
    let mut decoder = ParquetMetaDataPushDecoder::try_new_with_metadata(
        chunk_reader.file_size(),
        footer.metadata().as_ref().clone(),
    )
    .map_err(|error| parquet_error("load Parquet page indexes", error))?
    .with_page_index_policy(page_index_policy);
    let metadata = decode_metadata(
        &mut decoder,
        chunk_reader,
        context,
        "load Parquet page indexes",
    )
    .await?;
    ArrowReaderMetadata::try_new(Arc::new(metadata), options)
        .map_err(|error| parquet_error("bind Parquet page indexes", error))
}

/// Awaited `ArrowReaderMetadata::load`: the 8-byte footer, then the metadata,
/// then the page indexes `options` ask for, each through the chunk reader.
async fn load_arrow_metadata(
    chunk_reader: &BoundChunkReader,
    page_index_policy: PageIndexPolicy,
    context: &FileReadContext,
    operation: &'static str,
) -> FileResult<ArrowReaderMetadata> {
    let options = ArrowReaderOptions::new().with_page_index_policy(page_index_policy);
    let mut decoder = ParquetMetaDataPushDecoder::try_new(chunk_reader.file_size())
        .map_err(|error| parquet_error(operation, error))?
        .with_page_index_policy(page_index_policy);
    let metadata = decode_metadata(&mut decoder, chunk_reader, context, operation).await?;
    ArrowReaderMetadata::try_new(Arc::new(metadata), options)
        .map_err(|error| parquet_error(operation, error))
}

async fn decode_metadata(
    decoder: &mut ParquetMetaDataPushDecoder,
    chunk_reader: &BoundChunkReader,
    context: &FileReadContext,
    operation: &'static str,
) -> FileResult<ParquetMetaData> {
    loop {
        match decoder
            .try_decode()
            .map_err(|error| parquet_error(operation, error))?
        {
            DecodeResult::Data(metadata) => return Ok(metadata),
            DecodeResult::NeedsData(ranges) => {
                context.check_active()?;
                let mut data = Vec::with_capacity(ranges.len());
                for range in &ranges {
                    let length = usize::try_from(range.end - range.start).map_err(|_| {
                        FileError::new(
                            FileErrorKind::ResourceExhausted,
                            "Parquet metadata range is too large",
                        )
                    })?;
                    data.push(chunk_reader.read_bytes_async(range.start, length).await?);
                }
                decoder
                    .push_ranges(ranges, data)
                    .map_err(|error| parquet_error(operation, error))?;
            }
            DecodeResult::Finished => {
                return Err(FileError::new(
                    FileErrorKind::Internal,
                    format!("{operation}: metadata decoder finished without metadata"),
                ));
            }
        }
    }
}

/// Plan the exact projected physical column or page ranges without opening a
/// decoder or reading data pages. A later demand still asks the push decoder
/// for its authoritative ranges; this plan is safe for bounded preparation.
pub fn plan_parquet_input_ranges(
    request: &FileReadRequest,
    inspection: &ParquetMetadataInspection,
) -> FileResult<Vec<FileReadRange>> {
    if request.format != FileFormat::Parquet {
        return Err(FileError::invalid(
            "Parquet input planning requires Parquet format",
        ));
    }
    request.context.check_active()?;
    let cache_enabled = request
        .cache
        .as_ref()
        .is_some_and(crate::DataCacheContext::datacache_requested);
    let chunk_reader = BoundChunkReader::new(
        request.file.clone(),
        request.context.clone(),
        request.cache.clone(),
        crate::cache::parquet_cache::page_cache_enabled(cache_enabled),
        Arc::new(ReaderMetrics::default()),
    )
    .with_small_file_buffer(Arc::clone(&inspection.small_file));
    let automatic_page_pruning =
        request.options.enable_parquet_reader_page_index && !request.predicates.is_empty();
    let page_index_policy = if request.pruning.pages.is_empty() && !automatic_page_pruning {
        PageIndexPolicy::Skip
    } else {
        PageIndexPolicy::Optional
    };
    let metadata = inspection.metadata_for(
        &request.file,
        &chunk_reader,
        page_index_policy,
        &request.context,
    )?;
    let builder = ParquetPushDecoderBuilder::new_with_metadata(metadata.clone());
    let projection = ProjectionMask::roots(
        builder.parquet_schema(),
        projection_roots(&builder, &request.projection)?,
    );
    let parquet = metadata.metadata();
    let groups = select_row_groups(
        parquet,
        request.range,
        request.pruning.row_groups.as_deref(),
        &request.predicates,
    );
    let automatic = automatic_page_pruning
        .then(|| automatic_page_ranges(parquet, &groups, &request.predicates))
        .transpose()?;
    let (mut selection, _) = page_selection(
        parquet,
        &groups,
        &request.pruning.pages,
        automatic.as_ref().map(|ranges| &ranges.by_row_group),
    )?;
    let mut ranges = Vec::new();
    for group_index in groups {
        request.context.check_active()?;
        let group = parquet.row_group(group_index);
        let group_selection = if let Some(selection) = selection.as_mut() {
            let row_count = usize::try_from(group.num_rows()).map_err(|_| {
                FileError::new(
                    FileErrorKind::Corrupt,
                    "Parquet row-group row count is invalid",
                )
            })?;
            Some(selection.split_off(row_count))
        } else {
            None
        };
        for (column_index, column) in group.columns().iter().enumerate() {
            if !projection.leaf_included(column_index) {
                continue;
            }
            let (start, length) = column.byte_range();
            let end = start.checked_add(length).ok_or_else(|| {
                FileError::new(
                    FileErrorKind::Corrupt,
                    "Parquet column byte range overflows",
                )
            })?;
            if length == 0 {
                continue;
            }
            if let (Some(selection), Some(indexes)) =
                (group_selection.as_ref(), parquet.offset_index())
            {
                let index =
                    page_index_cell(indexes, group_index, column_index, "offset index", group)?;
                let locations = index.page_locations();
                if let Some(first) = locations.first() {
                    let first_offset = u64::try_from(first.offset).map_err(|_| {
                        FileError::new(FileErrorKind::Corrupt, "Parquet page offset is negative")
                    })?;
                    if first_offset < start || first_offset > end {
                        return Err(FileError::new(
                            FileErrorKind::Corrupt,
                            "Parquet page offset lies outside its column chunk",
                        ));
                    }
                    if first_offset > start {
                        ranges.push(start..first_offset);
                    }
                    for page in selection.scan_ranges(locations) {
                        if page.start < first_offset || page.end > end {
                            return Err(FileError::new(
                                FileErrorKind::Corrupt,
                                "Parquet page byte range lies outside its column chunk",
                            ));
                        }
                        if page.start < page.end {
                            ranges.push(page);
                        }
                    }
                    continue;
                }
            }
            ranges.push(start..end);
        }
    }
    coalesce_ranges(
        &ranges,
        request.file.identity().file_size(),
        request.options,
    )?
    .into_iter()
    .map(|group| FileReadRange::bounded(group.range.start, group.range.end - group.range.start))
    .collect()
}

/// Read and freeze the Parquet footer through the normal authorized file and
/// metadata-cache path. This is deliberately separate from `open_file_reader`:
/// callers can plan bounded physical leaves without opening a decoder or
/// reading data pages.
pub fn inspect_parquet_metadata(
    file: BoundFile,
    cache: Option<DataCacheContext>,
    context: FileReadContext,
) -> FileResult<ParquetMetadataInspection> {
    inspect_parquet_metadata_inner(file, cache, context, None)
}

/// Awaited [`inspect_parquet_metadata`] for a caller that must not block: the
/// footer's ranges are awaited through the chunk reader, which reads through
/// the source's range service.
pub async fn inspect_parquet_metadata_async(
    file: BoundFile,
    cache: Option<DataCacheContext>,
    context: FileReadContext,
) -> FileResult<ParquetMetadataInspection> {
    let inspection = InspectionReader::try_new(file, cache, context, None)?;
    let metadata = match inspection.cached() {
        Some(metadata) => metadata,
        None => {
            let metadata = load_arrow_metadata(
                &inspection.chunk_reader,
                PageIndexPolicy::Skip,
                &inspection.context,
                "inspect Parquet metadata",
            )
            .await?;
            inspection.remember(&metadata);
            metadata
        }
    };
    inspection.finish(metadata)
}

/// Determine the complete footer suffix from an authorized prepared tail.
/// The caller can request this range with `try_start_with_present` so only the
/// missing prefix reaches storage, then parse it on scan CPU.
pub fn parquet_footer_range(
    file: &BoundFile,
    tail: &PreparedFileInput,
) -> FileResult<FileReadRange> {
    tail.validate_for(file)?;
    let file_size = file.identity().file_size();
    let tail_range = tail.range();
    if file_size < FOOTER_SIZE as u64
        || tail_range.end != file_size
        || tail_range.start > file_size - FOOTER_SIZE as u64
    {
        return Err(FileError::new(
            FileErrorKind::Corrupt,
            "prepared Parquet tail does not cover the file footer",
        ));
    }
    let footer = &tail.bytes()[tail.bytes().len() - FOOTER_SIZE..];
    let footer: &[u8; FOOTER_SIZE] = footer.try_into().expect("exact footer slice");
    let footer = FooterTail::try_new(footer)
        .map_err(|error| parquet_error("decode prepared Parquet footer", error))?;
    if footer.is_encrypted_footer() {
        return Err(FileError::unsupported(
            "encrypted Parquet footer is not supported by prepared inspection",
        ));
    }
    let suffix_length = footer
        .metadata_length()
        .checked_add(FOOTER_SIZE)
        .ok_or_else(|| FileError::new(FileErrorKind::Corrupt, "Parquet footer length overflows"))?;
    let suffix_length = u64::try_from(suffix_length).map_err(|_| {
        FileError::new(
            FileErrorKind::Corrupt,
            "Parquet footer length exceeds address space",
        )
    })?;
    if suffix_length > file_size {
        return Err(FileError::new(
            FileErrorKind::Corrupt,
            "Parquet footer length exceeds bound file length",
        ));
    }
    FileReadRange::bounded(file_size - suffix_length, suffix_length)
}

/// Parse already prepared footer bytes without admitting a further object
/// request or populating a cache entry backed by untracked prepared bytes.
pub fn inspect_parquet_metadata_from_prepared(
    file: BoundFile,
    prepared: PreparedFileInput,
    context: FileReadContext,
) -> FileResult<ParquetMetadataInspection> {
    let required = parquet_footer_range(&file, &prepared)?;
    let FileReadRange::Bounded { offset, .. } = required else {
        unreachable!("footer range is bounded")
    };
    if prepared.range().start > offset {
        return Err(FileError::invalid(
            "prepared Parquet input does not cover the complete footer",
        ));
    }
    let mut context = context;
    context.range = None;
    inspect_parquet_metadata_inner(file, None, context, Some(prepared))
}

fn inspect_parquet_metadata_inner(
    file: BoundFile,
    cache: Option<DataCacheContext>,
    context: FileReadContext,
    prepared: Option<PreparedFileInput>,
) -> FileResult<ParquetMetadataInspection> {
    let inspection = InspectionReader::try_new(file, cache, context, prepared)?;
    let metadata = match inspection.cached() {
        Some(metadata) => metadata,
        None => {
            let options = ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Skip);
            let metadata = ArrowReaderMetadata::load(&inspection.chunk_reader, options)
                .map_err(|error| parquet_error("inspect Parquet metadata", error))?;
            inspection.remember(&metadata);
            metadata
        }
    };
    inspection.finish(metadata)
}

/// One footer inspection before and after its metadata is in hand; only
/// obtaining the metadata differs between the blocking and awaited paths.
struct InspectionReader {
    chunk_reader: BoundChunkReader,
    context: FileReadContext,
    cache_enabled: bool,
    access_domain: StorageAccessDomainId,
    identity: FileIdentity,
}

impl InspectionReader {
    fn try_new(
        file: BoundFile,
        cache: Option<DataCacheContext>,
        context: FileReadContext,
        prepared: Option<PreparedFileInput>,
    ) -> FileResult<Self> {
        context.check_active()?;
        let cache_enabled = cache
            .as_ref()
            .is_some_and(crate::DataCacheContext::datacache_requested);
        let access_domain = file.access_domain();
        let identity = file.identity().clone();
        let chunk_reader = BoundChunkReader::new(
            file,
            context.clone(),
            cache,
            crate::cache::parquet_cache::page_cache_enabled(cache_enabled),
            Arc::new(ReaderMetrics::default()),
        )
        .with_prepared_input(prepared)?;
        Ok(Self {
            chunk_reader,
            context,
            cache_enabled,
            access_domain,
            identity,
        })
    }

    fn cached(&self) -> Option<ArrowReaderMetadata> {
        crate::cache::parquet_cache::metadata_get(
            self.cache_enabled,
            self.access_domain,
            &self.identity,
            false,
        )
    }

    fn remember(&self, metadata: &ArrowReaderMetadata) {
        crate::cache::parquet_cache::metadata_put(
            self.cache_enabled,
            self.access_domain,
            &self.identity,
            metadata.clone(),
        );
    }

    fn finish(self, metadata: ArrowReaderMetadata) -> FileResult<ParquetMetadataInspection> {
        let InspectionReader {
            chunk_reader,
            context,
            access_domain,
            identity,
            ..
        } = self;
        finish_inspection(metadata, &chunk_reader, &context, access_domain, identity)
    }
}

fn finish_inspection(
    metadata: ArrowReaderMetadata,
    chunk_reader: &BoundChunkReader,
    context: &FileReadContext,
    access_domain: StorageAccessDomainId,
    identity: FileIdentity,
) -> FileResult<ParquetMetadataInspection> {
    context.check_active()?;
    let parquet = metadata.metadata();
    if parquet.num_row_groups() > MAX_PARQUET_INSPECTION_ROW_GROUPS {
        return Err(FileError::new(
            FileErrorKind::ResourceExhausted,
            format!(
                "Parquet row-group count {} exceeds inspection bound {MAX_PARQUET_INSPECTION_ROW_GROUPS}",
                parquet.num_row_groups()
            ),
        ));
    }
    let schema_descriptor = parquet.file_metadata().schema_descr();
    if schema_descriptor.num_columns() > MAX_PARQUET_INSPECTION_PHYSICAL_COLUMNS {
        return Err(FileError::new(
            FileErrorKind::ResourceExhausted,
            format!(
                "Parquet physical-column count {} exceeds inspection bound {MAX_PARQUET_INSPECTION_PHYSICAL_COLUMNS}",
                schema_descriptor.num_columns()
            ),
        ));
    }
    let statistic_cells = parquet
        .num_row_groups()
        .checked_mul(schema_descriptor.num_columns())
        .ok_or_else(|| {
            FileError::new(
                FileErrorKind::ResourceExhausted,
                "Parquet statistic-cell count overflows inspection accounting",
            )
        })?;
    if statistic_cells > MAX_PARQUET_INSPECTION_STATISTIC_CELLS {
        return Err(FileError::new(
            FileErrorKind::ResourceExhausted,
            format!(
                "Parquet statistic-cell count {statistic_cells} exceeds inspection bound {MAX_PARQUET_INSPECTION_STATISTIC_CELLS}",
            ),
        ));
    }

    let physical_columns = schema_descriptor
        .columns()
        .iter()
        .enumerate()
        .map(|(ordinal, column)| {
            let ordinal = u32::try_from(ordinal).map_err(|_| {
                FileError::new(
                    FileErrorKind::ResourceExhausted,
                    "Parquet physical-column ordinal does not fit u32",
                )
            })?;
            let basic = column.self_type().get_basic_info();
            Ok(ParquetPhysicalColumn {
                ordinal,
                path: column.path().parts().to_vec(),
                field_id: basic.has_id().then(|| basic.id()),
                physical_type: parquet_physical_type(column.physical_type()),
                sort_order: parquet_sort_order(column.sort_order()),
            })
        })
        .collect::<FileResult<Vec<_>>>()?;
    let row_groups = parquet
        .row_groups()
        .iter()
        .enumerate()
        .map(|(ordinal, row_group)| {
            context.check_active()?;
            let ordinal = u32::try_from(ordinal).map_err(|_| {
                FileError::new(
                    FileErrorKind::ResourceExhausted,
                    "Parquet row-group ordinal does not fit u32",
                )
            })?;
            let compressed_bytes = u64::try_from(row_group.compressed_size()).map_err(|_| {
                FileError::new(
                    FileErrorKind::Corrupt,
                    "Parquet row-group compressed size is negative",
                )
            })?;
            let row_count = u64::try_from(row_group.num_rows()).map_err(|_| {
                FileError::new(
                    FileErrorKind::Corrupt,
                    "Parquet row-group row count is negative",
                )
            })?;
            Ok(ParquetRowGroupLayout {
                ordinal,
                compressed_bytes,
                row_count,
            })
        })
        .collect::<FileResult<Vec<_>>>()?;
    let statistics = parquet
        .row_groups()
        .iter()
        .map(|row_group| {
            context.check_active()?;
            if row_group.columns().len() != physical_columns.len() {
                return Err(FileError::new(
                    FileErrorKind::Corrupt,
                    "Parquet row group physical-column count disagrees with footer schema",
                ));
            }
            row_group
                .columns()
                .iter()
                .zip(&physical_columns)
                .map(|(column, physical)| {
                    context.check_active()?;
                    let descriptor = column.column_descr();
                    if descriptor.path().parts() != physical.path() {
                        return Err(FileError::new(
                            FileErrorKind::Corrupt,
                            "Parquet row group physical-column path disagrees with footer schema",
                        ));
                    }
                    column
                        .statistics()
                        .map(|statistics| {
                            extract_parquet_column_statistics(statistics, physical.sort_order())
                        })
                        .transpose()
                })
                .collect::<FileResult<Vec<_>>>()
        })
        .collect::<FileResult<Vec<_>>>()?;
    let schema = metadata.schema().clone();
    Ok(ParquetMetadataInspection {
        footer: Arc::new(metadata),
        access_domain,
        identity,
        indexed_footer: Arc::default(),
        small_file: chunk_reader.small_file_buffer(),
        schema,
        physical_columns,
        row_groups,
        statistics,
    })
}

pub(crate) struct ParquetPhysicalReader {
    decoder: Option<ParquetPushDecoder>,
    chunk_reader: BoundChunkReader,
    options: FileReaderOptions,
    positions: VecDeque<PositionSpan>,
    context: crate::FileReadContext,
    metrics: Arc<ReaderMetrics>,
    closed: bool,
}

#[derive(Clone, Copy, Debug)]
struct PositionSpan {
    next: u64,
    remaining: usize,
}

/// One reader open before and after its metadata is in hand; only obtaining
/// the metadata differs between the blocking and awaited paths.
struct ReaderOpen {
    chunk_reader: BoundChunkReader,
    metrics: Arc<ReaderMetrics>,
    cache_enabled: bool,
    access_domain: StorageAccessDomainId,
    identity: FileIdentity,
    automatic_page_pruning: bool,
    page_index_policy: PageIndexPolicy,
}

impl ReaderOpen {
    fn try_new(
        request: &FileReadRequest,
        inspection: Option<&ParquetMetadataInspection>,
    ) -> FileResult<Self> {
        request.context.check_active()?;
        let metrics = Arc::new(ReaderMetrics::default());
        let cache_enabled = request
            .cache
            .as_ref()
            .is_some_and(crate::DataCacheContext::datacache_requested);
        let access_domain = request.file.access_domain();
        let identity = request.file.identity().clone();
        let chunk_reader = BoundChunkReader::new(
            request.file.clone(),
            request.context.clone(),
            request.cache.clone(),
            crate::cache::parquet_cache::page_cache_enabled(cache_enabled),
            Arc::clone(&metrics),
        )
        .with_prepared_input(request.prepared_input.clone())?;
        let chunk_reader = if let Some(inspection) = inspection {
            chunk_reader.with_small_file_buffer(Arc::clone(&inspection.small_file))
        } else {
            chunk_reader
        };
        let automatic_page_pruning =
            request.options.enable_parquet_reader_page_index && !request.predicates.is_empty();
        let page_index_policy = if request.pruning.pages.is_empty() && !automatic_page_pruning {
            PageIndexPolicy::Skip
        } else {
            PageIndexPolicy::Optional
        };
        Ok(Self {
            chunk_reader,
            metrics,
            cache_enabled,
            access_domain,
            identity,
            automatic_page_pruning,
            page_index_policy,
        })
    }

    fn cached(&self) -> Option<ArrowReaderMetadata> {
        crate::cache::parquet_cache::metadata_get(
            self.cache_enabled,
            self.access_domain,
            &self.identity,
            self.page_index_policy != PageIndexPolicy::Skip,
        )
    }

    fn remember(&self, metadata: &ArrowReaderMetadata) {
        crate::cache::parquet_cache::metadata_put(
            self.cache_enabled,
            self.access_domain,
            &self.identity,
            metadata.clone(),
        );
    }

    fn finish(
        self,
        request: FileReadRequest,
        arrow_metadata: ArrowReaderMetadata,
    ) -> FileResult<ParquetPhysicalReader> {
        let builder = ParquetPushDecoderBuilder::new_with_metadata(arrow_metadata.clone());
        request.context.check_active()?;

        let projected_roots = projection_roots(&builder, &request.projection)?;
        let metadata = builder.metadata().clone();
        let row_groups = select_row_groups(
            metadata.as_ref(),
            request.range,
            request.pruning.row_groups.as_deref(),
            &request.predicates,
        );
        self.metrics
            .record_row_group_selection(metadata.num_row_groups(), row_groups.len());
        let automatic_ranges = self
            .automatic_page_pruning
            .then(|| automatic_page_ranges(metadata.as_ref(), &row_groups, &request.predicates))
            .transpose()?;
        if let Some(automatic) = automatic_ranges.as_ref() {
            self.metrics.record_page_index(
                automatic.fallback,
                automatic.rows_considered,
                automatic.rows_pruned,
            );
        }
        let (selection, positions) = page_selection(
            metadata.as_ref(),
            &row_groups,
            &request.pruning.pages,
            automatic_ranges.as_ref().map(|ranges| &ranges.by_row_group),
        )?;
        let reader = build_projected_reader(
            arrow_metadata,
            &projected_roots,
            request.budget.max_rows.get(),
            &row_groups,
            selection,
        )?;

        Ok(ParquetPhysicalReader {
            decoder: Some(reader),
            chunk_reader: self.chunk_reader,
            options: request.options,
            positions,
            context: request.context,
            metrics: self.metrics,
            closed: false,
        })
    }
}

impl ParquetPhysicalReader {
    pub(crate) fn try_new(
        request: FileReadRequest,
        inspection: Option<&ParquetMetadataInspection>,
    ) -> FileResult<Self> {
        let open = ReaderOpen::try_new(&request, inspection)?;
        let metadata = match inspection {
            Some(inspection) => inspection.metadata_for(
                &request.file,
                &open.chunk_reader,
                open.page_index_policy,
                &request.context,
            )?,
            None => match open.cached() {
                Some(metadata) => metadata,
                None => {
                    let options =
                        ArrowReaderOptions::new().with_page_index_policy(open.page_index_policy);
                    let metadata = ArrowReaderMetadata::load(&open.chunk_reader, options)
                        .map_err(|error| parquet_error("open Parquet metadata", error))?;
                    open.remember(&metadata);
                    metadata
                }
            },
        };
        open.finish(request, metadata)
    }

    /// Awaited [`Self::try_new`]: footer and page-index ranges are awaited
    /// through the chunk reader.
    pub(crate) async fn try_new_async(
        request: FileReadRequest,
        inspection: Option<&ParquetMetadataInspection>,
    ) -> FileResult<Self> {
        let open = ReaderOpen::try_new(&request, inspection)?;
        let metadata = match inspection {
            Some(inspection) => {
                inspection
                    .metadata_for_async(
                        &request.file,
                        &open.chunk_reader,
                        open.page_index_policy,
                        &request.context,
                    )
                    .await?
            }
            None => match open.cached() {
                Some(metadata) => metadata,
                None => {
                    let metadata = load_arrow_metadata(
                        &open.chunk_reader,
                        open.page_index_policy,
                        &request.context,
                        "open Parquet metadata",
                    )
                    .await?;
                    open.remember(&metadata);
                    metadata
                }
            },
        };
        open.finish(request, metadata)
    }

    fn take_positions(&mut self, count: usize) -> FileResult<UInt64Array> {
        let mut output = Vec::with_capacity(count);
        while output.len() < count {
            let Some(span) = self.positions.front_mut() else {
                return Err(FileError::new(
                    FileErrorKind::Corrupt,
                    "Parquet decoder produced more rows than selected row-group metadata",
                ));
            };
            let take = span.remaining.min(count - output.len());
            output.extend(span.next..span.next + take as u64);
            span.next += take as u64;
            span.remaining -= take;
            if span.remaining == 0 {
                self.positions.pop_front();
            }
        }
        Ok(UInt64Array::from(output))
    }
}

impl ParquetPhysicalReader {
    /// Awaited [`FileBatchReader::next_batch`]: the decoder's input requests
    /// are awaited through the chunk reader, and the decoder itself runs on
    /// the calling task.
    pub(crate) async fn next_batch_async(&mut self) -> FileResult<Option<FileBatch>> {
        if self.closed {
            return Ok(None);
        }
        self.context.check_active()?;
        let began = Instant::now();
        let next = loop {
            let decoder = self
                .decoder
                .as_mut()
                .expect("Parquet decoder must exist before close");
            match decoder
                .try_decode()
                .map_err(|error| format_error("decode Parquet batch", error))?
            {
                DecodeResult::Data(batch) => break Some(batch),
                DecodeResult::Finished => break None,
                DecodeResult::NeedsData(ranges) => {
                    if ranges.is_empty() {
                        return Err(FileError::new(
                            FileErrorKind::Corrupt,
                            "Parquet decoder requested no data while waiting for input",
                        ));
                    }
                    self.context.check_active()?;
                    let data = read_decoder_ranges_async(&self.chunk_reader, &ranges, self.options)
                        .await?;
                    self.decoder
                        .as_mut()
                        .expect("Parquet decoder must exist before close")
                        .push_ranges(ranges, data)
                        .map_err(|error| format_error("push Parquet input", error))?;
                }
            }
        };
        self.deliver(next, began)
    }

    fn deliver(
        &mut self,
        next: Option<arrow::record_batch::RecordBatch>,
        began: Instant,
    ) -> FileResult<Option<FileBatch>> {
        self.context.check_active()?;
        let Some(batch) = next else {
            self.close()?;
            return Ok(None);
        };
        let positions = self.take_positions(batch.num_rows())?;
        self.metrics
            .record_decode(batch.num_rows(), began.elapsed().as_nanos());
        self.metrics.record_delivery();
        Ok(Some(FileBatch {
            batch,
            physical_row_positions: Some(positions),
        }))
    }
}

impl FileBatchReader for ParquetPhysicalReader {
    fn next_batch(&mut self) -> FileResult<Option<FileBatch>> {
        if self.closed {
            return Ok(None);
        }
        self.context.check_active()?;
        let began = Instant::now();
        let next = loop {
            let decoder = self
                .decoder
                .as_mut()
                .expect("Parquet decoder must exist before close");
            match decoder
                .try_decode()
                .map_err(|error| format_error("decode Parquet batch", error))?
            {
                DecodeResult::Data(batch) => break Some(batch),
                DecodeResult::Finished => break None,
                DecodeResult::NeedsData(ranges) => {
                    if ranges.is_empty() {
                        return Err(FileError::new(
                            FileErrorKind::Corrupt,
                            "Parquet decoder requested no data while waiting for input",
                        ));
                    }
                    self.context.check_active()?;
                    let data = read_decoder_ranges(&self.chunk_reader, &ranges, self.options)?;
                    decoder
                        .push_ranges(ranges, data)
                        .map_err(|error| format_error("push Parquet input", error))?;
                }
            }
        };
        self.deliver(next, began)
    }

    fn close(&mut self) -> FileResult<()> {
        if self.closed {
            return Ok(());
        }
        self.closed = true;
        self.decoder = None;
        self.positions.clear();
        Ok(())
    }

    fn metrics_snapshot(&self) -> FileMetricsSnapshot {
        self.metrics.snapshot()
    }
}

impl Drop for ParquetPhysicalReader {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

fn projection_roots(
    builder: &ParquetPushDecoderBuilder,
    projection: &FileProjection,
) -> FileResult<Vec<usize>> {
    let parquet_schema = builder.parquet_schema();
    let arrow_schema = builder.schema();
    let mut roots = match projection {
        FileProjection::All => (0..arrow_schema.fields().len()).collect(),
        FileProjection::RootNames(names) => {
            let by_name = arrow_schema
                .fields()
                .iter()
                .enumerate()
                .map(|(index, field)| (field.name().as_str(), index))
                .collect::<HashMap<_, _>>();
            let available_names = arrow_schema
                .fields()
                .iter()
                .map(|field| field.name().as_str())
                .collect::<Vec<_>>();
            names
                .iter()
                .map(|name| {
                    by_name.get(name.as_str()).copied().ok_or_else(|| {
                        FileError::invalid(format!(
                            "Parquet projection column does not exist: {name}; available root columns: {available_names:?}"
                        ))
                    })
                })
                .collect::<FileResult<Vec<_>>>()?
        }
        FileProjection::RootIndices(indices) => {
            for index in indices {
                if *index >= arrow_schema.fields().len() {
                    return Err(FileError::invalid(format!(
                        "Parquet root projection index out of bounds: {index}"
                    )));
                }
            }
            indices.clone()
        }
        FileProjection::FieldIds(field_ids) => {
            let wanted = field_ids.iter().copied().collect::<HashSet<_>>();
            let mut found = HashSet::new();
            let mut roots = Vec::new();
            for (index, field) in parquet_schema.root_schema().get_fields().iter().enumerate() {
                let info = field.get_basic_info();
                if info.has_id() && wanted.contains(&info.id()) {
                    roots.push(index);
                    found.insert(info.id());
                }
            }
            if found.len() != wanted.len() {
                let mut missing = wanted.difference(&found).copied().collect::<Vec<_>>();
                missing.sort_unstable();
                return Err(FileError::invalid(format!(
                    "Parquet field-ID projection contains unknown IDs: {missing:?}"
                )));
            }
            roots
        }
    };
    roots.sort_unstable();
    roots.dedup();
    Ok(roots)
}

fn build_projected_reader(
    metadata: ArrowReaderMetadata,
    projected_roots: &[usize],
    batch_size: usize,
    row_groups: &[usize],
    selection: Option<RowSelection>,
) -> FileResult<ParquetPushDecoder> {
    let mut builder = ParquetPushDecoderBuilder::new_with_metadata(metadata);
    let projection =
        ProjectionMask::roots(builder.parquet_schema(), projected_roots.iter().copied());
    builder = builder
        .with_projection(projection)
        .with_batch_size(batch_size)
        .with_row_groups(row_groups.to_vec());
    if let Some(selection) = selection {
        builder = builder.with_row_selection(selection);
    }
    builder
        .build()
        .map_err(|error| parquet_error("build Parquet reader", error))
}

fn select_row_groups(
    metadata: &ParquetMetaData,
    range: FileReadRange,
    explicit: Option<&[usize]>,
    predicates: &[ScanPredicate],
) -> Vec<usize> {
    let explicit = explicit.map(|groups| groups.iter().copied().collect::<HashSet<_>>());
    metadata
        .row_groups()
        .iter()
        .enumerate()
        .filter(|(index, row_group)| {
            explicit
                .as_ref()
                .is_none_or(|groups| groups.contains(index))
                && row_group_in_range(row_group, range)
                && row_group_may_match(row_group, predicates)
        })
        .map(|(index, _)| index)
        .collect()
}

fn row_group_in_range(row_group: &RowGroupMetaData, range: FileReadRange) -> bool {
    let FileReadRange::Bounded { offset, length } = range else {
        return true;
    };
    let end = offset.saturating_add(length);
    row_group_start_offset(row_group).is_none_or(|start| start >= offset && start < end)
}

fn row_group_start_offset(row_group: &RowGroupMetaData) -> Option<u64> {
    row_group
        .columns()
        .first()
        .map(|column| {
            column
                .dictionary_page_offset()
                .unwrap_or_else(|| column.data_page_offset())
                .min(column.data_page_offset())
        })
        .and_then(|offset| u64::try_from(offset).ok())
}

fn row_position_spans(
    metadata: &ParquetMetaData,
    selected: &[usize],
) -> FileResult<VecDeque<PositionSpan>> {
    let selected = selected.iter().copied().collect::<HashSet<_>>();
    let mut first_row = 0u64;
    let mut spans = VecDeque::new();
    for (index, row_group) in metadata.row_groups().iter().enumerate() {
        let rows = usize::try_from(row_group.num_rows()).map_err(|_| {
            FileError::new(
                FileErrorKind::Corrupt,
                "negative or overflowing Parquet row-group row count",
            )
        })?;
        if selected.contains(&index) {
            spans.push_back(PositionSpan {
                next: first_row,
                remaining: rows,
            });
        }
        first_row = first_row
            .checked_add(rows as u64)
            .ok_or_else(|| FileError::new(FileErrorKind::Corrupt, "Parquet row count overflow"))?;
    }
    Ok(spans)
}

fn page_selection(
    metadata: &ParquetMetaData,
    selected: &[usize],
    pages: &[crate::PhysicalPageSelection],
    automatic_ranges: Option<&HashMap<usize, Vec<std::ops::Range<usize>>>>,
) -> FileResult<(Option<RowSelection>, VecDeque<PositionSpan>)> {
    if pages.is_empty() && automatic_ranges.is_none() {
        return Ok((None, row_position_spans(metadata, selected)?));
    }

    let mut pages_by_row_group = HashMap::<usize, HashSet<usize>>::new();
    for page in pages {
        let entry = pages_by_row_group.entry(page.row_group).or_default();
        entry.extend(page.page_indices.iter().copied());
    }
    let first_rows = row_group_first_rows(metadata)?;
    let mut selected_ranges = Vec::new();
    let mut positions = VecDeque::new();
    let mut selection_offset = 0usize;

    for &row_group_index in selected {
        let row_group = metadata.row_groups().get(row_group_index).ok_or_else(|| {
            FileError::invalid(format!(
                "Parquet row-group selection is out of bounds: {row_group_index}"
            ))
        })?;
        let row_count = usize::try_from(row_group.num_rows()).map_err(|_| {
            FileError::new(
                FileErrorKind::Corrupt,
                "negative or overflowing Parquet row-group row count",
            )
        })?;
        let explicit_ranges = if let Some(selected_pages) = pages_by_row_group.get(&row_group_index)
        {
            let Some(offset_index) = metadata.offset_index() else {
                return Err(FileError::unsupported(
                    "explicit Parquet page selection requires an offset index",
                ));
            };
            let page_locations = offset_index
                .get(row_group_index)
                .and_then(|columns| columns.first())
                .ok_or_else(|| {
                    FileError::unsupported(format!(
                        "Parquet row group {row_group_index} has no offset-index column"
                    ))
                })?
                .page_locations();
            explicit_page_ranges(row_group_index, row_count, page_locations, selected_pages)?
        } else {
            std::iter::once(0..row_count).collect::<Vec<_>>()
        };
        let ranges = automatic_ranges
            .and_then(|ranges| ranges.get(&row_group_index))
            .map(|automatic| intersect_ranges(&explicit_ranges, automatic))
            .unwrap_or(explicit_ranges);

        for range in ranges {
            selected_ranges.push(selection_offset + range.start..selection_offset + range.end);
            positions.push_back(PositionSpan {
                next: first_rows[row_group_index] + range.start as u64,
                remaining: range.end - range.start,
            });
        }
        selection_offset = selection_offset.checked_add(row_count).ok_or_else(|| {
            FileError::new(FileErrorKind::Corrupt, "Parquet row selection overflow")
        })?;
    }

    Ok((
        Some(RowSelection::from_consecutive_ranges(
            selected_ranges.into_iter(),
            selection_offset,
        )),
        positions,
    ))
}

#[derive(Debug)]
struct AutomaticPageRanges {
    by_row_group: HashMap<usize, Vec<std::ops::Range<usize>>>,
    fallback: bool,
    rows_considered: u64,
    rows_pruned: u64,
}

/// Select row ranges from Parquet page indexes. This is deliberately an
/// optional optimization: a missing, unsupported, or non-comparable page
/// index leaves the whole row group selected. The only error path is malformed
/// structural metadata after the Parquet library has declared a page index
/// present, because continuing would make the reader's row-position contract
/// ambiguous.
fn automatic_page_ranges(
    metadata: &ParquetMetaData,
    selected: &[usize],
    predicates: &[ScanPredicate],
) -> FileResult<AutomaticPageRanges> {
    let Some(column_indexes) = metadata.column_index() else {
        return automatic_page_fallback(metadata, selected);
    };
    let Some(offset_indexes) = metadata.offset_index() else {
        return automatic_page_fallback(metadata, selected);
    };

    let mut by_row_group = HashMap::new();
    let mut fallback = false;
    let mut rows_considered = 0u64;
    let mut rows_pruned = 0u64;
    for &row_group_index in selected {
        let row_group = metadata.row_groups().get(row_group_index).ok_or_else(|| {
            FileError::invalid(format!(
                "Parquet row-group selection is out of bounds: {row_group_index}"
            ))
        })?;
        let row_count = usize::try_from(row_group.num_rows()).map_err(|_| {
            FileError::new(
                FileErrorKind::Corrupt,
                format!(
                    "Parquet row group {row_group_index} has a negative or overflowing row count"
                ),
            )
        })?;
        rows_considered = rows_considered.saturating_add(row_count as u64);
        let full = std::iter::once(0..row_count).collect::<Vec<_>>();
        let mut candidate = full.clone();
        let mut row_group_fallback = false;

        for predicate in predicates {
            let Some(column_ordinal) = predicate_column_ordinal(row_group, predicate) else {
                row_group_fallback = true;
                break;
            };
            let column_index = page_index_cell(
                column_indexes,
                row_group_index,
                column_ordinal,
                "column index",
                row_group,
            )?;
            let offset_index = page_index_cell(
                offset_indexes,
                row_group_index,
                column_ordinal,
                "offset index",
                row_group,
            )?;
            let Some(predicate_ranges) = predicate_page_ranges(
                row_group_index,
                row_count,
                column_index,
                offset_index.page_locations(),
                predicate,
                row_group,
            )?
            else {
                row_group_fallback = true;
                break;
            };
            if predicate_ranges.is_empty() {
                // A file-level page index is advisory. Even when every page
                // appears disjoint, retain a conservative path for writers
                // with incomplete page-index coverage.
                row_group_fallback = true;
                break;
            }
            candidate = intersect_ranges(&candidate, &predicate_ranges);
            if candidate.is_empty() {
                row_group_fallback = true;
                break;
            }
        }

        if row_group_fallback {
            fallback = true;
            by_row_group.insert(row_group_index, full);
        } else {
            let selected_rows = range_len(&candidate);
            rows_pruned = rows_pruned.saturating_add(
                u64::try_from(row_count.saturating_sub(selected_rows)).unwrap_or(u64::MAX),
            );
            by_row_group.insert(row_group_index, candidate);
        }
    }
    Ok(AutomaticPageRanges {
        by_row_group,
        fallback,
        rows_considered,
        rows_pruned,
    })
}

fn automatic_page_fallback(
    metadata: &ParquetMetaData,
    selected: &[usize],
) -> FileResult<AutomaticPageRanges> {
    let mut by_row_group = HashMap::new();
    let mut rows_considered = 0u64;
    for &row_group_index in selected {
        let row_group = metadata.row_groups().get(row_group_index).ok_or_else(|| {
            FileError::invalid(format!(
                "Parquet row-group selection is out of bounds: {row_group_index}"
            ))
        })?;
        let row_count = usize::try_from(row_group.num_rows()).map_err(|_| {
            FileError::new(
                FileErrorKind::Corrupt,
                format!(
                    "Parquet row group {row_group_index} has a negative or overflowing row count"
                ),
            )
        })?;
        rows_considered = rows_considered.saturating_add(row_count as u64);
        by_row_group.insert(
            row_group_index,
            std::iter::once(0..row_count).collect::<Vec<_>>(),
        );
    }
    Ok(AutomaticPageRanges {
        by_row_group,
        fallback: true,
        rows_considered,
        rows_pruned: 0,
    })
}

fn predicate_column_ordinal(
    row_group: &RowGroupMetaData,
    predicate: &ScanPredicate,
) -> Option<usize> {
    predicate
        .physical_field_id()
        .and_then(|field_id| {
            row_group.columns().iter().position(|column| {
                let info = column.column_descr().self_type().get_basic_info();
                info.has_id() && info.id() == field_id
            })
        })
        // File writers occasionally omit a field ID even where a table schema
        // has one. A full physical path match is still a safe fallback; do not
        // use a leaf-only or root-only comparison that could bind a nested
        // column with the same name.
        .or_else(|| {
            row_group
                .columns()
                .iter()
                .position(|column| column.column_path().parts().join(".") == predicate.column())
        })
}

fn page_index_cell<'a, T>(
    indexes: &'a [Vec<T>],
    row_group_index: usize,
    column_ordinal: usize,
    kind: &str,
    row_group: &RowGroupMetaData,
) -> FileResult<&'a T> {
    indexes
        .get(row_group_index)
        .and_then(|columns| columns.get(column_ordinal))
        .ok_or_else(|| {
            let path = row_group
                .columns()
                .get(column_ordinal)
                .map(|column| column.column_path().parts().join("."))
                .unwrap_or_else(|| "<unknown>".to_string());
            FileError::new(
                FileErrorKind::Corrupt,
                format!(
                    "Parquet {kind} is structurally incomplete for row group {row_group_index}, column chunk {column_ordinal} ({path})"
                ),
            )
        })
}

fn predicate_page_ranges(
    row_group_index: usize,
    row_count: usize,
    column_index: &ColumnIndexMetaData,
    page_locations: &[parquet::file::page_index::offset_index::PageLocation],
    predicate: &ScanPredicate,
    row_group: &RowGroupMetaData,
) -> FileResult<Option<Vec<std::ops::Range<usize>>>> {
    if matches!(column_index, ColumnIndexMetaData::NONE) {
        return Ok(None);
    }
    let page_count = usize::try_from(column_index.num_pages()).map_err(|_| {
        FileError::new(
            FileErrorKind::Corrupt,
            format!("Parquet page count overflows usize in row group {row_group_index}"),
        )
    })?;
    if page_count != page_locations.len() {
        return Err(FileError::new(
            FileErrorKind::Corrupt,
            format!(
                "Parquet page-index cardinality mismatch in row group {row_group_index}, column chunk {} ({})",
                predicate_column_ordinal(row_group, predicate).unwrap_or_default(),
                predicate.column(),
            ),
        ));
    }
    let mut ranges = Vec::new();
    for page_index in 0..page_count {
        let Some((min, max)) = page_index_bounds(column_index, page_index) else {
            return Ok(None);
        };
        if !predicate.domain().can_compare_bounds(&min, &max) {
            return Ok(None);
        }
        if predicate.domain().may_match_bounds(&min, &max) {
            ranges.push(page_range(
                row_group_index,
                row_count,
                page_locations,
                page_index,
                predicate.column(),
            )?);
        }
    }
    Ok(Some(merge_ranges(ranges)))
}

fn page_index_bounds(
    column_index: &ColumnIndexMetaData,
    page_index: usize,
) -> Option<(MinMaxPredicateValue, MinMaxPredicateValue)> {
    match column_index {
        ColumnIndexMetaData::BOOLEAN(index) => Some((
            MinMaxPredicateValue::Boolean(*index.min_value(page_index)?),
            MinMaxPredicateValue::Boolean(*index.max_value(page_index)?),
        )),
        ColumnIndexMetaData::INT32(index) => Some((
            MinMaxPredicateValue::Int32(*index.min_value(page_index)?),
            MinMaxPredicateValue::Int32(*index.max_value(page_index)?),
        )),
        ColumnIndexMetaData::INT64(index) => Some((
            MinMaxPredicateValue::Int64(*index.min_value(page_index)?),
            MinMaxPredicateValue::Int64(*index.max_value(page_index)?),
        )),
        _ => None,
    }
}

fn page_range(
    row_group_index: usize,
    row_count: usize,
    locations: &[parquet::file::page_index::offset_index::PageLocation],
    page_index: usize,
    column: &str,
) -> FileResult<std::ops::Range<usize>> {
    let start = usize::try_from(locations[page_index].first_row_index).map_err(|_| {
        FileError::new(
            FileErrorKind::Corrupt,
            format!(
                "Parquet page first-row index is negative in row group {row_group_index}, column {column}"
            ),
        )
    })?;
    let end = locations
        .get(page_index + 1)
        .map(|location| usize::try_from(location.first_row_index))
        .transpose()
        .map_err(|_| {
            FileError::new(
                FileErrorKind::Corrupt,
                format!(
                    "Parquet page first-row index is negative in row group {row_group_index}, column {column}"
                ),
            )
        })?
        .unwrap_or(row_count);
    if start >= end || end > row_count {
        return Err(FileError::new(
            FileErrorKind::Corrupt,
            format!(
                "Parquet page row range is invalid in row group {row_group_index}, column {column}"
            ),
        ));
    }
    Ok(start..end)
}

fn intersect_ranges(
    left: &[std::ops::Range<usize>],
    right: &[std::ops::Range<usize>],
) -> Vec<std::ops::Range<usize>> {
    let mut result = Vec::new();
    let (mut left_index, mut right_index) = (0, 0);
    while left_index < left.len() && right_index < right.len() {
        let start = left[left_index].start.max(right[right_index].start);
        let end = left[left_index].end.min(right[right_index].end);
        if start < end {
            result.push(start..end);
        }
        if left[left_index].end <= right[right_index].end {
            left_index += 1;
        } else {
            right_index += 1;
        }
    }
    result
}

fn merge_ranges(mut ranges: Vec<std::ops::Range<usize>>) -> Vec<std::ops::Range<usize>> {
    ranges.sort_unstable_by_key(|range| range.start);
    let mut merged = Vec::<std::ops::Range<usize>>::new();
    for range in ranges {
        if let Some(previous) = merged.last_mut()
            && previous.end >= range.start
        {
            previous.end = previous.end.max(range.end);
        } else {
            merged.push(range);
        }
    }
    merged
}

fn range_len(ranges: &[std::ops::Range<usize>]) -> usize {
    ranges.iter().fold(0usize, |total, range| {
        total.saturating_add(range.end.saturating_sub(range.start))
    })
}

fn row_group_first_rows(metadata: &ParquetMetaData) -> FileResult<Vec<u64>> {
    let mut first_rows = Vec::with_capacity(metadata.num_row_groups());
    let mut first_row = 0u64;
    for row_group in metadata.row_groups() {
        first_rows.push(first_row);
        let rows = u64::try_from(row_group.num_rows()).map_err(|_| {
            FileError::new(
                FileErrorKind::Corrupt,
                "negative Parquet row-group row count",
            )
        })?;
        first_row = first_row
            .checked_add(rows)
            .ok_or_else(|| FileError::new(FileErrorKind::Corrupt, "Parquet row count overflow"))?;
    }
    Ok(first_rows)
}

fn explicit_page_ranges(
    row_group_index: usize,
    row_count: usize,
    page_locations: &[parquet::file::page_index::offset_index::PageLocation],
    selected_pages: &HashSet<usize>,
) -> FileResult<Vec<std::ops::Range<usize>>> {
    if let Some(index) = selected_pages
        .iter()
        .copied()
        .find(|index| *index >= page_locations.len())
    {
        return Err(FileError::invalid(format!(
            "Parquet page selection is out of bounds for row group {row_group_index}: {index}"
        )));
    }

    let mut ranges = selected_pages
        .iter()
        .copied()
        .map(|index| {
            let start = usize::try_from(page_locations[index].first_row_index).map_err(|_| {
                FileError::new(
                    FileErrorKind::Corrupt,
                    "negative Parquet page first-row index",
                )
            })?;
            let end = page_locations
                .get(index + 1)
                .map(|page| usize::try_from(page.first_row_index))
                .transpose()
                .map_err(|_| {
                    FileError::new(
                        FileErrorKind::Corrupt,
                        "negative Parquet page first-row index",
                    )
                })?
                .unwrap_or(row_count);
            if start > end || end > row_count {
                return Err(FileError::new(
                    FileErrorKind::Corrupt,
                    "Parquet page row range exceeds its row group",
                ));
            }
            Ok(start..end)
        })
        .collect::<FileResult<Vec<_>>>()?;
    ranges.sort_unstable_by_key(|range| range.start);
    let mut merged = Vec::<std::ops::Range<usize>>::new();
    for range in ranges {
        if let Some(previous) = merged.last_mut()
            && previous.end == range.start
        {
            previous.end = range.end;
            continue;
        }
        merged.push(range);
    }
    Ok(merged)
}

fn row_group_may_match(row_group: &RowGroupMetaData, predicates: &[ScanPredicate]) -> bool {
    predicates.iter().all(|predicate| {
        let column = predicate_column_ordinal(row_group, predicate)
            .and_then(|ordinal| row_group.columns().get(ordinal));
        let Some(statistics) = column.and_then(|column| column.statistics()) else {
            return true;
        };
        predicate_may_match(statistics, predicate.domain())
    })
}

fn predicate_may_match(statistics: &Statistics, domain: &ScanPredicateDomain) -> bool {
    let Some((min, max)) = statistic_bounds(statistics) else {
        return true;
    };
    domain.may_match_bounds(&min, &max)
}

fn statistic_bounds(
    statistics: &Statistics,
) -> Option<(MinMaxPredicateValue, MinMaxPredicateValue)> {
    let extracted =
        extract_parquet_column_statistics(statistics, ParquetStatisticsSortOrder::Undefined)
            .ok()?;
    Some((
        predicate_value(extracted.min()?)?,
        predicate_value(extracted.max()?)?,
    ))
}

/// Decode the primitive values published in a Parquet statistics footer. The
/// same helper backs static physical pruning and metadata inspection, so their
/// byte decoding cannot drift. Logical interpretation is intentionally left to
/// connector owners.
fn extract_parquet_column_statistics(
    statistics: &Statistics,
    sort_order: ParquetStatisticsSortOrder,
) -> FileResult<ParquetColumnStatistics> {
    let (min, max) = parquet_statistics_values(statistics)?;
    Ok(ParquetColumnStatistics {
        null_count: statistics.null_count_opt(),
        min,
        max,
        min_is_exact: statistics.min_is_exact(),
        max_is_exact: statistics.max_is_exact(),
        min_max_deprecated: statistics.is_min_max_deprecated(),
        min_max_backwards_compatible: statistics.is_min_max_backwards_compatible(),
        sort_order,
    })
}

fn parquet_statistics_values(
    statistics: &Statistics,
) -> FileResult<(
    Option<ParquetStatisticsValue>,
    Option<ParquetStatisticsValue>,
)> {
    let values = match statistics {
        Statistics::Boolean(value) => (
            value
                .min_opt()
                .copied()
                .map(ParquetStatisticsValue::Boolean),
            value
                .max_opt()
                .copied()
                .map(ParquetStatisticsValue::Boolean),
        ),
        Statistics::Int32(value) => (
            value.min_opt().copied().map(ParquetStatisticsValue::Int32),
            value.max_opt().copied().map(ParquetStatisticsValue::Int32),
        ),
        Statistics::Int64(value) => (
            value.min_opt().copied().map(ParquetStatisticsValue::Int64),
            value.max_opt().copied().map(ParquetStatisticsValue::Int64),
        ),
        Statistics::Int96(value) => (
            value.min_opt().map(|value| {
                ParquetStatisticsValue::Int96(value.data().try_into().expect("INT96 has 3 words"))
            }),
            value.max_opt().map(|value| {
                ParquetStatisticsValue::Int96(value.data().try_into().expect("INT96 has 3 words"))
            }),
        ),
        Statistics::Float(value) => (
            value.min_opt().copied().map(ParquetStatisticsValue::Float),
            value.max_opt().copied().map(ParquetStatisticsValue::Float),
        ),
        Statistics::Double(value) => (
            value.min_opt().copied().map(ParquetStatisticsValue::Double),
            value.max_opt().copied().map(ParquetStatisticsValue::Double),
        ),
        Statistics::ByteArray(value) => (
            value
                .min_opt()
                .map(|value| ParquetStatisticsValue::ByteArray(value.data().to_vec())),
            value
                .max_opt()
                .map(|value| ParquetStatisticsValue::ByteArray(value.data().to_vec())),
        ),
        Statistics::FixedLenByteArray(value) => (
            value
                .min_opt()
                .map(|value| ParquetStatisticsValue::FixedLenByteArray(value.data().to_vec())),
            value
                .max_opt()
                .map(|value| ParquetStatisticsValue::FixedLenByteArray(value.data().to_vec())),
        ),
    };
    for value in [&values.0, &values.1].into_iter().flatten() {
        if let Some(length) = parquet_statistics_value_len(value)
            && length > MAX_PARQUET_INSPECTION_STATISTIC_VALUE_BYTES
        {
            return Err(FileError::new(
                FileErrorKind::ResourceExhausted,
                format!(
                    "Parquet statistic value length {length} exceeds inspection bound {MAX_PARQUET_INSPECTION_STATISTIC_VALUE_BYTES}",
                ),
            ));
        }
    }
    Ok(values)
}

fn parquet_statistics_value_len(value: &ParquetStatisticsValue) -> Option<usize> {
    match value {
        ParquetStatisticsValue::ByteArray(value)
        | ParquetStatisticsValue::FixedLenByteArray(value) => Some(value.len()),
        _ => None,
    }
}

fn predicate_value(value: &ParquetStatisticsValue) -> Option<MinMaxPredicateValue> {
    match value {
        ParquetStatisticsValue::Boolean(value) => Some(MinMaxPredicateValue::Boolean(*value)),
        ParquetStatisticsValue::Int32(value) => Some(MinMaxPredicateValue::Int32(*value)),
        ParquetStatisticsValue::Int64(value) => Some(MinMaxPredicateValue::Int64(*value)),
        ParquetStatisticsValue::Float(value) => Some(MinMaxPredicateValue::Float(*value)),
        ParquetStatisticsValue::Double(value) => Some(MinMaxPredicateValue::Double(*value)),
        ParquetStatisticsValue::ByteArray(value) => {
            Some(MinMaxPredicateValue::ByteArray(value.clone()))
        }
        ParquetStatisticsValue::FixedLenByteArray(value) => {
            Some(MinMaxPredicateValue::FixedLenByteArray(value.clone()))
        }
        ParquetStatisticsValue::Int96(_) => None,
    }
}

fn parquet_physical_type(value: ParquetType) -> ParquetPhysicalType {
    match value {
        ParquetType::BOOLEAN => ParquetPhysicalType::Boolean,
        ParquetType::INT32 => ParquetPhysicalType::Int32,
        ParquetType::INT64 => ParquetPhysicalType::Int64,
        ParquetType::INT96 => ParquetPhysicalType::Int96,
        ParquetType::FLOAT => ParquetPhysicalType::Float,
        ParquetType::DOUBLE => ParquetPhysicalType::Double,
        ParquetType::BYTE_ARRAY => ParquetPhysicalType::ByteArray,
        ParquetType::FIXED_LEN_BYTE_ARRAY => ParquetPhysicalType::FixedLenByteArray,
    }
}

fn parquet_sort_order(value: SortOrder) -> ParquetStatisticsSortOrder {
    match value {
        SortOrder::SIGNED => ParquetStatisticsSortOrder::Signed,
        SortOrder::UNSIGNED => ParquetStatisticsSortOrder::Unsigned,
        SortOrder::UNDEFINED => ParquetStatisticsSortOrder::Undefined,
    }
}

fn parquet_error(operation: &'static str, error: parquet::errors::ParquetError) -> FileError {
    let message = error.to_string();
    let kind = if message.contains("Cancelled:") {
        FileErrorKind::Cancelled
    } else if message.contains("DeadlineExceeded:") {
        FileErrorKind::DeadlineExceeded
    } else {
        FileErrorKind::Corrupt
    };
    FileError::with_source(kind, format!("{operation} failed"), error)
}

fn format_error(
    operation: &'static str,
    error: impl std::error::Error + Send + Sync + 'static,
) -> FileError {
    let message = error.to_string();
    let kind = if message.contains("Cancelled:") {
        FileErrorKind::Cancelled
    } else if message.contains("DeadlineExceeded:") {
        FileErrorKind::DeadlineExceeded
    } else {
        FileErrorKind::Corrupt
    };
    FileError::with_source(kind, format!("{operation} failed"), error)
}
