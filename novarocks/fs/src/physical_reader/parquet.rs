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

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::Instant;

use arrow::array::UInt64Array;
use arrow::datatypes::{Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::{
    ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReader,
    ParquetRecordBatchReaderBuilder, RowSelection,
};
use parquet::arrow::{PARQUET_FIELD_ID_META_KEY, ProjectionMask};
use parquet::file::metadata::{PageIndexPolicy, ParquetMetaData, RowGroupMetaData};
use parquet::file::statistics::Statistics;

use super::chunk_reader::{BoundChunkReader, ReaderMetrics};
use crate::{
    FileBatch, FileBatchReader, FileError, FileErrorKind, FileMetricsSnapshot, FileProjection,
    FileReadRange, FileReadRequest, FileResult, MinMaxPredicateOp, MinMaxPredicateValue,
    ScanPredicate, ScanPredicateDomain,
};

pub(crate) struct ParquetPhysicalReader {
    reader: Option<ParquetRangeReader>,
    positions: VecDeque<PositionSpan>,
    context: crate::FileReadContext,
    metrics: Arc<ReaderMetrics>,
    closed: bool,
}

enum ParquetRangeReader {
    Eager(ParquetRecordBatchReader),
    Delayed(DelayedMaterializeReader),
}

impl ParquetRangeReader {
    fn next_batch(&mut self) -> FileResult<Option<RecordBatch>> {
        match self {
            Self::Eager(reader) => reader
                .next()
                .transpose()
                .map_err(|error| format_error("decode Parquet batch", error)),
            Self::Delayed(reader) => reader.next_batch(),
        }
    }
}

struct DelayedMaterializeReader {
    active_reader: ParquetRecordBatchReader,
    lazy_reader: ParquetRecordBatchReader,
    output_sources: Vec<DelayedColumnSource>,
}

#[derive(Clone, Copy)]
enum DelayedColumnSource {
    Active(usize),
    Lazy(usize),
}

impl DelayedMaterializeReader {
    fn next_batch(&mut self) -> FileResult<Option<RecordBatch>> {
        let active = self
            .active_reader
            .next()
            .transpose()
            .map_err(|error| format_error("decode active Parquet columns", error))?;
        let lazy = self
            .lazy_reader
            .next()
            .transpose()
            .map_err(|error| format_error("decode lazy Parquet columns", error))?;
        match (active, lazy) {
            (None, None) => Ok(None),
            (Some(active), Some(lazy)) => {
                if active.num_rows() != lazy.num_rows() {
                    return Err(FileError::new(
                        FileErrorKind::Corrupt,
                        format!(
                            "delayed materialization batch row mismatch: active_rows={} lazy_rows={}",
                            active.num_rows(),
                            lazy.num_rows()
                        ),
                    ));
                }
                let active_schema = active.schema();
                let lazy_schema = lazy.schema();
                let mut fields = Vec::with_capacity(self.output_sources.len());
                let mut columns = Vec::with_capacity(self.output_sources.len());
                for source in &self.output_sources {
                    match source {
                        DelayedColumnSource::Active(index) => {
                            fields.push(active_schema.field(*index).as_ref().clone());
                            columns.push(active.column(*index).clone());
                        }
                        DelayedColumnSource::Lazy(index) => {
                            fields.push(lazy_schema.field(*index).as_ref().clone());
                            columns.push(lazy.column(*index).clone());
                        }
                    }
                }
                RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
                    .map(Some)
                    .map_err(|error| {
                        FileError::with_source(
                            FileErrorKind::Corrupt,
                            "assemble delayed Parquet batch failed",
                            error,
                        )
                    })
            }
            (Some(_), None) => Err(FileError::new(
                FileErrorKind::Corrupt,
                "delayed materialization stream mismatch: active has rows but lazy reached EOF",
            )),
            (None, Some(_)) => Err(FileError::new(
                FileErrorKind::Corrupt,
                "delayed materialization stream mismatch: lazy has rows but active reached EOF",
            )),
        }
    }
}

struct DelayedProjectionPlan {
    active_roots: Vec<usize>,
    lazy_roots: Vec<usize>,
    output_sources: Vec<DelayedColumnSource>,
}

#[derive(Clone, Copy, Debug)]
struct PositionSpan {
    next: u64,
    remaining: usize,
}

impl ParquetPhysicalReader {
    pub(crate) fn try_new(request: FileReadRequest) -> FileResult<Self> {
        request.context.check_active()?;
        let metrics = Arc::new(ReaderMetrics::default());
        let cache_enabled = request
            .cache
            .as_ref()
            .is_some_and(crate::DataCacheContext::datacache_requested);
        let identity = request.file.identity().clone();
        let chunk_reader = BoundChunkReader::new(
            request.file,
            request.context.clone(),
            request.cache,
            crate::cache::parquet_cache::page_cache_enabled(cache_enabled),
            Arc::clone(&metrics),
        );
        let page_index_policy = if request.pruning.pages.is_empty() {
            PageIndexPolicy::Skip
        } else {
            PageIndexPolicy::Optional
        };
        let options = ArrowReaderOptions::new().with_page_index_policy(page_index_policy);
        let arrow_metadata = if let Some(metadata) =
            crate::cache::parquet_cache::metadata_get(cache_enabled, &identity)
        {
            metadata
        } else {
            let metadata = ArrowReaderMetadata::load(&chunk_reader, options)
                .map_err(|error| parquet_error("open Parquet metadata", error))?;
            crate::cache::parquet_cache::metadata_put(cache_enabled, &identity, metadata.clone());
            metadata
        };
        let builder = ParquetRecordBatchReaderBuilder::new_with_metadata(
            chunk_reader.clone(),
            arrow_metadata.clone(),
        );
        request.context.check_active()?;

        let projected_roots = projection_roots(&builder, &request.projection)?;
        let metadata = builder.metadata().clone();
        let row_groups = select_row_groups(
            metadata.as_ref(),
            request.range,
            request.pruning.row_groups.as_deref(),
            &request.predicates,
        );
        metrics.record_row_group_selection(metadata.num_row_groups(), row_groups.len());
        let (selection, positions) =
            page_selection(metadata.as_ref(), &row_groups, &request.pruning.pages)?;
        let selected_rows = positions.iter().map(|span| span.remaining).sum::<usize>();
        let row_group_rows = selected_row_count(metadata.as_ref(), &row_groups)?;
        let delayed = selection.as_ref().and_then(|_| {
            (selected_rows > 0 && selected_rows < row_group_rows)
                .then(|| {
                    delayed_projection_plan(
                        builder.schema().clone(),
                        &projected_roots,
                        &request.predicates,
                    )
                })
                .flatten()
        });
        let reader = if let Some(plan) = delayed {
            let active_reader = build_projected_reader(
                chunk_reader.clone(),
                arrow_metadata.clone(),
                &plan.active_roots,
                request.budget.max_rows.get(),
                &row_groups,
                selection.clone(),
            )?;
            let lazy_reader = build_projected_reader(
                chunk_reader,
                arrow_metadata,
                &plan.lazy_roots,
                request.budget.max_rows.get(),
                &row_groups,
                selection,
            )?;
            metrics.record_delayed_materialization();
            ParquetRangeReader::Delayed(DelayedMaterializeReader {
                active_reader,
                lazy_reader,
                output_sources: plan.output_sources,
            })
        } else {
            ParquetRangeReader::Eager(build_projected_reader(
                chunk_reader,
                arrow_metadata,
                &projected_roots,
                request.budget.max_rows.get(),
                &row_groups,
                selection,
            )?)
        };

        Ok(Self {
            reader: Some(reader),
            positions,
            context: request.context,
            metrics,
            closed: false,
        })
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

impl FileBatchReader for ParquetPhysicalReader {
    fn next_batch(&mut self) -> FileResult<Option<FileBatch>> {
        if self.closed {
            return Ok(None);
        }
        self.context.check_active()?;
        let began = Instant::now();
        let next = self
            .reader
            .as_mut()
            .expect("Parquet reader must exist before close")
            .next_batch()?;
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

    fn close(&mut self) -> FileResult<()> {
        if self.closed {
            return Ok(());
        }
        self.closed = true;
        self.reader = None;
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
    builder: &ParquetRecordBatchReaderBuilder<BoundChunkReader>,
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
    chunk_reader: BoundChunkReader,
    metadata: ArrowReaderMetadata,
    projected_roots: &[usize],
    batch_size: usize,
    row_groups: &[usize],
    selection: Option<RowSelection>,
) -> FileResult<ParquetRecordBatchReader> {
    let mut builder = ParquetRecordBatchReaderBuilder::new_with_metadata(chunk_reader, metadata);
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

fn selected_row_count(metadata: &ParquetMetaData, selected: &[usize]) -> FileResult<usize> {
    selected.iter().try_fold(0usize, |total, index| {
        let row_group = metadata.row_groups().get(*index).ok_or_else(|| {
            FileError::invalid(format!(
                "Parquet row-group selection is out of bounds: {index}"
            ))
        })?;
        let rows = usize::try_from(row_group.num_rows()).map_err(|_| {
            FileError::new(
                FileErrorKind::Corrupt,
                "negative or overflowing Parquet row-group row count",
            )
        })?;
        total
            .checked_add(rows)
            .ok_or_else(|| FileError::new(FileErrorKind::Corrupt, "Parquet row count overflow"))
    })
}

fn delayed_projection_plan(
    schema: SchemaRef,
    projected_roots: &[usize],
    predicates: &[ScanPredicate],
) -> Option<DelayedProjectionPlan> {
    if projected_roots.len() < 2 || predicates.is_empty() {
        return None;
    }
    let predicate_roots = predicates
        .iter()
        .filter_map(|predicate| {
            if let Some(field_id) = predicate.physical_field_id() {
                return schema.fields().iter().position(|field| {
                    field
                        .metadata()
                        .get(PARQUET_FIELD_ID_META_KEY)
                        .and_then(|value| value.parse::<i32>().ok())
                        == Some(field_id)
                });
            }
            let root_name = predicate.column().split('.').next()?;
            schema.index_of(root_name).ok()
        })
        .collect::<HashSet<_>>();
    if predicate_roots.is_empty() {
        return None;
    }

    let active_roots = projected_roots
        .iter()
        .copied()
        .filter(|index| predicate_roots.contains(index))
        .collect::<Vec<_>>();
    let lazy_roots = projected_roots
        .iter()
        .copied()
        .filter(|index| !predicate_roots.contains(index))
        .collect::<Vec<_>>();
    if active_roots.is_empty() || lazy_roots.is_empty() {
        return None;
    }

    let active_indices = active_roots
        .iter()
        .enumerate()
        .map(|(output_index, root)| (*root, output_index))
        .collect::<HashMap<_, _>>();
    let lazy_indices = lazy_roots
        .iter()
        .enumerate()
        .map(|(output_index, root)| (*root, output_index))
        .collect::<HashMap<_, _>>();
    let output_sources = projected_roots
        .iter()
        .map(|root| {
            active_indices
                .get(root)
                .copied()
                .map(DelayedColumnSource::Active)
                .or_else(|| {
                    lazy_indices
                        .get(root)
                        .copied()
                        .map(DelayedColumnSource::Lazy)
                })
                .expect("projected root must belong to active or lazy projection")
        })
        .collect();
    Some(DelayedProjectionPlan {
        active_roots,
        lazy_roots,
        output_sources,
    })
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
) -> FileResult<(Option<RowSelection>, VecDeque<PositionSpan>)> {
    if pages.is_empty() {
        return Ok((None, row_position_spans(metadata, selected)?));
    }

    let mut pages_by_row_group = HashMap::<usize, HashSet<usize>>::new();
    for page in pages {
        let entry = pages_by_row_group.entry(page.row_group).or_default();
        entry.extend(page.page_indices.iter().copied());
    }
    let Some(offset_index) = metadata.offset_index() else {
        return Err(FileError::unsupported(
            "explicit Parquet page selection requires an offset index",
        ));
    };

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
        let ranges = if let Some(selected_pages) = pages_by_row_group.get(&row_group_index) {
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
        let column = row_group.columns().iter().find(|column| {
            if let Some(field_id) = predicate.physical_field_id() {
                let info = column.column_descr().self_type().get_basic_info();
                return info.has_id() && info.id() == field_id;
            }
            column
                .column_path()
                .parts()
                .first()
                .is_some_and(|name| name == predicate.column())
        });
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
    match domain {
        ScanPredicateDomain::Range { op, value } => match op {
            MinMaxPredicateOp::Le => {
                compare(&min, value).is_some_and(|order| order != Ordering::Greater)
            }
            MinMaxPredicateOp::Lt => {
                compare(&min, value).is_some_and(|order| order == Ordering::Less)
            }
            MinMaxPredicateOp::Ge => {
                compare(&max, value).is_some_and(|order| order != Ordering::Less)
            }
            MinMaxPredicateOp::Gt => {
                compare(&max, value).is_some_and(|order| order == Ordering::Greater)
            }
            MinMaxPredicateOp::Eq => {
                compare(&min, value).is_some_and(|order| order != Ordering::Greater)
                    && compare(&max, value).is_some_and(|order| order != Ordering::Less)
            }
        },
        ScanPredicateDomain::DiscreteSet { values, .. }
        | ScanPredicateDomain::Membership { values } => values.iter().any(|value| {
            compare(&min, value).is_some_and(|order| order != Ordering::Greater)
                && compare(&max, value).is_some_and(|order| order != Ordering::Less)
        }),
    }
}

fn statistic_bounds(
    statistics: &Statistics,
) -> Option<(MinMaxPredicateValue, MinMaxPredicateValue)> {
    match statistics {
        Statistics::Boolean(value) => Some((
            MinMaxPredicateValue::Boolean(*value.min_opt()?),
            MinMaxPredicateValue::Boolean(*value.max_opt()?),
        )),
        Statistics::Int32(value) => Some((
            MinMaxPredicateValue::Int32(*value.min_opt()?),
            MinMaxPredicateValue::Int32(*value.max_opt()?),
        )),
        Statistics::Int64(value) => Some((
            MinMaxPredicateValue::Int64(*value.min_opt()?),
            MinMaxPredicateValue::Int64(*value.max_opt()?),
        )),
        Statistics::Float(value) => Some((
            MinMaxPredicateValue::Float(*value.min_opt()?),
            MinMaxPredicateValue::Float(*value.max_opt()?),
        )),
        Statistics::Double(value) => Some((
            MinMaxPredicateValue::Double(*value.min_opt()?),
            MinMaxPredicateValue::Double(*value.max_opt()?),
        )),
        Statistics::ByteArray(value) => Some((
            MinMaxPredicateValue::ByteArray(value.min_opt()?.data().to_vec()),
            MinMaxPredicateValue::ByteArray(value.max_opt()?.data().to_vec()),
        )),
        Statistics::FixedLenByteArray(value) => Some((
            MinMaxPredicateValue::FixedLenByteArray(value.min_opt()?.data().to_vec()),
            MinMaxPredicateValue::FixedLenByteArray(value.max_opt()?.data().to_vec()),
        )),
        _ => None,
    }
}

fn compare(left: &MinMaxPredicateValue, right: &MinMaxPredicateValue) -> Option<Ordering> {
    match (left, right) {
        (MinMaxPredicateValue::Boolean(a), MinMaxPredicateValue::Boolean(b)) => a.partial_cmp(b),
        (MinMaxPredicateValue::Int32(a), MinMaxPredicateValue::Int32(b)) => a.partial_cmp(b),
        (MinMaxPredicateValue::Int64(a), MinMaxPredicateValue::Int64(b)) => a.partial_cmp(b),
        (MinMaxPredicateValue::Float(a), MinMaxPredicateValue::Float(b)) => a.partial_cmp(b),
        (MinMaxPredicateValue::Double(a), MinMaxPredicateValue::Double(b)) => a.partial_cmp(b),
        (MinMaxPredicateValue::ByteArray(a), MinMaxPredicateValue::ByteArray(b))
        | (
            MinMaxPredicateValue::FixedLenByteArray(a),
            MinMaxPredicateValue::FixedLenByteArray(b),
        ) => a.partial_cmp(b),
        _ => None,
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
