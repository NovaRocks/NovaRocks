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
mod cache;

pub use cache::{
    ParquetCacheOptions, init_datacache_parquet_cache, parquet_meta_cache_get,
    parquet_meta_cache_put, parquet_page_cache_get, parquet_page_cache_put,
};

use anyhow::{Context, Result};
use arrow::array::{
    Array, ArrayRef, BinaryArray, LargeBinaryArray, LargeBinaryBuilder, RecordBatch, StructArray,
};
use arrow::datatypes::DataType;
use bytes::Bytes;
use parquet::arrow::arrow_reader::{
    ArrowReaderOptions, ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder, RowSelection,
};
use parquet::file::metadata::{ParquetMetaData, RowGroupMetaData};
use parquet::file::page_index::column_index::ColumnIndexMetaData;
use parquet::file::page_index::offset_index::OffsetIndexMetaData;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::file::statistics::Statistics;
use std::collections::{HashMap, HashSet};
use std::ops::Range;
use std::sync::Arc;

use crate::cache::DataCacheContext;
use crate::common::ids::SlotId;
use crate::exec::chunk::{Chunk, field_slot_id, field_with_slot_id};
use crate::exec::expr::LiteralValue;
use crate::exec::node::BoxedExecIter;
use crate::exec::node::scan::RuntimeFilterContext;
use crate::exec::variant::VariantValue;
use crate::fs::opendal::OpendalRangeReaderFactory;
use crate::fs::scan_context::FileScanRange;
use crate::metrics;
use crate::novarocks_logging::debug;
use crate::runtime::profile::{RuntimeProfile, clamp_u128_to_i64};
use crate::types;

#[derive(Clone, Debug)]
pub struct ParquetProbe {
    pub path: String,
    pub num_rows: i64,
    pub num_row_groups: usize,
    pub created_by: Option<String>,
    pub schema: String,
    pub arrow_schema: Option<String>,
    pub arrow_schema_skip_meta: Option<String>,
    pub arrow_schema_error: Option<String>,
    pub arrow_schema_skip_meta_error: Option<String>,
}

pub fn probe_parquet_bytes(path: &str, bytes: Bytes) -> Result<ParquetProbe> {
    let reader = SerializedFileReader::new(bytes.clone())
        .with_context(|| format!("parquet open: {path}"))?;
    let metadata = reader.metadata();
    let file_meta = metadata.file_metadata();
    let arrow_schema = ParquetRecordBatchReaderBuilder::try_new(bytes.clone())
        .ok()
        .map(|builder| format!("{:?}", builder.schema()));
    let arrow_schema_error = ParquetRecordBatchReaderBuilder::try_new(bytes.clone())
        .err()
        .map(|e| e.to_string());
    let arrow_schema_skip_meta = ParquetRecordBatchReaderBuilder::try_new_with_options(
        bytes.clone(),
        ArrowReaderOptions::new().with_skip_arrow_metadata(true),
    )
    .ok()
    .map(|builder| format!("{:?}", builder.schema()));
    let arrow_schema_skip_meta_error = ParquetRecordBatchReaderBuilder::try_new_with_options(
        bytes,
        ArrowReaderOptions::new().with_skip_arrow_metadata(true),
    )
    .err()
    .map(|e| e.to_string());

    Ok(ParquetProbe {
        path: path.to_string(),
        num_rows: file_meta.num_rows(),
        num_row_groups: metadata.num_row_groups(),
        created_by: file_meta.created_by().map(|s: &str| s.to_string()),
        schema: format!("{:?}", file_meta.schema()),
        arrow_schema,
        arrow_schema_skip_meta,
        arrow_schema_error,
        arrow_schema_skip_meta_error,
    })
}

#[derive(Clone, Debug)]
pub enum MinMaxPredicate {
    Le { column: String, value: LiteralValue }, // <=
    Ge { column: String, value: LiteralValue }, // >=
    Lt { column: String, value: LiteralValue }, // <
    Gt { column: String, value: LiteralValue }, // >
    Eq { column: String, value: LiteralValue }, // =
}

fn runtime_filters_to_min_max_predicates(
    cfg: &ParquetScanConfig,
    runtime_filters: &RuntimeFilterContext,
) -> Vec<MinMaxPredicate> {
    let snapshot = runtime_filters.snapshot();
    if snapshot.is_empty() {
        return Vec::new();
    }
    if cfg.slot_ids.is_empty() || cfg.columns.is_empty() || cfg.slot_ids.len() != cfg.columns.len()
    {
        return Vec::new();
    }

    let mut slot_to_index = HashMap::new();
    for (idx, slot_id) in cfg.slot_ids.iter().enumerate() {
        slot_to_index.insert(*slot_id, idx.to_string());
    }

    let mut preds = Vec::new();
    for rf in snapshot.in_filters() {
        let Some(column) = slot_to_index.get(&rf.slot_id()) else {
            continue;
        };
        let Some((min_value, max_value)) = rf.min_max_literal() else {
            continue;
        };
        preds.push(MinMaxPredicate::Ge {
            column: column.clone(),
            value: min_value,
        });
        preds.push(MinMaxPredicate::Le {
            column: column.clone(),
            value: max_value,
        });
    }
    for rf in snapshot.membership_filters() {
        let Some(column) = slot_to_index.get(&rf.slot_id()) else {
            continue;
        };
        let Some((min_value, max_value)) = rf.min_max().min_max_literal() else {
            continue;
        };
        preds.push(MinMaxPredicate::Ge {
            column: column.clone(),
            value: min_value,
        });
        preds.push(MinMaxPredicate::Le {
            column: column.clone(),
            value: max_value,
        });
    }
    preds
}

fn literal_int64(value: &LiteralValue) -> Option<i64> {
    match value {
        LiteralValue::Int8(v) => Some(*v as i64),
        LiteralValue::Int16(v) => Some(*v as i64),
        LiteralValue::Int32(v) => Some(*v as i64),
        LiteralValue::Int64(v) => Some(*v),
        _ => None,
    }
}

fn literal_int32(value: &LiteralValue) -> Option<i32> {
    match value {
        LiteralValue::Int8(v) => Some(*v as i32),
        LiteralValue::Int16(v) => Some(*v as i32),
        LiteralValue::Int32(v) => Some(*v),
        LiteralValue::Int64(v) => i32::try_from(*v).ok(),
        LiteralValue::Date32(v) => Some(*v),
        _ => None,
    }
}

fn literal_float64(value: &LiteralValue) -> Option<f64> {
    match value {
        LiteralValue::Float32(v) => Some(*v as f64),
        LiteralValue::Float64(v) => Some(*v),
        _ => None,
    }
}

#[derive(Clone, Debug)]
pub struct ParquetScanConfig {
    pub columns: Vec<String>,
    pub slot_ids: Vec<SlotId>,
    pub slot_types: Vec<types::TPrimitiveType>,
    pub case_sensitive: bool,
    pub enable_page_index: bool,
    pub min_max_predicates: Vec<MinMaxPredicate>,
    pub batch_size: Option<usize>,
    pub datacache: DataCacheContext,
    pub cache_policy: ParquetReadCachePolicy,
    pub profile_label: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ParquetReadCachePolicy {
    pub enable_metacache: bool,
    pub enable_pagecache: bool,
    pub page_cache_min_read_bytes: usize,
    pub page_cache_max_read_bytes: usize,
    pub page_cache_evict_probability: Option<u32>,
}

impl ParquetReadCachePolicy {
    pub const DEFAULT_PAGE_CACHE_MIN_READ_BYTES: usize = 1024;
    pub const DEFAULT_PAGE_CACHE_MAX_READ_BYTES: usize = 2 * 1024 * 1024;

    pub fn with_flags(
        enable_metacache: bool,
        enable_pagecache: bool,
        page_cache_evict_probability: Option<u32>,
    ) -> Self {
        Self {
            enable_metacache,
            enable_pagecache,
            page_cache_min_read_bytes: Self::DEFAULT_PAGE_CACHE_MIN_READ_BYTES,
            page_cache_max_read_bytes: Self::DEFAULT_PAGE_CACHE_MAX_READ_BYTES,
            page_cache_evict_probability,
        }
    }

    pub fn should_cache_page_read(&self, length: usize) -> bool {
        self.enable_pagecache
            && (self.page_cache_min_read_bytes..=self.page_cache_max_read_bytes).contains(&length)
    }
}

pub fn build_parquet_iter(
    scan: crate::fs::scan_context::FileScanContext,
    cfg: ParquetScanConfig,
    limit: Option<usize>,
    profile: Option<RuntimeProfile>,
    runtime_filters: Option<&RuntimeFilterContext>,
) -> Result<BoxedExecIter, String> {
    let runtime_filters = runtime_filters.cloned();
    if scan.ranges.is_empty() {
        return Ok(Box::new(std::iter::empty()));
    }
    let factory = scan
        .factory
        .with_datacache_context(cfg.datacache.clone())
        .with_parquet_cache_policy(Some(cfg.cache_policy.clone()));
    let iter = ParquetScanIter::new(cfg, scan.ranges, factory, limit, profile, runtime_filters);
    Ok(Box::new(iter))
}

struct RangeState {
    range: FileScanRange,
    metadata: Arc<ParquetMetaData>,
    row_groups: Vec<usize>,
    cursor: usize,
    read: HashSet<usize>,
    last_filter_version: u64,
}

struct ParquetScanIter {
    cfg: ParquetScanConfig,
    ranges: Vec<FileScanRange>,
    factory: OpendalRangeReaderFactory,
    range_idx: usize,
    reader: Option<ParquetRecordBatchReader>,
    remaining: usize,
    limit: Option<usize>,
    profile: Option<RuntimeProfile>,
    runtime_filters: Option<RuntimeFilterContext>,
    current_range: Option<RangeState>,
}

impl ParquetScanIter {
    fn current_predicates(&self) -> Vec<MinMaxPredicate> {
        let mut predicates = self.cfg.min_max_predicates.clone();
        if let Some(filters) = self.runtime_filters.as_ref() {
            let mut runtime_preds = runtime_filters_to_min_max_predicates(&self.cfg, filters);
            if !runtime_preds.is_empty() {
                predicates.append(&mut runtime_preds);
            }
        }
        predicates
    }

    fn current_filter_version(&self) -> u64 {
        self.runtime_filters
            .as_ref()
            .map(|f| f.version())
            .unwrap_or(0)
    }

    fn new(
        cfg: ParquetScanConfig,
        ranges: Vec<FileScanRange>,
        factory: OpendalRangeReaderFactory,
        limit: Option<usize>,
        profile: Option<RuntimeProfile>,
        runtime_filters: Option<RuntimeFilterContext>,
    ) -> Self {
        let remaining = limit.unwrap_or(usize::MAX);
        Self {
            cfg,
            ranges,
            factory,
            range_idx: 0,
            reader: None,
            remaining,
            limit,
            profile,
            runtime_filters,
            current_range: None,
        }
    }

    fn open_next_reader(&mut self) -> Result<bool, String> {
        loop {
            if self.current_range.is_none() {
                if self.range_idx >= self.ranges.len() {
                    return Ok(false);
                }
                let prep_start = std::time::Instant::now();
                let idx = self.range_idx;
                let range = self.ranges[idx].clone();
                self.range_idx += 1;

                let path = range.path.clone();
                let file_len = range.file_len;
                let len = (file_len > 0).then_some(file_len);
                let range_modification_time = range
                    .external_datacache
                    .as_ref()
                    .and_then(|opts| opts.modification_time);

                if let Some(profile) = self.profile.as_ref() {
                    profile.counter_add("ParquetRanges", metrics::TUnit::UNIT, 1);
                }

                // Get file metadata for cache key
                let temp_reader = self
                    .factory
                    .open_with_len(&path, len)
                    .map(|r| r.with_modification_time_override(range_modification_time))
                    .map_err(|e| e.to_string())?;
                let (actual_file_len, mtime) = temp_reader
                    .get_file_meta()
                    .map_err(|e| format!("failed to get file metadata for cache key: {}", e))?;
                let meta_cache_evict_probability = u32::try_from(
                    self.cfg
                        .datacache
                        .cache_options()
                        .datacache_evict_probability,
                )
                .ok();

                // Try to get metadata from cache
                let cached_metadata = cache::parquet_meta_cache_get(
                    self.cfg.cache_policy.enable_metacache,
                    &path,
                    mtime,
                    actual_file_len,
                );

                let reader = self
                    .factory
                    .open_with_len(&path, len)
                    .map(|r| r.with_modification_time_override(range_modification_time))
                    .map_err(|e| e.to_string())?;

                // Build reader - parquet crate will still read footer, but we can use cached metadata
                // for row group filtering to avoid re-parsing
                let mut opts = ArrowReaderOptions::new().with_skip_arrow_metadata(true);
                if self.cfg.enable_page_index {
                    opts = opts.with_page_index(true);
                }
                let builder = ParquetRecordBatchReaderBuilder::try_new_with_options(reader, opts)
                    .map_err(|e| e.to_string())?;

                // If we have cached metadata, verify it matches and use it for optimization
                if let Some(cached_meta) = cached_metadata {
                    let current_meta = builder.metadata();
                    // Verify cached metadata matches (same num_row_groups and file_size)
                    if cached_meta.num_row_groups() == current_meta.num_row_groups()
                        && cached_meta.file_metadata().num_rows()
                            == current_meta.file_metadata().num_rows()
                    {
                        debug!("parquet metadata cache HIT for file: {} (verified)", path);
                        // Metadata matches, we can use cached one for row group selection
                        // Note: builder still uses its own metadata, but we've verified cache is valid
                    } else {
                        debug!(
                            "parquet metadata cache STALE for file: {} (re-caching)",
                            path
                        );
                        // Cache is stale, update it
                        // builder.metadata() returns Arc<ParquetMetaData>, clone it
                        let metadata = current_meta.clone();
                        cache::parquet_meta_cache_put(
                            self.cfg.cache_policy.enable_metacache,
                            &path,
                            mtime,
                            actual_file_len,
                            metadata,
                            meta_cache_evict_probability,
                        );
                    }
                } else if self.cfg.cache_policy.enable_metacache {
                    debug!("parquet metadata cache MISS for file: {}", path);
                    // Cache the metadata for future use
                    // builder.metadata() returns Arc<ParquetMetaData>, clone it
                    let metadata = builder.metadata().clone();
                    cache::parquet_meta_cache_put(
                        self.cfg.cache_policy.enable_metacache,
                        &path,
                        mtime,
                        actual_file_len,
                        metadata,
                        meta_cache_evict_probability,
                    );
                    debug!("parquet metadata cached for file: {}", path);
                }

                let metadata = builder.metadata().clone();
                let predicates = self.current_predicates();
                let limit_rows = self.limit.map(|_| self.remaining);
                let selected_row_groups = select_row_groups_for_range(
                    &metadata,
                    &range,
                    limit_rows,
                    &predicates,
                    &self.cfg.columns,
                    self.cfg.case_sensitive,
                );

                let row_groups = if let Some(row_groups) = selected_row_groups {
                    let rg_total = metadata.num_row_groups() as u128;
                    let mut bytes_total: u128 = 0;
                    for rg in metadata.row_groups() {
                        bytes_total += rg.total_byte_size().max(0) as u128;
                    }

                    let mut bytes_selected: u128 = 0;
                    for &rg_idx in &row_groups {
                        if let Some(rg) = metadata.row_groups().get(rg_idx) {
                            bytes_selected += rg.total_byte_size().max(0) as u128;
                        }
                    }
                    let rg_selected = row_groups.len() as u128;
                    let rg_pruned = rg_total.saturating_sub(rg_selected);
                    let bytes_pruned = bytes_total.saturating_sub(bytes_selected);

                    if let Some(profile) = self.profile.as_ref() {
                        profile.counter_add(
                            "ParquetRowGroupsTotal",
                            metrics::TUnit::UNIT,
                            clamp_u128_to_i64(rg_total),
                        );
                        profile.counter_add(
                            "ParquetRowGroupsSelected",
                            metrics::TUnit::UNIT,
                            clamp_u128_to_i64(rg_selected),
                        );
                        profile.counter_add(
                            "ParquetRowGroupsPruned",
                            metrics::TUnit::UNIT,
                            clamp_u128_to_i64(rg_pruned),
                        );
                        profile.counter_add(
                            "ParquetRowGroupBytesTotal",
                            metrics::TUnit::BYTES,
                            clamp_u128_to_i64(bytes_total),
                        );
                        profile.counter_add(
                            "ParquetRowGroupBytesSelected",
                            metrics::TUnit::BYTES,
                            clamp_u128_to_i64(bytes_selected),
                        );
                        profile.counter_add(
                            "ParquetRowGroupBytesPruned",
                            metrics::TUnit::BYTES,
                            clamp_u128_to_i64(bytes_pruned),
                        );
                    }

                    if row_groups.is_empty() {
                        debug!("all row groups filtered out for file: {}", path);
                        continue;
                    }
                    debug!(
                        "selected {}/{} row groups for file: {}",
                        row_groups.len(),
                        metadata.num_row_groups(),
                        path
                    );
                    row_groups
                } else {
                    (0..metadata.num_row_groups()).collect()
                };

                let prep_ns = prep_start.elapsed().as_nanos() as u128;
                if let Some(profile) = self.profile.as_ref() {
                    profile.counter_add(
                        "PrepareChunkSourceTime",
                        metrics::TUnit::TIME_NS,
                        clamp_u128_to_i64(prep_ns),
                    );
                }

                self.current_range = Some(RangeState {
                    range,
                    metadata,
                    row_groups,
                    cursor: 0,
                    read: HashSet::new(),
                    last_filter_version: self.current_filter_version(),
                });
                continue;
            }

            let version = self.current_filter_version();
            let predicates = self.current_predicates();
            let limit_rows = self.limit.map(|_| self.remaining);
            let state = self.current_range.as_mut().expect("range state is missing");
            if version != state.last_filter_version {
                let selected_row_groups = select_row_groups_for_range(
                    &state.metadata,
                    &state.range,
                    limit_rows,
                    &predicates,
                    &self.cfg.columns,
                    self.cfg.case_sensitive,
                );
                let mut row_groups = if let Some(row_groups) = selected_row_groups {
                    row_groups
                } else {
                    (0..state.metadata.num_row_groups()).collect()
                };
                row_groups.retain(|rg| !state.read.contains(rg));
                state.row_groups = row_groups;
                state.cursor = 0;
                state.last_filter_version = version;
            }

            if state.cursor >= state.row_groups.len() {
                self.current_range = None;
                continue;
            }

            let rg_idx = state.row_groups[state.cursor];
            state.cursor += 1;
            state.read.insert(rg_idx);

            let path = state.range.path.clone();
            let file_len = state.range.file_len;
            let len = (file_len > 0).then_some(file_len);
            let range_modification_time = state
                .range
                .external_datacache
                .as_ref()
                .and_then(|opts| opts.modification_time);

            let reader = self
                .factory
                .open_with_len(&path, len)
                .map(|r| r.with_modification_time_override(range_modification_time))
                .map_err(|e| e.to_string())?;

            let mut opts = ArrowReaderOptions::new().with_skip_arrow_metadata(true);
            if self.cfg.enable_page_index {
                opts = opts.with_page_index(true);
            }
            let mut builder = ParquetRecordBatchReaderBuilder::try_new_with_options(reader, opts)
                .map_err(|e| e.to_string())?;

            // Set batch size if configured
            if let Some(batch_size) = self.cfg.batch_size {
                builder = builder.with_batch_size(batch_size);
                debug!("parquet reader: batch_size={}", batch_size);
            }

            if !self.cfg.columns.is_empty() {
                let mask = {
                    let arrow_schema = builder.schema();
                    let parquet_schema = builder.parquet_schema();
                    let mut indices = Vec::new();
                    for col_name in &self.cfg.columns {
                        // Skip ___count___ column - it's a special optimization column for count(*)
                        // that doesn't exist in the parquet file
                        if col_name == "___count___" {
                            continue;
                        }

                        let idx = if self.cfg.case_sensitive {
                            arrow_schema.index_of(col_name).ok()
                        } else {
                            arrow_schema
                                .fields()
                                .iter()
                                .position(|f| f.name().eq_ignore_ascii_case(col_name))
                        };

                        if let Some(i) = idx {
                            indices.push(i);
                        } else {
                            return Err(format!("Column {} not found in parquet file", col_name));
                        }
                    }
                    parquet::arrow::ProjectionMask::roots(parquet_schema, indices)
                };
                builder = builder.with_projection(mask);
            }

            if self.limit.is_some() {
                builder = builder.with_limit(self.remaining);
            }

            if self.cfg.enable_page_index && !predicates.is_empty() {
                let selection = build_row_selection_for_row_groups(
                    &state.metadata,
                    &[rg_idx],
                    &predicates,
                    &self.cfg.columns,
                    self.cfg.case_sensitive,
                );

                if selection.rows_selected == 0 {
                    continue;
                }

                if let Some(sel) = selection.selection {
                    builder = builder.with_row_selection(sel);
                }
            }

            builder = builder.with_row_groups(vec![rg_idx]);
            let reader = builder.build().map_err(|e| e.to_string())?;
            self.reader = Some(reader);
            return Ok(true);
        }
    }
}

impl Iterator for ParquetScanIter {
    type Item = Result<Chunk, String>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.remaining == 0 {
                return None;
            }
            if self.reader.is_none() {
                match self.open_next_reader() {
                    Ok(true) => {}
                    Ok(false) => return None,
                    Err(e) => return Some(Err(e)),
                }
            }

            let reader = self.reader.as_mut().expect("reader");
            match reader.next() {
                Some(Ok(batch)) => {
                    if batch.num_rows() == 0 {
                        continue;
                    }
                    let batch = match reorder_batch(&self.cfg, batch)
                        .and_then(|b| convert_variant_columns(&self.cfg, b))
                    {
                        Ok(batch) => batch,
                        Err(e) => return Some(Err(e)),
                    };

                    let to_take = std::cmp::min(batch.num_rows(), self.remaining);
                    let batch = if to_take < batch.num_rows() {
                        batch.slice(0, to_take)
                    } else {
                        batch
                    };
                    self.remaining -= to_take;
                    if let Some(profile) = self.profile.as_ref() {
                        profile.counter_add("ParquetBatchesOut", metrics::TUnit::UNIT, 1);
                        profile.counter_add(
                            "ParquetRowsOut",
                            metrics::TUnit::UNIT,
                            clamp_u128_to_i64(to_take as u128),
                        );
                        profile.counter_add(
                            "RowsRead",
                            metrics::TUnit::UNIT,
                            clamp_u128_to_i64(to_take as u128),
                        );
                        profile.counter_add(
                            "RawRowsRead",
                            metrics::TUnit::UNIT,
                            clamp_u128_to_i64(to_take as u128),
                        );
                    }
                    return Some(Ok(Chunk::new(batch)));
                }
                Some(Err(e)) => {
                    self.reader = None;
                    return Some(Err(e.to_string()));
                }
                None => {
                    self.reader = None;
                }
            }
        }
    }
}

fn reorder_batch(cfg: &ParquetScanConfig, batch: RecordBatch) -> Result<RecordBatch, String> {
    let batch_schema = batch.schema();

    if !cfg.columns.is_empty() {
        let has_virtual_count = cfg.columns.iter().any(|c| c == "___count___");

        // Fast path: when we already project columns, ParquetRecordBatchReader returns the
        // projected columns in the requested order. Avoid rebuilding RecordBatches per chunk.
        if !has_virtual_count && cfg.columns.len() == batch_schema.fields().len() {
            let matches = batch_schema
                .fields()
                .iter()
                .zip(cfg.columns.iter())
                .all(|(f, n)| {
                    if cfg.case_sensitive {
                        f.name() == n
                    } else {
                        f.name().eq_ignore_ascii_case(n)
                    }
                });
            if matches {
                return attach_slot_ids_to_batch(cfg, batch);
            }
        }

        // Slow path: handle virtual columns (e.g. ___count___) or mismatch cases.
        let mut new_columns = Vec::with_capacity(cfg.columns.len());
        let mut new_fields = Vec::with_capacity(cfg.columns.len());

        for col_name in &cfg.columns {
            if col_name == "___count___" {
                let row_count = batch.num_rows();
                let count_array: ArrayRef =
                    Arc::new(arrow::array::BooleanArray::from(vec![true; row_count]));
                let count_field = Arc::new(arrow::datatypes::Field::new(
                    "___count___",
                    arrow::datatypes::DataType::Boolean,
                    false,
                ));
                new_columns.push(count_array);
                new_fields.push(count_field);
                continue;
            }

            let idx = if cfg.case_sensitive {
                batch_schema.index_of(col_name).ok()
            } else {
                batch_schema
                    .fields()
                    .iter()
                    .position(|f| f.name().eq_ignore_ascii_case(col_name))
            };

            let Some(i) = idx else {
                return Err(format!("Column {} missing in read batch", col_name));
            };

            new_columns.push(batch.column(i).clone());
            new_fields.push(batch_schema.field(i).clone().into());
        }

        let new_schema = Arc::new(arrow::datatypes::Schema::new(new_fields));
        let batch = RecordBatch::try_new(new_schema, new_columns)
            .map_err(|e: arrow::error::ArrowError| e.to_string())?;
        return attach_slot_ids_to_batch(cfg, batch);
    }

    attach_slot_ids_to_batch(cfg, batch)
}

fn attach_slot_ids_to_batch(
    cfg: &ParquetScanConfig,
    batch: RecordBatch,
) -> Result<RecordBatch, String> {
    if batch.num_columns() == 0 {
        return Ok(batch);
    }

    if cfg.slot_ids.is_empty() {
        return Err(format!(
            "parquet scan missing slot_ids for non-empty batch: num_columns={}",
            batch.num_columns()
        ));
    }

    if batch.num_columns() != cfg.slot_ids.len() {
        return Err(format!(
            "parquet scan output columns/slot_ids mismatch: num_columns={}, slot_ids={:?}",
            batch.num_columns(),
            cfg.slot_ids
        ));
    }

    let schema = batch.schema();
    let mut already_aligned = true;
    for (f, slot_id) in schema.fields().iter().zip(cfg.slot_ids.iter()) {
        let input_slot_id = field_slot_id(f.as_ref())?;
        if input_slot_id != Some(*slot_id) {
            already_aligned = false;
            break;
        }
    }
    if already_aligned {
        return Ok(batch);
    }

    let mut new_fields = Vec::with_capacity(schema.fields().len());
    for (f, slot_id) in schema.fields().iter().zip(cfg.slot_ids.iter()) {
        new_fields.push(Arc::new(field_with_slot_id((**f).clone(), *slot_id)));
    }
    let new_schema = Arc::new(arrow::datatypes::Schema::new_with_metadata(
        new_fields,
        schema.metadata().clone(),
    ));
    RecordBatch::try_new(new_schema, batch.columns().to_vec())
        .map_err(|e: arrow::error::ArrowError| e.to_string())
}

fn convert_variant_columns(
    cfg: &ParquetScanConfig,
    batch: RecordBatch,
) -> Result<RecordBatch, String> {
    if cfg.slot_types.is_empty() {
        return Ok(batch);
    }
    let mut has_variant = false;
    for t in &cfg.slot_types {
        if *t == types::TPrimitiveType::VARIANT {
            has_variant = true;
            break;
        }
    }
    if !has_variant {
        return Ok(batch);
    }

    if batch.num_columns() != cfg.slot_types.len() {
        return Err(format!(
            "parquet scan slot_types mismatch: columns={} slot_types={}",
            batch.num_columns(),
            cfg.slot_types.len()
        ));
    }

    let schema = batch.schema();
    let mut new_fields = Vec::with_capacity(schema.fields().len());
    let mut new_columns = Vec::with_capacity(batch.num_columns());

    for (idx, field) in schema.fields().iter().enumerate() {
        let col = batch.column(idx);
        if cfg.slot_types[idx] != types::TPrimitiveType::VARIANT {
            new_fields.push(field.clone());
            new_columns.push(col.clone());
            continue;
        }

        match col.data_type() {
            DataType::LargeBinary => {
                new_fields.push(field.clone());
                new_columns.push(col.clone());
            }
            DataType::Struct(_) => {
                let struct_arr = col
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .ok_or_else(|| "failed to downcast to StructArray".to_string())?;
                let mut metadata_idx = None;
                let mut value_idx = None;
                for (i, f) in struct_arr.fields().iter().enumerate() {
                    if f.name() == "metadata" {
                        metadata_idx = Some(i);
                    } else if f.name() == "value" {
                        value_idx = Some(i);
                    }
                }
                let metadata_idx = metadata_idx
                    .ok_or_else(|| "VARIANT struct missing metadata field".to_string())?;
                let value_idx =
                    value_idx.ok_or_else(|| "VARIANT struct missing value field".to_string())?;

                let metadata_col = struct_arr.column(metadata_idx).clone();
                let value_col = struct_arr.column(value_idx).clone();

                let mut builder = LargeBinaryBuilder::new();
                for row in 0..batch.num_rows() {
                    if struct_arr.is_null(row) {
                        builder.append_null();
                        continue;
                    }
                    let metadata = binary_value_at(&metadata_col, row);
                    let value = binary_value_at(&value_col, row);
                    let serialized = match (metadata, value) {
                        (Ok(Some(m)), Ok(Some(v))) => VariantValue::create(m, v)
                            .unwrap_or_else(|_| VariantValue::null_value())
                            .serialize(),
                        _ => VariantValue::null_value().serialize(),
                    };
                    builder.append_value(serialized.as_slice());
                }

                let mut meta = field.metadata().clone();
                let new_field = Arc::new(
                    arrow::datatypes::Field::new(
                        field.name(),
                        DataType::LargeBinary,
                        field.is_nullable(),
                    )
                    .with_metadata(meta.drain().collect()),
                );
                new_fields.push(new_field);
                new_columns.push(Arc::new(builder.finish()) as ArrayRef);
            }
            other => {
                return Err(format!("VARIANT column has unsupported type: {:?}", other));
            }
        }
    }

    let new_schema = Arc::new(arrow::datatypes::Schema::new_with_metadata(
        new_fields,
        schema.metadata().clone(),
    ));
    RecordBatch::try_new(new_schema, new_columns)
        .map_err(|e: arrow::error::ArrowError| e.to_string())
}

fn binary_value_at(array: &ArrayRef, row: usize) -> Result<Option<&[u8]>, String> {
    if array.is_null(row) {
        return Ok(None);
    }
    if let Some(arr) = array.as_any().downcast_ref::<BinaryArray>() {
        return Ok(Some(arr.value(row)));
    }
    if let Some(arr) = array.as_any().downcast_ref::<LargeBinaryArray>() {
        return Ok(Some(arr.value(row)));
    }
    Err("expected binary array".to_string())
}

fn select_row_groups_for_range(
    metadata: &ParquetMetaData,
    range: &FileScanRange,
    mut remaining_rows: Option<usize>,
    min_max_predicates: &[MinMaxPredicate],
    columns: &[String],
    case_sensitive: bool,
) -> Option<Vec<usize>> {
    if range.length == 0 && remaining_rows.is_none() && min_max_predicates.is_empty() {
        return None;
    }
    let split_start = range.offset;
    let mut split_end = split_start.saturating_add(range.length);
    if range.file_len > 0 && split_end > range.file_len {
        split_end = range.file_len;
    }
    if range.length == 0 && range.file_len == 0 {
        split_end = u64::MAX;
    }

    let mut row_groups = Vec::new();
    let mut filtered_count = 0;

    for (idx, row_group) in metadata.row_groups().iter().enumerate() {
        let rg_start = row_group_start_offset(row_group)?;
        if rg_start >= split_start && rg_start < split_end {
            // Check min-max predicates.
            if !min_max_predicates.is_empty() {
                match should_read_row_group(
                    row_group,
                    metadata,
                    min_max_predicates,
                    columns,
                    case_sensitive,
                ) {
                    Ok(true) => {
                        // Passed pruning.
                    }
                    Ok(false) => {
                        // Pruned out.
                        filtered_count += 1;
                        continue;
                    }
                    Err(e) => {
                        // On error, conservatively keep the row group.
                        debug!("error checking row group predicates: {}", e);
                    }
                }
            }

            row_groups.push(idx);
            if let Some(rows_left) = remaining_rows.as_mut() {
                let rg_rows = row_group.num_rows().max(0) as usize;
                if rg_rows >= *rows_left {
                    break;
                }
                *rows_left = rows_left.saturating_sub(rg_rows);
                if *rows_left == 0 {
                    break;
                }
            }
        }
    }

    if filtered_count > 0 {
        debug!(
            "min_max filter: filtered {} row groups, kept {}",
            filtered_count,
            row_groups.len()
        );
    }

    Some(row_groups)
}

struct PageSelectionResult {
    selection: Option<RowSelection>,
    rows_total: usize,
    rows_selected: usize,
    ranges: usize,
    pages_total: u128,
    pages_selected: u128,
    pages_pruned: u128,
    page_index_missing: u128,
    offset_index_missing: u128,
    predicates_unsupported: u128,
}

struct PageRangeResult {
    ranges: Vec<Range<usize>>,
    pages_total: usize,
    pages_selected: usize,
}

fn build_row_selection_for_row_groups(
    metadata: &ParquetMetaData,
    row_groups: &[usize],
    min_max_predicates: &[MinMaxPredicate],
    columns: &[String],
    case_sensitive: bool,
) -> PageSelectionResult {
    let mut result = PageSelectionResult {
        selection: None,
        rows_total: 0,
        rows_selected: 0,
        ranges: 0,
        pages_total: 0,
        pages_selected: 0,
        pages_pruned: 0,
        page_index_missing: 0,
        offset_index_missing: 0,
        predicates_unsupported: 0,
    };

    if min_max_predicates.is_empty() {
        return result;
    }

    let Some(column_index) = metadata.column_index() else {
        let total_rows = row_groups
            .iter()
            .map(|&rg_idx| metadata.row_group(rg_idx).num_rows().max(0) as usize)
            .sum::<usize>();
        result.rows_total = total_rows;
        result.rows_selected = total_rows;
        result.page_index_missing = row_groups.len() as u128;
        return result;
    };
    let Some(offset_index) = metadata.offset_index() else {
        let total_rows = row_groups
            .iter()
            .map(|&rg_idx| metadata.row_group(rg_idx).num_rows().max(0) as usize)
            .sum::<usize>();
        result.rows_total = total_rows;
        result.rows_selected = total_rows;
        result.offset_index_missing = row_groups.len() as u128;
        return result;
    };

    let mut global_ranges: Vec<Range<usize>> = Vec::new();
    let mut row_offset = 0usize;
    let mut any_pruned = false;

    for &rg_idx in row_groups {
        let row_group = metadata.row_group(rg_idx);
        let rg_rows = row_group.num_rows().max(0) as usize;
        result.rows_total += rg_rows;

        let mut ranges: Vec<Range<usize>> = vec![0..rg_rows];
        let mut any_supported = false;

        for pred in min_max_predicates {
            let col_idx_str = pred.column();
            let Ok(col_idx) = col_idx_str.parse::<usize>() else {
                result.predicates_unsupported += 1;
                continue;
            };
            if col_idx >= columns.len() {
                result.predicates_unsupported += 1;
                continue;
            }
            let col_name = &columns[col_idx];

            let col_chunk_idx = row_group.columns().iter().position(|c| {
                let path_str = c.column_path().string();
                if case_sensitive {
                    path_str == *col_name
                } else {
                    path_str.eq_ignore_ascii_case(col_name)
                }
            });

            let Some(col_chunk_idx) = col_chunk_idx else {
                result.predicates_unsupported += 1;
                continue;
            };

            let Some(rg_col_index) = column_index.get(rg_idx).and_then(|v| v.get(col_chunk_idx))
            else {
                result.page_index_missing += 1;
                continue;
            };
            let Some(rg_offset_index) = offset_index.get(rg_idx).and_then(|v| v.get(col_chunk_idx))
            else {
                result.offset_index_missing += 1;
                continue;
            };

            let page_ranges =
                match page_ranges_for_predicate(rg_col_index, rg_offset_index, rg_rows, pred) {
                    Some(r) => r,
                    None => {
                        result.predicates_unsupported += 1;
                        continue;
                    }
                };

            let pages_total = page_ranges.pages_total as u128;
            let pages_selected = page_ranges.pages_selected as u128;
            result.pages_total += pages_total;
            result.pages_selected += pages_selected;
            if pages_total >= pages_selected {
                result.pages_pruned += pages_total - pages_selected;
            }

            any_supported = true;
            ranges = intersect_ranges(&ranges, &page_ranges.ranges);
            if ranges.is_empty() {
                any_pruned = true;
                break;
            }
        }

        if any_supported {
            any_pruned = true;
        }

        if !ranges.is_empty() {
            let merged = merge_ranges(ranges);
            for r in &merged {
                result.rows_selected += r.end.saturating_sub(r.start);
                result.ranges += 1;
                global_ranges.push((r.start + row_offset)..(r.end + row_offset));
            }
        } else {
            // No rows selected in this row group.
        }
        row_offset = row_offset.saturating_add(rg_rows);
    }

    if result.rows_total == 0 {
        return result;
    }

    if !any_pruned || result.rows_selected == result.rows_total {
        return result;
    }

    let selection = RowSelection::from_consecutive_ranges(global_ranges.into_iter(), row_offset);
    result.selection = Some(selection);
    result
}

fn page_ranges_for_predicate(
    column_index: &ColumnIndexMetaData,
    offset_index: &OffsetIndexMetaData,
    total_rows: usize,
    predicate: &MinMaxPredicate,
) -> Option<PageRangeResult> {
    if matches!(column_index, ColumnIndexMetaData::NONE) {
        return None;
    }

    let page_locations = offset_index.page_locations();
    let num_pages = page_locations.len();
    if num_pages == 0 {
        return None;
    }

    let mut ranges: Vec<Range<usize>> = Vec::new();
    let mut pages_selected = 0usize;

    let mut push_page_range = |page_idx: usize| {
        let start = page_locations[page_idx].first_row_index.max(0) as usize;
        let end = if page_idx + 1 < num_pages {
            page_locations[page_idx + 1].first_row_index.max(0) as usize
        } else {
            total_rows
        };
        if start < end {
            ranges.push(start..end);
            pages_selected += 1;
        }
    };

    match column_index {
        ColumnIndexMetaData::INT32(index) => {
            let v = literal_int32(predicate.value())?;
            for page_idx in 0..num_pages {
                if index.is_null_page(page_idx) {
                    continue;
                }
                let min = index.min_values()[page_idx];
                let max = index.max_values()[page_idx];
                if page_satisfies_predicate_i32(min, max, v, predicate) {
                    push_page_range(page_idx);
                }
            }
        }
        ColumnIndexMetaData::INT64(index) => {
            let v = literal_int64(predicate.value())?;
            for page_idx in 0..num_pages {
                if index.is_null_page(page_idx) {
                    continue;
                }
                let min = index.min_values()[page_idx];
                let max = index.max_values()[page_idx];
                if page_satisfies_predicate_i64(min, max, v, predicate) {
                    push_page_range(page_idx);
                }
            }
        }
        ColumnIndexMetaData::FLOAT(index) => {
            let v = literal_float64(predicate.value())? as f32;
            for page_idx in 0..num_pages {
                if index.is_null_page(page_idx) {
                    continue;
                }
                let min = index.min_values()[page_idx];
                let max = index.max_values()[page_idx];
                if min.is_nan() || max.is_nan() {
                    push_page_range(page_idx);
                    continue;
                }
                if page_satisfies_predicate_f32(min, max, v, predicate) {
                    push_page_range(page_idx);
                }
            }
        }
        ColumnIndexMetaData::DOUBLE(index) => {
            let v = literal_float64(predicate.value())?;
            for page_idx in 0..num_pages {
                if index.is_null_page(page_idx) {
                    continue;
                }
                let min = index.min_values()[page_idx];
                let max = index.max_values()[page_idx];
                if min.is_nan() || max.is_nan() {
                    push_page_range(page_idx);
                    continue;
                }
                if page_satisfies_predicate_f64(min, max, v, predicate) {
                    push_page_range(page_idx);
                }
            }
        }
        _ => return None,
    }

    Some(PageRangeResult {
        ranges: merge_ranges(ranges),
        pages_total: num_pages,
        pages_selected,
    })
}

fn merge_ranges(ranges: Vec<Range<usize>>) -> Vec<Range<usize>> {
    if ranges.is_empty() {
        return ranges;
    }
    let mut merged = Vec::with_capacity(ranges.len());
    let mut current = ranges[0].clone();
    for r in ranges.into_iter().skip(1) {
        if r.start <= current.end {
            if r.end > current.end {
                current.end = r.end;
            }
        } else {
            merged.push(current);
            current = r;
        }
    }
    merged.push(current);
    merged
}

fn intersect_ranges(a: &[Range<usize>], b: &[Range<usize>]) -> Vec<Range<usize>> {
    let mut out = Vec::new();
    let mut i = 0usize;
    let mut j = 0usize;
    while i < a.len() && j < b.len() {
        let start = a[i].start.max(b[j].start);
        let end = a[i].end.min(b[j].end);
        if start < end {
            out.push(start..end);
        }
        if a[i].end < b[j].end {
            i += 1;
        } else {
            j += 1;
        }
    }
    out
}

fn row_group_start_offset(row_group: &RowGroupMetaData) -> Option<u64> {
    let mut start: Option<u64> = None;
    for column in row_group.columns() {
        let col_start = column.data_page_offset();
        if col_start < 0 {
            continue;
        }
        let col_start = col_start as u64;
        start = Some(match start {
            Some(v) => v.min(col_start),
            None => col_start,
        });
    }
    start
}

fn should_read_row_group(
    row_group: &RowGroupMetaData,
    _metadata: &ParquetMetaData,
    predicates: &[MinMaxPredicate],
    columns: &[String],
    case_sensitive: bool,
) -> Result<bool, String> {
    for pred in predicates {
        // pred.column() currently stores the index into `columns` as a string
        let col_idx_str = pred.column();
        let Ok(col_idx) = col_idx_str.parse::<usize>() else {
            continue;
        };

        if col_idx >= columns.len() {
            continue;
        }
        let col_name = &columns[col_idx];

        // Find the column chunk
        let chunk = row_group.columns().iter().find(|c| {
            let path_str = c.column_path().string();
            if case_sensitive {
                path_str == *col_name
            } else {
                path_str.eq_ignore_ascii_case(col_name)
            }
        });

        if let Some(chunk) = chunk {
            if let Some(stats) = chunk.statistics() {
                let satisfies = match pred {
                    MinMaxPredicate::Le { value, .. } => check_min_satisfies_le(stats, value)?,
                    MinMaxPredicate::Ge { value, .. } => check_max_satisfies_ge(stats, value)?,
                    MinMaxPredicate::Lt { value, .. } => check_min_satisfies_lt(stats, value)?,
                    MinMaxPredicate::Gt { value, .. } => check_max_satisfies_gt(stats, value)?,
                    MinMaxPredicate::Eq { value, .. } => {
                        check_max_satisfies_ge(stats, value)?
                            && check_min_satisfies_le(stats, value)?
                    }
                };

                if !satisfies {
                    return Ok(false);
                }
            }
        }
    }
    Ok(true)
}

impl MinMaxPredicate {
    fn column(&self) -> &str {
        match self {
            MinMaxPredicate::Le { column, .. }
            | MinMaxPredicate::Ge { column, .. }
            | MinMaxPredicate::Lt { column, .. }
            | MinMaxPredicate::Gt { column, .. }
            | MinMaxPredicate::Eq { column, .. } => column,
        }
    }

    fn value(&self) -> &LiteralValue {
        match self {
            MinMaxPredicate::Le { value, .. }
            | MinMaxPredicate::Ge { value, .. }
            | MinMaxPredicate::Lt { value, .. }
            | MinMaxPredicate::Gt { value, .. }
            | MinMaxPredicate::Eq { value, .. } => value,
        }
    }
}

// Check whether max satisfies >=.
fn check_max_satisfies_ge(stats: &Statistics, value: &LiteralValue) -> Result<bool, String> {
    match stats {
        Statistics::Int64(s) => {
            let Some(v) = literal_int64(value) else {
                return Ok(true);
            };
            if let Some(max) = s.max_opt() {
                Ok(*max >= v)
            } else {
                Ok(true)
            }
        }
        Statistics::Int32(s) => {
            let Some(v) = literal_int32(value) else {
                return Ok(true);
            };
            if let Some(max) = s.max_opt() {
                Ok(*max >= v)
            } else {
                Ok(true)
            }
        }
        Statistics::Double(s) => {
            let Some(v) = literal_float64(value) else {
                return Ok(true);
            };
            if let Some(max) = s.max_opt() {
                Ok(*max >= v)
            } else {
                Ok(true)
            }
        }
        _ => Ok(true),
    }
}

// Check whether min satisfies <=.
fn check_min_satisfies_le(stats: &Statistics, value: &LiteralValue) -> Result<bool, String> {
    match stats {
        Statistics::Int64(s) => {
            let Some(v) = literal_int64(value) else {
                return Ok(true);
            };
            if let Some(min) = s.min_opt() {
                Ok(*min <= v)
            } else {
                Ok(true)
            }
        }
        Statistics::Int32(s) => {
            let Some(v) = literal_int32(value) else {
                return Ok(true);
            };
            if let Some(min) = s.min_opt() {
                Ok(*min <= v)
            } else {
                Ok(true)
            }
        }
        Statistics::Double(s) => {
            let Some(v) = literal_float64(value) else {
                return Ok(true);
            };
            if let Some(min) = s.min_opt() {
                Ok(*min <= v)
            } else {
                Ok(true)
            }
        }
        _ => Ok(true),
    }
}

// Check whether max satisfies >.
fn check_max_satisfies_gt(stats: &Statistics, value: &LiteralValue) -> Result<bool, String> {
    match stats {
        Statistics::Int64(s) => {
            let Some(v) = literal_int64(value) else {
                return Ok(true);
            };
            if let Some(max) = s.max_opt() {
                Ok(*max > v)
            } else {
                Ok(true)
            }
        }
        Statistics::Int32(s) => {
            let Some(v) = literal_int32(value) else {
                return Ok(true);
            };
            if let Some(max) = s.max_opt() {
                Ok(*max > v)
            } else {
                Ok(true)
            }
        }
        Statistics::Double(s) => {
            let Some(v) = literal_float64(value) else {
                return Ok(true);
            };
            if let Some(max) = s.max_opt() {
                Ok(*max > v)
            } else {
                Ok(true)
            }
        }
        _ => Ok(true),
    }
}

// Check whether min satisfies <.
fn check_min_satisfies_lt(stats: &Statistics, value: &LiteralValue) -> Result<bool, String> {
    match stats {
        Statistics::Int64(s) => {
            let Some(v) = literal_int64(value) else {
                return Ok(true);
            };
            if let Some(min) = s.min_opt() {
                Ok(*min < v)
            } else {
                Ok(true)
            }
        }
        Statistics::Int32(s) => {
            let Some(v) = literal_int32(value) else {
                return Ok(true);
            };
            if let Some(min) = s.min_opt() {
                Ok(*min < v)
            } else {
                Ok(true)
            }
        }
        Statistics::Double(s) => {
            let Some(v) = literal_float64(value) else {
                return Ok(true);
            };
            if let Some(min) = s.min_opt() {
                Ok(*min < v)
            } else {
                Ok(true)
            }
        }
        _ => Ok(true),
    }
}

fn page_satisfies_predicate_i32(min: i32, max: i32, v: i32, pred: &MinMaxPredicate) -> bool {
    match pred {
        MinMaxPredicate::Le { .. } => min <= v,
        MinMaxPredicate::Ge { .. } => max >= v,
        MinMaxPredicate::Lt { .. } => min < v,
        MinMaxPredicate::Gt { .. } => max > v,
        MinMaxPredicate::Eq { .. } => min <= v && max >= v,
    }
}

fn page_satisfies_predicate_i64(min: i64, max: i64, v: i64, pred: &MinMaxPredicate) -> bool {
    match pred {
        MinMaxPredicate::Le { .. } => min <= v,
        MinMaxPredicate::Ge { .. } => max >= v,
        MinMaxPredicate::Lt { .. } => min < v,
        MinMaxPredicate::Gt { .. } => max > v,
        MinMaxPredicate::Eq { .. } => min <= v && max >= v,
    }
}

fn page_satisfies_predicate_f32(min: f32, max: f32, v: f32, pred: &MinMaxPredicate) -> bool {
    match pred {
        MinMaxPredicate::Le { .. } => min <= v,
        MinMaxPredicate::Ge { .. } => max >= v,
        MinMaxPredicate::Lt { .. } => min < v,
        MinMaxPredicate::Gt { .. } => max > v,
        MinMaxPredicate::Eq { .. } => min <= v && max >= v,
    }
}

fn page_satisfies_predicate_f64(min: f64, max: f64, v: f64, pred: &MinMaxPredicate) -> bool {
    match pred {
        MinMaxPredicate::Le { .. } => min <= v,
        MinMaxPredicate::Ge { .. } => max >= v,
        MinMaxPredicate::Lt { .. } => min < v,
        MinMaxPredicate::Gt { .. } => max > v,
        MinMaxPredicate::Eq { .. } => min <= v && max >= v,
    }
}
