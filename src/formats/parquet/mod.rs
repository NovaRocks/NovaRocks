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
mod page_selection;
mod reader;
mod row_group_selector;

pub use crate::common::min_max_predicate::{
    MinMaxPredicate, MinMaxPredicateOp, MinMaxPredicateValue,
};
pub use cache::{
    ParquetCacheOptions, init_datacache_parquet_cache, parquet_meta_cache_get,
    parquet_meta_cache_put, parquet_page_cache_get, parquet_page_cache_put,
};

use anyhow::Result;
use arrow::array::{
    Array, ArrayRef, BinaryArray, LargeBinaryArray, LargeBinaryBuilder, RecordBatch, StructArray,
};
use arrow::datatypes::DataType;
use parquet::arrow::arrow_reader::{
    ArrowReaderOptions, ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder,
};
use parquet::file::metadata::ParquetMetaData;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::cache::{CachedRangeReader, DataCacheContext};
use crate::common::config;
use crate::common::ids::SlotId;
use crate::exec::chunk::{Chunk, field_slot_id, field_with_slot_id};
use crate::exec::node::BoxedExecIter;
use crate::exec::node::scan::RuntimeFilterContext;
use crate::exec::variant::VariantValue;
use crate::fs::coalesce_policy::AdaptiveCoalesceController;
use crate::fs::opendal::OpendalRangeReaderFactory;
use crate::fs::range_plan::PlannedIoRanges;
use crate::fs::scan_context::FileScanRange;
use crate::metrics;
use crate::novarocks_logging::debug;
use crate::runtime::profile::{RuntimeProfile, clamp_u128_to_i64};
use crate::types;
use page_selection::build_row_selection_for_row_groups;
use reader::ParquetCachedReader;
use row_group_selector::select_row_groups_for_range;

static PARQUET_COALESCE_CONTROLLER: AdaptiveCoalesceController = AdaptiveCoalesceController::new();

fn runtime_filters_to_min_max_predicates(
    cfg: &ParquetScanConfig,
    runtime_filters: &RuntimeFilterContext,
) -> Result<Vec<MinMaxPredicate>, String> {
    let snapshot = runtime_filters.snapshot();
    if snapshot.is_empty() {
        return Ok(Vec::new());
    }
    if cfg.slot_ids.is_empty() || cfg.columns.is_empty() || cfg.slot_ids.len() != cfg.columns.len()
    {
        return Ok(Vec::new());
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
        let Some((min_value, max_value)) = rf.min_max_predicate_values().map_err(|e| {
            format!(
                "parquet runtime in-filter min/max conversion failed (slot_id={}): {}",
                rf.slot_id(),
                e
            )
        })?
        else {
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
        let Some((min_value, max_value)) =
            rf.min_max().min_max_predicate_values().map_err(|e| {
                format!(
                    "parquet runtime membership-filter min/max conversion failed (slot_id={}): {}",
                    rf.slot_id(),
                    e
                )
            })?
        else {
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
    Ok(preds)
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
    let iter = ParquetScanIter::new(
        cfg,
        scan.ranges,
        scan.factory,
        limit,
        profile,
        runtime_filters,
    );
    Ok(Box::new(iter))
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
}

impl ParquetScanIter {
    fn current_predicates(&self) -> Result<Vec<MinMaxPredicate>, String> {
        let mut predicates = self.cfg.min_max_predicates.clone();
        if let Some(filters) = self.runtime_filters.as_ref() {
            let mut runtime_preds = runtime_filters_to_min_max_predicates(&self.cfg, filters)?;
            if !runtime_preds.is_empty() {
                predicates.append(&mut runtime_preds);
            }
        }
        Ok(predicates)
    }

    fn build_parquet_reader(
        &self,
        mut builder: ParquetRecordBatchReaderBuilder<ParquetCachedReader>,
        metadata: &Arc<ParquetMetaData>,
        row_groups: &[usize],
        predicates: &[MinMaxPredicate],
    ) -> Result<Option<ParquetRecordBatchReader>, String> {
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
                metadata,
                row_groups,
                predicates,
                &self.cfg.columns,
                self.cfg.case_sensitive,
            );

            if selection.rows_selected == 0 {
                return Ok(None);
            }

            if let Some(sel) = selection.selection {
                builder = builder.with_row_selection(sel);
            }
        }

        builder = builder.with_row_groups(row_groups.to_vec());
        let reader = builder.build().map_err(|e| e.to_string())?;
        Ok(Some(reader))
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
        }
    }

    fn open_next_reader(&mut self) -> Result<bool, String> {
        loop {
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

            let reader = self
                .factory
                .open_with_len(&path, len)
                .map(|r| r.with_modification_time_override(range_modification_time))
                .map_err(|e| e.to_string())?;
            let identity = reader.file_identity().clone();
            let meta_cache_evict_probability = u32::try_from(
                self.cfg
                    .datacache
                    .cache_options()
                    .datacache_evict_probability,
            )
            .ok();

            let meta_cache_available =
                cache::parquet_meta_cache_available(self.cfg.cache_policy.enable_metacache);
            // Try to get metadata from cache when cache is actually available.
            let cached_metadata = if meta_cache_available {
                cache::parquet_meta_cache_get(self.cfg.cache_policy.enable_metacache, &identity)
            } else {
                None
            };

            // Build reader - parquet crate will still read footer, but we can use cached metadata
            // for row group filtering to avoid re-parsing
            let mut opts = ArrowReaderOptions::new().with_skip_arrow_metadata(true);
            if self.cfg.enable_page_index {
                opts = opts.with_page_index(true);
            }
            let cached_reader = CachedRangeReader::new(reader, Some(self.cfg.datacache.clone()));
            let parquet_reader =
                ParquetCachedReader::new(cached_reader.clone(), self.cfg.cache_policy.clone());
            let builder =
                ParquetRecordBatchReaderBuilder::try_new_with_options(parquet_reader, opts)
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
                    let _ = cache::parquet_meta_cache_put(
                        self.cfg.cache_policy.enable_metacache,
                        &identity,
                        metadata,
                        meta_cache_evict_probability,
                    );
                }
            } else if meta_cache_available {
                debug!("parquet metadata cache MISS for file: {}", path);
                // Cache the metadata for future use
                // builder.metadata() returns Arc<ParquetMetaData>, clone it
                let metadata = builder.metadata().clone();
                if cache::parquet_meta_cache_put(
                    self.cfg.cache_policy.enable_metacache,
                    &identity,
                    metadata,
                    meta_cache_evict_probability,
                ) {
                    debug!("parquet metadata cached for file: {}", path);
                }
            }

            let metadata = builder.metadata().clone();
            let predicates = self.current_predicates()?;
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

            if row_groups.is_empty() {
                continue;
            }
            let active_projection_columns = build_active_projection_columns(
                &predicates,
                &self.cfg.columns,
                self.cfg.case_sensitive,
            );
            let io_ranges = collect_parquet_coalesce_io_ranges(
                &metadata,
                &row_groups,
                &self.cfg.columns,
                self.cfg.case_sensitive,
                &active_projection_columns,
            );
            let coalesce_together = PARQUET_COALESCE_CONTROLLER.decide_and_record(
                config::io_coalesce_adaptive_lazy_active(),
                !io_ranges.lazy.is_empty(),
            );
            cached_reader.set_coalesce_io_ranges(io_ranges, coalesce_together);

            // TODO: Unlike StarRocks, this implementation fixes the row-group set when opening
            // the range-level reader. Late-arriving runtime filters cannot re-prune row groups
            // within the same range and may reduce pruning efficiency.
            if let Some(reader) =
                self.build_parquet_reader(builder, &metadata, &row_groups, &predicates)?
            {
                let prep_ns = prep_start.elapsed().as_nanos() as u128;
                if let Some(profile) = self.profile.as_ref() {
                    profile.counter_add(
                        "PrepareChunkSourceTime",
                        metrics::TUnit::TIME_NS,
                        clamp_u128_to_i64(prep_ns),
                    );
                }
                self.reader = Some(reader);
                return Ok(true);
            }
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

fn collect_parquet_coalesce_io_ranges(
    metadata: &ParquetMetaData,
    row_groups: &[usize],
    projected_columns: &[String],
    case_sensitive: bool,
    active_projection_columns: &HashSet<String>,
) -> PlannedIoRanges {
    let include_all_columns = projected_columns.is_empty();
    let selected_columns: Vec<&str> = projected_columns
        .iter()
        .map(String::as_str)
        .filter(|name| *name != "___count___")
        .collect();
    if !include_all_columns && selected_columns.is_empty() {
        return PlannedIoRanges::default();
    }

    let mut ranges = PlannedIoRanges::default();
    for &row_group_idx in row_groups {
        let Some(row_group) = metadata.row_groups().get(row_group_idx) else {
            continue;
        };
        for column in row_group.columns() {
            if !include_all_columns {
                let path = column.column_path().string();
                let matched = selected_columns.iter().any(|name| {
                    if case_sensitive {
                        path == *name
                    } else {
                        path.eq_ignore_ascii_case(name)
                    }
                });
                if !matched {
                    continue;
                }
            }
            let (offset, size) = column.byte_range();
            if size > 0 {
                let path = column.column_path().string();
                if active_projection_columns.is_empty()
                    || is_active_projection_column(&path, active_projection_columns, case_sensitive)
                {
                    ranges.push_active(offset, size);
                } else {
                    ranges.push_lazy(offset, size);
                }
            }
        }
    }
    ranges
}

fn build_active_projection_columns(
    predicates: &[MinMaxPredicate],
    projected_columns: &[String],
    case_sensitive: bool,
) -> HashSet<String> {
    let mut active_projection_columns = HashSet::new();
    for pred in predicates {
        let Ok(col_idx) = pred.column().parse::<usize>() else {
            continue;
        };
        let Some(col_name) = projected_columns.get(col_idx) else {
            continue;
        };
        if col_name == "___count___" {
            continue;
        }
        if case_sensitive {
            active_projection_columns.insert(col_name.clone());
        } else {
            active_projection_columns.insert(col_name.to_ascii_lowercase());
        }
    }
    active_projection_columns
}

fn is_active_projection_column(
    path: &str,
    active_projection_columns: &HashSet<String>,
    case_sensitive: bool,
) -> bool {
    if case_sensitive {
        active_projection_columns.contains(path)
    } else {
        active_projection_columns.contains(&path.to_ascii_lowercase())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::fs::{self, File};
    use std::sync::Arc;

    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use parquet::arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder};
    use parquet::file::reader::{FileReader, SerializedFileReader};

    use crate::cache::{CachedRangeReader, DataCacheManager, DataCachePageCacheOptions};
    use crate::fs::opendal::{OpendalRangeReaderFactory, build_fs_operator};

    use super::{
        ParquetReadCachePolicy, build_active_projection_columns,
        collect_parquet_coalesce_io_ranges, reader::ParquetCachedReader,
    };

    #[test]
    fn parquet_cached_reader_smoke_test() {
        let _ = DataCacheManager::instance().init_page_cache(DataCachePageCacheOptions {
            capacity: 64,
            evict_probability: 100,
        });

        let temp_dir = tempfile::tempdir().expect("tempdir");
        let file_path = temp_dir.path().join("sample.parquet");
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int32,
            false,
        )]));
        let batch = arrow::record_batch::RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .expect("record batch");

        let file = File::create(&file_path).expect("create parquet file");
        let mut writer =
            ArrowWriter::try_new(file, Arc::clone(&schema), None).expect("parquet writer");
        writer.write(&batch).expect("write batch");
        writer.close().expect("close writer");

        let file_len = fs::metadata(&file_path).expect("file metadata").len();
        let op = build_fs_operator(temp_dir.path().to_str().expect("temp dir path"))
            .expect("build fs operator");
        let factory = OpendalRangeReaderFactory::from_operator(op).expect("reader factory");
        let reader = factory
            .open_with_len("sample.parquet", Some(file_len))
            .expect("open with len");
        let reader = ParquetCachedReader::new(
            CachedRangeReader::new(reader, None),
            ParquetReadCachePolicy::with_flags(true, true, Some(100)),
        );
        let mut batches = ParquetRecordBatchReaderBuilder::try_new(reader)
            .expect("parquet builder")
            .with_batch_size(8)
            .build()
            .expect("build batch reader");

        let batch = batches
            .next()
            .expect("first batch")
            .expect("decode first batch");
        let values = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("int32 column");
        assert_eq!(values.values(), &[1, 2, 3]);
    }

    #[test]
    fn collect_parquet_coalesce_io_ranges_respects_projection() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let file_path = temp_dir.path().join("projection.parquet");
        let schema = Arc::new(Schema::new(vec![
            Field::new("value_a", DataType::Int32, false),
            Field::new("value_b", DataType::Int32, false),
        ]));
        let batch = arrow::record_batch::RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(Int32Array::from(vec![10, 20, 30])),
            ],
        )
        .expect("record batch");

        let file = File::create(&file_path).expect("create parquet file");
        let mut writer =
            ArrowWriter::try_new(file, Arc::clone(&schema), None).expect("parquet writer");
        writer.write(&batch).expect("write batch");
        writer.close().expect("close writer");

        let file = File::open(&file_path).expect("open parquet");
        let reader = SerializedFileReader::new(file).expect("metadata reader");
        let metadata = reader.metadata();
        let row_groups = vec![0usize];

        let all_ranges =
            collect_parquet_coalesce_io_ranges(metadata, &row_groups, &[], true, &HashSet::new());
        assert!(!all_ranges.active.is_empty());

        let projected_ranges = collect_parquet_coalesce_io_ranges(
            metadata,
            &row_groups,
            &["value_a".to_string()],
            true,
            &HashSet::new(),
        );
        assert!(!projected_ranges.active.is_empty());
        assert!(projected_ranges.active.len() <= all_ranges.active.len());

        let count_only_ranges = collect_parquet_coalesce_io_ranges(
            metadata,
            &row_groups,
            &["___count___".to_string()],
            true,
            &HashSet::new(),
        );
        assert!(count_only_ranges.active.is_empty());
        assert!(count_only_ranges.lazy.is_empty());
    }

    #[test]
    fn collect_parquet_coalesce_io_ranges_splits_active_and_lazy_by_predicates() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let file_path = temp_dir.path().join("active_lazy.parquet");
        let schema = Arc::new(Schema::new(vec![
            Field::new("value_a", DataType::Int32, false),
            Field::new("value_b", DataType::Int32, false),
        ]));
        let batch = arrow::record_batch::RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(Int32Array::from(vec![10, 20, 30])),
            ],
        )
        .expect("record batch");

        let file = File::create(&file_path).expect("create parquet file");
        let mut writer =
            ArrowWriter::try_new(file, Arc::clone(&schema), None).expect("parquet writer");
        writer.write(&batch).expect("write batch");
        writer.close().expect("close writer");

        let file = File::open(&file_path).expect("open parquet");
        let reader = SerializedFileReader::new(file).expect("metadata reader");
        let metadata = reader.metadata();
        let row_groups = vec![0usize];

        let active_projection_columns = build_active_projection_columns(
            &[super::MinMaxPredicate::Ge {
                column: "0".to_string(),
                value: super::MinMaxPredicateValue::Int32(1),
            }],
            &["value_a".to_string(), "value_b".to_string()],
            true,
        );
        let io_ranges = collect_parquet_coalesce_io_ranges(
            metadata,
            &row_groups,
            &["value_a".to_string(), "value_b".to_string()],
            true,
            &active_projection_columns,
        );
        assert!(!io_ranges.active.is_empty());
        assert!(!io_ranges.lazy.is_empty());
    }
}
