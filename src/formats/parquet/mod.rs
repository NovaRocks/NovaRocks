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
#[cfg(test)]
use arrow::array::{
    Date32Array, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array,
    UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow::datatypes::{DataType, Schema};
use parquet::arrow::arrow_reader::{
    ArrowReaderOptions, ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder, RowSelection,
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

enum ParquetRangeReader {
    Eager(ParquetRecordBatchReader),
    Delayed(DelayedMaterializeReader),
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

enum DelayedReaderDecision {
    Use(DelayedMaterializeReader),
    SkipRange,
    Fallback,
}

impl DelayedMaterializeReader {
    fn next_batch(&mut self) -> Option<Result<RecordBatch, String>> {
        let active_next = self.active_reader.next();
        let lazy_next = self.lazy_reader.next();
        match (active_next, lazy_next) {
            (Some(Ok(active_batch)), Some(Ok(lazy_batch))) => {
                if active_batch.num_rows() != lazy_batch.num_rows() {
                    return Some(Err(format!(
                        "delayed materialization batch row mismatch: active_rows={} lazy_rows={}",
                        active_batch.num_rows(),
                        lazy_batch.num_rows()
                    )));
                }
                let active_schema = active_batch.schema();
                let lazy_schema = lazy_batch.schema();
                let mut fields = Vec::with_capacity(self.output_sources.len());
                let mut columns = Vec::with_capacity(self.output_sources.len());
                for source in &self.output_sources {
                    match source {
                        DelayedColumnSource::Active(idx) => {
                            fields.push(active_schema.field(*idx).as_ref().clone());
                            columns.push(active_batch.column(*idx).clone());
                        }
                        DelayedColumnSource::Lazy(idx) => {
                            fields.push(lazy_schema.field(*idx).as_ref().clone());
                            columns.push(lazy_batch.column(*idx).clone());
                        }
                    }
                }
                let schema = Arc::new(Schema::new(fields));
                match RecordBatch::try_new(schema, columns) {
                    Ok(batch) => Some(Ok(batch)),
                    Err(e) => Some(Err(e.to_string())),
                }
            }
            (Some(Err(e)), _) => Some(Err(e.to_string())),
            (_, Some(Err(e))) => Some(Err(e.to_string())),
            (None, None) => None,
            (Some(Ok(_)), None) => Some(Err(
                "delayed materialization stream mismatch: active has rows but lazy reached EOF"
                    .to_string(),
            )),
            (None, Some(Ok(_))) => Some(Err(
                "delayed materialization stream mismatch: lazy has rows but active reached EOF"
                    .to_string(),
            )),
        }
    }
}

struct ParquetScanIter {
    cfg: ParquetScanConfig,
    ranges: Vec<FileScanRange>,
    factory: OpendalRangeReaderFactory,
    range_idx: usize,
    reader: Option<ParquetRangeReader>,
    remaining: usize,
    limit: Option<usize>,
    profile: Option<RuntimeProfile>,
    runtime_filters: Option<RuntimeFilterContext>,
}

impl ParquetScanIter {
    fn record_delayed_decision(&self, counter: &str) {
        if let Some(profile) = self.profile.as_ref() {
            profile.counter_add(counter, metrics::TUnit::UNIT, 1);
        }
    }

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

    fn new_parquet_builder(
        &self,
        cached_reader: &CachedRangeReader,
    ) -> Result<ParquetRecordBatchReaderBuilder<ParquetCachedReader>, String> {
        let mut opts = ArrowReaderOptions::new().with_skip_arrow_metadata(true);
        if self.cfg.enable_page_index {
            opts = opts.with_page_index(true);
        }
        let parquet_reader =
            ParquetCachedReader::new(cached_reader.clone(), self.cfg.cache_policy.clone());
        ParquetRecordBatchReaderBuilder::try_new_with_options(parquet_reader, opts)
            .map_err(|e| e.to_string())
    }

    fn build_projected_parquet_reader(
        &self,
        mut builder: ParquetRecordBatchReaderBuilder<ParquetCachedReader>,
        metadata: &Arc<ParquetMetaData>,
        row_groups: &[usize],
        projected_columns: &[String],
        predicates: &[MinMaxPredicate],
        explicit_row_selection: Option<RowSelection>,
        apply_page_selection: bool,
    ) -> Result<Option<ParquetRecordBatchReader>, String> {
        if let Some(batch_size) = self.cfg.batch_size {
            builder = builder.with_batch_size(batch_size);
            debug!("parquet reader: batch_size={}", batch_size);
        }

        if !projected_columns.is_empty() {
            let mask = {
                let arrow_schema = builder.schema();
                let parquet_schema = builder.parquet_schema();
                let mut indices = Vec::new();
                for col_name in projected_columns {
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

        if let Some(selection) = explicit_row_selection {
            builder = builder.with_row_selection(selection);
        } else if apply_page_selection && self.cfg.enable_page_index && !predicates.is_empty() {
            let selection = build_row_selection_for_row_groups(
                metadata,
                row_groups,
                predicates,
                projected_columns,
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

    fn build_parquet_reader(
        &self,
        builder: ParquetRecordBatchReaderBuilder<ParquetCachedReader>,
        metadata: &Arc<ParquetMetaData>,
        row_groups: &[usize],
        predicates: &[MinMaxPredicate],
    ) -> Result<Option<ParquetRecordBatchReader>, String> {
        self.build_projected_parquet_reader(
            builder,
            metadata,
            row_groups,
            &self.cfg.columns,
            predicates,
            None,
            true,
        )
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

    fn maybe_build_delayed_reader(
        &self,
        cached_reader: &CachedRangeReader,
        metadata: &Arc<ParquetMetaData>,
        row_groups: &[usize],
        predicates: &[MinMaxPredicate],
    ) -> Result<DelayedReaderDecision, String> {
        self.record_delayed_decision("ParquetDelayedDecisionTry");
        let Some(plan) =
            build_delayed_projection_plan(predicates, &self.cfg.columns, self.cfg.case_sensitive)
        else {
            self.record_delayed_decision("ParquetDelayedDecisionFallbackNoPlan");
            return Ok(DelayedReaderDecision::Fallback);
        };

        // Use page index as a cheap pre-check: only enable delayed materialization when
        // it can actually prune rows in this range. This avoids an expensive pre-scan
        // that would otherwise fallback to eager path with no pruning benefit.
        let selection = build_row_selection_for_row_groups(
            metadata,
            row_groups,
            predicates,
            &self.cfg.columns,
            self.cfg.case_sensitive,
        );

        if selection.rows_selected == 0 {
            self.record_delayed_decision("ParquetDelayedDecisionSkipRangeNoRows");
            return Ok(DelayedReaderDecision::SkipRange);
        }
        if selection.selection.is_none() || selection.rows_selected == selection.rows_total {
            self.record_delayed_decision("ParquetDelayedDecisionFallbackNoPagePrune");
            return Ok(DelayedReaderDecision::Fallback);
        }
        let base_selection = selection.selection.clone().expect("checked is_some");

        let output_sources = build_delayed_output_sources(
            &self.cfg.columns,
            &plan.active_columns,
            &plan.lazy_columns,
            self.cfg.case_sensitive,
        )?;
        let active_selection = base_selection.clone();
        let lazy_selection = base_selection;

        let active_builder = self.new_parquet_builder(cached_reader)?;
        let lazy_builder = self.new_parquet_builder(cached_reader)?;
        let Some(active_reader) = self.build_projected_parquet_reader(
            active_builder,
            metadata,
            row_groups,
            &plan.active_columns,
            predicates,
            Some(active_selection),
            false,
        )?
        else {
            return Ok(DelayedReaderDecision::SkipRange);
        };
        let Some(lazy_reader) = self.build_projected_parquet_reader(
            lazy_builder,
            metadata,
            row_groups,
            &plan.lazy_columns,
            predicates,
            Some(lazy_selection),
            false,
        )?
        else {
            self.record_delayed_decision("ParquetDelayedDecisionSkipRangeLazyReaderEmpty");
            return Ok(DelayedReaderDecision::SkipRange);
        };

        self.record_delayed_decision("ParquetDelayedDecisionUse");
        if let Some(profile) = self.profile.as_ref() {
            profile.counter_add("ParquetDelayedRange", metrics::TUnit::UNIT, 1);
            profile.counter_add(
                "ParquetDelayedRowsTotal",
                metrics::TUnit::UNIT,
                clamp_u128_to_i64(selection.rows_total as u128),
            );
            profile.counter_add(
                "ParquetDelayedRowsSelected",
                metrics::TUnit::UNIT,
                clamp_u128_to_i64(selection.rows_selected as u128),
            );
            profile.counter_add(
                "ParquetDelayedRowsPruned",
                metrics::TUnit::UNIT,
                clamp_u128_to_i64(selection.rows_total.saturating_sub(selection.rows_selected) as u128),
            );
        }

        Ok(DelayedReaderDecision::Use(DelayedMaterializeReader {
            active_reader,
            lazy_reader,
            output_sources,
        }))
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

            let open_file_start = std::time::Instant::now();
            let reader = self
                .factory
                .open_with_len(&path, len)
                .map(|r| r.with_modification_time_override(range_modification_time))
                .map_err(|e| e.to_string())?;
            let open_file_ns = open_file_start.elapsed().as_nanos() as u128;
            if let Some(profile) = self.profile.as_ref() {
                profile.counter_add(
                    "OpenFile",
                    metrics::TUnit::TIME_NS,
                    clamp_u128_to_i64(open_file_ns),
                );
            }
            let reader_init_start = std::time::Instant::now();
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

            let cached_reader = CachedRangeReader::new(reader, Some(self.cfg.datacache.clone()));
            // Build reader - parquet crate will still read footer, but we can use cached metadata
            // for row group filtering to avoid re-parsing
            let footer_read_start = std::time::Instant::now();
            let builder = self.new_parquet_builder(&cached_reader)?;
            let footer_read_ns = footer_read_start.elapsed().as_nanos() as u128;
            if let Some(profile) = self.profile.as_ref() {
                profile.counter_add(
                    "ReaderInitFooterRead",
                    metrics::TUnit::TIME_NS,
                    clamp_u128_to_i64(footer_read_ns),
                );
            }

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
                    let reader_init_ns = reader_init_start.elapsed().as_nanos() as u128;
                    if let Some(profile) = self.profile.as_ref() {
                        profile.counter_add(
                            "ReaderInit",
                            metrics::TUnit::TIME_NS,
                            clamp_u128_to_i64(reader_init_ns),
                        );
                    }
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
                let reader_init_ns = reader_init_start.elapsed().as_nanos() as u128;
                if let Some(profile) = self.profile.as_ref() {
                    profile.counter_add(
                        "ReaderInit",
                        metrics::TUnit::TIME_NS,
                        clamp_u128_to_i64(reader_init_ns),
                    );
                }
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
            match self.maybe_build_delayed_reader(
                &cached_reader,
                &metadata,
                &row_groups,
                &predicates,
            )? {
                DelayedReaderDecision::Use(reader) => {
                    let reader_init_ns = reader_init_start.elapsed().as_nanos() as u128;
                    if let Some(profile) = self.profile.as_ref() {
                        profile.counter_add(
                            "ReaderInit",
                            metrics::TUnit::TIME_NS,
                            clamp_u128_to_i64(reader_init_ns),
                        );
                    }
                    let prep_ns = prep_start.elapsed().as_nanos() as u128;
                    if let Some(profile) = self.profile.as_ref() {
                        profile.counter_add(
                            "PrepareChunkSourceTime",
                            metrics::TUnit::TIME_NS,
                            clamp_u128_to_i64(prep_ns),
                        );
                    }
                    self.reader = Some(ParquetRangeReader::Delayed(reader));
                    return Ok(true);
                }
                DelayedReaderDecision::SkipRange => {
                    let reader_init_ns = reader_init_start.elapsed().as_nanos() as u128;
                    if let Some(profile) = self.profile.as_ref() {
                        profile.counter_add(
                            "ReaderInit",
                            metrics::TUnit::TIME_NS,
                            clamp_u128_to_i64(reader_init_ns),
                        );
                    }
                    continue;
                }
                DelayedReaderDecision::Fallback => {}
            }

            let maybe_reader =
                self.build_parquet_reader(builder, &metadata, &row_groups, &predicates)?;
            let reader_init_ns = reader_init_start.elapsed().as_nanos() as u128;
            if let Some(profile) = self.profile.as_ref() {
                profile.counter_add(
                    "ReaderInit",
                    metrics::TUnit::TIME_NS,
                    clamp_u128_to_i64(reader_init_ns),
                );
            }
            if let Some(reader) = maybe_reader {
                let prep_ns = prep_start.elapsed().as_nanos() as u128;
                if let Some(profile) = self.profile.as_ref() {
                    profile.counter_add(
                        "PrepareChunkSourceTime",
                        metrics::TUnit::TIME_NS,
                        clamp_u128_to_i64(prep_ns),
                    );
                }
                self.reader = Some(ParquetRangeReader::Eager(reader));
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
            let column_read_start = std::time::Instant::now();
            let next_batch = match reader {
                ParquetRangeReader::Eager(reader) => {
                    reader.next().map(|r| r.map_err(|e| e.to_string()))
                }
                ParquetRangeReader::Delayed(reader) => reader.next_batch(),
            };
            let column_read_ns = column_read_start.elapsed().as_nanos() as u128;
            if let Some(profile) = self.profile.as_ref() {
                profile.counter_add(
                    "ColumnReadTime",
                    metrics::TUnit::TIME_NS,
                    clamp_u128_to_i64(column_read_ns),
                );
            }
            match next_batch {
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
                    return Some(Err(e));
                }
                None => {
                    self.reader = None;
                }
            }
        }
    }
}

struct DelayedProjectionPlan {
    active_columns: Vec<String>,
    lazy_columns: Vec<String>,
}

fn build_delayed_projection_plan(
    predicates: &[MinMaxPredicate],
    projected_columns: &[String],
    case_sensitive: bool,
) -> Option<DelayedProjectionPlan> {
    if predicates.is_empty() || projected_columns.is_empty() {
        return None;
    }
    if projected_columns.iter().any(|c| c == "___count___") {
        return None;
    }

    let active_projection_columns =
        build_active_projection_columns(predicates, projected_columns, case_sensitive);
    if active_projection_columns.is_empty() {
        return None;
    }

    let mut active_columns = Vec::new();
    let mut lazy_columns = Vec::new();
    for col in projected_columns {
        if is_active_projection_column(col, &active_projection_columns, case_sensitive) {
            active_columns.push(col.clone());
        } else {
            lazy_columns.push(col.clone());
        }
    }
    if active_columns.is_empty() || lazy_columns.is_empty() {
        return None;
    }

    Some(DelayedProjectionPlan {
        active_columns,
        lazy_columns,
    })
}

fn build_delayed_output_sources(
    output_columns: &[String],
    active_columns: &[String],
    lazy_columns: &[String],
    case_sensitive: bool,
) -> Result<Vec<DelayedColumnSource>, String> {
    let mut output_sources = Vec::with_capacity(output_columns.len());
    for col_name in output_columns {
        if let Some(idx) = find_column_index_by_name(active_columns, col_name, case_sensitive) {
            output_sources.push(DelayedColumnSource::Active(idx));
            continue;
        }
        if let Some(idx) = find_column_index_by_name(lazy_columns, col_name, case_sensitive) {
            output_sources.push(DelayedColumnSource::Lazy(idx));
            continue;
        }
        return Err(format!(
            "delayed materialization output column {} not found in active/lazy projection",
            col_name
        ));
    }
    Ok(output_sources)
}

fn find_column_index_by_name(
    columns: &[String],
    col_name: &str,
    case_sensitive: bool,
) -> Option<usize> {
    columns.iter().position(|c| {
        if case_sensitive {
            c == col_name
        } else {
            c.eq_ignore_ascii_case(col_name)
        }
    })
}

#[cfg(test)]
fn predicate_column_name<'a>(
    predicate: &MinMaxPredicate,
    projected_columns: &'a [String],
) -> Option<&'a str> {
    let idx = predicate.column().parse::<usize>().ok()?;
    let col_name = projected_columns.get(idx)?;
    if col_name == "___count___" {
        return None;
    }
    Some(col_name.as_str())
}

#[cfg(test)]
fn find_column_index_in_schema(
    schema: &Schema,
    col_name: &str,
    case_sensitive: bool,
) -> Option<usize> {
    if case_sensitive {
        return schema.index_of(col_name).ok();
    }
    schema
        .fields()
        .iter()
        .position(|f| f.name().eq_ignore_ascii_case(col_name))
}

#[cfg(test)]
fn evaluate_batch_predicate_mask(
    batch: &RecordBatch,
    predicates: &[MinMaxPredicate],
    projected_columns: &[String],
    case_sensitive: bool,
) -> Result<(Vec<bool>, bool), String> {
    let row_count = batch.num_rows();
    let mut selected = vec![true; row_count];
    let mut has_effective_predicate = false;
    let schema = batch.schema();

    for predicate in predicates {
        let Some(col_name) = predicate_column_name(predicate, projected_columns) else {
            continue;
        };
        let Some(col_idx) = find_column_index_in_schema(schema.as_ref(), col_name, case_sensitive)
        else {
            continue;
        };

        let Some(predicate_mask) =
            evaluate_min_max_predicate_mask(batch.column(col_idx), predicate)?
        else {
            continue;
        };

        has_effective_predicate = true;
        let mut any_selected = false;
        for (selected_row, predicate_ok) in selected.iter_mut().zip(predicate_mask.into_iter()) {
            *selected_row = *selected_row && predicate_ok;
            if *selected_row {
                any_selected = true;
            }
        }
        if !any_selected {
            break;
        }
    }

    Ok((selected, has_effective_predicate))
}

#[cfg(test)]
fn evaluate_min_max_predicate_mask(
    array: &ArrayRef,
        predicate: &MinMaxPredicate,
) -> Result<Option<Vec<bool>>, String> {
    match array.data_type() {
        DataType::Int8 => {
            let Some(v) = predicate.value().as_i64() else {
                return Ok(None);
            };
            let arr = array
                .as_any()
                .downcast_ref::<Int8Array>()
                .ok_or_else(|| "failed to downcast INT8 array".to_string())?;
            let mut out = Vec::with_capacity(arr.len());
            for idx in 0..arr.len() {
                out.push(
                    !arr.is_null(idx)
                        && value_satisfies_predicate_i64(arr.value(idx) as i64, v, predicate),
                );
            }
            Ok(Some(out))
        }
        DataType::Int16 => {
            let Some(v) = predicate.value().as_i64() else {
                return Ok(None);
            };
            let arr = array
                .as_any()
                .downcast_ref::<Int16Array>()
                .ok_or_else(|| "failed to downcast INT16 array".to_string())?;
            let mut out = Vec::with_capacity(arr.len());
            for idx in 0..arr.len() {
                out.push(
                    !arr.is_null(idx)
                        && value_satisfies_predicate_i64(arr.value(idx) as i64, v, predicate),
                );
            }
            Ok(Some(out))
        }
        DataType::Int32 => {
            let Some(v) = predicate.value().as_i64() else {
                return Ok(None);
            };
            let arr = array
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| "failed to downcast INT32 array".to_string())?;
            let mut out = Vec::with_capacity(arr.len());
            for idx in 0..arr.len() {
                out.push(
                    !arr.is_null(idx)
                        && value_satisfies_predicate_i64(arr.value(idx) as i64, v, predicate),
                );
            }
            Ok(Some(out))
        }
        DataType::Int64 => {
            let Some(v) = predicate.value().as_i64() else {
                return Ok(None);
            };
            let arr = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| "failed to downcast INT64 array".to_string())?;
            let mut out = Vec::with_capacity(arr.len());
            for idx in 0..arr.len() {
                out.push(
                    !arr.is_null(idx)
                        && value_satisfies_predicate_i64(arr.value(idx), v, predicate),
                );
            }
            Ok(Some(out))
        }
        DataType::UInt8 => {
            let Some(v) = predicate.value().as_i64() else {
                return Ok(None);
            };
            let Ok(v) = u64::try_from(v) else {
                return Ok(None);
            };
            let arr = array
                .as_any()
                .downcast_ref::<UInt8Array>()
                .ok_or_else(|| "failed to downcast UINT8 array".to_string())?;
            let mut out = Vec::with_capacity(arr.len());
            for idx in 0..arr.len() {
                out.push(
                    !arr.is_null(idx)
                        && value_satisfies_predicate_u64(arr.value(idx) as u64, v, predicate),
                );
            }
            Ok(Some(out))
        }
        DataType::UInt16 => {
            let Some(v) = predicate.value().as_i64() else {
                return Ok(None);
            };
            let Ok(v) = u64::try_from(v) else {
                return Ok(None);
            };
            let arr = array
                .as_any()
                .downcast_ref::<UInt16Array>()
                .ok_or_else(|| "failed to downcast UINT16 array".to_string())?;
            let mut out = Vec::with_capacity(arr.len());
            for idx in 0..arr.len() {
                out.push(
                    !arr.is_null(idx)
                        && value_satisfies_predicate_u64(arr.value(idx) as u64, v, predicate),
                );
            }
            Ok(Some(out))
        }
        DataType::UInt32 => {
            let Some(v) = predicate.value().as_i64() else {
                return Ok(None);
            };
            let Ok(v) = u64::try_from(v) else {
                return Ok(None);
            };
            let arr = array
                .as_any()
                .downcast_ref::<UInt32Array>()
                .ok_or_else(|| "failed to downcast UINT32 array".to_string())?;
            let mut out = Vec::with_capacity(arr.len());
            for idx in 0..arr.len() {
                out.push(
                    !arr.is_null(idx)
                        && value_satisfies_predicate_u64(arr.value(idx) as u64, v, predicate),
                );
            }
            Ok(Some(out))
        }
        DataType::UInt64 => {
            let Some(v) = predicate.value().as_i64() else {
                return Ok(None);
            };
            let Ok(v) = u64::try_from(v) else {
                return Ok(None);
            };
            let arr = array
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or_else(|| "failed to downcast UINT64 array".to_string())?;
            let mut out = Vec::with_capacity(arr.len());
            for idx in 0..arr.len() {
                out.push(
                    !arr.is_null(idx)
                        && value_satisfies_predicate_u64(arr.value(idx), v, predicate),
                );
            }
            Ok(Some(out))
        }
        DataType::Date32 => {
            let Some(v) = predicate.value().as_date32() else {
                return Ok(None);
            };
            let arr = array
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(|| "failed to downcast DATE32 array".to_string())?;
            let mut out = Vec::with_capacity(arr.len());
            for idx in 0..arr.len() {
                out.push(
                    !arr.is_null(idx)
                        && value_satisfies_predicate_i32(arr.value(idx), v, predicate),
                );
            }
            Ok(Some(out))
        }
        DataType::Float32 => {
            let Some(v) = predicate.value().as_f64() else {
                return Ok(None);
            };
            let arr = array
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| "failed to downcast FLOAT32 array".to_string())?;
            let mut out = Vec::with_capacity(arr.len());
            for idx in 0..arr.len() {
                let value = arr.value(idx) as f64;
                out.push(!arr.is_null(idx) && value_satisfies_predicate_f64(value, v, predicate));
            }
            Ok(Some(out))
        }
        DataType::Float64 => {
            let Some(v) = predicate.value().as_f64() else {
                return Ok(None);
            };
            let arr = array
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| "failed to downcast FLOAT64 array".to_string())?;
            let mut out = Vec::with_capacity(arr.len());
            for idx in 0..arr.len() {
                out.push(
                    !arr.is_null(idx)
                        && value_satisfies_predicate_f64(arr.value(idx), v, predicate),
                );
            }
            Ok(Some(out))
        }
        _ => Ok(None),
    }
}

#[cfg(test)]
fn value_satisfies_predicate_i64(value: i64, target: i64, predicate: &MinMaxPredicate) -> bool {
    match predicate {
        MinMaxPredicate::Le { .. } => value <= target,
        MinMaxPredicate::Ge { .. } => value >= target,
        MinMaxPredicate::Lt { .. } => value < target,
        MinMaxPredicate::Gt { .. } => value > target,
        MinMaxPredicate::Eq { .. } => value == target,
    }
}

#[cfg(test)]
fn value_satisfies_predicate_u64(value: u64, target: u64, predicate: &MinMaxPredicate) -> bool {
    match predicate {
        MinMaxPredicate::Le { .. } => value <= target,
        MinMaxPredicate::Ge { .. } => value >= target,
        MinMaxPredicate::Lt { .. } => value < target,
        MinMaxPredicate::Gt { .. } => value > target,
        MinMaxPredicate::Eq { .. } => value == target,
    }
}

#[cfg(test)]
fn value_satisfies_predicate_i32(value: i32, target: i32, predicate: &MinMaxPredicate) -> bool {
    match predicate {
        MinMaxPredicate::Le { .. } => value <= target,
        MinMaxPredicate::Ge { .. } => value >= target,
        MinMaxPredicate::Lt { .. } => value < target,
        MinMaxPredicate::Gt { .. } => value > target,
        MinMaxPredicate::Eq { .. } => value == target,
    }
}

#[cfg(test)]
fn value_satisfies_predicate_f64(value: f64, target: f64, predicate: &MinMaxPredicate) -> bool {
    match predicate {
        MinMaxPredicate::Le { .. } => value <= target,
        MinMaxPredicate::Ge { .. } => value >= target,
        MinMaxPredicate::Lt { .. } => value < target,
        MinMaxPredicate::Gt { .. } => value > target,
        MinMaxPredicate::Eq { .. } => value == target,
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

    use arrow::array::{Float64Array, Int32Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use parquet::arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder};
    use parquet::file::reader::{FileReader, SerializedFileReader};

    use crate::cache::{CachedRangeReader, DataCacheManager, DataCachePageCacheOptions};
    use crate::fs::opendal::{OpendalRangeReaderFactory, build_fs_operator};

    use super::{
        ParquetReadCachePolicy, build_active_projection_columns, build_delayed_output_sources,
        build_delayed_projection_plan, collect_parquet_coalesce_io_ranges,
        evaluate_batch_predicate_mask, reader::ParquetCachedReader,
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

    #[test]
    fn delayed_projection_plan_splits_active_and_lazy_columns() {
        let predicates = vec![super::MinMaxPredicate::Ge {
            column: "0".to_string(),
            value: super::MinMaxPredicateValue::Int32(10),
        }];
        let plan = build_delayed_projection_plan(
            &predicates,
            &[
                "predicate_col".to_string(),
                "lazy_col_a".to_string(),
                "lazy_col_b".to_string(),
            ],
            true,
        )
        .expect("delayed plan");
        assert_eq!(plan.active_columns, vec!["predicate_col".to_string()]);
        assert_eq!(
            plan.lazy_columns,
            vec!["lazy_col_a".to_string(), "lazy_col_b".to_string()]
        );
    }

    #[test]
    fn evaluate_batch_predicate_mask_applies_min_max_predicates() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("discount", DataType::Int32, true),
            Field::new("quantity", DataType::Int32, true),
            Field::new("revenue", DataType::Float64, true),
        ]));
        let batch = arrow::record_batch::RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![Some(1), Some(2), Some(4), None])),
                Arc::new(Int32Array::from(vec![
                    Some(10),
                    Some(30),
                    Some(20),
                    Some(5),
                ])),
                Arc::new(Float64Array::from(vec![
                    Some(10.0),
                    Some(20.0),
                    Some(30.0),
                    Some(40.0),
                ])),
            ],
        )
        .expect("record batch");
        let predicates = vec![
            super::MinMaxPredicate::Ge {
                column: "0".to_string(),
                value: super::MinMaxPredicateValue::Int32(2),
            },
            super::MinMaxPredicate::Lt {
                column: "1".to_string(),
                value: super::MinMaxPredicateValue::Int32(25),
            },
        ];
        let (mask, has_effective_predicate) = evaluate_batch_predicate_mask(
            &batch,
            &predicates,
            &[
                "discount".to_string(),
                "quantity".to_string(),
                "revenue".to_string(),
            ],
            true,
        )
        .expect("evaluate mask");
        assert!(has_effective_predicate);
        assert_eq!(mask, vec![false, false, true, false]);
    }

    #[test]
    fn build_delayed_output_sources_supports_case_insensitive_mapping() {
        let sources = build_delayed_output_sources(
            &["ColA".to_string(), "colb".to_string()],
            &["cola".to_string()],
            &["COLB".to_string()],
            false,
        )
        .expect("output sources");
        assert_eq!(sources.len(), 2);
        assert!(matches!(sources[0], super::DelayedColumnSource::Active(0)));
        assert!(matches!(sources[1], super::DelayedColumnSource::Lazy(0)));
    }
}
