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
mod variant_pruning;
mod variant_read;

pub use crate::common::min_max_predicate::{
    MinMaxPredicate, MinMaxPredicateOp, MinMaxPredicateValue,
};
pub use cache::{
    ParquetCacheOptions, init_datacache_parquet_cache, parquet_meta_cache_get,
    parquet_meta_cache_put, parquet_page_cache_get, parquet_page_cache_put,
};

use anyhow::Result;
use arrow::array::{Array, ArrayRef, RecordBatch, StructArray, new_null_array};
#[cfg(test)]
use arrow::array::{
    Date32Array, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array,
    UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, FieldRef, Schema, SchemaRef};
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use parquet::arrow::arrow_reader::{
    ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReader,
    ParquetRecordBatchReaderBuilder, RowSelection,
};
use parquet::basic::Encoding;
use parquet::file::metadata::{PageIndexPolicy, ParquetMetaData};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::cache::{CachedRangeReader, DataCacheContext};
use crate::common::config;
use crate::common::ids::SlotId;
use crate::common::runtime_scan_predicate::{
    RuntimeScanPredicateBindings, RuntimeScanPredicateCounters, RuntimeScanPredicateOptions,
    runtime_filters_to_scan_predicates as build_runtime_scan_predicates,
};
use crate::common::scan_predicate::{ScanPredicate, ScanPredicateSource};
use crate::exec::chunk::{Chunk, ChunkSchema, ChunkSchemaRef, ChunkSlotSchema};
use crate::exec::expr::cast_with_special_rules;
use crate::exec::node::BoxedExecIter;
use crate::exec::node::scan::RuntimeFilterContext;
use crate::fs::coalesce_policy::AdaptiveCoalesceController;
use crate::fs::opendal::OpendalRangeReaderFactory;
use crate::fs::range_plan::PlannedIoRanges;
use crate::fs::scan_context::FileScanRange;
use crate::novarocks_logging::debug;
use crate::runtime::profile::{RuntimeProfile, clamp_u128_to_i64};
use crate::thrift::metrics;
use page_selection::build_row_selection_for_row_groups;
pub(crate) use reader::ParquetCachedReader;
use row_group_selector::select_row_groups_for_range;
pub use variant_pruning::VariantPathPruningPredicate;
pub(crate) use variant_pruning::{
    BoundVariantPathPruningPredicate, bind_variant_path_pruning_predicates,
    variant_residual_value_all_null_for_row_group,
};
use variant_read::{
    collapse_variant_struct_to_largebinary, convert_variant_columns, is_variant_struct_data_type,
    materialize_variant_path_columns,
};

static PARQUET_COALESCE_CONTROLLER: AdaptiveCoalesceController = AdaptiveCoalesceController::new();
const IO_TASK_EXEC_TIME_COUNTER: &str = "IOTaskExecTime";
const PARQUET_PROFILE_GROUP: &str = "Parquet";
const INPUT_STREAM_PROFILE_GROUP: &str = "InputStream";
const SHARED_BUFFERED_PROFILE_GROUP: &str = "SharedBuffered";

fn read_app_io_time_ns(profile: &RuntimeProfile) -> i64 {
    profile
        .add_child_timer("AppIOTime", INPUT_STREAM_PROFILE_GROUP)
        .value()
}

fn normalize_batch_to_chunk_schema(
    batch: RecordBatch,
    chunk_schema: &ChunkSchemaRef,
) -> Result<RecordBatch, String> {
    if batch.num_columns() != chunk_schema.slots().len() {
        return Err(format!(
            "parquet batch/chunk schema length mismatch: batch_columns={} chunk_slots={}",
            batch.num_columns(),
            chunk_schema.slots().len()
        ));
    }
    let mut fields = Vec::with_capacity(batch.num_columns());
    let mut columns = Vec::with_capacity(batch.num_columns());
    let batch_schema = batch.schema();
    for (idx, slot) in chunk_schema.slots().iter().enumerate() {
        let column = batch.column(idx).clone();
        let preserve_dictionary =
            is_dictionary_string_carrier_for_slot(column.data_type(), slot.data_type());
        let casted = if column.data_type() == slot.data_type() || preserve_dictionary {
            column
        } else {
            cast_with_special_rules(&column, slot.data_type()).map_err(|e| {
                format!(
                    "cast parquet scan column {} from {:?} to {:?} failed: {e}",
                    slot.name(),
                    column.data_type(),
                    slot.data_type()
                )
            })?
        };
        let mut field = if slot.field().data_type() == casted.data_type() {
            slot.field().clone()
        } else {
            slot.field()
                .clone()
                .with_data_type(casted.data_type().clone())
        };
        if casted.null_count() > 0 && !field.is_nullable() {
            field = field.with_nullable(true);
        };
        let source_field = batch_schema.field(idx);
        if !source_field.metadata().is_empty() {
            let mut metadata = field.metadata().clone();
            metadata.extend(source_field.metadata().clone());
            field = field.with_metadata(metadata);
        }
        fields.push(field);
        columns.push(casted);
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
        .map_err(|e| format!("normalize parquet scan batch failed: {e}"))
}

fn is_dictionary_string_carrier_for_slot(actual: &DataType, slot: &DataType) -> bool {
    let DataType::Dictionary(key_type, value_type) = actual else {
        return false;
    };
    key_type.as_ref() == &DataType::Int32
        && ((slot == &DataType::Utf8 && value_type.as_ref() == &DataType::Utf8)
            || (slot == &DataType::LargeUtf8 && value_type.as_ref() == &DataType::LargeUtf8))
}

fn is_string_data_type(data_type: &DataType) -> bool {
    matches!(data_type, DataType::Utf8 | DataType::LargeUtf8)
}

fn dictionary_carrier_type_for_string(data_type: &DataType) -> Option<DataType> {
    if is_string_data_type(data_type) {
        Some(DataType::Dictionary(
            Box::new(DataType::Int32),
            Box::new(data_type.clone()),
        ))
    } else {
        None
    }
}

fn normalized_column_name(name: &str, case_sensitive: bool) -> String {
    if case_sensitive {
        name.to_string()
    } else {
        name.to_ascii_lowercase()
    }
}

fn parquet_column_uses_dictionary_encoding(metadata: &ParquetMetaData, column_idx: usize) -> bool {
    metadata.num_row_groups() > 0
        && (0..metadata.num_row_groups()).all(|row_group_idx| {
            metadata
                .row_group(row_group_idx)
                .column(column_idx)
                .encodings()
                .any(|encoding| {
                    matches!(
                        encoding,
                        Encoding::RLE_DICTIONARY | Encoding::PLAIN_DICTIONARY
                    )
                })
        })
}

fn top_level_parquet_column_index(
    metadata: &ParquetMetaData,
    field_name: &str,
    case_sensitive: bool,
) -> Option<usize> {
    metadata
        .file_metadata()
        .schema_descr()
        .columns()
        .iter()
        .enumerate()
        .find_map(|(idx, column)| {
            let parts = column.path().parts();
            if parts.len() == 1
                && normalized_column_name(&parts[0], case_sensitive)
                    == normalized_column_name(field_name, case_sensitive)
            {
                Some(idx)
            } else {
                None
            }
        })
}

const PARQUET_DISCRETE_SET_MAX_VALUES: usize = 256;

fn min_max_predicates_to_scan_predicates(
    predicates: &[MinMaxPredicate],
    source: ScanPredicateSource,
) -> Vec<ScanPredicate> {
    predicates
        .iter()
        .cloned()
        .map(|predicate| ScanPredicate::from_min_max_predicate(predicate, source))
        .collect()
}

fn scan_predicates_to_min_max_predicates(predicates: &[ScanPredicate]) -> Vec<MinMaxPredicate> {
    // A3a keeps page-index pruning byte-identical by letting page selection consume only
    // min/max-compatible predicates. A3b replaces this boundary with a page ScanPruner
    // that can accept DiscreteSet directly.
    predicates
        .iter()
        .flat_map(ScanPredicate::to_min_max_predicates)
        .collect()
}

fn runtime_scan_predicate_bindings(cfg: &ParquetScanConfig) -> RuntimeScanPredicateBindings {
    let mut bindings = RuntimeScanPredicateBindings::default();
    if cfg.chunk_schema.slot_ids().is_empty() || cfg.columns.is_empty() {
        return bindings;
    }
    let variant_output_slots = cfg
        .variant_path_columns
        .iter()
        .map(|spec| spec.output_slot_id)
        .collect::<HashSet<_>>();
    for slot in cfg.chunk_schema.slots() {
        let slot_id = slot.slot_id();
        if variant_output_slots.contains(&slot_id) {
            continue;
        }
        let Some(idx) = find_column_index_by_name(&cfg.columns, slot.name(), cfg.case_sensitive)
        else {
            continue;
        };
        bindings.slot_to_column.insert(slot_id, idx.to_string());
    }

    bindings.min_max_filter_columns = cfg
        .runtime_min_max_filter_columns
        .iter()
        .filter_map(|(filter_id, column_name)| {
            find_column_index_by_name(&cfg.columns, column_name, cfg.case_sensitive)
                .map(|idx| (*filter_id, idx.to_string()))
        })
        .collect();
    bindings
}

fn runtime_filters_to_scan_predicates(
    cfg: &ParquetScanConfig,
    runtime_filters: &RuntimeFilterContext,
) -> Result<Vec<ScanPredicate>, String> {
    let bindings = runtime_scan_predicate_bindings(cfg);
    let mut counters = RuntimeScanPredicateCounters::default();
    build_runtime_scan_predicates(
        runtime_filters,
        &bindings,
        RuntimeScanPredicateOptions {
            discrete_set_max_values: PARQUET_DISCRETE_SET_MAX_VALUES,
            label: "parquet",
        },
        &mut counters,
    )
}

#[derive(Clone, Debug)]
pub struct VariantPathSpec {
    pub source_slot_id: SlotId,
    pub source_read_slot_id: SlotId,
    pub output_slot_id: SlotId,
    pub source_field_id: Option<i32>,
    pub source_name: String,
    pub output_name: String,
    pub source_field: Field,
    pub output_field: Field,
    pub canonical_path: String,
    pub requested_type: DataType,
    pub strict: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ParquetSlotKind {
    Regular,
    Variant,
}

impl ParquetSlotKind {
    pub(crate) fn is_variant(self) -> bool {
        self == Self::Variant
    }
}

#[derive(Clone, Debug)]
pub struct ParquetScanConfig {
    pub columns: Vec<String>,
    pub chunk_schema: ChunkSchemaRef,
    pub slot_kinds: Vec<ParquetSlotKind>,
    pub case_sensitive: bool,
    pub enable_page_index: bool,
    pub min_max_predicates: Vec<MinMaxPredicate>,
    pub runtime_min_max_filter_columns: HashMap<i32, String>,
    pub variant_path_predicates: Vec<VariantPathPruningPredicate>,
    pub batch_size: Option<usize>,
    pub datacache: DataCacheContext,
    pub cache_policy: ParquetReadCachePolicy,
    pub profile_label: Option<String>,
    pub iceberg_output_schema: Option<SchemaRef>,
    pub variant_path_columns: Vec<VariantPathSpec>,
    /// Per-slot global dictionary encode maps. Non-empty only for dict-encoded
    /// scans. When set, the iterator reads the dict columns as Utf8 and maps
    /// them to Int32 dict ids.
    pub query_global_dicts: crate::exec::dict_encode::QueryGlobalDictEncodeMap,
}

fn materialized_variant_path_schema_and_slot_kinds(
    cfg: &ParquetScanConfig,
) -> Result<(ChunkSchemaRef, Vec<ParquetSlotKind>), String> {
    if cfg.variant_path_columns.is_empty() {
        return Ok((cfg.chunk_schema.clone(), cfg.slot_kinds.clone()));
    }
    if cfg.chunk_schema.slot_ids().len() != cfg.slot_kinds.len() {
        return Err(format!(
            "variant path scan schema/slot_kinds mismatch: slots={} slot_kinds={}",
            cfg.chunk_schema.slot_ids().len(),
            cfg.slot_kinds.len()
        ));
    }

    let hidden_source_reads = cfg
        .variant_path_columns
        .iter()
        .filter_map(|spec| {
            (spec.source_read_slot_id != spec.source_slot_id).then_some(spec.source_read_slot_id)
        })
        .collect::<HashSet<_>>();

    let slot_kind_by_id = cfg
        .chunk_schema
        .slot_ids()
        .iter()
        .copied()
        .zip(cfg.slot_kinds.iter().copied())
        .collect::<HashMap<_, _>>();
    let mut materialized_slots = Vec::new();
    let mut materialized_slot_kinds = Vec::new();
    let mut seen = HashSet::new();

    for slot in cfg.chunk_schema.slots() {
        let slot_id = slot.slot_id();
        if hidden_source_reads.contains(&slot_id) {
            continue;
        }
        seen.insert(slot_id);
        materialized_slots.push(slot.clone());
        materialized_slot_kinds.push(
            *slot_kind_by_id
                .get(&slot_id)
                .unwrap_or(&ParquetSlotKind::Regular),
        );
    }

    for spec in &cfg.variant_path_columns {
        if !seen.insert(spec.output_slot_id) {
            return Err(format!(
                "duplicate variant path materialized output_slot_id={}",
                spec.output_slot_id
            ));
        }
        materialized_slots.push(ChunkSlotSchema::try_new_with_field(
            spec.output_slot_id,
            spec.output_field.clone(),
            None,
            None,
        )?);
        materialized_slot_kinds.push(ParquetSlotKind::Regular);
    }

    Ok((
        Arc::new(ChunkSchema::try_new(materialized_slots)?),
        materialized_slot_kinds,
    ))
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
    )?;
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
    scan_read_chunk_schema: ChunkSchemaRef,
    materialized_chunk_schema: ChunkSchemaRef,
    materialized_slot_kinds: Vec<ParquetSlotKind>,
    has_dict_encoded_output: bool,
}

#[derive(Clone, Debug, Default)]
struct CurrentPruningPredicates {
    physical: Vec<ScanPredicate>,
    variant: Vec<VariantPathPruningPredicate>,
    counters: RuntimeScanPredicateCounters,
}

impl ParquetScanIter {
    fn has_iceberg_schema_evolution(&self) -> bool {
        self.cfg.iceberg_output_schema.is_some()
    }

    fn arrow_reader_options(&self) -> ArrowReaderOptions {
        let mut opts = ArrowReaderOptions::new().with_skip_arrow_metadata(true);
        if self.cfg.enable_page_index {
            opts = opts.with_page_index_policy(PageIndexPolicy::Required);
        }
        opts
    }

    fn dictionary_string_scan_candidates(
        &self,
        arrow_schema: &Schema,
    ) -> HashMap<String, DataType> {
        if self.cfg.query_global_dicts.is_empty() {
            let mut candidates = HashMap::new();
            if self.cfg.columns.is_empty() {
                if self.materialized_chunk_schema.slots().len() != arrow_schema.fields().len() {
                    return candidates;
                }
                for (field, slot) in arrow_schema
                    .fields()
                    .iter()
                    .zip(self.materialized_chunk_schema.slots())
                {
                    if is_string_data_type(slot.data_type()) {
                        candidates.insert(
                            normalized_column_name(field.name(), self.cfg.case_sensitive),
                            slot.data_type().clone(),
                        );
                    }
                }
            } else if self.cfg.columns.len() == self.materialized_chunk_schema.slots().len() {
                for (column_name, slot) in self
                    .cfg
                    .columns
                    .iter()
                    .zip(self.materialized_chunk_schema.slots())
                {
                    if column_name == "___count___" {
                        continue;
                    }
                    if is_string_data_type(slot.data_type()) {
                        candidates.insert(
                            normalized_column_name(column_name, self.cfg.case_sensitive),
                            slot.data_type().clone(),
                        );
                    }
                }
            }
            candidates
        } else {
            HashMap::new()
        }
    }

    fn dictionary_preserving_arrow_schema(
        &self,
        metadata: &ParquetMetaData,
        arrow_schema: &SchemaRef,
    ) -> Option<SchemaRef> {
        let candidates = self.dictionary_string_scan_candidates(arrow_schema.as_ref());
        if candidates.is_empty() {
            return None;
        }

        let root_fields = metadata
            .file_metadata()
            .schema_descr()
            .root_schema()
            .get_fields();
        let mut changed = false;
        let mut fields = Vec::with_capacity(arrow_schema.fields().len());
        for (field_idx, field) in arrow_schema.fields().iter().enumerate() {
            let candidate = candidates.get(&normalized_column_name(
                field.name(),
                self.cfg.case_sensitive,
            ));
            let Some(slot_type) = candidate else {
                fields.push(field.as_ref().clone());
                continue;
            };
            let Some(root_field) = root_fields.get(field_idx) else {
                fields.push(field.as_ref().clone());
                continue;
            };
            let Some(dictionary_type) = dictionary_carrier_type_for_string(slot_type) else {
                fields.push(field.as_ref().clone());
                continue;
            };
            if !is_string_data_type(field.data_type()) {
                fields.push(field.as_ref().clone());
                continue;
            }
            let Some(column_idx) =
                top_level_parquet_column_index(metadata, field.name(), self.cfg.case_sensitive)
            else {
                fields.push(field.as_ref().clone());
                continue;
            };
            if root_field.is_primitive()
                && parquet_column_uses_dictionary_encoding(metadata, column_idx)
            {
                fields.push(field.as_ref().clone().with_data_type(dictionary_type));
                changed = true;
            } else {
                fields.push(field.as_ref().clone());
            }
        }

        changed.then(|| Arc::new(Schema::new(fields)))
    }

    fn record_delayed_decision(&self, counter: &str) {
        if let Some(profile) = self.profile.as_ref() {
            profile.counter_add(counter, metrics::TUnit::UNIT, 1);
        }
    }

    fn current_pruning_predicates(&self) -> Result<CurrentPruningPredicates, String> {
        if self.has_iceberg_schema_evolution() {
            return Ok(CurrentPruningPredicates {
                physical: Vec::new(),
                variant: self.cfg.variant_path_predicates.clone(),
                counters: RuntimeScanPredicateCounters::default(),
            });
        }
        let mut counters = RuntimeScanPredicateCounters {
            range: self.cfg.min_max_predicates.len() as u128,
            ..Default::default()
        };
        let mut predicates = CurrentPruningPredicates {
            physical: min_max_predicates_to_scan_predicates(
                &self.cfg.min_max_predicates,
                ScanPredicateSource::Static,
            ),
            variant: self.cfg.variant_path_predicates.clone(),
            counters: RuntimeScanPredicateCounters::default(),
        };
        if let Some(filters) = self.runtime_filters.as_ref() {
            let bindings = runtime_scan_predicate_bindings(&self.cfg);
            let mut runtime_preds = build_runtime_scan_predicates(
                filters,
                &bindings,
                RuntimeScanPredicateOptions {
                    discrete_set_max_values: PARQUET_DISCRETE_SET_MAX_VALUES,
                    label: "parquet",
                },
                &mut counters,
            )?;
            if !runtime_preds.is_empty() {
                predicates.physical.append(&mut runtime_preds);
            }
        }
        predicates.counters = counters;
        Ok(predicates)
    }

    fn new_parquet_builder(
        &self,
        cached_reader: &CachedRangeReader,
    ) -> Result<ParquetRecordBatchReaderBuilder<ParquetCachedReader>, String> {
        let parquet_reader =
            ParquetCachedReader::new(cached_reader.clone(), self.cfg.cache_policy.clone());
        let builder = ParquetRecordBatchReaderBuilder::try_new_with_options(
            parquet_reader.clone(),
            self.arrow_reader_options(),
        )
        .map_err(|e| e.to_string())?;

        let Some(dict_schema) =
            self.dictionary_preserving_arrow_schema(builder.metadata(), builder.schema())
        else {
            return Ok(builder);
        };
        let dict_opts = self.arrow_reader_options().with_schema(dict_schema);
        match ArrowReaderMetadata::try_new(builder.metadata().clone(), dict_opts) {
            Ok(metadata) => Ok(ParquetRecordBatchReaderBuilder::new_with_metadata(
                parquet_reader,
                metadata,
            )),
            Err(e) => {
                debug!(
                    "parquet dictionary carrier schema rejected, falling back to flat strings: {}",
                    e
                );
                Ok(builder)
            }
        }
    }

    fn build_projected_parquet_reader(
        &self,
        mut builder: ParquetRecordBatchReaderBuilder<ParquetCachedReader>,
        metadata: &Arc<ParquetMetaData>,
        row_groups: &[usize],
        projected_columns: &[String],
        predicates: &[MinMaxPredicate],
        variant_predicates: &[BoundVariantPathPruningPredicate],
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
                let indices = if let Some(output_schema) = self.cfg.iceberg_output_schema.as_ref() {
                    build_iceberg_root_projection_indices(
                        output_schema,
                        arrow_schema.as_ref(),
                        parquet_schema,
                        self.cfg.case_sensitive,
                    )?
                } else {
                    build_name_projection_indices(
                        projected_columns,
                        arrow_schema.as_ref(),
                        self.cfg.case_sensitive,
                    )?
                };
                parquet::arrow::ProjectionMask::roots(parquet_schema, indices)
            };
            builder = builder.with_projection(mask);
        }

        if self.limit.is_some() {
            builder = builder.with_limit(self.remaining);
        }

        if let Some(selection) = explicit_row_selection {
            builder = builder.with_row_selection(selection);
        } else if apply_page_selection
            && self.cfg.enable_page_index
            && (!predicates.is_empty() || !variant_predicates.is_empty())
        {
            let selection = build_row_selection_for_row_groups(
                metadata,
                row_groups,
                predicates,
                variant_predicates,
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
        variant_predicates: &[BoundVariantPathPruningPredicate],
    ) -> Result<Option<ParquetRecordBatchReader>, String> {
        self.build_projected_parquet_reader(
            builder,
            metadata,
            row_groups,
            &self.cfg.columns,
            predicates,
            variant_predicates,
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
    ) -> Result<Self, String> {
        let remaining = limit.unwrap_or(usize::MAX);
        let (materialized_chunk_schema, materialized_slot_kinds) =
            materialized_variant_path_schema_and_slot_kinds(&cfg)?;
        let (scan_read_chunk_schema, has_dict_encoded_output) = if cfg.query_global_dicts.is_empty()
        {
            (materialized_chunk_schema.clone(), false)
        } else {
            let out_arrow = materialized_chunk_schema.arrow_schema_ref();
            let (scan_arrow, has_dict) =
                crate::exec::dict_encode::build_scan_schema_for_global_dict_encoding(
                    &out_arrow,
                    &materialized_chunk_schema,
                    &cfg.query_global_dicts,
                )?;
            if has_dict {
                let scan_chunk = crate::exec::chunk::ChunkSchema::try_ref_from_schema_and_slot_ids(
                    scan_arrow.as_ref(),
                    materialized_chunk_schema.slot_ids(),
                )?;
                (scan_chunk, true)
            } else {
                (materialized_chunk_schema.clone(), false)
            }
        };
        Ok(Self {
            cfg,
            ranges,
            factory,
            range_idx: 0,
            reader: None,
            remaining,
            limit,
            profile,
            runtime_filters,
            scan_read_chunk_schema,
            materialized_chunk_schema,
            materialized_slot_kinds,
            has_dict_encoded_output,
        })
    }

    fn maybe_build_delayed_reader(
        &self,
        cached_reader: &CachedRangeReader,
        metadata: &Arc<ParquetMetaData>,
        row_groups: &[usize],
        predicates: &[MinMaxPredicate],
        variant_predicates: &[BoundVariantPathPruningPredicate],
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
            variant_predicates,
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
            &[],
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
            &[],
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
                clamp_u128_to_i64(
                    selection.rows_total.saturating_sub(selection.rows_selected) as u128
                ),
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
                let _ = profile.add_child_counter(
                    PARQUET_PROFILE_GROUP,
                    metrics::TUnit::NONE,
                    IO_TASK_EXEC_TIME_COUNTER,
                );
                let _ = profile.add_child_counter(
                    SHARED_BUFFERED_PROFILE_GROUP,
                    metrics::TUnit::NONE,
                    IO_TASK_EXEC_TIME_COUNTER,
                );
            }

            let open_file_start = std::time::Instant::now();
            let reader = self
                .factory
                .open_with_len(&path, len)
                .map(|r| r.with_modification_time_override(range_modification_time))
                .map_err(|e| e.to_string())?;
            let open_file_ns = open_file_start.elapsed().as_nanos();
            let reader_init_start = std::time::Instant::now();
            let app_io_before_reader_init = self.profile.as_ref().map(read_app_io_time_ns);
            let record_reader_init = |profile: &RuntimeProfile, reader_init_wall_ns: u128| {
                let reader_init_ns = clamp_u128_to_i64(reader_init_wall_ns);
                let reader_init_io_ns = app_io_before_reader_init
                    .map(|before| read_app_io_time_ns(profile).saturating_sub(before))
                    .unwrap_or(0);
                profile.counter_add_with_parent(
                    "ReaderInit",
                    metrics::TUnit::TIME_NS,
                    std::cmp::max(reader_init_ns, reader_init_io_ns),
                    IO_TASK_EXEC_TIME_COUNTER,
                );
            };
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
            let app_io_before_footer = self.profile.as_ref().map(read_app_io_time_ns);
            let footer_read_start = std::time::Instant::now();
            // Annotate footer-parse failures with the offending file path —
            // an unannotated "Corrupt footer" makes it impossible to tell
            // which range produced the error when a scan covers many files
            // or when the path comes from an iceberg manifest the user did
            // not write directly.
            let builder = self
                .new_parquet_builder(&cached_reader)
                .map_err(|e| format!("parquet open {path} (file_len={file_len}): {e}"))?;
            let footer_read_ns = footer_read_start.elapsed().as_nanos();
            if let Some(profile) = self.profile.as_ref() {
                let footer_read_ns = clamp_u128_to_i64(footer_read_ns);
                let footer_io_ns = app_io_before_footer
                    .map(|before| read_app_io_time_ns(profile).saturating_sub(before))
                    .unwrap_or(0);
                let footer_ns = std::cmp::max(footer_read_ns, footer_io_ns);
                profile.counter_add_with_parent(
                    "ReaderInitFooterRead",
                    metrics::TUnit::TIME_NS,
                    footer_ns,
                    PARQUET_PROFILE_GROUP,
                );
                profile.counter_add_with_parent(
                    "OpenFile",
                    metrics::TUnit::TIME_NS,
                    std::cmp::max(clamp_u128_to_i64(open_file_ns), footer_ns),
                    IO_TASK_EXEC_TIME_COUNTER,
                );
                profile.counter_add_with_parent(
                    "DirectIOTime",
                    metrics::TUnit::TIME_NS,
                    footer_ns,
                    SHARED_BUFFERED_PROFILE_GROUP,
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
            let predicates = self.current_pruning_predicates()?;
            if let Some(profile) = self.profile.as_ref() {
                profile.counter_add(
                    "ParquetScanPredicatesRange",
                    metrics::TUnit::UNIT,
                    clamp_u128_to_i64(predicates.counters.range),
                );
                profile.counter_add(
                    "ParquetScanPredicatesDiscreteSet",
                    metrics::TUnit::UNIT,
                    clamp_u128_to_i64(predicates.counters.discrete_set),
                );
                profile.counter_add(
                    "ParquetScanPredicatesEnvelopeFallback",
                    metrics::TUnit::UNIT,
                    clamp_u128_to_i64(predicates.counters.envelope_fallback),
                );
                profile.counter_add(
                    "ParquetScanPredicatesUnsupported",
                    metrics::TUnit::UNIT,
                    clamp_u128_to_i64(predicates.counters.unsupported),
                );
            }
            let bound_variant_predicates = bind_variant_path_pruning_predicates(
                &metadata,
                &self.cfg.variant_path_columns,
                &predicates.variant,
            );
            let limit_rows = self.limit.map(|_| self.remaining);
            let selected_row_groups = select_row_groups_for_range(
                &metadata,
                &range,
                limit_rows,
                &predicates.physical,
                &bound_variant_predicates,
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
                    let reader_init_ns = reader_init_start.elapsed().as_nanos();
                    if let Some(profile) = self.profile.as_ref() {
                        record_reader_init(profile, reader_init_ns);
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
                let reader_init_ns = reader_init_start.elapsed().as_nanos();
                if let Some(profile) = self.profile.as_ref() {
                    record_reader_init(profile, reader_init_ns);
                }
                continue;
            }
            let page_min_max_predicates =
                scan_predicates_to_min_max_predicates(&predicates.physical);
            let use_name_based_projection = !self.has_iceberg_schema_evolution();
            let active_projection_columns = if use_name_based_projection {
                build_active_projection_columns(
                    &page_min_max_predicates,
                    &self.cfg.columns,
                    self.cfg.case_sensitive,
                )
            } else {
                HashSet::new()
            };
            let io_ranges = collect_parquet_coalesce_io_ranges(
                &metadata,
                &row_groups,
                if use_name_based_projection {
                    &self.cfg.columns
                } else {
                    &[] as &[String]
                },
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
                &page_min_max_predicates,
                &bound_variant_predicates,
            )? {
                DelayedReaderDecision::Use(reader) => {
                    let reader_init_ns = reader_init_start.elapsed().as_nanos();
                    if let Some(profile) = self.profile.as_ref() {
                        record_reader_init(profile, reader_init_ns);
                    }
                    let prep_ns = prep_start.elapsed().as_nanos();
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
                    let reader_init_ns = reader_init_start.elapsed().as_nanos();
                    if let Some(profile) = self.profile.as_ref() {
                        record_reader_init(profile, reader_init_ns);
                    }
                    continue;
                }
                DelayedReaderDecision::Fallback => {}
            }

            let maybe_reader = self.build_parquet_reader(
                builder,
                &metadata,
                &row_groups,
                &page_min_max_predicates,
                &bound_variant_predicates,
            )?;
            let reader_init_ns = reader_init_start.elapsed().as_nanos();
            if let Some(profile) = self.profile.as_ref() {
                record_reader_init(profile, reader_init_ns);
            }
            if let Some(reader) = maybe_reader {
                let prep_ns = prep_start.elapsed().as_nanos();
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
            let app_io_before_batch = self.profile.as_ref().map(read_app_io_time_ns);
            let next_batch = match reader {
                ParquetRangeReader::Eager(reader) => {
                    reader.next().map(|r| r.map_err(|e| e.to_string()))
                }
                ParquetRangeReader::Delayed(reader) => reader.next_batch(),
            };
            let column_read_ns = column_read_start.elapsed().as_nanos();
            if let Some(profile) = self.profile.as_ref() {
                let column_read_ns = clamp_u128_to_i64(column_read_ns);
                let shared_io_ns = app_io_before_batch
                    .map(|before| read_app_io_time_ns(profile).saturating_sub(before))
                    .unwrap_or(0);
                profile.counter_add_with_parent(
                    "ColumnReadTime",
                    metrics::TUnit::TIME_NS,
                    column_read_ns,
                    IO_TASK_EXEC_TIME_COUNTER,
                );
                profile.counter_add_with_parent(
                    "GroupChunkRead",
                    metrics::TUnit::TIME_NS,
                    column_read_ns,
                    PARQUET_PROFILE_GROUP,
                );
                profile.counter_add_with_parent(
                    "PageReadTime",
                    metrics::TUnit::TIME_NS,
                    column_read_ns,
                    PARQUET_PROFILE_GROUP,
                );
                profile.counter_add_with_parent(
                    "SharedIOTime",
                    metrics::TUnit::TIME_NS,
                    shared_io_ns,
                    SHARED_BUFFERED_PROFILE_GROUP,
                );
            }
            match next_batch {
                Some(Ok(batch)) => {
                    if batch.num_rows() == 0 {
                        continue;
                    }
                    let batch = match reorder_batch(&self.cfg, batch)
                        .and_then(|b| {
                            materialize_variant_path_columns(
                                b,
                                self.cfg.chunk_schema.slot_ids(),
                                self.scan_read_chunk_schema.slot_ids(),
                                &self.cfg.variant_path_columns,
                            )
                        })
                        .and_then(|b| convert_variant_columns(&self.materialized_slot_kinds, b))
                        .and_then(|b| {
                            normalize_batch_to_chunk_schema(b, &self.scan_read_chunk_schema)
                        })
                        .and_then(|b| {
                            if self.has_dict_encoded_output {
                                crate::exec::dict_encode::encode_batch_with_query_global_dicts(
                                    b,
                                    &self.materialized_chunk_schema.arrow_schema_ref(),
                                    &self.materialized_chunk_schema,
                                    &self.cfg.query_global_dicts,
                                )
                            } else {
                                Ok(b)
                            }
                        }) {
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
                    let chunk_schema = match self.materialized_chunk_schema.with_fields_in_order(
                        batch
                            .schema()
                            .fields()
                            .iter()
                            .map(|field| field.as_ref().clone())
                            .collect(),
                    ) {
                        Ok(schema) => Arc::new(schema),
                        Err(e) => return Some(Err(e)),
                    };
                    return Some(Chunk::try_new_with_chunk_schema(batch, chunk_schema));
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

fn parse_parquet_field_id(field: &Field) -> Result<Option<i32>, String> {
    let Some(raw) = field.metadata().get(PARQUET_FIELD_ID_META_KEY) else {
        return Ok(None);
    };
    raw.parse::<i32>().map(Some).map_err(|e| {
        format!(
            "invalid parquet field_id metadata: field={} key={} value={} error={}",
            field.name(),
            PARQUET_FIELD_ID_META_KEY,
            raw,
            e
        )
    })
}

fn find_matching_field_index(
    fields: &[FieldRef],
    target: &Field,
    case_sensitive: bool,
) -> Result<Option<usize>, String> {
    let target_field_id = parse_parquet_field_id(target)?;
    if let Some(target_field_id) = target_field_id {
        let mut source_has_field_ids = false;
        for (idx, source) in fields.iter().enumerate() {
            let source_field_id = parse_parquet_field_id(source.as_ref())?;
            source_has_field_ids |= source_field_id.is_some();
            if source_field_id == Some(target_field_id) {
                return Ok(Some(idx));
            }
        }
        if source_has_field_ids {
            return Ok(None);
        }
    }
    Ok(fields.iter().position(|field| {
        if case_sensitive {
            field.name() == target.name()
        } else {
            field.name().eq_ignore_ascii_case(target.name())
        }
    }))
}

fn build_name_projection_indices(
    projected_columns: &[String],
    arrow_schema: &Schema,
    case_sensitive: bool,
) -> Result<Vec<usize>, String> {
    let mut indices = Vec::new();
    for col_name in projected_columns {
        if col_name == "___count___" {
            continue;
        }
        let idx = if case_sensitive {
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
    Ok(indices)
}

fn build_iceberg_root_projection_indices(
    output_schema: &SchemaRef,
    arrow_schema: &Schema,
    parquet_schema: &parquet::schema::types::SchemaDescriptor,
    case_sensitive: bool,
) -> Result<Vec<usize>, String> {
    let root_fields = parquet_schema.root_schema().get_fields();
    let mut indices = Vec::new();
    for target in output_schema.fields() {
        let target_field_id = parse_parquet_field_id(target.as_ref())?;
        let idx = if let Some(target_field_id) = target_field_id {
            let root_has_field_ids = root_fields
                .iter()
                .any(|field| field.get_basic_info().has_id());
            let field_id_match = root_fields.iter().position(|field| {
                let info = field.get_basic_info();
                info.has_id() && info.id() == target_field_id
            });
            if field_id_match.is_some() || root_has_field_ids {
                field_id_match
            } else if case_sensitive {
                arrow_schema.index_of(target.name()).ok()
            } else {
                arrow_schema
                    .fields()
                    .iter()
                    .position(|field| field.name().eq_ignore_ascii_case(target.name()))
            }
        } else if case_sensitive {
            arrow_schema.index_of(target.name()).ok()
        } else {
            arrow_schema
                .fields()
                .iter()
                .position(|field| field.name().eq_ignore_ascii_case(target.name()))
        };
        if let Some(idx) = idx {
            indices.push(idx);
        }
    }
    Ok(indices)
}

fn align_iceberg_array_to_field(
    source_field: &Field,
    source_array: ArrayRef,
    target_field: &Field,
    row_count: usize,
    case_sensitive: bool,
) -> Result<ArrayRef, String> {
    // Iceberg V3 variant: parquet stores variant as
    // `Struct{ metadata: Binary, value: Binary }`, but NovaRocks carries
    // variants internally as `LargeBinary` (`[size:u32 LE | metadata |
    // value]`). When the iceberg-output schema requests `LargeBinary` for
    // a column whose source is the variant struct, collapse it inline —
    // arrow's generic `cast()` cannot do this conversion.
    if matches!(target_field.data_type(), DataType::LargeBinary)
        && is_variant_struct_data_type(source_field.data_type())
    {
        return collapse_variant_struct_to_largebinary(&source_array, target_field.name());
    }
    match (source_field.data_type(), target_field.data_type()) {
        (DataType::Struct(source_children), DataType::Struct(target_children)) => {
            let struct_array = source_array
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| {
                    format!(
                        "expected StructArray for iceberg schema evolution column {}",
                        source_field.name()
                    )
                })?;
            let mut columns = Vec::with_capacity(target_children.len());
            for target_child in target_children {
                if let Some(source_idx) = find_matching_field_index(
                    source_children,
                    target_child.as_ref(),
                    case_sensitive,
                )? {
                    let source_child = source_children[source_idx].as_ref();
                    let aligned = align_iceberg_array_to_field(
                        source_child,
                        struct_array.column(source_idx).clone(),
                        target_child.as_ref(),
                        row_count,
                        case_sensitive,
                    )?;
                    columns.push(aligned);
                } else {
                    columns.push(build_iceberg_default_array(
                        target_child.as_ref(),
                        row_count,
                    )?);
                }
            }
            let array = StructArray::try_new(
                target_children.clone(),
                columns,
                struct_array.nulls().cloned(),
            )
            .map_err(|e| e.to_string())?;
            Ok(Arc::new(array))
        }
        _ => {
            if is_dictionary_string_carrier_for_slot(
                source_array.data_type(),
                target_field.data_type(),
            ) {
                return Ok(source_array);
            }
            if source_array.data_type() == target_field.data_type() {
                return Ok(source_array);
            }
            let casted = cast(source_array.as_ref(), target_field.data_type()).map_err(|e| {
                format!(
                    "iceberg parquet cast failed for column {} from {:?} to {:?}: {}",
                    target_field.name(),
                    source_array.data_type(),
                    target_field.data_type(),
                    e
                )
            })?;
            if casted.null_count() > source_array.null_count() {
                return Err(format!(
                    "iceberg parquet cast introduced nulls for column {} from {:?} to {:?}",
                    target_field.name(),
                    source_array.data_type(),
                    target_field.data_type()
                ));
            }
            Ok(casted)
        }
    }
}

fn build_iceberg_default_array(target_field: &Field, row_count: usize) -> Result<ArrayRef, String> {
    use crate::connector::iceberg::default_value::literal_to_constant_array;
    use crate::connector::iceberg::schema::ICEBERG_INITIAL_DEFAULT_META_KEY;
    use iceberg::spec::Literal;

    let Some(json) = target_field
        .metadata()
        .get(ICEBERG_INITIAL_DEFAULT_META_KEY)
    else {
        return Ok(new_null_array(target_field.data_type(), row_count));
    };
    let json_value: serde_json::Value = serde_json::from_str(json).map_err(|e| {
        format!(
            "corrupted initial-default JSON for column {}: {e}",
            target_field.name()
        )
    })?;
    let iceberg_type = arrow_type_to_iceberg_type(target_field.data_type()).map_err(|e| {
        format!(
            "unsupported initial-default for column {}: {e}",
            target_field.name()
        )
    })?;
    let literal = Literal::try_from_json(json_value, &iceberg_type)
        .map_err(|e| {
            format!(
                "decode initial-default for column {}: {e}",
                target_field.name()
            )
        })?
        .ok_or_else(|| {
            format!(
                "initial-default JSON for column {} produced no literal",
                target_field.name()
            )
        })?;

    literal_to_constant_array(&literal, target_field.data_type(), row_count)
}

fn arrow_type_to_iceberg_type(
    dt: &arrow::datatypes::DataType,
) -> Result<iceberg::spec::Type, String> {
    use arrow::datatypes::TimeUnit;
    use iceberg::spec::{ListType, MapType, NestedField, PrimitiveType, Type};
    Ok(match dt {
        DataType::Boolean => Type::Primitive(PrimitiveType::Boolean),
        DataType::Int32 => Type::Primitive(PrimitiveType::Int),
        DataType::Int64 => Type::Primitive(PrimitiveType::Long),
        DataType::Float32 => Type::Primitive(PrimitiveType::Float),
        DataType::Float64 => Type::Primitive(PrimitiveType::Double),
        DataType::Decimal128(precision, scale) => Type::Primitive(PrimitiveType::Decimal {
            precision: *precision as u32,
            scale: *scale as u32,
        }),
        DataType::Utf8 => Type::Primitive(PrimitiveType::String),
        DataType::Date32 => Type::Primitive(PrimitiveType::Date),
        DataType::Timestamp(TimeUnit::Microsecond, None) => {
            Type::Primitive(PrimitiveType::Timestamp)
        }
        DataType::Timestamp(TimeUnit::Microsecond, Some(_)) => {
            Type::Primitive(PrimitiveType::Timestamptz)
        }
        DataType::Timestamp(TimeUnit::Nanosecond, None) => {
            Type::Primitive(PrimitiveType::TimestampNs)
        }
        DataType::Timestamp(TimeUnit::Nanosecond, Some(_)) => {
            Type::Primitive(PrimitiveType::TimestamptzNs)
        }
        DataType::Binary | DataType::LargeBinary => Type::Primitive(PrimitiveType::Binary),
        // List type: construct a ListType with the element type inferred from the Arrow field.
        // Field id is a placeholder (used only during empty-default JSON round-trip).
        DataType::List(element_field) => {
            let element_type = arrow_type_to_iceberg_type(element_field.data_type())?;
            Type::List(ListType::new(Arc::new(NestedField::optional(
                1,
                "element",
                element_type,
            ))))
        }
        // Map type: construct from the Arrow entries struct fields (key at index 0, value at 1).
        DataType::Map(entries_field, _) => {
            let DataType::Struct(entry_fields) = entries_field.data_type() else {
                return Err(format!(
                    "arrow Map field entries must be a Struct, got {:?}",
                    entries_field.data_type()
                ));
            };
            if entry_fields.len() < 2 {
                return Err(format!(
                    "arrow Map entries struct must have at least 2 fields (key, value), got {}",
                    entry_fields.len()
                ));
            }
            let key_type = arrow_type_to_iceberg_type(entry_fields[0].data_type())?;
            let value_type = arrow_type_to_iceberg_type(entry_fields[1].data_type())?;
            Type::Map(MapType::new(
                Arc::new(NestedField::required(1, "key", key_type)),
                Arc::new(NestedField::optional(2, "value", value_type)),
            ))
        }
        other => {
            return Err(format!(
                "arrow type {other:?} cannot carry an iceberg default"
            ));
        }
    })
}

fn iceberg_output_field_for_array(target_field: &Field, array: &ArrayRef) -> Field {
    let mut field =
        if is_dictionary_string_carrier_for_slot(array.data_type(), target_field.data_type()) {
            target_field
                .clone()
                .with_data_type(array.data_type().clone())
        } else {
            target_field.clone()
        };
    if array.null_count() > 0 && !field.is_nullable() {
        field = field.with_nullable(true);
    }
    field
}

fn align_batch_to_iceberg_schema(
    output_schema: &SchemaRef,
    batch: RecordBatch,
    case_sensitive: bool,
) -> Result<RecordBatch, String> {
    let row_count = batch.num_rows();
    let batch_schema = batch.schema();
    let mut fields = Vec::with_capacity(output_schema.fields().len());
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(output_schema.fields().len());
    for target in output_schema.fields() {
        if target.name() == "___count___" {
            if target.data_type() != &DataType::Boolean {
                return Err(format!(
                    "iceberg virtual count column expects Boolean type, got {:?}",
                    target.data_type()
                ));
            }
            let count_array: ArrayRef =
                Arc::new(arrow::array::BooleanArray::from(vec![true; row_count]));
            fields.push(target.as_ref().clone());
            columns.push(count_array);
            continue;
        }
        if let Some(source_idx) =
            find_matching_field_index(batch_schema.fields(), target.as_ref(), case_sensitive)?
        {
            let source_field = batch_schema.field(source_idx);
            let array = align_iceberg_array_to_field(
                source_field,
                batch.column(source_idx).clone(),
                target.as_ref(),
                row_count,
                case_sensitive,
            )?;
            fields.push(iceberg_output_field_for_array(target.as_ref(), &array));
            columns.push(array);
        } else {
            fields.push(target.as_ref().clone());
            columns.push(build_iceberg_default_array(target.as_ref(), row_count)?);
        }
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).map_err(|e| e.to_string())
}

fn reorder_batch(cfg: &ParquetScanConfig, batch: RecordBatch) -> Result<RecordBatch, String> {
    if let Some(output_schema) = cfg.iceberg_output_schema.as_ref() {
        let batch = align_batch_to_iceberg_schema(output_schema, batch, cfg.case_sensitive)?;
        return validate_batch_slot_count(cfg, batch);
    }

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
                return validate_batch_slot_count(cfg, batch);
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
        return validate_batch_slot_count(cfg, batch);
    }

    validate_batch_slot_count(cfg, batch)
}

fn validate_batch_slot_count(
    cfg: &ParquetScanConfig,
    batch: RecordBatch,
) -> Result<RecordBatch, String> {
    if batch.num_columns() == 0 {
        return Ok(batch);
    }

    if cfg.chunk_schema.slot_ids().is_empty() {
        return Err(format!(
            "parquet scan missing chunk schema for non-empty batch: num_columns={}",
            batch.num_columns()
        ));
    }

    if batch.num_columns() != cfg.chunk_schema.slot_ids().len() {
        return Err(format!(
            "parquet scan output columns/chunk schema mismatch: num_columns={}, slot_ids={:?}",
            batch.num_columns(),
            cfg.chunk_schema.slot_ids()
        ));
    }

    Ok(batch)
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
    use std::collections::HashMap;
    use std::collections::HashSet;
    use std::fs::{self, File};
    use std::io::Cursor;
    use std::path::Path;
    use std::sync::Arc;

    use arrow::array::{
        Array, ArrayRef, DictionaryArray, Float64Array, Int32Array, Int64Array, StringArray,
        StructArray,
    };
    use arrow::datatypes::{DataType, Field, Int32Type, Schema};
    use parquet::arrow::{
        ArrowWriter, PARQUET_FIELD_ID_META_KEY,
        arrow_reader::{ArrowReaderOptions, ParquetRecordBatchReaderBuilder},
    };
    use parquet::basic::Encoding;
    use parquet::file::metadata::ParquetMetaData;
    use parquet::file::properties::{EnabledStatistics, WriterProperties};
    use parquet::file::reader::{FileReader, SerializedFileReader};
    use parquet::variant::{ShreddedSchemaBuilder, json_to_variant, shred_variant};

    use crate::cache::{
        CacheOptions, CachedRangeReader, DataCacheManager, DataCachePageCacheOptions,
    };
    use crate::common::ids::SlotId;
    use crate::common::scan_predicate::{MembershipPredicate, ScanPredicateDomain};
    use crate::exec::chunk::ChunkSchema;
    use crate::fs::opendal::{OpendalRangeReaderFactory, build_fs_operator};
    use crate::fs::scan_context::{FileScanContext, FileScanRange};
    use crate::thrift::types;

    use super::{
        MinMaxPredicate, MinMaxPredicateValue, ParquetReadCachePolicy, ParquetScanConfig,
        ParquetScanIter, ParquetSlotKind, ScanPredicate, ScanPredicateSource,
        VariantPathPruningPredicate, VariantPathSpec, bind_variant_path_pruning_predicates,
        build_active_projection_columns, build_delayed_output_sources,
        build_delayed_projection_plan, build_parquet_iter, build_row_selection_for_row_groups,
        collect_parquet_coalesce_io_ranges, evaluate_batch_predicate_mask,
        reader::ParquetCachedReader, runtime_filters_to_scan_predicates,
        scan_predicates_to_min_max_predicates, select_row_groups_for_range,
    };

    fn field_id_meta(field_id: i32) -> HashMap<String, String> {
        HashMap::from([(PARQUET_FIELD_ID_META_KEY.to_string(), field_id.to_string())])
    }

    fn field_with_id(name: &str, data_type: DataType, nullable: bool, field_id: i32) -> Field {
        Field::new(name, data_type, nullable).with_metadata(field_id_meta(field_id))
    }

    fn test_datacache_context() -> crate::cache::DataCacheContext {
        let cache_options = CacheOptions::from_query_options(None).expect("cache options");
        DataCacheManager::instance().external_context(cache_options)
    }

    fn test_parquet_scan_cfg(
        columns: Vec<String>,
        slot_types: Vec<types::TPrimitiveType>,
        iceberg_output_schema: Option<Schema>,
    ) -> ParquetScanConfig {
        let slot_ids = (0..columns.len())
            .map(|idx| SlotId::try_from((idx + 1) as i32).expect("slot id"))
            .collect::<Vec<_>>();
        let chunk_schema_schema = iceberg_output_schema.clone().unwrap_or_else(|| {
            let fields = columns
                .iter()
                .zip(slot_types.iter().copied())
                .map(|(name, primitive)| {
                    let data_type =
                        crate::lower::type_lowering::arrow_type_from_primitive(primitive)
                            .expect("arrow type");
                    Field::new(name.clone(), data_type, true)
                })
                .collect::<Vec<_>>();
            Schema::new(fields)
        });
        ParquetScanConfig {
            columns,
            chunk_schema: ChunkSchema::try_ref_from_schema_and_slot_ids(
                &chunk_schema_schema,
                &slot_ids,
            )
            .expect("chunk schema"),
            slot_kinds: slot_types
                .iter()
                .map(|primitive| {
                    if *primitive == types::TPrimitiveType::VARIANT {
                        ParquetSlotKind::Variant
                    } else {
                        ParquetSlotKind::Regular
                    }
                })
                .collect(),
            case_sensitive: true,
            enable_page_index: false,
            min_max_predicates: Vec::new(),
            runtime_min_max_filter_columns: HashMap::new(),
            variant_path_predicates: Vec::new(),
            batch_size: Some(1024),
            datacache: test_datacache_context(),
            cache_policy: ParquetReadCachePolicy::with_flags(false, false, None),
            profile_label: None,
            iceberg_output_schema: iceberg_output_schema.map(Arc::new),
            variant_path_columns: Vec::new(),
            query_global_dicts: Default::default(),
        }
    }

    fn test_scan_range() -> FileScanRange {
        FileScanRange {
            path: "memory.parquet".to_string(),
            file_len: 0,
            offset: 0,
            length: 0,
            scan_range_id: 0,
            first_row_id: None,
            data_sequence_number: None,
            ivm_change_op: None,
            included_positions: None,
            external_datacache: None,
            delete_files: Vec::new(),
            iceberg_file_pruning: None,
        }
    }

    fn test_scan_iter_for_predicates_with_runtime_filters(
        cfg: ParquetScanConfig,
        runtime_filters: Option<crate::exec::node::scan::RuntimeFilterContext>,
    ) -> ParquetScanIter {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let op =
            build_fs_operator(temp_dir.path().to_str().expect("temp dir path")).expect("operator");
        let factory = OpendalRangeReaderFactory::from_operator(op).expect("reader factory");
        ParquetScanIter::new(cfg, Vec::new(), factory, None, None, runtime_filters)
            .expect("scan iter")
    }

    #[test]
    fn page_min_max_conversion_keeps_discrete_set_envelope_for_a3a() {
        let predicate = ScanPredicate::discrete_set(
            "0".to_string(),
            vec![
                MinMaxPredicateValue::Int64(100),
                MinMaxPredicateValue::Int64(1),
            ],
            ScanPredicateSource::RuntimeIn,
        )
        .expect("discrete predicate");

        assert_eq!(
            scan_predicates_to_min_max_predicates(&[predicate]),
            vec![
                MinMaxPredicate::Ge {
                    column: "0".to_string(),
                    value: MinMaxPredicateValue::Int64(1),
                },
                MinMaxPredicate::Le {
                    column: "0".to_string(),
                    value: MinMaxPredicateValue::Int64(100),
                },
            ]
        );
    }

    #[test]
    fn page_min_max_conversion_drops_membership_without_fallback_for_a3a() {
        let predicate = ScanPredicate::new(
            "0".to_string(),
            ScanPredicateDomain::Membership(MembershipPredicate::BloomProbe {
                values: vec![MinMaxPredicateValue::Int64(1)],
            }),
            ScanPredicateSource::RuntimeMembership,
        );

        assert!(scan_predicates_to_min_max_predicates(&[predicate]).is_empty());
    }

    fn variant_row_group_metadata(stats: EnabledStatistics) -> ParquetMetaData {
        let leaf_values = Arc::new(Int64Array::from(vec![1, 2, 3, 10, 11, 12])) as ArrayRef;
        let typed_value_field = Arc::new(Field::new("typed_value", DataType::Int64, true));
        let typed_value_node = StructArray::try_new(
            vec![Arc::clone(&typed_value_field)].into(),
            vec![leaf_values],
            None,
        )
        .expect("typed value node");
        let object_field = Arc::new(Field::new("a", typed_value_node.data_type().clone(), true));
        let object_node = StructArray::try_new(
            vec![Arc::clone(&object_field)].into(),
            vec![Arc::new(typed_value_node) as ArrayRef],
            None,
        )
        .expect("object node");
        let root_typed_value_field = Arc::new(Field::new(
            "typed_value",
            object_node.data_type().clone(),
            true,
        ));
        let payload = StructArray::try_new(
            vec![Arc::clone(&root_typed_value_field)].into(),
            vec![Arc::new(object_node) as ArrayRef],
            None,
        )
        .expect("payload");
        let schema = Arc::new(Schema::new(vec![field_with_id(
            "payload_physical",
            payload.data_type().clone(),
            true,
            10,
        )]));
        let batch =
            arrow::record_batch::RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(payload)])
                .expect("batch");
        let props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(3))
            .set_statistics_enabled(stats)
            .build();

        let mut buffer = Vec::new();
        {
            let cursor = Cursor::new(&mut buffer);
            let mut writer =
                ArrowWriter::try_new(cursor, schema, Some(props)).expect("parquet writer");
            writer.write(&batch).expect("write parquet batch");
            writer.close().expect("close parquet writer");
        }

        let reader =
            SerializedFileReader::new(bytes::Bytes::from(buffer)).expect("metadata reader");
        reader.metadata().clone()
    }

    fn variant_path_spec(source_field_id: Option<i32>) -> VariantPathSpec {
        VariantPathSpec {
            source_slot_id: SlotId::new(3),
            source_read_slot_id: SlotId::new(3),
            output_slot_id: SlotId::new(2),
            source_field_id,
            source_name: "payload".to_string(),
            output_name: "__nr_var_payload_a".to_string(),
            source_field: Field::new("payload", DataType::LargeBinary, true),
            output_field: Field::new("__nr_var_payload_a", DataType::Int64, true),
            canonical_path: "$.a".to_string(),
            requested_type: DataType::Int64,
            strict: true,
        }
    }

    fn variant_path_predicate(source_field_id: Option<i32>) -> VariantPathPruningPredicate {
        VariantPathPruningPredicate {
            output_slot_id: SlotId::new(2),
            source_slot_id: SlotId::new(3),
            source_field_id,
            canonical_path: "$.a".to_string(),
            requested_type: DataType::Int64,
            predicate: MinMaxPredicate::Gt {
                column: "__nr_var_payload_a".to_string(),
                value: MinMaxPredicateValue::Int64(5),
            },
        }
    }

    fn variant_struct_from_json_rows(shredded: bool, rows: Vec<Option<&str>>) -> ArrayRef {
        let json: ArrayRef = Arc::new(StringArray::from(rows));
        let unshredded = json_to_variant(&json).expect("json_to_variant");
        let variant = if shredded {
            let shred_type = ShreddedSchemaBuilder::new()
                .with_path("a", &DataType::Int64)
                .expect("with_path")
                .build();
            shred_variant(&unshredded, &shred_type).expect("shred_variant")
        } else {
            unshredded
        };
        Arc::new(variant.into_inner()) as ArrayRef
    }

    fn write_variant_pruning_file(path: &Path, shredded: bool) -> ParquetMetaData {
        let payload = variant_struct_from_json_rows(
            shredded,
            vec![
                Some(r#"{"a": 1}"#),
                Some(r#"{"a": 2}"#),
                Some(r#"{"a": 3}"#),
                Some(r#"{"a": 10}"#),
                Some(r#"{"a": 11}"#),
                Some(r#"{"a": 12}"#),
            ],
        );
        let schema = Arc::new(Schema::new(vec![
            Field::new("payload", payload.data_type().clone(), true)
                .with_metadata(field_id_meta(10)),
        ]));
        let batch = arrow::record_batch::RecordBatch::try_new(Arc::clone(&schema), vec![payload])
            .expect("variant pruning batch");
        let props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(3))
            .set_statistics_enabled(EnabledStatistics::Chunk)
            .build();
        let file = File::create(path).expect("create variant pruning parquet");
        let mut writer =
            ArrowWriter::try_new(file, Arc::clone(&schema), Some(props)).expect("parquet writer");
        writer.write(&batch).expect("write variant pruning batch");
        writer.close().expect("close variant pruning writer");

        let file = File::open(path).expect("open variant pruning parquet");
        let reader = SerializedFileReader::new(file).expect("metadata reader");
        reader.metadata().clone()
    }

    fn variant_pruning_scan_cfg(enable_variant_pruning: bool) -> ParquetScanConfig {
        let chunk_schema = ChunkSchema::try_ref_from_schema_and_slot_ids(
            &Schema::new(vec![Field::new("payload", DataType::LargeBinary, true)]),
            &[SlotId::new(3)],
        )
        .expect("variant pruning chunk schema");
        ParquetScanConfig {
            columns: vec!["payload".to_string()],
            chunk_schema,
            slot_kinds: vec![ParquetSlotKind::Variant],
            case_sensitive: true,
            enable_page_index: false,
            min_max_predicates: Vec::new(),
            runtime_min_max_filter_columns: HashMap::new(),
            variant_path_predicates: if enable_variant_pruning {
                vec![variant_path_predicate(Some(10))]
            } else {
                Vec::new()
            },
            batch_size: Some(1024),
            datacache: test_datacache_context(),
            cache_policy: ParquetReadCachePolicy::with_flags(false, false, None),
            profile_label: None,
            iceberg_output_schema: None,
            variant_path_columns: vec![variant_path_spec(Some(10))],
            query_global_dicts: Default::default(),
        }
    }

    fn variant_a_values(batch: &arrow::record_batch::RecordBatch) -> Vec<Option<i64>> {
        let values = batch
            .column_by_name("__nr_var_payload_a")
            .expect("variant synthetic output")
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("variant synthetic int64 output");
        (0..values.len())
            .map(|idx| {
                if values.is_null(idx) {
                    None
                } else {
                    Some(values.value(idx))
                }
            })
            .collect()
    }

    fn residual_variant_a_gt_5_values(batch: &arrow::record_batch::RecordBatch) -> Vec<i64> {
        let predicates = vec![MinMaxPredicate::Gt {
            column: "1".to_string(),
            value: MinMaxPredicateValue::Int64(5),
        }];
        let projected_columns = vec!["payload".to_string(), "__nr_var_payload_a".to_string()];
        let (mask, has_effective_predicate) =
            evaluate_batch_predicate_mask(batch, &predicates, &projected_columns, true)
                .expect("evaluate residual predicate");
        assert!(has_effective_predicate);

        let values = batch
            .column_by_name("__nr_var_payload_a")
            .expect("variant synthetic output")
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("variant synthetic int64 output");
        mask.into_iter()
            .enumerate()
            .filter_map(|(idx, keep)| keep.then(|| values.value(idx)))
            .collect()
    }

    #[test]
    fn variant_row_group_pruning_wiring_binds_and_selects_typed_leaf() {
        let metadata = variant_row_group_metadata(EnabledStatistics::Chunk);
        let specs = vec![variant_path_spec(Some(10))];
        let predicates = vec![variant_path_predicate(Some(10))];

        let bound = bind_variant_path_pruning_predicates(&metadata, &specs, &predicates);
        let selected = select_row_groups_for_range(
            &metadata,
            &test_scan_range(),
            None,
            &[],
            &bound,
            &[],
            true,
        )
        .expect("row groups selected");

        assert_eq!(selected, vec![1]);
    }

    #[test]
    fn variant_pruning_selects_expected_row_groups_without_changing_results() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let file_path = temp_dir.path().join("variant_pruning.parquet");
        let metadata = write_variant_pruning_file(&file_path, true);
        let specs = vec![variant_path_spec(Some(10))];
        let predicates = vec![variant_path_predicate(Some(10))];

        assert_eq!(metadata.num_row_groups(), 2);
        let bound = bind_variant_path_pruning_predicates(&metadata, &specs, &predicates);
        let selected = select_row_groups_for_range(
            &metadata,
            &test_scan_range(),
            None,
            &[],
            &bound,
            &["payload".to_string()],
            true,
        )
        .expect("row groups selected");

        assert_eq!(bound.len(), 1);
        assert_eq!(selected, vec![1]);

        let full_batch = read_single_batch(variant_pruning_scan_cfg(false), &file_path);
        let pruned_batch = read_single_batch(variant_pruning_scan_cfg(true), &file_path);
        let full_filtered = residual_variant_a_gt_5_values(&full_batch);
        let pruned_filtered = residual_variant_a_gt_5_values(&pruned_batch);

        assert_eq!(full_filtered, vec![10, 11, 12]);
        assert_eq!(pruned_filtered, full_filtered);
    }

    #[test]
    fn variant_row_group_pruning_wiring_reads_all_when_binding_fails() {
        let metadata = variant_row_group_metadata(EnabledStatistics::Chunk);
        let specs = vec![variant_path_spec(Some(10))];
        let predicates = vec![variant_path_predicate(Some(11))];

        let bound = bind_variant_path_pruning_predicates(&metadata, &specs, &predicates);
        let selected = select_row_groups_for_range(
            &metadata,
            &test_scan_range(),
            Some(usize::MAX),
            &[],
            &bound,
            &[],
            true,
        )
        .expect("row groups selected");

        assert!(bound.is_empty());
        assert_eq!(selected, vec![0, 1]);
    }

    #[test]
    fn variant_pruning_degrades_to_read_all_for_unshredded_or_wrong_type_file() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let file_path = temp_dir.path().join("variant_unshredded.parquet");
        let metadata = write_variant_pruning_file(&file_path, false);
        let specs = vec![variant_path_spec(Some(10))];
        let predicates = vec![variant_path_predicate(Some(10))];
        let all_row_groups = (0..metadata.num_row_groups()).collect::<Vec<_>>();

        assert_eq!(all_row_groups, vec![0, 1]);
        let bound = bind_variant_path_pruning_predicates(&metadata, &specs, &predicates);
        let selected = select_row_groups_for_range(
            &metadata,
            &test_scan_range(),
            Some(usize::MAX),
            &[],
            &bound,
            &["payload".to_string()],
            true,
        )
        .expect("row groups selected");
        let page_selection = build_row_selection_for_row_groups(
            &metadata,
            &all_row_groups,
            &[],
            &bound,
            &["payload".to_string()],
            true,
        );

        assert!(bound.is_empty());
        assert_eq!(selected, all_row_groups);
        assert!(page_selection.selection.is_none());
        assert_eq!(page_selection.pages_pruned, 0);
        assert_eq!(page_selection.rows_selected, page_selection.rows_total);

        let full_batch = read_single_batch(variant_pruning_scan_cfg(false), &file_path);
        let degraded_batch = read_single_batch(variant_pruning_scan_cfg(true), &file_path);
        let full_values = variant_a_values(&full_batch);
        let degraded_values = variant_a_values(&degraded_batch);

        assert_eq!(full_batch.num_rows(), 6);
        assert_eq!(degraded_batch.num_rows(), full_batch.num_rows());
        assert_eq!(
            full_values,
            vec![Some(1), Some(2), Some(3), Some(10), Some(11), Some(12)]
        );
        assert_eq!(degraded_values, full_values);
    }

    #[test]
    fn current_pruning_predicates_schema_evolution_preserves_variant_only() {
        let chunk_schema = ChunkSchema::try_ref_from_schema_and_slot_ids(
            &Schema::new(vec![
                Field::new("id", DataType::Int32, true),
                Field::new("__nr_var_payload_a", DataType::Int64, true),
                Field::new("payload", DataType::LargeBinary, true),
            ]),
            &[SlotId::new(1), SlotId::new(2), SlotId::new(3)],
        )
        .expect("chunk schema");
        let mut cfg = ParquetScanConfig {
            columns: vec!["id".to_string(), "payload".to_string()],
            chunk_schema,
            slot_kinds: vec![
                ParquetSlotKind::Regular,
                ParquetSlotKind::Regular,
                ParquetSlotKind::Variant,
            ],
            case_sensitive: true,
            enable_page_index: false,
            min_max_predicates: Vec::new(),
            runtime_min_max_filter_columns: HashMap::new(),
            variant_path_predicates: Vec::new(),
            batch_size: Some(1024),
            datacache: test_datacache_context(),
            cache_policy: ParquetReadCachePolicy::with_flags(false, false, None),
            profile_label: None,
            iceberg_output_schema: Some(Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int32, true),
                Field::new("payload", DataType::LargeBinary, true),
            ]))),
            variant_path_columns: Vec::new(),
            query_global_dicts: Default::default(),
        };
        cfg.min_max_predicates.push(MinMaxPredicate::Gt {
            column: "0".to_string(),
            value: MinMaxPredicateValue::Int32(5),
        });
        cfg.variant_path_predicates
            .push(variant_path_predicate(Some(10)));
        let specs = [crate::exec::node::join::JoinRuntimeFilterSpec {
            filter_id: 1,
            expr_order: 0,
            probe_slot_id: SlotId::new(1),
            build_data_type: DataType::Int32,
            merge_nodes: Vec::new(),
            has_remote_targets: false,
        }];
        let key_arrays: Vec<ArrayRef> = vec![Arc::new(Int32Array::from(vec![10, 20]))];
        let mut local_filters =
            crate::exec::runtime_filter::LocalRuntimeInFilterSet::new(&specs, &key_arrays)
                .expect("local runtime filters");
        local_filters
            .add_build_arrays(&key_arrays)
            .expect("runtime filter values");
        let runtime_filters = crate::exec::node::scan::RuntimeFilterContext::new(
            local_filters.into_filters(),
            Vec::new(),
        );
        let iter = test_scan_iter_for_predicates_with_runtime_filters(cfg, Some(runtime_filters));

        let predicates = iter
            .current_pruning_predicates()
            .expect("current predicates");

        assert!(predicates.physical.is_empty());
        assert_eq!(predicates.variant, vec![variant_path_predicate(Some(10))]);
    }

    #[test]
    fn current_pruning_predicates_keep_variant_config_and_physical_runtime_filters_separate() {
        let chunk_schema = ChunkSchema::try_ref_from_schema_and_slot_ids(
            &Schema::new(vec![
                Field::new("id", DataType::Int32, true),
                Field::new("payload", DataType::LargeBinary, true),
            ]),
            &[SlotId::new(1), SlotId::new(3)],
        )
        .expect("chunk schema");
        let cfg = ParquetScanConfig {
            columns: vec!["id".to_string(), "payload".to_string()],
            chunk_schema,
            slot_kinds: vec![ParquetSlotKind::Regular, ParquetSlotKind::Variant],
            case_sensitive: true,
            enable_page_index: false,
            min_max_predicates: vec![MinMaxPredicate::Gt {
                column: "0".to_string(),
                value: MinMaxPredicateValue::Int32(5),
            }],
            runtime_min_max_filter_columns: HashMap::new(),
            variant_path_predicates: vec![variant_path_predicate(Some(10))],
            batch_size: Some(1024),
            datacache: test_datacache_context(),
            cache_policy: ParquetReadCachePolicy::with_flags(false, false, None),
            profile_label: None,
            iceberg_output_schema: None,
            variant_path_columns: vec![variant_path_spec(Some(10))],
            query_global_dicts: Default::default(),
        };
        let specs = [
            crate::exec::node::join::JoinRuntimeFilterSpec {
                filter_id: 1,
                expr_order: 0,
                probe_slot_id: SlotId::new(1),
                build_data_type: DataType::Int32,
                merge_nodes: Vec::new(),
                has_remote_targets: false,
            },
            crate::exec::node::join::JoinRuntimeFilterSpec {
                filter_id: 2,
                expr_order: 1,
                probe_slot_id: SlotId::new(2),
                build_data_type: DataType::Int64,
                merge_nodes: Vec::new(),
                has_remote_targets: false,
            },
        ];
        let key_arrays: Vec<ArrayRef> = vec![
            Arc::new(Int32Array::from(vec![10, 20])),
            Arc::new(Int64Array::from(vec![100, 200])),
        ];
        let mut local_filters =
            crate::exec::runtime_filter::LocalRuntimeInFilterSet::new(&specs, &key_arrays)
                .expect("local runtime filters");
        local_filters
            .add_build_arrays(&key_arrays)
            .expect("runtime filter values");
        let runtime_filters = crate::exec::node::scan::RuntimeFilterContext::new(
            local_filters.into_filters(),
            Vec::new(),
        );
        let iter = test_scan_iter_for_predicates_with_runtime_filters(cfg, Some(runtime_filters));

        let predicates = iter
            .current_pruning_predicates()
            .expect("current predicates");

        assert_eq!(
            predicates.physical,
            vec![
                ScanPredicate::from_min_max_predicate(
                    MinMaxPredicate::Gt {
                        column: "0".to_string(),
                        value: MinMaxPredicateValue::Int32(5),
                    },
                    ScanPredicateSource::Static,
                ),
                ScanPredicate::discrete_set(
                    "0".to_string(),
                    vec![
                        MinMaxPredicateValue::Int32(10),
                        MinMaxPredicateValue::Int32(20),
                    ],
                    ScanPredicateSource::RuntimeIn,
                )
                .expect("runtime in scan predicate"),
            ]
        );
        assert_eq!(predicates.variant, vec![variant_path_predicate(Some(10))]);
        assert!(
            !predicates
                .physical
                .iter()
                .any(|predicate| predicate.column() == "1")
        );
    }

    #[test]
    fn runtime_filters_skip_variant_synthetic_slots_but_keep_physical_slots() {
        let chunk_schema = ChunkSchema::try_ref_from_schema_and_slot_ids(
            &Schema::new(vec![
                Field::new("id", DataType::Int32, true),
                Field::new("__nr_var_payload_a", DataType::Int64, true),
                Field::new("payload", DataType::LargeBinary, true),
            ]),
            &[SlotId::new(1), SlotId::new(2), SlotId::new(3)],
        )
        .expect("chunk schema");
        let cfg = ParquetScanConfig {
            columns: vec!["id".to_string(), "payload".to_string()],
            chunk_schema,
            slot_kinds: vec![
                ParquetSlotKind::Regular,
                ParquetSlotKind::Regular,
                ParquetSlotKind::Variant,
            ],
            case_sensitive: true,
            enable_page_index: false,
            min_max_predicates: Vec::new(),
            runtime_min_max_filter_columns: HashMap::new(),
            variant_path_predicates: Vec::new(),
            batch_size: Some(1024),
            datacache: test_datacache_context(),
            cache_policy: ParquetReadCachePolicy::with_flags(false, false, None),
            profile_label: None,
            iceberg_output_schema: None,
            variant_path_columns: vec![VariantPathSpec {
                source_slot_id: SlotId::new(3),
                source_read_slot_id: SlotId::new(3),
                output_slot_id: SlotId::new(2),
                source_field_id: None,
                source_name: "payload".to_string(),
                output_name: "__nr_var_payload_a".to_string(),
                source_field: Field::new("payload", DataType::LargeBinary, true),
                output_field: Field::new("__nr_var_payload_a", DataType::Int64, true),
                canonical_path: "$.a".to_string(),
                requested_type: DataType::Int64,
                strict: true,
            }],
            query_global_dicts: Default::default(),
        };

        let specs = [
            crate::exec::node::join::JoinRuntimeFilterSpec {
                filter_id: 1,
                expr_order: 0,
                probe_slot_id: SlotId::new(1),
                build_data_type: DataType::Int32,
                merge_nodes: Vec::new(),
                has_remote_targets: false,
            },
            crate::exec::node::join::JoinRuntimeFilterSpec {
                filter_id: 2,
                expr_order: 1,
                probe_slot_id: SlotId::new(2),
                build_data_type: DataType::Int64,
                merge_nodes: Vec::new(),
                has_remote_targets: false,
            },
        ];
        let key_arrays: Vec<ArrayRef> = vec![
            Arc::new(Int32Array::from(vec![10, 20])),
            Arc::new(Int64Array::from(vec![100, 200])),
        ];
        let mut local_filters =
            crate::exec::runtime_filter::LocalRuntimeInFilterSet::new(&specs, &key_arrays)
                .expect("local runtime filters");
        local_filters
            .add_build_arrays(&key_arrays)
            .expect("runtime filter values");
        let runtime_filters = crate::exec::node::scan::RuntimeFilterContext::new(
            local_filters.into_filters(),
            Vec::new(),
        );

        let predicates =
            runtime_filters_to_scan_predicates(&cfg, &runtime_filters).expect("predicates");

        assert_eq!(
            predicates,
            vec![
                ScanPredicate::discrete_set(
                    "0".to_string(),
                    vec![
                        MinMaxPredicateValue::Int32(10),
                        MinMaxPredicateValue::Int32(20),
                    ],
                    ScanPredicateSource::RuntimeIn,
                )
                .expect("runtime in scan predicate")
            ]
        );
    }

    #[test]
    fn parquet_scan_iter_encodes_dict_columns_utf8_to_int32() {
        use crate::exec::chunk::{ChunkSchema, ChunkSlotSchema};
        use crate::exec::dict_encode::QueryGlobalDictEncodeMap;
        use arrow::array::{Int32Array, StringArray};
        use arrow::datatypes::{DataType, Field};
        use arrow::record_batch::RecordBatch;
        use std::collections::HashMap;

        let chunk_schema = Arc::new(
            ChunkSchema::try_new(vec![ChunkSlotSchema::new_with_field(
                SlotId::new(7),
                Field::new("s", DataType::Int32, true),
                None,
                None,
            )])
            .expect("chunk schema"),
        );
        let mut dict_values = HashMap::new();
        dict_values.insert(b"a".to_vec(), 11);
        let mut dicts = QueryGlobalDictEncodeMap::new();
        dicts.insert(SlotId::new(7), Arc::new(dict_values));

        // 1) scan-read schema must rewrite the Int32 dict slot to Utf8:
        let out_arrow = chunk_schema.arrow_schema_ref();
        let (scan_arrow, changed) =
            crate::exec::dict_encode::build_scan_schema_for_global_dict_encoding(
                &out_arrow,
                &chunk_schema,
                &dicts,
            )
            .expect("scan schema");
        assert!(changed);
        assert_eq!(scan_arrow.field(0).data_type(), &DataType::Utf8);

        // 2) encode Utf8 -> Int32 ids:
        let scan_batch = RecordBatch::try_new(
            scan_arrow,
            vec![Arc::new(StringArray::from(vec![Some("a"), None]))],
        )
        .expect("scan batch");
        let encoded = crate::exec::dict_encode::encode_batch_with_query_global_dicts(
            scan_batch,
            &out_arrow,
            &chunk_schema,
            &dicts,
        )
        .expect("encode");
        let ids = encoded
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("int32");
        assert_eq!(ids.value(0), 11);
        assert!(ids.is_null(1));
    }

    #[test]
    fn parquet_scan_iter_rejects_missing_query_global_dict_value() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let file_path = temp_dir.path().join("dict_status_miss.parquet");
        write_status_parquet(&file_path, false);

        let mut cfg = test_parquet_scan_cfg(
            vec!["status".to_string()],
            vec![types::TPrimitiveType::INT],
            None,
        );
        let mut dict_values = HashMap::new();
        dict_values.insert(b"NEW".to_vec(), 11);
        cfg.query_global_dicts
            .insert(SlotId::new(1), Arc::new(dict_values));

        let file_len = fs::metadata(&file_path).expect("file metadata").len();
        let scan = FileScanContext::build(
            vec![FileScanRange {
                path: file_path.to_string_lossy().to_string(),
                file_len,
                offset: 0,
                length: file_len,
                scan_range_id: -1,
                first_row_id: None,
                data_sequence_number: None,
                ivm_change_op: None,
                included_positions: None,
                external_datacache: None,
                delete_files: Vec::new(),
                iceberg_file_pruning: None,
            }],
            None,
            None,
        )
        .expect("file scan context");
        let mut iter = build_parquet_iter(scan, cfg, None, None, None).expect("build parquet iter");
        let err = iter.next().expect("first batch").unwrap_err();
        assert!(
            err.contains("value not found in query global dict")
                && err.contains("slot_id=1")
                && err.contains("output_column=status"),
            "{err}"
        );
    }

    fn read_single_batch(cfg: ParquetScanConfig, path: &Path) -> arrow::record_batch::RecordBatch {
        let file_len = fs::metadata(path).expect("file metadata").len();
        let scan = FileScanContext::build(
            vec![FileScanRange {
                path: path.to_string_lossy().to_string(),
                file_len,
                offset: 0,
                length: file_len,
                scan_range_id: -1,
                first_row_id: None,
                data_sequence_number: None,
                ivm_change_op: None,
                included_positions: None,
                external_datacache: None,
                delete_files: Vec::new(),
                iceberg_file_pruning: None,
            }],
            None,
            None,
        )
        .expect("file scan context");
        let mut iter = build_parquet_iter(scan, cfg, None, None, None).expect("build parquet iter");
        iter.next()
            .expect("first batch")
            .expect("decode batch")
            .batch
    }

    fn write_status_parquet(path: &Path, dictionary_enabled: bool) {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "status",
            DataType::Utf8,
            true,
        )]));
        let batch = arrow::record_batch::RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(StringArray::from(vec![
                Some("NEW"),
                Some("PAID"),
                Some("NEW"),
                None,
                Some("PAID"),
            ]))],
        )
        .expect("record batch");
        let props = WriterProperties::builder()
            .set_dictionary_enabled(dictionary_enabled)
            .build();
        let file = File::create(path).expect("create parquet file");
        let mut writer =
            ArrowWriter::try_new(file, Arc::clone(&schema), Some(props)).expect("parquet writer");
        writer.write(&batch).expect("write batch");
        writer.close().expect("close writer");
    }

    #[test]
    fn parquet_reader_preserves_requested_dictionary_string_column() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let file_path = temp_dir.path().join("dict_status.parquet");
        write_status_parquet(&file_path, true);

        let dict_schema = Arc::new(Schema::new(vec![Field::new(
            "status",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            true,
        )]));
        let opts = ArrowReaderOptions::new().with_schema(dict_schema);
        let file = File::open(&file_path).expect("open parquet");
        let mut reader = ParquetRecordBatchReaderBuilder::try_new_with_options(file, opts)
            .expect("parquet builder")
            .build()
            .expect("reader");
        let batch = reader.next().expect("first batch").expect("decode batch");

        assert!(matches!(
            batch.column(0).data_type(),
            DataType::Dictionary(key, value)
                if key.as_ref() == &DataType::Int32 && value.as_ref() == &DataType::Utf8
        ));
        let dict = batch
            .column(0)
            .as_any()
            .downcast_ref::<DictionaryArray<Int32Type>>()
            .expect("dictionary array");
        assert_eq!(dict.keys().len(), 5);
    }

    #[test]
    fn parquet_metadata_marks_dictionary_encoded_string_column() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let file_path = temp_dir.path().join("dict_status.parquet");
        write_status_parquet(&file_path, true);

        let file = File::open(&file_path).expect("open parquet");
        let reader = SerializedFileReader::new(file).expect("metadata reader");
        let encodings = reader
            .metadata()
            .row_group(0)
            .column(0)
            .encodings()
            .collect::<Vec<_>>();

        assert!(
            encodings.contains(&Encoding::RLE_DICTIONARY)
                || encodings.contains(&Encoding::PLAIN_DICTIONARY),
            "expected dictionary encoding, got {encodings:?}"
        );
    }

    #[test]
    fn scan_preserves_dictionary_string_carrier_for_encoded_parquet() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let file_path = temp_dir.path().join("dict_status.parquet");
        write_status_parquet(&file_path, true);

        let batch = read_single_batch(
            test_parquet_scan_cfg(
                vec!["status".to_string()],
                vec![types::TPrimitiveType::VARCHAR],
                None,
            ),
            &file_path,
        );

        assert!(matches!(
            batch.column(0).data_type(),
            DataType::Dictionary(key, value)
                if key.as_ref() == &DataType::Int32 && value.as_ref() == &DataType::Utf8
        ));
        let flat = arrow::compute::cast(batch.column(0).as_ref(), &DataType::Utf8)
            .expect("cast dictionary to utf8");
        let values = flat
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("utf8 values");
        assert_eq!(values.value(0), "NEW");
        assert_eq!(values.value(1), "PAID");
        assert!(values.is_null(3));
    }

    #[test]
    fn scan_preserves_dictionary_string_carrier_with_iceberg_output_schema() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let file_path = temp_dir.path().join("dict_status.parquet");
        write_status_parquet(&file_path, true);

        let batch = read_single_batch(
            test_parquet_scan_cfg(
                vec!["status".to_string()],
                vec![types::TPrimitiveType::VARCHAR],
                Some(Schema::new(vec![field_with_id(
                    "status",
                    DataType::Utf8,
                    true,
                    1,
                )])),
            ),
            &file_path,
        );

        assert!(matches!(
            batch.column(0).data_type(),
            DataType::Dictionary(key, value)
                if key.as_ref() == &DataType::Int32 && value.as_ref() == &DataType::Utf8
        ));
    }

    #[test]
    fn scan_keeps_plain_string_flat_without_dictionary_encoding() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let file_path = temp_dir.path().join("plain_status.parquet");
        write_status_parquet(&file_path, false);

        let batch = read_single_batch(
            test_parquet_scan_cfg(
                vec!["status".to_string()],
                vec![types::TPrimitiveType::VARCHAR],
                None,
            ),
            &file_path,
        );

        assert_eq!(batch.column(0).data_type(), &DataType::Utf8);
        let values = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("utf8 values");
        assert_eq!(values.value(0), "NEW");
        assert!(values.is_null(3));
    }

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
    fn iceberg_schema_evolution_reads_renamed_columns_by_field_id() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let file_path = temp_dir.path().join("rename.parquet");
        let source_schema = Arc::new(Schema::new(vec![
            field_with_id("old_id", DataType::Int32, true, 1),
            field_with_id("payload", DataType::Int32, true, 2),
        ]));
        let source_batch = arrow::record_batch::RecordBatch::try_new(
            Arc::clone(&source_schema),
            vec![
                Arc::new(Int32Array::from(vec![Some(1), Some(2), Some(3)])),
                Arc::new(Int32Array::from(vec![Some(10), Some(20), Some(30)])),
            ],
        )
        .expect("source batch");
        let file = File::create(&file_path).expect("create parquet");
        let mut writer =
            ArrowWriter::try_new(file, Arc::clone(&source_schema), None).expect("writer");
        writer.write(&source_batch).expect("write batch");
        writer.close().expect("close writer");

        let target_schema = Schema::new(vec![
            field_with_id("new_id", DataType::Int32, true, 1),
            field_with_id("payload", DataType::Int32, true, 2),
        ]);
        let batch = read_single_batch(
            test_parquet_scan_cfg(
                vec!["new_id".to_string(), "payload".to_string()],
                vec![types::TPrimitiveType::INT, types::TPrimitiveType::INT],
                Some(target_schema),
            ),
            &file_path,
        );

        assert_eq!(batch.schema().field(0).name(), "new_id");
        let values = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("new_id int32");
        assert_eq!(values.value(0), 1);
        assert_eq!(values.value(2), 3);
    }

    #[test]
    fn iceberg_schema_evolution_supports_add_drop_and_reorder() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let file_path = temp_dir.path().join("add_drop_reorder.parquet");
        let source_schema = Arc::new(Schema::new(vec![
            field_with_id("id", DataType::Int32, true, 1),
            field_with_id("value", DataType::Int32, true, 2),
            field_with_id("removed", DataType::Int32, true, 3),
        ]));
        let source_batch = arrow::record_batch::RecordBatch::try_new(
            Arc::clone(&source_schema),
            vec![
                Arc::new(Int32Array::from(vec![Some(7), Some(8)])),
                Arc::new(Int32Array::from(vec![Some(70), Some(80)])),
                Arc::new(Int32Array::from(vec![Some(700), Some(800)])),
            ],
        )
        .expect("source batch");
        let file = File::create(&file_path).expect("create parquet");
        let mut writer =
            ArrowWriter::try_new(file, Arc::clone(&source_schema), None).expect("writer");
        writer.write(&source_batch).expect("write batch");
        writer.close().expect("close writer");

        let target_schema = Schema::new(vec![
            field_with_id("value", DataType::Int32, true, 2),
            field_with_id("id", DataType::Int32, true, 1),
            field_with_id("extra", DataType::Int32, true, 4),
        ]);
        let batch = read_single_batch(
            test_parquet_scan_cfg(
                vec!["value".to_string(), "id".to_string(), "extra".to_string()],
                vec![
                    types::TPrimitiveType::INT,
                    types::TPrimitiveType::INT,
                    types::TPrimitiveType::INT,
                ],
                Some(target_schema),
            ),
            &file_path,
        );

        let value = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("value int32");
        let id = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("id int32");
        let extra = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("extra int32");
        assert_eq!(value.value(0), 70);
        assert_eq!(id.value(1), 8);
        assert!(extra.is_null(0));
        assert!(extra.is_null(1));
    }

    #[test]
    fn iceberg_schema_evolution_readded_same_name_uses_new_field_id() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let file_path = temp_dir.path().join("readd_same_name.parquet");
        let source_schema = Arc::new(Schema::new(vec![
            field_with_id("id", DataType::Int32, true, 1),
            field_with_id("note_text", DataType::Utf8, true, 3),
        ]));
        let source_batch = arrow::record_batch::RecordBatch::try_new(
            Arc::clone(&source_schema),
            vec![
                Arc::new(Int32Array::from(vec![Some(1), Some(2)])),
                Arc::new(StringArray::from(vec![Some("old"), Some("dropped")])),
            ],
        )
        .expect("source batch");
        let file = File::create(&file_path).expect("create parquet");
        let mut writer =
            ArrowWriter::try_new(file, Arc::clone(&source_schema), None).expect("writer");
        writer.write(&source_batch).expect("write batch");
        writer.close().expect("close writer");

        let target_schema = Schema::new(vec![
            field_with_id("id", DataType::Int32, true, 1),
            field_with_id("note_text", DataType::Utf8, true, 4),
        ]);
        let batch = read_single_batch(
            test_parquet_scan_cfg(
                vec!["id".to_string(), "note_text".to_string()],
                vec![types::TPrimitiveType::INT, types::TPrimitiveType::VARCHAR],
                Some(target_schema),
            ),
            &file_path,
        );

        let note_text = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("note_text string");
        assert!(note_text.is_null(0));
        assert!(note_text.is_null(1));
    }

    #[test]
    fn iceberg_schema_evolution_falls_back_to_name_matching_without_field_ids() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let file_path = temp_dir.path().join("no_field_id.parquet");
        let source_schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Int32, true),
        ]));
        let source_batch = arrow::record_batch::RecordBatch::try_new(
            Arc::clone(&source_schema),
            vec![
                Arc::new(Int32Array::from(vec![Some(1), Some(2)])),
                Arc::new(Int32Array::from(vec![Some(10), Some(20)])),
            ],
        )
        .expect("source batch");
        let file = File::create(&file_path).expect("create parquet");
        let mut writer =
            ArrowWriter::try_new(file, Arc::clone(&source_schema), None).expect("writer");
        writer.write(&source_batch).expect("write batch");
        writer.close().expect("close writer");

        let target_schema = Schema::new(vec![
            Field::new("b", DataType::Int32, true),
            Field::new("a", DataType::Int32, true),
        ]);
        let batch = read_single_batch(
            test_parquet_scan_cfg(
                vec!["b".to_string(), "a".to_string()],
                vec![types::TPrimitiveType::INT, types::TPrimitiveType::INT],
                Some(target_schema),
            ),
            &file_path,
        );
        let b = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("b int32");
        let a = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("a int32");
        assert_eq!(b.value(0), 10);
        assert_eq!(a.value(1), 2);
    }

    #[test]
    fn iceberg_schema_evolution_aligns_struct_children_by_field_id() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let file_path = temp_dir.path().join("struct_evolution.parquet");
        let source_children = vec![
            Arc::new(field_with_id("a", DataType::Int32, true, 2)),
            Arc::new(field_with_id("b", DataType::Int32, true, 3)),
        ];
        let source_struct = StructArray::try_new(
            source_children.clone().into(),
            vec![
                Arc::new(Int32Array::from(vec![Some(1), Some(2)])),
                Arc::new(Int32Array::from(vec![Some(10), Some(20)])),
            ],
            None,
        )
        .expect("source struct");
        let source_schema = Arc::new(Schema::new(vec![field_with_id(
            "payload",
            DataType::Struct(source_children.into()),
            true,
            1,
        )]));
        let source_batch = arrow::record_batch::RecordBatch::try_new(
            Arc::clone(&source_schema),
            vec![Arc::new(source_struct)],
        )
        .expect("source batch");
        let file = File::create(&file_path).expect("create parquet");
        let mut writer =
            ArrowWriter::try_new(file, Arc::clone(&source_schema), None).expect("writer");
        writer.write(&source_batch).expect("write batch");
        writer.close().expect("close writer");

        let target_children = vec![
            Arc::new(field_with_id("b", DataType::Int32, true, 3)),
            Arc::new(field_with_id("a", DataType::Int32, true, 2)),
            Arc::new(field_with_id("c", DataType::Int32, true, 4)),
        ];
        let target_schema = Schema::new(vec![field_with_id(
            "payload",
            DataType::Struct(target_children.into()),
            true,
            1,
        )]);
        let batch = read_single_batch(
            test_parquet_scan_cfg(
                vec!["payload".to_string()],
                vec![types::TPrimitiveType::INVALID_TYPE],
                Some(target_schema),
            ),
            &file_path,
        );

        let payload = batch
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("payload struct");
        let b = payload
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("b int32");
        let a = payload
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("a int32");
        let c = payload
            .column(2)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("c int32");
        assert_eq!(b.value(0), 10);
        assert_eq!(a.value(1), 2);
        assert!(c.is_null(0));
        assert!(c.is_null(1));
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

    #[test]
    fn iceberg_schema_evolution_fills_missing_column_with_initial_default() {
        // Build a parquet file with only column `a`, then read with an output
        // schema that includes `b` carrying ICEBERG_INITIAL_DEFAULT_META_KEY=99.
        // Expect b column to be filled with 99 instead of NULL.
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let file_path = temp_dir.path().join("initial_default.parquet");

        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, true).with_metadata(
                std::iter::once((PARQUET_FIELD_ID_META_KEY.to_string(), "1".to_string())).collect(),
            ),
        ]));
        let batch = arrow::record_batch::RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![10, 20])) as _],
        )
        .unwrap();
        let file = File::create(&file_path).expect("create parquet");
        let mut w = ArrowWriter::try_new(file, schema, None).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();

        // Output schema includes `b` with initial-default JSON metadata.
        let mut b_meta = HashMap::new();
        b_meta.insert(PARQUET_FIELD_ID_META_KEY.to_string(), "2".to_string());
        b_meta.insert(
            crate::connector::iceberg::schema::ICEBERG_INITIAL_DEFAULT_META_KEY.to_string(),
            "99".to_string(),
        );
        let out_schema = Schema::new(vec![
            Field::new("a", DataType::Int32, true).with_metadata(
                std::iter::once((PARQUET_FIELD_ID_META_KEY.to_string(), "1".to_string())).collect(),
            ),
            Field::new("b", DataType::Int32, true).with_metadata(b_meta),
        ]);

        let result = read_single_batch(
            test_parquet_scan_cfg(
                vec!["a".to_string(), "b".to_string()],
                vec![types::TPrimitiveType::INT, types::TPrimitiveType::INT],
                Some(out_schema),
            ),
            &file_path,
        );

        let b = result
            .column_by_name("b")
            .expect("b column")
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("Int32Array");
        assert_eq!(b.value(0), 99);
        assert_eq!(b.value(1), 99);
    }

    #[test]
    fn nanosecond_timestamp_maps_to_iceberg_timestamp_ns() {
        use arrow::datatypes::{DataType, TimeUnit};
        use iceberg::spec::{PrimitiveType, Type};
        let t = super::arrow_type_to_iceberg_type(&DataType::Timestamp(TimeUnit::Nanosecond, None))
            .unwrap();
        assert!(matches!(t, Type::Primitive(PrimitiveType::TimestampNs)));
    }

    // --- RF empty-range short-circuit guard tests (RF milestone M4) ---
    //
    // These tests characterize an already-working production behavior: when a
    // runtime filter's min/max range is disjoint from every row group's
    // statistics, `open_next_reader` prunes all row groups for a range and
    // `continue`s *before* building any column-chunk reader (see the
    // `row_groups.is_empty()` short-circuits in this file). They exist to
    // guard that behavior against regression, not to introduce it.

    const RUNTIME_FILTER_ROW_GROUP_ROWS: i32 = 100;
    const RUNTIME_FILTER_ROW_GROUP_COUNT: i32 = 10;

    /// Writes a single-column (`id: Int32`) parquet file containing
    /// `RUNTIME_FILTER_ROW_GROUP_COUNT` row groups of
    /// `RUNTIME_FILTER_ROW_GROUP_ROWS` rows each, with values `0..(count*rows)`
    /// in ascending order so each row group covers a disjoint, known key
    /// sub-range. Per-row-group statistics are enabled so min/max pruning can
    /// take effect.
    fn write_runtime_filter_row_group_fixture(path: &Path) -> ParquetMetaData {
        let total_rows = (RUNTIME_FILTER_ROW_GROUP_ROWS * RUNTIME_FILTER_ROW_GROUP_COUNT) as usize;
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false).with_metadata(field_id_meta(1)),
        ]));
        let ids: Vec<i32> = (0..total_rows as i32).collect();
        let batch = arrow::record_batch::RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(ids)) as ArrayRef],
        )
        .expect("runtime filter fixture batch");
        let props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(RUNTIME_FILTER_ROW_GROUP_ROWS as usize))
            .set_statistics_enabled(EnabledStatistics::Chunk)
            .build();
        let file = File::create(path).expect("create runtime filter fixture parquet");
        let mut writer =
            ArrowWriter::try_new(file, Arc::clone(&schema), Some(props)).expect("parquet writer");
        writer.write(&batch).expect("write runtime filter fixture");
        writer.close().expect("close runtime filter fixture writer");

        let file = File::open(path).expect("open runtime filter fixture parquet");
        let reader = SerializedFileReader::new(file).expect("metadata reader");
        reader.metadata().clone()
    }

    fn runtime_filter_row_group_scan_cfg() -> ParquetScanConfig {
        let chunk_schema = ChunkSchema::try_ref_from_schema_and_slot_ids(
            &Schema::new(vec![Field::new("id", DataType::Int32, false)]),
            &[SlotId::new(1)],
        )
        .expect("runtime filter fixture chunk schema");
        ParquetScanConfig {
            columns: vec!["id".to_string()],
            chunk_schema,
            slot_kinds: vec![ParquetSlotKind::Regular],
            case_sensitive: true,
            enable_page_index: false,
            min_max_predicates: Vec::new(),
            runtime_min_max_filter_columns: HashMap::new(),
            variant_path_predicates: Vec::new(),
            batch_size: Some(1024),
            datacache: test_datacache_context(),
            cache_policy: ParquetReadCachePolicy::with_flags(false, false, None),
            profile_label: None,
            iceberg_output_schema: None,
            variant_path_columns: Vec::new(),
            query_global_dicts: Default::default(),
        }
    }

    /// Builds an in-filter `RuntimeFilterContext` (round-tripped through
    /// `RuntimeFilterSnapshot`/`RuntimeFilterContext::from_snapshot`, matching
    /// how the scan-runner path reconstructs a context from a hub snapshot)
    /// whose min/max range is derived from `key_values` and bound to slot 1
    /// (the `id` column).
    fn runtime_filter_ctx_for_keys(
        key_values: Vec<i32>,
    ) -> crate::exec::node::scan::RuntimeFilterContext {
        let specs = [crate::exec::node::join::JoinRuntimeFilterSpec {
            filter_id: 1,
            expr_order: 0,
            probe_slot_id: SlotId::new(1),
            build_data_type: DataType::Int32,
            merge_nodes: Vec::new(),
            has_remote_targets: false,
        }];
        let key_arrays: Vec<ArrayRef> = vec![Arc::new(Int32Array::from(key_values))];
        let mut local_filters =
            crate::exec::runtime_filter::LocalRuntimeInFilterSet::new(&specs, &key_arrays)
                .expect("local runtime in-filter");
        local_filters
            .add_build_arrays(&key_arrays)
            .expect("runtime in-filter build values");
        let snapshot = crate::runtime::runtime_filter_hub::RuntimeFilterSnapshot::from_filters(
            local_filters.into_filters(),
            Vec::new(),
        );
        crate::exec::node::scan::RuntimeFilterContext::from_snapshot(snapshot)
    }

    fn runtime_min_max_filter_ctx_for_i32(
        filter_id: i32,
        min: i32,
        max: i32,
    ) -> crate::exec::node::scan::RuntimeFilterContext {
        use crate::exec::runtime_filter::{
            RuntimeFilterType, RuntimeMinMaxFilter, min_max::MinMaxValue,
        };

        let mut snapshot = crate::runtime::runtime_filter_hub::RuntimeFilterSnapshot::from_filters(
            Vec::new(),
            Vec::new(),
        );
        snapshot.min_max_filters.push((
            filter_id,
            Arc::new(RuntimeMinMaxFilter::new(
                RuntimeFilterType::Int32,
                true,
                MinMaxValue::Int32(min),
                MinMaxValue::Int32(max),
            )),
        ));
        crate::exec::node::scan::RuntimeFilterContext::from_snapshot(snapshot)
    }

    fn runtime_membership_filter_ctx_for_i32(
        min: i32,
        max: i32,
    ) -> crate::exec::node::scan::RuntimeFilterContext {
        use crate::exec::runtime_filter::{
            RuntimeEmptyFilter, RuntimeFilterType, RuntimeMembershipFilter, RuntimeMinMaxFilter,
            min_max::MinMaxValue,
        };

        let min_max = RuntimeMinMaxFilter::new(
            RuntimeFilterType::Int32,
            true,
            MinMaxValue::Int32(min),
            MinMaxValue::Int32(max),
        );
        let membership = RuntimeMembershipFilter::Empty(RuntimeEmptyFilter::new(
            77,
            SlotId::new(1),
            RuntimeFilterType::Int32,
            false,
            0,
            2,
            min_max,
        ));
        let snapshot = crate::runtime::runtime_filter_hub::RuntimeFilterSnapshot::from_filters(
            Vec::new(),
            vec![membership],
        );
        crate::exec::node::scan::RuntimeFilterContext::from_snapshot(snapshot)
    }

    #[test]
    fn runtime_membership_filter_min_max_records_range_predicates() {
        let cfg = runtime_filter_row_group_scan_cfg();
        let runtime_filters = runtime_membership_filter_ctx_for_i32(0, 99);
        let bindings = super::runtime_scan_predicate_bindings(&cfg);
        let mut counters =
            crate::common::runtime_scan_predicate::RuntimeScanPredicateCounters::default();

        let predicates = crate::common::runtime_scan_predicate::runtime_filters_to_scan_predicates(
            &runtime_filters,
            &bindings,
            crate::common::runtime_scan_predicate::RuntimeScanPredicateOptions {
                discrete_set_max_values: super::PARQUET_DISCRETE_SET_MAX_VALUES,
                label: "parquet",
            },
            &mut counters,
        )
        .expect("runtime scan predicates");

        assert_eq!(predicates.len(), 2);
        assert_eq!(counters.range, 2);
        assert_eq!(counters.unsupported, 0);
    }

    /// Opens a `ParquetScanIter` over the whole fixture file with the given
    /// runtime filter context attached and a fresh profile, so row-group
    /// pruning counters can be asserted after the iterator is drained.
    fn open_runtime_filter_scan_iter(
        path: &Path,
        runtime_filters: crate::exec::node::scan::RuntimeFilterContext,
        profile: crate::runtime::profile::RuntimeProfile,
    ) -> ParquetScanIter {
        let file_len = fs::metadata(path).expect("fixture file metadata").len();
        let temp_dir = path.parent().expect("fixture parent dir");
        let op = build_fs_operator(temp_dir.to_str().expect("temp dir path")).expect("fs operator");
        let factory = OpendalRangeReaderFactory::from_operator(op)
            .expect("reader factory")
            .with_profile(Some(profile.clone()));
        let range = FileScanRange {
            path: path
                .file_name()
                .expect("fixture file name")
                .to_string_lossy()
                .to_string(),
            file_len,
            offset: 0,
            length: file_len,
            scan_range_id: -1,
            first_row_id: None,
            data_sequence_number: None,
            ivm_change_op: None,
            included_positions: None,
            external_datacache: None,
            delete_files: Vec::new(),
            iceberg_file_pruning: None,
        };
        ParquetScanIter::new(
            runtime_filter_row_group_scan_cfg(),
            vec![range],
            factory,
            None,
            Some(profile),
            Some(runtime_filters),
        )
        .expect("runtime filter scan iter")
    }

    #[test]
    fn parquet_direct_min_max_filter_uses_column_binding() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let file_path = temp_dir.path().join("rf_direct_min_max.parquet");
        write_runtime_filter_row_group_fixture(&file_path);

        let runtime_filters = runtime_min_max_filter_ctx_for_i32(99, 0, 99);
        let profile = crate::runtime::profile::RuntimeProfile::new("rf_direct_min_max_test");
        let mut cfg = runtime_filter_row_group_scan_cfg();
        cfg.runtime_min_max_filter_columns
            .insert(99, "id".to_string());

        let file_len = fs::metadata(&file_path)
            .expect("fixture file metadata")
            .len();
        let temp_dir_path = file_path.parent().expect("fixture parent dir");
        let op =
            build_fs_operator(temp_dir_path.to_str().expect("temp dir path")).expect("fs operator");
        let factory = OpendalRangeReaderFactory::from_operator(op)
            .expect("reader factory")
            .with_profile(Some(profile.clone()));
        let range = FileScanRange {
            path: file_path.file_name().unwrap().to_string_lossy().to_string(),
            file_len,
            offset: 0,
            length: file_len,
            scan_range_id: -1,
            first_row_id: None,
            data_sequence_number: None,
            ivm_change_op: None,
            included_positions: None,
            external_datacache: None,
            delete_files: Vec::new(),
            iceberg_file_pruning: None,
        };
        let mut iter = ParquetScanIter::new(
            cfg,
            vec![range],
            factory,
            None,
            Some(profile.clone()),
            Some(runtime_filters),
        )
        .expect("scan iter");

        for item in &mut iter {
            item.expect("chunk");
        }

        assert_eq!(
            profile
                .counter_value("ParquetRowGroupsSelected")
                .expect("selected counter"),
            1
        );
    }

    #[test]
    fn parquet_direct_min_max_filter_without_binding_is_storage_noop() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let file_path = temp_dir.path().join("rf_direct_min_max_unbound.parquet");
        write_runtime_filter_row_group_fixture(&file_path);

        let runtime_filters = runtime_min_max_filter_ctx_for_i32(99, 0, 99);
        let profile =
            crate::runtime::profile::RuntimeProfile::new("rf_direct_min_max_unbound_test");
        let mut iter = open_runtime_filter_scan_iter(&file_path, runtime_filters, profile.clone());

        for item in &mut iter {
            item.expect("chunk");
        }

        assert_eq!(
            profile
                .counter_value("ParquetRowGroupsSelected")
                .expect("selected counter"),
            RUNTIME_FILTER_ROW_GROUP_COUNT as i64
        );
    }

    fn collect_id_values(chunks: &[arrow::record_batch::RecordBatch]) -> Vec<i32> {
        let mut out = Vec::new();
        for batch in chunks {
            let ids = batch
                .column_by_name("id")
                .expect("id column")
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("id Int32Array");
            out.extend((0..ids.len()).map(|i| ids.value(i)));
        }
        out
    }

    #[test]
    fn runtime_filter_disjoint_range_short_circuits_with_zero_row_groups_selected() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let file_path = temp_dir.path().join("rf_empty_range.parquet");
        let metadata = write_runtime_filter_row_group_fixture(&file_path);
        assert_eq!(
            metadata.num_row_groups(),
            RUNTIME_FILTER_ROW_GROUP_COUNT as usize
        );

        // Data keys span [0, 999]; this runtime filter's min/max range is
        // [10_000, 10_001], disjoint from every row group's statistics.
        let runtime_filters = runtime_filter_ctx_for_keys(vec![10_000, 10_001]);
        let profile = crate::runtime::profile::RuntimeProfile::new("rf_empty_range_test");
        let mut iter = open_runtime_filter_scan_iter(&file_path, runtime_filters, profile.clone());

        let mut chunks_yielded = 0usize;
        for item in &mut iter {
            let chunk = item.expect("scan iter should not error");
            chunks_yielded += chunk.batch.num_rows();
        }
        assert_eq!(
            chunks_yielded, 0,
            "disjoint runtime filter must prune every row and yield zero rows"
        );
        assert!(
            iter.next().is_none(),
            "exhausted scan iter must return None (EOF) on further next() calls"
        );

        let total = profile
            .counter_value("ParquetRowGroupsTotal")
            .expect("ParquetRowGroupsTotal counter recorded");
        let selected = profile
            .counter_value("ParquetRowGroupsSelected")
            .expect("ParquetRowGroupsSelected counter recorded");
        let pruned = profile
            .counter_value("ParquetRowGroupsPruned")
            .expect("ParquetRowGroupsPruned counter recorded");

        assert_eq!(total, RUNTIME_FILTER_ROW_GROUP_COUNT as i64);
        assert_eq!(
            selected, 0,
            "disjoint runtime filter must select zero row groups"
        );
        assert_eq!(
            pruned, total,
            "every row group must be pruned when the runtime filter range is disjoint"
        );
    }

    #[test]
    fn runtime_filter_sparse_in_set_prunes_middle_row_groups() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let file_path = temp_dir.path().join("rf_sparse_in.parquet");
        let metadata = write_runtime_filter_row_group_fixture(&file_path);
        assert_eq!(
            metadata.num_row_groups(),
            RUNTIME_FILTER_ROW_GROUP_COUNT as usize
        );

        // Envelope [0, 999] overlaps every row group. The exact sparse set only
        // intersects the first and last row groups.
        let runtime_filters = runtime_filter_ctx_for_keys(vec![0, 999]);
        let profile = crate::runtime::profile::RuntimeProfile::new("rf_sparse_in_test");
        let mut iter = open_runtime_filter_scan_iter(&file_path, runtime_filters, profile.clone());

        let mut rf_on_batches = Vec::new();
        for item in &mut iter {
            rf_on_batches.push(item.expect("scan chunk").batch);
        }
        let values = collect_id_values(&rf_on_batches);

        let selected = profile
            .counter_value("ParquetRowGroupsSelected")
            .expect("ParquetRowGroupsSelected counter recorded");
        let pruned = profile
            .counter_value("ParquetRowGroupsPruned")
            .expect("ParquetRowGroupsPruned counter recorded");

        assert_eq!(
            selected, 2,
            "sparse IN should keep first and last row groups"
        );
        assert_eq!(
            pruned,
            RUNTIME_FILTER_ROW_GROUP_COUNT as i64 - 2,
            "sparse IN should prune every middle row group"
        );
        assert!(
            !values.is_empty(),
            "surviving row groups should still emit their rows"
        );
        assert_eq!(
            values.len(),
            2 * RUNTIME_FILTER_ROW_GROUP_ROWS as usize,
            "row-group pruning should emit both selected row groups in full"
        );
        let last_group_start = (RUNTIME_FILTER_ROW_GROUP_COUNT - 1) * RUNTIME_FILTER_ROW_GROUP_ROWS;
        let last_group_end = RUNTIME_FILTER_ROW_GROUP_COUNT * RUNTIME_FILTER_ROW_GROUP_ROWS;
        assert!(
            values.iter().all(|&value| {
                (0..RUNTIME_FILTER_ROW_GROUP_ROWS).contains(&value)
                    || (last_group_start..last_group_end).contains(&value)
            }),
            "sparse IN row-group pruning should keep only first and last row groups"
        );
        assert!(values.contains(&0), "first boundary key should be present");
        assert!(
            values.contains(&(last_group_end - 1)),
            "last boundary key should be present"
        );
        assert_eq!(
            profile
                .counter_value("ParquetScanPredicatesDiscreteSet")
                .expect("discrete set counter"),
            1
        );
        assert_eq!(
            profile
                .counter_value("ParquetScanPredicatesEnvelopeFallback")
                .unwrap_or(0),
            0
        );
    }

    #[test]
    fn oversized_in_filter_records_envelope_fallback() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let file_path = temp_dir.path().join("rf_large_in.parquet");
        write_runtime_filter_row_group_fixture(&file_path);

        let runtime_filters = runtime_filter_ctx_for_keys((0..300).collect());
        let profile = crate::runtime::profile::RuntimeProfile::new("rf_large_in_test");
        let mut iter = open_runtime_filter_scan_iter(&file_path, runtime_filters, profile.clone());
        for item in &mut iter {
            item.expect("chunk");
        }

        assert_eq!(
            profile
                .counter_value("ParquetScanPredicatesEnvelopeFallback")
                .expect("envelope fallback counter"),
            1
        );
    }

    #[test]
    fn runtime_filter_partial_overlap_prunes_to_single_row_group() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let file_path = temp_dir.path().join("rf_partial_overlap.parquet");
        let metadata = write_runtime_filter_row_group_fixture(&file_path);
        assert_eq!(
            metadata.num_row_groups(),
            RUNTIME_FILTER_ROW_GROUP_COUNT as usize
        );

        // Row group 0 covers keys [0, 99]. A runtime filter range of
        // [0, RUNTIME_FILTER_ROW_GROUP_ROWS - 1] overlaps only that row group
        // and is disjoint from every other row group's statistics.
        let overlap_max = RUNTIME_FILTER_ROW_GROUP_ROWS - 1;
        let runtime_filters = runtime_filter_ctx_for_keys(vec![0, overlap_max]);
        let profile = crate::runtime::profile::RuntimeProfile::new("rf_partial_overlap_test");
        let mut iter = open_runtime_filter_scan_iter(&file_path, runtime_filters, profile.clone());

        let mut rf_on_batches = Vec::new();
        for item in &mut iter {
            let chunk = item.expect("scan iter should not error");
            rf_on_batches.push(chunk.batch);
        }
        let rf_on_values = collect_id_values(&rf_on_batches);

        let selected = profile
            .counter_value("ParquetRowGroupsSelected")
            .expect("ParquetRowGroupsSelected counter recorded");
        assert_eq!(
            selected, 1,
            "runtime filter overlapping only the first row group must select exactly one"
        );
        assert!(
            !rf_on_values.is_empty(),
            "surviving row group must yield rows"
        );
        assert!(
            rf_on_values
                .iter()
                .all(|&v| (0..RUNTIME_FILTER_ROW_GROUP_ROWS).contains(&v)),
            "all RF-on rows must come from the first row group's key range"
        );

        // RF-off: same fixture, same config, no runtime filter attached.
        let rf_off_batch = read_single_batch(runtime_filter_row_group_scan_cfg(), &file_path);
        let rf_off_values = (0..rf_off_batch.num_rows())
            .map(|i| {
                rf_off_batch
                    .column_by_name("id")
                    .expect("id column")
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .expect("id Int32Array")
                    .value(i)
            })
            .collect::<Vec<_>>();

        let rf_off_set: HashSet<i32> = rf_off_values.into_iter().collect();
        assert!(
            rf_on_values.iter().all(|v| rf_off_set.contains(v)),
            "RF-on rows must be a subset of RF-off rows"
        );
        assert!(
            rf_on_values.len() < rf_off_set.len(),
            "partial pruning must strictly reduce the row count versus RF-off"
        );
    }
}
