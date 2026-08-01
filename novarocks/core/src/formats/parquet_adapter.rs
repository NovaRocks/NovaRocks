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

//! Core-only correctness adaptation for physical Parquet batches.
//!
//! Byte access, metadata/page caching, projection, pruning, ranges and
//! physical decoding are owned by `novarocks-fs`.

#[path = "parquet/local_io.rs"]
pub(crate) mod local_io;
#[path = "parquet/variant_pruning.rs"]
mod variant_pruning;
#[path = "parquet/variant_read.rs"]
mod variant_read;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use anyhow::Result;
use arrow::array::{Array, ArrayRef, RecordBatch, StructArray, new_null_array};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, FieldRef, Schema, SchemaRef};
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

use crate::common::ids::SlotId;
pub use crate::common::min_max_predicate::{
    MinMaxPredicate, MinMaxPredicateOp, MinMaxPredicateValue,
};
use crate::common::runtime_scan_predicate::{
    RuntimeScanPredicateBindings, RuntimeScanPredicateCounters, RuntimeScanPredicateOptions,
    runtime_filters_to_scan_predicates as build_runtime_scan_predicates,
};
use crate::common::scan_predicate::{
    MembershipPredicate, ScanPredicate, ScanPredicateDomain, ScanPredicateSource,
};
use crate::exec::chunk::{ChunkSchema, ChunkSchemaRef, ChunkSlotSchema};
use crate::exec::expr::cast_with_special_rules;
use crate::exec::node::scan::RuntimeFilterContext;
use novarocks_fs::DataCacheContext;
pub use variant_pruning::VariantPathPruningPredicate;
pub use variant_read::{
    collapse_variant_struct_to_largebinary, convert_variant_columns, is_variant_struct_data_type,
    materialize_variant_path_columns,
};

const PARQUET_DISCRETE_SET_MAX_VALUES: usize = 256;

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
    pub query_global_dicts: crate::exec::dict_encode::QueryGlobalDictEncodeMap,
}

pub(crate) struct FoundationParquetAdapter {
    cfg: ParquetScanConfig,
    scan_read_chunk_schema: ChunkSchemaRef,
    materialized_chunk_schema: ChunkSchemaRef,
    materialized_slot_kinds: Vec<ParquetSlotKind>,
    has_dict_encoded_output: bool,
}

impl FoundationParquetAdapter {
    pub(crate) fn try_new(cfg: ParquetScanConfig) -> Result<Self, String> {
        let (materialized_chunk_schema, materialized_slot_kinds) =
            materialized_variant_path_schema_and_slot_kinds(&cfg)?;
        let (scan_read_chunk_schema, has_dict_encoded_output) = if cfg.query_global_dicts.is_empty()
        {
            (materialized_chunk_schema.clone(), false)
        } else {
            let output = materialized_chunk_schema.arrow_schema_ref();
            let (scan, has_dict) =
                crate::exec::dict_encode::build_scan_schema_for_global_dict_encoding(
                    &output,
                    &materialized_chunk_schema,
                    &cfg.query_global_dicts,
                )?;
            if has_dict {
                (
                    ChunkSchema::try_ref_from_schema_and_slot_ids(
                        scan.as_ref(),
                        materialized_chunk_schema.slot_ids(),
                    )?,
                    true,
                )
            } else {
                (materialized_chunk_schema.clone(), false)
            }
        };
        Ok(Self {
            cfg,
            scan_read_chunk_schema,
            materialized_chunk_schema,
            materialized_slot_kinds,
            has_dict_encoded_output,
        })
    }

    pub(crate) fn adapt(
        &self,
        batch: RecordBatch,
    ) -> Result<(RecordBatch, ChunkSchemaRef), String> {
        let batch = reorder_batch(&self.cfg, batch)
            .and_then(|batch| {
                materialize_variant_path_columns(
                    batch,
                    self.cfg.chunk_schema.slot_ids(),
                    self.scan_read_chunk_schema.slot_ids(),
                    &self.cfg.variant_path_columns,
                )
            })
            .and_then(|batch| convert_variant_columns(&self.materialized_slot_kinds, batch))
            .and_then(|batch| normalize_batch_to_chunk_schema(batch, &self.scan_read_chunk_schema))
            .and_then(|batch| {
                if self.has_dict_encoded_output {
                    crate::exec::dict_encode::encode_batch_with_query_global_dicts(
                        batch,
                        &self.materialized_chunk_schema.arrow_schema_ref(),
                        &self.materialized_chunk_schema,
                        &self.cfg.query_global_dicts,
                    )
                } else {
                    Ok(batch)
                }
            })?;
        let schema = Arc::new(
            self.materialized_chunk_schema.with_fields_in_order(
                batch
                    .schema()
                    .fields()
                    .iter()
                    .map(|field| field.as_ref().clone())
                    .collect(),
            )?,
        );
        Ok((batch, schema))
    }
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
    for (index, slot) in chunk_schema.slots().iter().enumerate() {
        let column = batch.column(index).clone();
        let preserve_dictionary =
            is_dictionary_string_carrier_for_slot(column.data_type(), slot.data_type());
        let casted = if column.data_type() == slot.data_type() || preserve_dictionary {
            column
        } else {
            cast_with_special_rules(&column, slot.data_type()).map_err(|error| {
                format!(
                    "cast parquet scan column {} from {:?} to {:?} failed: {error}",
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
        }
        let source_field = batch_schema.field(index);
        if !source_field.metadata().is_empty() {
            let mut metadata = field.metadata().clone();
            metadata.extend(source_field.metadata().clone());
            field = field.with_metadata(metadata);
        }
        fields.push(field);
        columns.push(casted);
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
        .map_err(|error| format!("normalize parquet scan batch failed: {error}"))
}

fn is_dictionary_string_carrier_for_slot(actual: &DataType, slot: &DataType) -> bool {
    let DataType::Dictionary(key_type, value_type) = actual else {
        return false;
    };
    key_type.as_ref() == &DataType::Int32
        && ((slot == &DataType::Utf8 && value_type.as_ref() == &DataType::Utf8)
            || (slot == &DataType::LargeUtf8 && value_type.as_ref() == &DataType::LargeUtf8))
}

fn min_max_predicates_to_scan_predicates(
    predicates: &[MinMaxPredicate],
    source: ScanPredicateSource,
) -> Vec<ScanPredicate> {
    let mut out = Vec::with_capacity(predicates.len());
    for predicate in predicates {
        out.push(ScanPredicate::from_min_max_predicate(
            predicate.clone(),
            source,
        ));
        if let MinMaxPredicate::Eq { column, value } = predicate {
            out.push(ScanPredicate::new(
                column.clone(),
                ScanPredicateDomain::Membership(MembershipPredicate::BloomProbe {
                    values: vec![value.clone()],
                }),
                source,
            ));
        }
    }
    out
}

fn append_bloom_probes_for_discrete_sets(predicates: &mut Vec<ScanPredicate>) {
    let probes = predicates
        .iter()
        .filter_map(|predicate| match predicate.domain() {
            ScanPredicateDomain::DiscreteSet { values, .. } => Some(ScanPredicate::new(
                predicate.column().to_string(),
                ScanPredicateDomain::Membership(MembershipPredicate::BloomProbe {
                    values: values.clone(),
                }),
                predicate.source(),
            )),
            ScanPredicateDomain::Range { .. } | ScanPredicateDomain::Membership(_) => None,
        })
        .collect::<Vec<_>>();
    predicates.extend(probes);
}

fn find_column_index_by_name(
    columns: &[String],
    column_name: &str,
    case_sensitive: bool,
) -> Option<usize> {
    columns.iter().position(|column| {
        if case_sensitive {
            column == column_name
        } else {
            column.eq_ignore_ascii_case(column_name)
        }
    })
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
        let Some(index) = find_column_index_by_name(&cfg.columns, slot.name(), cfg.case_sensitive)
        else {
            continue;
        };
        bindings.slot_to_column.insert(slot_id, index.to_string());
    }
    bindings.min_max_filter_columns = cfg
        .runtime_min_max_filter_columns
        .iter()
        .filter_map(|(filter_id, column_name)| {
            find_column_index_by_name(&cfg.columns, column_name, cfg.case_sensitive)
                .map(|index| (*filter_id, index.to_string()))
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
    let mut predicates = build_runtime_scan_predicates(
        runtime_filters,
        &bindings,
        RuntimeScanPredicateOptions {
            discrete_set_max_values: PARQUET_DISCRETE_SET_MAX_VALUES,
            label: "parquet",
        },
        &mut counters,
    )?;
    append_bloom_probes_for_discrete_sets(&mut predicates);
    Ok(predicates)
}

pub(crate) fn foundation_scan_predicates(
    cfg: &ParquetScanConfig,
    runtime_filters: Option<&RuntimeFilterContext>,
) -> Result<Vec<novarocks_fs::ScanPredicate>, String> {
    if cfg.iceberg_output_schema.is_some() {
        return Ok(Vec::new());
    }
    let mut predicates =
        min_max_predicates_to_scan_predicates(&cfg.min_max_predicates, ScanPredicateSource::Static);
    if let Some(runtime_filters) = runtime_filters {
        predicates.extend(runtime_filters_to_scan_predicates(cfg, runtime_filters)?);
    }
    predicates
        .iter()
        .filter_map(|predicate| {
            let column = predicate
                .column()
                .parse::<usize>()
                .ok()
                .and_then(|index| cfg.columns.get(index))
                .map(String::as_str)
                .unwrap_or_else(|| predicate.column());
            (column != "___count___").then(|| to_foundation_predicate(column, predicate))
        })
        .collect()
}

fn to_foundation_predicate(
    column: &str,
    predicate: &ScanPredicate,
) -> Result<novarocks_fs::ScanPredicate, String> {
    let source = match predicate.source() {
        ScanPredicateSource::Static => novarocks_fs::ScanPredicateSource::Static,
        ScanPredicateSource::RuntimeIn => novarocks_fs::ScanPredicateSource::RuntimeIn,
        ScanPredicateSource::RuntimeMembership => {
            novarocks_fs::ScanPredicateSource::RuntimeMembership
        }
        ScanPredicateSource::RuntimeMinMax => novarocks_fs::ScanPredicateSource::RuntimeMinMax,
    };
    let domain = match predicate.domain() {
        ScanPredicateDomain::Range { op, value } => novarocks_fs::ScanPredicateDomain::Range {
            op: match op {
                MinMaxPredicateOp::Le => novarocks_fs::MinMaxPredicateOp::Le,
                MinMaxPredicateOp::Ge => novarocks_fs::MinMaxPredicateOp::Ge,
                MinMaxPredicateOp::Lt => novarocks_fs::MinMaxPredicateOp::Lt,
                MinMaxPredicateOp::Gt => novarocks_fs::MinMaxPredicateOp::Gt,
                MinMaxPredicateOp::Eq => novarocks_fs::MinMaxPredicateOp::Eq,
            },
            value: to_foundation_value(value),
        },
        ScanPredicateDomain::DiscreteSet { values, min, max } => {
            novarocks_fs::ScanPredicateDomain::DiscreteSet {
                values: values.iter().map(to_foundation_value).collect(),
                min: to_foundation_value(min),
                max: to_foundation_value(max),
            }
        }
        ScanPredicateDomain::Membership(MembershipPredicate::BloomProbe { values }) => {
            novarocks_fs::ScanPredicateDomain::Membership {
                values: values.iter().map(to_foundation_value).collect(),
            }
        }
    };
    Ok(novarocks_fs::ScanPredicate::new(column, domain, source))
}

fn to_foundation_value(value: &MinMaxPredicateValue) -> novarocks_fs::MinMaxPredicateValue {
    match value {
        MinMaxPredicateValue::Boolean(value) => novarocks_fs::MinMaxPredicateValue::Boolean(*value),
        MinMaxPredicateValue::Int32(value) => novarocks_fs::MinMaxPredicateValue::Int32(*value),
        MinMaxPredicateValue::Int64(value) => novarocks_fs::MinMaxPredicateValue::Int64(*value),
        MinMaxPredicateValue::Float(value) => novarocks_fs::MinMaxPredicateValue::Float(*value),
        MinMaxPredicateValue::Double(value) => novarocks_fs::MinMaxPredicateValue::Double(*value),
        MinMaxPredicateValue::ByteArray(value) => {
            novarocks_fs::MinMaxPredicateValue::ByteArray(value.clone())
        }
        MinMaxPredicateValue::FixedLenByteArray(value) => {
            novarocks_fs::MinMaxPredicateValue::FixedLenByteArray(value.clone())
        }
        MinMaxPredicateValue::Date32(value) => novarocks_fs::MinMaxPredicateValue::Date32(*value),
        MinMaxPredicateValue::DateTimeMicros(value) => {
            novarocks_fs::MinMaxPredicateValue::DateTimeMicros(*value)
        }
        MinMaxPredicateValue::DateTimeNanos(value) => {
            novarocks_fs::MinMaxPredicateValue::DateTimeNanos(*value)
        }
        MinMaxPredicateValue::LargeInt(value) => {
            novarocks_fs::MinMaxPredicateValue::LargeInt(*value)
        }
        MinMaxPredicateValue::Decimal128 {
            value,
            precision,
            scale,
        } => novarocks_fs::MinMaxPredicateValue::Decimal128 {
            value: *value,
            precision: *precision,
            scale: *scale,
        },
    }
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

fn parse_parquet_field_id(field: &Field) -> Result<Option<i32>, String> {
    let Some(raw) = field.metadata().get(PARQUET_FIELD_ID_META_KEY) else {
        return Ok(None);
    };
    raw.parse::<i32>().map(Some).map_err(|error| {
        format!(
            "invalid parquet field_id metadata: field={} key={} value={} error={error}",
            field.name(),
            PARQUET_FIELD_ID_META_KEY,
            raw
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
        for (index, source) in fields.iter().enumerate() {
            let source_field_id = parse_parquet_field_id(source.as_ref())?;
            source_has_field_ids |= source_field_id.is_some();
            if source_field_id == Some(target_field_id) {
                return Ok(Some(index));
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

fn align_iceberg_array_to_field(
    source_field: &Field,
    source_array: ArrayRef,
    target_field: &Field,
    row_count: usize,
    case_sensitive: bool,
) -> Result<ArrayRef, String> {
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
                if let Some(source_index) = find_matching_field_index(
                    source_children,
                    target_child.as_ref(),
                    case_sensitive,
                )? {
                    columns.push(align_iceberg_array_to_field(
                        source_children[source_index].as_ref(),
                        struct_array.column(source_index).clone(),
                        target_child.as_ref(),
                        row_count,
                        case_sensitive,
                    )?);
                } else {
                    columns.push(build_iceberg_default_array(
                        target_child.as_ref(),
                        row_count,
                    )?);
                }
            }
            Ok(Arc::new(
                StructArray::try_new(
                    target_children.clone(),
                    columns,
                    struct_array.nulls().cloned(),
                )
                .map_err(|error| error.to_string())?,
            ))
        }
        _ => {
            if is_dictionary_string_carrier_for_slot(
                source_array.data_type(),
                target_field.data_type(),
            ) || source_array.data_type() == target_field.data_type()
            {
                return Ok(source_array);
            }
            let casted =
                cast(source_array.as_ref(), target_field.data_type()).map_err(|error| {
                    format!(
                        "iceberg parquet cast failed for column {} from {:?} to {:?}: {error}",
                        target_field.name(),
                        source_array.data_type(),
                        target_field.data_type()
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

pub(crate) fn build_iceberg_default_array(
    target_field: &Field,
    row_count: usize,
) -> Result<ArrayRef, String> {
    use crate::connector::iceberg::default_value::literal_to_constant_array;
    use crate::connector::iceberg::schema::ICEBERG_INITIAL_DEFAULT_META_KEY;
    use iceberg::spec::Literal;

    let Some(json) = target_field
        .metadata()
        .get(ICEBERG_INITIAL_DEFAULT_META_KEY)
    else {
        return Ok(new_null_array(target_field.data_type(), row_count));
    };
    let json_value: serde_json::Value = serde_json::from_str(json).map_err(|error| {
        format!(
            "corrupted initial-default JSON for column {}: {error}",
            target_field.name()
        )
    })?;
    let iceberg_type = arrow_type_to_iceberg_type(target_field.data_type()).map_err(|error| {
        format!(
            "unsupported initial-default for column {}: {error}",
            target_field.name()
        )
    })?;
    let literal = Literal::try_from_json(json_value, &iceberg_type)
        .map_err(|error| {
            format!(
                "decode initial-default for column {}: {error}",
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

fn arrow_type_to_iceberg_type(data_type: &DataType) -> Result<iceberg::spec::Type, String> {
    use arrow::datatypes::TimeUnit;
    use iceberg::spec::{ListType, MapType, NestedField, PrimitiveType, Type};
    Ok(match data_type {
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
        DataType::List(element_field) => {
            Type::List(ListType::new(Arc::new(NestedField::optional(
                1,
                "element",
                arrow_type_to_iceberg_type(element_field.data_type())?,
            ))))
        }
        DataType::Map(entries_field, _) => {
            let DataType::Struct(entry_fields) = entries_field.data_type() else {
                return Err(format!(
                    "arrow Map field entries must be a Struct, got {:?}",
                    entries_field.data_type()
                ));
            };
            if entry_fields.len() < 2 {
                return Err(format!(
                    "arrow Map entries struct must have at least 2 fields, got {}",
                    entry_fields.len()
                ));
            }
            Type::Map(MapType::new(
                Arc::new(NestedField::required(
                    1,
                    "key",
                    arrow_type_to_iceberg_type(entry_fields[0].data_type())?,
                )),
                Arc::new(NestedField::optional(
                    2,
                    "value",
                    arrow_type_to_iceberg_type(entry_fields[1].data_type())?,
                )),
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
    let mut columns = Vec::with_capacity(output_schema.fields().len());
    for target in output_schema.fields() {
        if target.name() == "___count___" {
            if target.data_type() != &DataType::Boolean {
                return Err(format!(
                    "iceberg virtual count column expects Boolean type, got {:?}",
                    target.data_type()
                ));
            }
            fields.push(target.as_ref().clone());
            columns.push(
                Arc::new(arrow::array::BooleanArray::from(vec![true; row_count])) as ArrayRef,
            );
            continue;
        }
        if let Some(source_index) =
            find_matching_field_index(batch_schema.fields(), target.as_ref(), case_sensitive)?
        {
            let array = align_iceberg_array_to_field(
                batch_schema.field(source_index),
                batch.column(source_index).clone(),
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
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).map_err(|error| error.to_string())
}

fn reorder_batch(cfg: &ParquetScanConfig, batch: RecordBatch) -> Result<RecordBatch, String> {
    if let Some(output_schema) = cfg.iceberg_output_schema.as_ref() {
        return align_batch_to_iceberg_schema(output_schema, batch, cfg.case_sensitive)
            .and_then(|batch| validate_batch_slot_count(cfg, batch));
    }
    if cfg.columns.is_empty() {
        return validate_batch_slot_count(cfg, batch);
    }
    let batch_schema = batch.schema();
    let mut columns = Vec::with_capacity(cfg.columns.len());
    let mut fields = Vec::with_capacity(cfg.columns.len());
    for column_name in &cfg.columns {
        if column_name == "___count___" {
            columns.push(Arc::new(arrow::array::BooleanArray::from(vec![
                true;
                batch.num_rows()
            ])) as ArrayRef);
            fields.push(Arc::new(Field::new(
                "___count___",
                DataType::Boolean,
                false,
            )));
            continue;
        }
        let index = if cfg.case_sensitive {
            batch_schema.index_of(column_name).ok()
        } else {
            batch_schema
                .fields()
                .iter()
                .position(|field| field.name().eq_ignore_ascii_case(column_name))
        }
        .ok_or_else(|| format!("Column {column_name} missing in read batch"))?;
        columns.push(batch.column(index).clone());
        fields.push(batch_schema.field(index).clone().into());
    }
    validate_batch_slot_count(
        cfg,
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
            .map_err(|error| error.to_string())?,
    )
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

#[cfg(test)]
mod file_read_contract_tests {
    use super::*;
    use arrow::array::{Int32Array, TimestampMicrosecondArray};
    use arrow::datatypes::TimeUnit;
    use std::collections::HashMap;

    fn predicate_config() -> ParquetScanConfig {
        ParquetScanConfig {
            columns: vec!["id".to_string()],
            chunk_schema: Arc::new(ChunkSchema::try_new(Vec::new()).expect("empty chunk schema")),
            slot_kinds: Vec::new(),
            case_sensitive: true,
            enable_page_index: true,
            min_max_predicates: vec![MinMaxPredicate::Ge {
                column: "0".to_string(),
                value: MinMaxPredicateValue::Int32(7),
            }],
            runtime_min_max_filter_columns: HashMap::new(),
            variant_path_predicates: Vec::new(),
            batch_size: None,
            datacache: novarocks_fs::DataCacheManager::instance().external_context(
                crate::cache::CacheOptions::from_query_options(None)
                    .expect("cache options")
                    .to_file_cache_options(),
            ),
            cache_policy: ParquetReadCachePolicy::with_flags(false, false, None),
            profile_label: None,
            iceberg_output_schema: None,
            variant_path_columns: Vec::new(),
            query_global_dicts: HashMap::new(),
        }
    }

    #[test]
    fn file_read_static_predicate_snapshot_uses_physical_column_name() {
        let predicates =
            foundation_scan_predicates(&predicate_config(), None).expect("foundation predicates");
        assert_eq!(predicates.len(), 1);
        assert_eq!(predicates[0].column(), "id");
        assert_eq!(
            predicates[0].source(),
            novarocks_fs::ScanPredicateSource::Static
        );
        assert_eq!(
            predicates[0].domain(),
            &novarocks_fs::ScanPredicateDomain::Range {
                op: novarocks_fs::MinMaxPredicateOp::Ge,
                value: novarocks_fs::MinMaxPredicateValue::Int32(7),
            }
        );
    }

    #[test]
    fn file_read_schema_adapter_materializes_defaults_and_retags_timestamps() {
        let input = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("a", DataType::Int32, true).with_metadata(HashMap::from([(
                    PARQUET_FIELD_ID_META_KEY.to_string(),
                    "1".to_string(),
                )])),
            ])),
            vec![Arc::new(Int32Array::from(vec![10, 20])) as ArrayRef],
        )
        .expect("input batch");
        let output_schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, true).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "1".to_string(),
            )])),
            Field::new("b", DataType::Int32, true).with_metadata(HashMap::from([
                (PARQUET_FIELD_ID_META_KEY.to_string(), "2".to_string()),
                (
                    crate::connector::iceberg::schema::ICEBERG_INITIAL_DEFAULT_META_KEY.to_string(),
                    "99".to_string(),
                ),
            ])),
        ]));
        let output =
            align_batch_to_iceberg_schema(&output_schema, input, true).expect("align schema");
        let defaulted = output
            .column_by_name("b")
            .expect("defaulted column")
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("int32 default");
        assert_eq!(defaulted.values(), &[99, 99]);

        let target_type = DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into()));
        let target_schema = Schema::new(vec![Field::new("ts", target_type.clone(), true)]);
        let chunk_schema =
            ChunkSchema::try_ref_from_schema_and_slot_ids(&target_schema, &[SlotId::new(1)])
                .expect("timestamp chunk schema");
        let physical = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                true,
            )])),
            vec![Arc::new(TimestampMicrosecondArray::from(vec![Some(42)])) as ArrayRef],
        )
        .expect("physical timestamp batch");
        let normalized =
            normalize_batch_to_chunk_schema(physical, &chunk_schema).expect("normalize timestamp");
        assert_eq!(normalized.column(0).data_type(), &target_type);
    }
}
