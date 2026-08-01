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
use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Decimal256Array,
    Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array, ListArray,
    ListBuilder, StringArray, StringBuilder, TimestampMicrosecondArray, new_null_array,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use arrow_buffer::i256;

use crate::protocol::starrocks::decode::layout::{
    Layout, chunk_slot_schemas_for_layout, find_tuple_descriptor, layout_for_row_tuples,
    layout_from_slot_ids, schema_for_layout,
};
use crate::protocol::starrocks::decode::node::Lowered;
use crate::protocol::starrocks::decode::{DraftDependencyValue, StarRocksExternalDependencyDraft};
use crate::thrift::{descriptors, plan_nodes, types};
use novarocks::connector::starrocks::lake_meta::{
    LakeMetaColumnKind, LakeMetaColumnRequest, LakeMetaStorageFacts, LakeMetaStorageRequest,
    LakeMetaTabletRequest,
};
use novarocks::exec::chunk::{Chunk, ChunkSchema, ChunkSlotSchema};
use novarocks::exec::node::values::ValuesNode;
use novarocks::exec::node::{ExecNode, ExecNodeKind};
use novarocks_types::QueryId;
use novarocks_types::largeint;

#[derive(Clone, Debug, PartialEq, Eq)]
enum MetaMetricKind {
    Rows { _column_id: String },
    Count { column_id: String },
    DictMerge { column_id: String },
    Min { column_id: String },
    Max { column_id: String },
}

#[derive(Clone, Debug)]
enum MetaScalarValue {
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    LargeInt(i128),
    Float32(f32),
    Float64(f64),
    Boolean(bool),
    Date32(i32),
    TimestampMicrosecond(i64),
    Decimal128(i128),
    Decimal256(i256),
    Utf8(String),
    Binary(Vec<u8>),
}

#[derive(Clone, Debug, Default)]
struct MetaColumnMinMax {
    min: Option<MetaScalarValue>,
    max: Option<MetaScalarValue>,
}

#[derive(Clone, Debug)]
pub(crate) struct LakeMetaValuesPatch {
    dependency_id: u64,
    node_id: i32,
    template: LakeMetaValuesTemplate,
}

impl LakeMetaValuesPatch {
    pub(crate) const fn dependency_id(&self) -> u64 {
        self.dependency_id
    }

    pub(crate) const fn node_id(&self) -> i32 {
        self.node_id
    }

    pub(crate) fn materialize(&self, facts: &LakeMetaStorageFacts) -> Result<Chunk, String> {
        self.template.materialize(facts)
    }
}

#[derive(Clone, Debug)]
struct LakeMetaValuesTemplate {
    output_schema: Arc<Schema>,
    output_chunk_schema: Arc<ChunkSchema>,
    slot_ids: Vec<i32>,
    metric_by_slot: HashMap<i32, MetaMetricKind>,
    column_requests: Vec<LakeMetaColumnRequest>,
    min_max_type_by_column: HashMap<String, DataType>,
    min_partition_id: Option<i64>,
    max_partition_id: Option<i64>,
    table_id: i64,
}

/// Lower a LAKE_META_SCAN_NODE to a one-row `ValuesNode`.
///
/// The FE side must provide tablet ids, versions, schema id, row-count hints,
/// and table identity through descriptors plus `per_node_scan_ranges`. Catalog
/// identity is part of the explicit internal scan range contract; this lower
/// path must not read process-local StarRocks catalog configuration.
///
/// Supported metrics:
/// - rows_<column_id>       => total row count
/// - count_<column_id>      => total non-null row count for non-nullable columns
/// - dict_merge_<column_id> => collected distinct string words as ARRAY<VARCHAR>
pub(crate) fn lower_lake_meta_scan_node(
    node: &plan_nodes::TPlanNode,
    desc_tbl: Option<&descriptors::TDescriptorTable>,
    tuple_slots: &HashMap<types::TTupleId, Vec<types::TSlotId>>,
    layout_hints: &HashMap<types::TTupleId, Vec<types::TSlotId>>,
    scan_ranges: Option<&[crate::protocol::starrocks::decode::LakeMetaScanRangeFact]>,
    query_id: Option<QueryId>,
    db_name_hint: Option<&str>,
    external_dependencies: Option<&StarRocksExternalDependencyDraft>,
) -> Result<Lowered, String> {
    if node.num_children != 0 {
        return Err(format!(
            "LAKE_META_SCAN_NODE expected 0 children, got {}",
            node.num_children
        ));
    }
    let meta = node
        .meta_scan_node
        .as_ref()
        .ok_or_else(|| "LAKE_META_SCAN_NODE missing meta_scan_node payload".to_string())?;
    let tuple_id = node
        .row_tuples
        .first()
        .copied()
        .ok_or_else(|| "LAKE_META_SCAN_NODE missing row_tuples".to_string())?;
    let schema_id = meta
        .schema_id
        .ok_or_else(|| "LAKE_META_SCAN_NODE missing schema_id".to_string())?;

    let out_layout = if let Some(hint) = layout_hints.get(&tuple_id).filter(|v| !v.is_empty()) {
        layout_from_slot_ids(tuple_id, hint.iter().copied())
    } else {
        layout_for_row_tuples(&[tuple_id], tuple_slots)
    };
    if out_layout.order.is_empty() {
        return Err(format!(
            "LAKE_META_SCAN_NODE tuple_id={} has empty output layout",
            tuple_id
        ));
    }

    let desc_tbl = desc_tbl.ok_or_else(|| {
        format!(
            "LAKE_META_SCAN_NODE node_id={} requires descriptor table",
            node.node_id
        )
    })?;
    let base_output_schema = schema_for_layout(desc_tbl, &out_layout)?;

    let id_to_names = meta
        .id_to_names
        .as_ref()
        .ok_or_else(|| "LAKE_META_SCAN_NODE missing id_to_names".to_string())?;
    if id_to_names.is_empty() {
        return Err("LAKE_META_SCAN_NODE id_to_names is empty".to_string());
    }
    let metric_by_slot = build_metric_mapping(meta, id_to_names)?;
    let output_schema =
        rewrite_meta_output_schema(&base_output_schema, &out_layout, &metric_by_slot)?;

    let scan_ranges = scan_ranges.ok_or_else(|| {
        format!(
            "LAKE_META_SCAN_NODE node_id={} missing normalized range facts",
            node.node_id
        )
    })?;

    let mut tablet_versions: HashMap<i64, i64> = HashMap::new();
    let mut tablet_row_count_hints: HashMap<i64, i64> = HashMap::new();
    let mut internal_db_name: Option<String> = None;
    let mut internal_table_name: Option<String> = None;
    let mut min_partition_id: Option<i64> = None;
    let mut max_partition_id: Option<i64> = None;
    let mut has_more = false;
    for p in scan_ranges {
        if p.empty {
            if p.has_more {
                has_more = true;
            }
            continue;
        }
        if p.tablet_id <= 0 {
            return Err(format!(
                "LAKE_META_SCAN_NODE has invalid tablet_id={}",
                p.tablet_id
            ));
        }
        let version = p.version;
        if version <= 0 {
            return Err(format!(
                "LAKE_META_SCAN_NODE has non-positive version for tablet_id={}: {}",
                p.tablet_id, version
            ));
        }
        if let Some(existing_version) = tablet_versions.insert(p.tablet_id, version)
            && existing_version != version
        {
            return Err(format!(
                "LAKE_META_SCAN_NODE has inconsistent versions for tablet_id={}: {} vs {}",
                p.tablet_id, existing_version, version
            ));
        }
        if let Some(row_count) = p.row_count {
            if row_count < 0 {
                return Err(format!(
                    "LAKE_META_SCAN_NODE has negative row_count for tablet_id={}: {}",
                    p.tablet_id, row_count
                ));
            }
            if let Some(existing_row_count) = tablet_row_count_hints.insert(p.tablet_id, row_count)
                && existing_row_count != row_count
            {
                return Err(format!(
                    "LAKE_META_SCAN_NODE has inconsistent row_count for tablet_id={}: {} vs {}",
                    p.tablet_id, existing_row_count, row_count
                ));
            }
        }
        if let Some(partition_id) = p.partition_id {
            min_partition_id = Some(match min_partition_id {
                Some(current) => current.min(partition_id),
                None => partition_id,
            });
            max_partition_id = Some(match max_partition_id {
                Some(current) => current.max(partition_id),
                None => partition_id,
            });
        }
        if internal_db_name.is_none() {
            internal_db_name = p.db_name.clone();
        }
        if internal_table_name.is_none()
            && let Some(name) = p.table_name.as_deref()
        {
            internal_table_name = Some(name.to_string());
        }
        if p.has_more {
            has_more = true;
        }
    }
    if has_more {
        return Err(format!(
            "LAKE_META_SCAN_NODE node_id={} has incremental scan ranges which are not supported",
            node.node_id
        ));
    }
    if tablet_versions.is_empty() {
        return Err(format!(
            "LAKE_META_SCAN_NODE node_id={} has no effective scan ranges",
            node.node_id
        ));
    }

    let tuple_desc = find_tuple_descriptor(desc_tbl, tuple_id)?;
    let table_id_from_tuple = tuple_desc
        .table_id
        .ok_or_else(|| format!("LAKE_META_SCAN_NODE tuple_id={} missing table_id", tuple_id))?;
    let table_descs = desc_tbl
        .table_descriptors
        .as_ref()
        .ok_or_else(|| "LAKE_META_SCAN_NODE missing table_descriptors in desc_tbl".to_string())?;
    let table_desc = table_descs
        .iter()
        .find(|t| t.id == table_id_from_tuple)
        .ok_or_else(|| {
            format!(
                "LAKE_META_SCAN_NODE missing table descriptor for table_id={}",
                table_id_from_tuple
            )
        })?;

    let db_name = if table_desc.db_name.trim().is_empty() {
        if let Some(name) = db_name_hint
            .map(|v| v.trim())
            .filter(|v| !v.is_empty())
            .map(|v| v.to_string())
        {
            name
        } else if let Some(name) = internal_db_name {
            name
        } else {
            "__unknown_db__".to_string()
        }
    } else {
        table_desc.db_name.trim().to_string()
    };
    let table_name = if table_desc.table_name.trim().is_empty() {
        internal_table_name.unwrap_or_else(|| "__unknown_table__".to_string())
    } else {
        table_desc.table_name.trim().to_string()
    };
    let table_identity = super::lake_scan::internal_lake_table_identity(
        db_name,
        table_name,
        0,
        table_desc.id,
        schema_id,
    );
    let mut min_max_type_by_column: HashMap<String, DataType> = HashMap::new();
    for (idx, (_tuple_id, slot_id)) in out_layout.order.iter().enumerate() {
        let metric = metric_by_slot.get(slot_id).ok_or_else(|| {
            format!(
                "LAKE_META_SCAN_NODE output slot_id={} missing metric mapping while collecting min/max inputs",
                slot_id
            )
        })?;
        let column_id = match metric {
            MetaMetricKind::Min { column_id } | MetaMetricKind::Max { column_id } => column_id,
            _ => continue,
        };
        if is_partition_or_table_meta_metric_column(column_id) {
            continue;
        }
        let data_type = output_schema.field(idx).data_type().clone();
        if let Some(existing) = min_max_type_by_column.get(column_id) {
            if existing != &data_type {
                return Err(format!(
                    "LAKE_META_SCAN_NODE min/max metrics for column_id={} have inconsistent output types: {:?} vs {:?}",
                    column_id, existing, data_type
                ));
            }
            continue;
        }
        min_max_type_by_column.insert(column_id.clone(), data_type);
    }

    let query_id = query_id.ok_or_else(|| "LAKE_META_SCAN_NODE missing query id".to_string())?;
    let mut tablets = tablet_versions.into_iter().collect::<Vec<_>>();
    tablets.sort_by_key(|(tablet_id, _)| *tablet_id);
    let tablet_requests = tablets
        .into_iter()
        .map(|(tablet_id, version)| LakeMetaTabletRequest {
            tablet_id,
            version,
            row_count_hint: tablet_row_count_hints.get(&tablet_id).copied(),
        })
        .collect::<Vec<_>>();
    let mut column_requests = Vec::new();
    for metric in metric_by_slot.values() {
        if let MetaMetricKind::DictMerge { column_id } = metric {
            let request = LakeMetaColumnRequest {
                column_id: column_id.clone(),
                kind: LakeMetaColumnKind::Dictionary,
            };
            if !column_requests.contains(&request) {
                column_requests.push(request);
            }
        }
    }
    for (column_id, data_type) in &min_max_type_by_column {
        column_requests.push(LakeMetaColumnRequest {
            column_id: column_id.clone(),
            kind: LakeMetaColumnKind::Value(data_type.clone()),
        });
    }
    column_requests.sort_by_key(LakeMetaColumnRequest::storage_key);
    let storage_request = LakeMetaStorageRequest::new(
        query_id,
        table_identity.catalog,
        table_identity.db_name,
        table_identity.table_name,
        table_identity.db_id,
        table_identity.table_id,
        table_identity.schema_id,
        tablet_requests,
        column_requests.clone(),
    );
    let output_chunk_schema = chunk_schema_for_meta_output(desc_tbl, &out_layout, &output_schema)?;
    let template = LakeMetaValuesTemplate {
        output_schema,
        output_chunk_schema,
        slot_ids: out_layout
            .order
            .iter()
            .map(|(_, slot_id)| *slot_id)
            .collect(),
        metric_by_slot,
        column_requests,
        min_max_type_by_column,
        min_partition_id,
        max_partition_id,
        table_id: table_desc.id,
    };
    let dependencies = external_dependencies
        .ok_or_else(|| "LAKE_META_SCAN_NODE missing external dependency draft".to_string())?;
    let (storage_facts, pending_dependency_id) =
        match dependencies.lake_meta_storage_value(&storage_request)? {
            DraftDependencyValue::Resolved(facts) => (facts, None),
            DraftDependencyValue::Pending(id) => (
                LakeMetaStorageFacts {
                    total_rows: 0,
                    column_arrays: storage_request
                        .columns
                        .iter()
                        .map(|column| (column.storage_key(), Vec::new()))
                        .collect(),
                },
                Some(id),
            ),
        };
    let chunk = template.materialize(&storage_facts)?;
    if let Some(dependency_id) = pending_dependency_id {
        dependencies.record_lake_meta_values_patch(LakeMetaValuesPatch {
            dependency_id,
            node_id: node.node_id,
            template,
        });
    }

    Ok(Lowered {
        node: ExecNode {
            kind: ExecNodeKind::Values(ValuesNode {
                chunk,
                node_id: node.node_id,
            }),
        },
        layout: out_layout,
    })
}

impl LakeMetaValuesTemplate {
    fn materialize(&self, storage_facts: &LakeMetaStorageFacts) -> Result<Chunk, String> {
        let mut dict_words_by_column: HashMap<String, Vec<String>> = HashMap::new();
        for request in self
            .column_requests
            .iter()
            .filter(|request| request.kind == LakeMetaColumnKind::Dictionary)
        {
            let arrays = storage_facts
                .column_arrays
                .get(&request.storage_key())
                .ok_or_else(|| {
                    format!(
                        "LAKE_META_SCAN_NODE missing dictionary storage facts for column_id={}",
                        request.column_id
                    )
                })?;
            let mut words = BTreeSet::new();
            for array in arrays {
                collect_words_from_column_array(array, &mut words)?;
            }
            dict_words_by_column.insert(request.column_id.clone(), words.into_iter().collect());
        }

        let mut min_max_by_column: HashMap<String, MetaColumnMinMax> = HashMap::new();
        for (column_id, data_type) in &self.min_max_type_by_column {
            let request = LakeMetaColumnRequest {
                column_id: column_id.clone(),
                kind: LakeMetaColumnKind::Value(data_type.clone()),
            };
            let arrays = storage_facts
                .column_arrays
                .get(&request.storage_key())
                .ok_or_else(|| {
                    format!(
                        "LAKE_META_SCAN_NODE missing min/max storage facts for column_id={}",
                        column_id
                    )
                })?;
            let mut min_max = MetaColumnMinMax::default();
            for array in arrays {
                update_min_max_from_array(array, data_type, &mut min_max)?;
            }
            min_max_by_column.insert(column_id.clone(), min_max);
        }

        let mut columns = Vec::with_capacity(self.slot_ids.len());
        for (idx, slot_id) in self.slot_ids.iter().enumerate() {
            let field = self.output_schema.field(idx);
            let metric = self.metric_by_slot.get(slot_id).ok_or_else(|| {
                format!(
                    "LAKE_META_SCAN_NODE output slot_id={} missing metric mapping in id_to_names",
                    slot_id
                )
            })?;
            let array: ArrayRef = match metric {
                MetaMetricKind::Rows { .. } | MetaMetricKind::Count { .. } => {
                    if field.data_type() != &DataType::Int64 {
                        return Err(format!(
                            "LAKE_META_SCAN_NODE output slot_id={} requires Int64 for rows/count metrics, got {:?}",
                            slot_id,
                            field.data_type()
                        ));
                    }
                    Arc::new(Int64Array::from(vec![Some(storage_facts.total_rows)]))
                }
                MetaMetricKind::DictMerge { column_id } => {
                    if !matches!(field.data_type(), DataType::List(item) if matches!(item.data_type(), DataType::Utf8))
                    {
                        return Err(format!(
                            "LAKE_META_SCAN_NODE output slot_id={} for dict_merge metric requires List(Utf8), got {:?}",
                            slot_id,
                            field.data_type()
                        ));
                    }
                    let words = dict_words_by_column.get(column_id).ok_or_else(|| {
                        format!(
                            "LAKE_META_SCAN_NODE missing dict words for column_id={} (slot_id={})",
                            column_id, slot_id
                        )
                    })?;
                    build_single_row_utf8_list_array(words)
                }
                MetaMetricKind::Min { column_id } | MetaMetricKind::Max { column_id }
                    if is_partition_or_table_meta_metric_column(column_id) =>
                {
                    if field.data_type() != &DataType::Int64 {
                        return Err(format!(
                            "LAKE_META_SCAN_NODE output slot_id={} for min/max metric on {} requires Int64, got {:?}",
                            slot_id,
                            column_id,
                            field.data_type()
                        ));
                    }
                    let value = resolve_meta_i64_min_max_metric(
                        column_id,
                        matches!(metric, MetaMetricKind::Min { .. }),
                        self.min_partition_id,
                        self.max_partition_id,
                        self.table_id,
                    )?;
                    Arc::new(Int64Array::from(vec![value]))
                }
                MetaMetricKind::Min { column_id } => {
                    let min_max = min_max_by_column.get(column_id).ok_or_else(|| {
                        format!(
                            "LAKE_META_SCAN_NODE missing collected min/max data for column_id={} (slot_id={})",
                            column_id, slot_id
                        )
                    })?;
                    build_single_row_scalar_array(field.data_type(), min_max.min.as_ref())?
                }
                MetaMetricKind::Max { column_id } => {
                    let min_max = min_max_by_column.get(column_id).ok_or_else(|| {
                        format!(
                            "LAKE_META_SCAN_NODE missing collected min/max data for column_id={} (slot_id={})",
                            column_id, slot_id
                        )
                    })?;
                    build_single_row_scalar_array(field.data_type(), min_max.max.as_ref())?
                }
            };
            columns.push(array);
        }
        let batch = RecordBatch::try_new(Arc::clone(&self.output_schema), columns)
            .map_err(|error| format!("LAKE_META_SCAN_NODE build output batch failed: {error}"))?;
        Chunk::try_new_with_chunk_schema(batch, Arc::clone(&self.output_chunk_schema))
    }
}

fn build_single_row_utf8_list_array(words: &[String]) -> ArrayRef {
    let mut builder = ListBuilder::new(StringBuilder::new());
    for word in words {
        builder.values().append_value(word);
    }
    builder.append(true);
    Arc::new(builder.finish())
}

fn rewrite_meta_output_schema(
    base_output_schema: &Arc<Schema>,
    out_layout: &Layout,
    metric_by_slot: &HashMap<i32, MetaMetricKind>,
) -> Result<Arc<Schema>, String> {
    if base_output_schema.fields().len() != out_layout.order.len() {
        return Err(format!(
            "LAKE_META_SCAN_NODE output schema/layout size mismatch: schema_fields={} layout_slots={}",
            base_output_schema.fields().len(),
            out_layout.order.len()
        ));
    }

    let mut fields = Vec::with_capacity(base_output_schema.fields().len());
    for (idx, field_ref) in base_output_schema.fields().iter().enumerate() {
        let (_tuple_id, slot_id) = out_layout.order[idx];
        let metric = metric_by_slot.get(&slot_id).ok_or_else(|| {
            format!(
                "LAKE_META_SCAN_NODE output slot_id={} missing metric mapping while rewriting schema",
                slot_id
            )
        })?;
        let data_type = match metric {
            MetaMetricKind::Rows { .. } | MetaMetricKind::Count { .. } => DataType::Int64,
            MetaMetricKind::DictMerge { .. } => {
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)))
            }
            MetaMetricKind::Min { .. } | MetaMetricKind::Max { .. } => {
                field_ref.data_type().clone()
            }
        };
        fields.push(Field::new(field_ref.name(), data_type, true));
    }
    Ok(Arc::new(Schema::new(fields)))
}

fn rewrite_chunk_slots_for_output_schema(
    base_slots: Vec<ChunkSlotSchema>,
    output_schema: &Arc<Schema>,
) -> Result<Vec<ChunkSlotSchema>, String> {
    if base_slots.len() != output_schema.fields().len() {
        return Err(format!(
            "LAKE_META_SCAN_NODE output chunk schema size mismatch: base_slots={} output_fields={}",
            base_slots.len(),
            output_schema.fields().len()
        ));
    }

    base_slots
        .into_iter()
        .zip(output_schema.fields().iter())
        .map(|(slot, field)| {
            ChunkSlotSchema::try_new_with_field(
                slot.slot_id(),
                field.as_ref().clone(),
                None,
                slot.unique_id(),
            )
        })
        .collect()
}

fn chunk_schema_for_meta_output(
    desc_tbl: &descriptors::TDescriptorTable,
    out_layout: &Layout,
    output_schema: &Arc<Schema>,
) -> Result<Arc<ChunkSchema>, String> {
    let base_slots = chunk_slot_schemas_for_layout(desc_tbl, out_layout)?;
    Ok(Arc::new(ChunkSchema::try_new(
        rewrite_chunk_slots_for_output_schema(base_slots, output_schema)?,
    )?))
}

fn resolve_meta_i64_min_max_metric(
    column_id: &str,
    is_min: bool,
    min_partition_id: Option<i64>,
    max_partition_id: Option<i64>,
    table_id: i64,
) -> Result<Option<i64>, String> {
    let normalized = column_id.trim().to_ascii_lowercase();
    match normalized.as_str() {
        "partition_id" => Ok(if is_min {
            min_partition_id
        } else {
            max_partition_id
        }),
        "table_id" => Ok(Some(table_id)),
        _ => Err(format!(
            "LAKE_META_SCAN_NODE {} metric currently only supports partition_id/table_id, got column_id={}",
            if is_min { "min" } else { "max" },
            column_id
        )),
    }
}

fn is_partition_or_table_meta_metric_column(column_id: &str) -> bool {
    matches!(
        column_id.trim().to_ascii_lowercase().as_str(),
        "partition_id" | "table_id"
    )
}

fn update_min_max_from_array(
    array: &ArrayRef,
    output_type: &DataType,
    min_max: &mut MetaColumnMinMax,
) -> Result<(), String> {
    match output_type {
        DataType::Int8 => {
            let typed = array
                .as_any()
                .downcast_ref::<Int8Array>()
                .ok_or_else(|| "min/max collect failed to downcast Int8 array".to_string())?;
            for row in 0..typed.len() {
                if !typed.is_null(row) {
                    update_min_max_with_value(min_max, MetaScalarValue::Int8(typed.value(row)))?;
                }
            }
            Ok(())
        }
        DataType::Int16 => {
            let typed = array
                .as_any()
                .downcast_ref::<Int16Array>()
                .ok_or_else(|| "min/max collect failed to downcast Int16 array".to_string())?;
            for row in 0..typed.len() {
                if !typed.is_null(row) {
                    update_min_max_with_value(min_max, MetaScalarValue::Int16(typed.value(row)))?;
                }
            }
            Ok(())
        }
        DataType::Int32 => {
            let typed = array
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| "min/max collect failed to downcast Int32 array".to_string())?;
            for row in 0..typed.len() {
                if !typed.is_null(row) {
                    update_min_max_with_value(min_max, MetaScalarValue::Int32(typed.value(row)))?;
                }
            }
            Ok(())
        }
        DataType::Int64 => {
            let typed = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| "min/max collect failed to downcast Int64 array".to_string())?;
            for row in 0..typed.len() {
                if !typed.is_null(row) {
                    update_min_max_with_value(min_max, MetaScalarValue::Int64(typed.value(row)))?;
                }
            }
            Ok(())
        }
        DataType::FixedSizeBinary(width) if *width == largeint::LARGEINT_BYTE_WIDTH => {
            let typed = largeint::as_fixed_size_binary_array(
                array,
                "min/max collect downcast LARGEINT column",
            )?;
            for row in 0..typed.len() {
                if !typed.is_null(row) {
                    let value = largeint::value_at(typed, row)?;
                    update_min_max_with_value(min_max, MetaScalarValue::LargeInt(value))?;
                }
            }
            Ok(())
        }
        DataType::Float32 => {
            let typed = array
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| "min/max collect failed to downcast Float32 array".to_string())?;
            for row in 0..typed.len() {
                if !typed.is_null(row) {
                    update_min_max_with_value(min_max, MetaScalarValue::Float32(typed.value(row)))?;
                }
            }
            Ok(())
        }
        DataType::Float64 => {
            let typed = array
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| "min/max collect failed to downcast Float64 array".to_string())?;
            for row in 0..typed.len() {
                if !typed.is_null(row) {
                    update_min_max_with_value(min_max, MetaScalarValue::Float64(typed.value(row)))?;
                }
            }
            Ok(())
        }
        DataType::Boolean => {
            let typed = array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| "min/max collect failed to downcast Boolean array".to_string())?;
            for row in 0..typed.len() {
                if !typed.is_null(row) {
                    update_min_max_with_value(min_max, MetaScalarValue::Boolean(typed.value(row)))?;
                }
            }
            Ok(())
        }
        DataType::Date32 => {
            let typed = array
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(|| "min/max collect failed to downcast Date32 array".to_string())?;
            for row in 0..typed.len() {
                if !typed.is_null(row) {
                    update_min_max_with_value(min_max, MetaScalarValue::Date32(typed.value(row)))?;
                }
            }
            Ok(())
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            let typed = array
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .ok_or_else(|| {
                    "min/max collect failed to downcast Timestamp(Microsecond) array".to_string()
                })?;
            for row in 0..typed.len() {
                if !typed.is_null(row) {
                    update_min_max_with_value(
                        min_max,
                        MetaScalarValue::TimestampMicrosecond(typed.value(row)),
                    )?;
                }
            }
            Ok(())
        }
        DataType::Decimal128(_, _) => {
            let typed = array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .ok_or_else(|| "min/max collect failed to downcast Decimal128 array".to_string())?;
            for row in 0..typed.len() {
                if !typed.is_null(row) {
                    update_min_max_with_value(
                        min_max,
                        MetaScalarValue::Decimal128(typed.value(row)),
                    )?;
                }
            }
            Ok(())
        }
        DataType::Decimal256(_, _) => {
            let typed = array
                .as_any()
                .downcast_ref::<Decimal256Array>()
                .ok_or_else(|| "min/max collect failed to downcast Decimal256 array".to_string())?;
            for row in 0..typed.len() {
                if !typed.is_null(row) {
                    update_min_max_with_value(
                        min_max,
                        MetaScalarValue::Decimal256(typed.value(row)),
                    )?;
                }
            }
            Ok(())
        }
        DataType::Utf8 => {
            let typed = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| "min/max collect failed to downcast Utf8 array".to_string())?;
            for row in 0..typed.len() {
                if !typed.is_null(row) {
                    update_min_max_with_value(
                        min_max,
                        MetaScalarValue::Utf8(typed.value(row).to_string()),
                    )?;
                }
            }
            Ok(())
        }
        DataType::Binary => {
            let typed = array
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| "min/max collect failed to downcast Binary array".to_string())?;
            for row in 0..typed.len() {
                if !typed.is_null(row) {
                    update_min_max_with_value(
                        min_max,
                        MetaScalarValue::Binary(typed.value(row).to_vec()),
                    )?;
                }
            }
            Ok(())
        }
        other => Err(format!(
            "LAKE_META_SCAN_NODE min/max collect does not support output type {:?}",
            other
        )),
    }
}

fn update_min_max_with_value(
    min_max: &mut MetaColumnMinMax,
    value: MetaScalarValue,
) -> Result<(), String> {
    if let Some(current_min) = min_max.min.as_ref() {
        if compare_meta_scalar_values(&value, current_min)? == Ordering::Less {
            min_max.min = Some(value.clone());
        }
    } else {
        min_max.min = Some(value.clone());
    }

    if let Some(current_max) = min_max.max.as_ref() {
        if compare_meta_scalar_values(&value, current_max)? == Ordering::Greater {
            min_max.max = Some(value);
        }
    } else {
        min_max.max = Some(value);
    }
    Ok(())
}

fn compare_meta_scalar_values(
    lhs: &MetaScalarValue,
    rhs: &MetaScalarValue,
) -> Result<Ordering, String> {
    match (lhs, rhs) {
        (MetaScalarValue::Int8(a), MetaScalarValue::Int8(b)) => Ok(a.cmp(b)),
        (MetaScalarValue::Int16(a), MetaScalarValue::Int16(b)) => Ok(a.cmp(b)),
        (MetaScalarValue::Int32(a), MetaScalarValue::Int32(b)) => Ok(a.cmp(b)),
        (MetaScalarValue::Int64(a), MetaScalarValue::Int64(b)) => Ok(a.cmp(b)),
        (MetaScalarValue::LargeInt(a), MetaScalarValue::LargeInt(b)) => Ok(a.cmp(b)),
        (MetaScalarValue::Float32(a), MetaScalarValue::Float32(b)) => {
            a.partial_cmp(b).ok_or_else(|| {
                "LAKE_META_SCAN_NODE cannot compare Float32 values with NaN".to_string()
            })
        }
        (MetaScalarValue::Float64(a), MetaScalarValue::Float64(b)) => {
            a.partial_cmp(b).ok_or_else(|| {
                "LAKE_META_SCAN_NODE cannot compare Float64 values with NaN".to_string()
            })
        }
        (MetaScalarValue::Boolean(a), MetaScalarValue::Boolean(b)) => Ok(a.cmp(b)),
        (MetaScalarValue::Date32(a), MetaScalarValue::Date32(b)) => Ok(a.cmp(b)),
        (MetaScalarValue::TimestampMicrosecond(a), MetaScalarValue::TimestampMicrosecond(b)) => {
            Ok(a.cmp(b))
        }
        (MetaScalarValue::Decimal128(a), MetaScalarValue::Decimal128(b)) => Ok(a.cmp(b)),
        (MetaScalarValue::Decimal256(a), MetaScalarValue::Decimal256(b)) => Ok(a.cmp(b)),
        (MetaScalarValue::Utf8(a), MetaScalarValue::Utf8(b)) => Ok(a.cmp(b)),
        (MetaScalarValue::Binary(a), MetaScalarValue::Binary(b)) => Ok(a.cmp(b)),
        _ => Err(format!(
            "LAKE_META_SCAN_NODE min/max compare type mismatch: left={:?}, right={:?}",
            lhs, rhs
        )),
    }
}

fn build_single_row_scalar_array(
    data_type: &DataType,
    value: Option<&MetaScalarValue>,
) -> Result<ArrayRef, String> {
    match data_type {
        DataType::Int8 => {
            let value = match value {
                Some(MetaScalarValue::Int8(v)) => Some(*v),
                Some(other) => {
                    return Err(format!(
                        "LAKE_META_SCAN_NODE scalar type mismatch: expected Int8, got {:?}",
                        other
                    ));
                }
                None => None,
            };
            Ok(Arc::new(Int8Array::from(vec![value])))
        }
        DataType::Int16 => {
            let value = match value {
                Some(MetaScalarValue::Int16(v)) => Some(*v),
                Some(other) => {
                    return Err(format!(
                        "LAKE_META_SCAN_NODE scalar type mismatch: expected Int16, got {:?}",
                        other
                    ));
                }
                None => None,
            };
            Ok(Arc::new(Int16Array::from(vec![value])))
        }
        DataType::Int32 => {
            let value = match value {
                Some(MetaScalarValue::Int32(v)) => Some(*v),
                Some(other) => {
                    return Err(format!(
                        "LAKE_META_SCAN_NODE scalar type mismatch: expected Int32, got {:?}",
                        other
                    ));
                }
                None => None,
            };
            Ok(Arc::new(Int32Array::from(vec![value])))
        }
        DataType::Int64 => {
            let value = match value {
                Some(MetaScalarValue::Int64(v)) => Some(*v),
                Some(other) => {
                    return Err(format!(
                        "LAKE_META_SCAN_NODE scalar type mismatch: expected Int64, got {:?}",
                        other
                    ));
                }
                None => None,
            };
            Ok(Arc::new(Int64Array::from(vec![value])))
        }
        DataType::FixedSizeBinary(width) if *width == largeint::LARGEINT_BYTE_WIDTH => {
            let value = match value {
                Some(MetaScalarValue::LargeInt(v)) => Some(*v),
                Some(other) => {
                    return Err(format!(
                        "LAKE_META_SCAN_NODE scalar type mismatch: expected LARGEINT, got {:?}",
                        other
                    ));
                }
                None => None,
            };
            largeint::array_from_i128(&[value])
        }
        DataType::Float32 => {
            let value = match value {
                Some(MetaScalarValue::Float32(v)) => Some(*v),
                Some(other) => {
                    return Err(format!(
                        "LAKE_META_SCAN_NODE scalar type mismatch: expected Float32, got {:?}",
                        other
                    ));
                }
                None => None,
            };
            Ok(Arc::new(Float32Array::from(vec![value])))
        }
        DataType::Float64 => {
            let value = match value {
                Some(MetaScalarValue::Float64(v)) => Some(*v),
                Some(other) => {
                    return Err(format!(
                        "LAKE_META_SCAN_NODE scalar type mismatch: expected Float64, got {:?}",
                        other
                    ));
                }
                None => None,
            };
            Ok(Arc::new(Float64Array::from(vec![value])))
        }
        DataType::Boolean => {
            let value = match value {
                Some(MetaScalarValue::Boolean(v)) => Some(*v),
                Some(other) => {
                    return Err(format!(
                        "LAKE_META_SCAN_NODE scalar type mismatch: expected Boolean, got {:?}",
                        other
                    ));
                }
                None => None,
            };
            Ok(Arc::new(BooleanArray::from(vec![value])))
        }
        DataType::Date32 => {
            let value = match value {
                Some(MetaScalarValue::Date32(v)) => Some(*v),
                Some(other) => {
                    return Err(format!(
                        "LAKE_META_SCAN_NODE scalar type mismatch: expected Date32, got {:?}",
                        other
                    ));
                }
                None => None,
            };
            Ok(Arc::new(Date32Array::from(vec![value])))
        }
        DataType::Timestamp(TimeUnit::Microsecond, None) => {
            let value = match value {
                Some(MetaScalarValue::TimestampMicrosecond(v)) => Some(*v),
                Some(other) => {
                    return Err(format!(
                        "LAKE_META_SCAN_NODE scalar type mismatch: expected Timestamp(Microsecond,None), got {:?}",
                        other
                    ));
                }
                None => None,
            };
            Ok(Arc::new(TimestampMicrosecondArray::from(vec![value])))
        }
        DataType::Decimal128(precision, scale) => {
            let value = match value {
                Some(MetaScalarValue::Decimal128(v)) => Some(*v),
                Some(other) => {
                    return Err(format!(
                        "LAKE_META_SCAN_NODE scalar type mismatch: expected Decimal128({},{}), got {:?}",
                        precision, scale, other
                    ));
                }
                None => None,
            };
            let array = Decimal128Array::from(vec![value])
                .with_precision_and_scale(*precision, *scale)
                .map_err(|e| {
                    format!(
                        "LAKE_META_SCAN_NODE set Decimal128 precision/scale failed: precision={}, scale={}, error={}",
                        precision, scale, e
                    )
            })?;
            Ok(Arc::new(array))
        }
        DataType::Decimal256(precision, scale) => {
            let value = match value {
                Some(MetaScalarValue::Decimal256(v)) => Some(*v),
                Some(other) => {
                    return Err(format!(
                        "LAKE_META_SCAN_NODE scalar type mismatch: expected Decimal256({},{}), got {:?}",
                        precision, scale, other
                    ));
                }
                None => None,
            };
            let array = Decimal256Array::from(vec![value])
                .with_precision_and_scale(*precision, *scale)
                .map_err(|e| {
                    format!(
                        "LAKE_META_SCAN_NODE set Decimal256 precision/scale failed: precision={}, scale={}, error={}",
                        precision, scale, e
                    )
                })?;
            Ok(Arc::new(array))
        }
        DataType::Utf8 => {
            let value = match value {
                Some(MetaScalarValue::Utf8(v)) => Some(v.as_str()),
                Some(other) => {
                    return Err(format!(
                        "LAKE_META_SCAN_NODE scalar type mismatch: expected Utf8, got {:?}",
                        other
                    ));
                }
                None => None,
            };
            Ok(Arc::new(StringArray::from(vec![value])))
        }
        DataType::Binary => {
            let value = match value {
                Some(MetaScalarValue::Binary(v)) => Some(v.as_slice()),
                Some(other) => {
                    return Err(format!(
                        "LAKE_META_SCAN_NODE scalar type mismatch: expected Binary, got {:?}",
                        other
                    ));
                }
                None => None,
            };
            Ok(Arc::new(BinaryArray::from(vec![value])))
        }
        other => {
            if value.is_some() {
                return Err(format!(
                    "LAKE_META_SCAN_NODE scalar output for type {:?} is not implemented",
                    other
                ));
            }
            Ok(new_null_array(other, 1))
        }
    }
}

fn collect_words_from_column_array(
    array: &ArrayRef,
    words: &mut BTreeSet<String>,
) -> Result<(), String> {
    match array.data_type() {
        DataType::Utf8 => {
            let string_array = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| "dict_merge collect failed to downcast Utf8 array".to_string())?;
            for row in 0..string_array.len() {
                if !string_array.is_null(row) {
                    words.insert(string_array.value(row).to_string());
                }
            }
            Ok(())
        }
        DataType::List(item) if matches!(item.data_type(), DataType::Utf8) => {
            let list_array = array
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| "dict_merge collect failed to downcast List array".to_string())?;
            let values = list_array
                .values()
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| "dict_merge collect list item type must be Utf8".to_string())?;
            let offsets = list_array.value_offsets();
            for row in 0..list_array.len() {
                if list_array.is_null(row) {
                    continue;
                }
                let start = offsets[row] as usize;
                let end = offsets[row + 1] as usize;
                for idx in start..end {
                    if !values.is_null(idx) {
                        words.insert(values.value(idx).to_string());
                    }
                }
            }
            Ok(())
        }
        other => Err(format!(
            "dict_merge collect only supports Utf8 or List(Utf8), got {:?}",
            other
        )),
    }
}

fn build_metric_mapping(
    _meta: &plan_nodes::TMetaScanNode,
    id_to_names: &std::collections::BTreeMap<i32, String>,
) -> Result<HashMap<i32, MetaMetricKind>, String> {
    let mut metric_by_slot = HashMap::with_capacity(id_to_names.len());
    for (slot_id, metric_name) in id_to_names {
        let metric = parse_metric_name(metric_name)?;
        if metric_by_slot.insert(*slot_id, metric).is_some() {
            return Err(format!(
                "LAKE_META_SCAN_NODE duplicate metric mapping for slot_id={}",
                slot_id
            ));
        }
    }
    Ok(metric_by_slot)
}

fn parse_metric_name(raw_metric: &str) -> Result<MetaMetricKind, String> {
    let metric = raw_metric.trim();
    if metric.is_empty() {
        return Err("LAKE_META_SCAN_NODE metric name is empty".to_string());
    }

    let metric_lower = metric.to_ascii_lowercase();
    if metric_lower.starts_with("rows_") {
        let column_id = metric["rows_".len()..].trim();
        if column_id.is_empty() {
            return Err(format!(
                "LAKE_META_SCAN_NODE invalid metric name '{}': missing column_id suffix",
                metric
            ));
        }
        return Ok(MetaMetricKind::Rows {
            _column_id: column_id.to_string(),
        });
    }

    if metric_lower.starts_with("count_") {
        let column_id = metric["count_".len()..].trim();
        if column_id.is_empty() {
            return Err(format!(
                "LAKE_META_SCAN_NODE invalid metric name '{}': missing column_id suffix",
                metric
            ));
        }
        return Ok(MetaMetricKind::Count {
            column_id: column_id.to_string(),
        });
    }

    if metric_lower.starts_with("dict_merge_") {
        let column_id = metric["dict_merge_".len()..].trim();
        if column_id.is_empty() {
            return Err(format!(
                "LAKE_META_SCAN_NODE invalid metric name '{}': missing column_id suffix",
                metric
            ));
        }
        return Ok(MetaMetricKind::DictMerge {
            column_id: column_id.to_string(),
        });
    }

    if metric_lower.starts_with("min_") {
        let column_id = metric["min_".len()..].trim();
        if column_id.is_empty() {
            return Err(format!(
                "LAKE_META_SCAN_NODE invalid metric name '{}': missing column_id suffix",
                metric
            ));
        }
        return Ok(MetaMetricKind::Min {
            column_id: column_id.to_string(),
        });
    }

    if metric_lower.starts_with("max_") {
        let column_id = metric["max_".len()..].trim();
        if column_id.is_empty() {
            return Err(format!(
                "LAKE_META_SCAN_NODE invalid metric name '{}': missing column_id suffix",
                metric
            ));
        }
        return Ok(MetaMetricKind::Max {
            column_id: column_id.to_string(),
        });
    }

    Err(format!(
        "LAKE_META_SCAN_NODE metric '{}' is not supported (supported prefixes: rows_/count_/dict_merge_/min_/max_)",
        metric
    ))
}
