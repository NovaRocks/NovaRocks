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
//! Iceberg table sink for writing query results.
//!
//! Responsibilities:
//! - Converts output chunks into Iceberg/Parquet writer input and commits generated data files.
//! - Coordinates partitioning, file rolling, and commit metadata publication semantics.
//!
//! Key exported interfaces:
//! - Types: `IcebergTableSinkFactory`.
//!
//! Current limitations:
//! - Implements only the execution semantics currently wired by novarocks plan lowering and pipeline builder.
//! - Unsupported states should be surfaced as explicit runtime errors instead of fallback behavior.

use std::collections::HashMap;
use std::fs;
use std::io::Cursor;
use std::path::PathBuf;
use std::sync::Arc;

use arrow::array::{
    ArrayRef, BinaryArray, Decimal128Array, Int32Array, Int64Array, RecordBatch, StringArray,
    TimestampMicrosecondArray, UInt32Array,
};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use base64::Engine;
use parquet::arrow::ArrowWriter;
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;

use crate::exec::chunk::Chunk;
use crate::exec::expr::{ExprArena, ExprId};
use crate::exec::pipeline::operator::{Operator, ProcessorOperator};
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::lower::expr::lower_t_expr;
use crate::lower::layout::Layout;
use crate::lower::type_lowering::arrow_type_from_desc;
use crate::runtime::global_async_runtime::data_block_on;
use crate::runtime::runtime_state::RuntimeState;
use crate::runtime::starlet_shard_registry::S3StoreConfig;
use crate::{data_sinks, descriptors, exprs, types};

#[derive(Clone)]
/// Factory for Iceberg table sinks that write output chunks into committed table files.
pub struct IcebergTableSinkFactory {
    name: String,
    arena: Arc<ExprArena>,
    plan: Arc<IcebergSinkPlan>,
}

#[derive(Clone)]
struct IcebergSinkPlan {
    data_location: String,
    object_store_s3: Option<S3StoreConfig>,
    file_format: String,
    compression: types::TCompressionType,
    output_schema: SchemaRef,
    output_exprs: Vec<ExprId>,
    partition_exprs: Vec<ExprId>,
    partition_column_names: Vec<String>,
    transform_exprs: Vec<String>,
}

impl IcebergTableSinkFactory {
    pub(crate) fn try_new(
        sink: data_sinks::TIcebergTableSink,
        output_exprs: &[exprs::TExpr],
        layout: &Layout,
        desc_tbl: &descriptors::TDescriptorTable,
        last_query_id: Option<&str>,
        fe_addr: Option<&types::TNetworkAddress>,
    ) -> Result<Self, String> {
        let mut arena = ExprArena::default();
        let lowered_output_exprs =
            lower_output_exprs(output_exprs, &mut arena, layout, last_query_id, fe_addr)?;

        let iceberg_table = resolve_iceberg_table(desc_tbl, sink.target_table_id)?;
        let output_schema = build_output_schema(&iceberg_table)?;

        if output_exprs.len() != output_schema.fields().len() {
            return Err(format!(
                "iceberg sink output expr count mismatch: exprs={} columns={}",
                output_exprs.len(),
                output_schema.fields().len()
            ));
        }

        let (partition_column_names, transform_exprs, mut partition_exprs) =
            build_partition_exprs(&iceberg_table)?;
        if !partition_exprs.is_empty() {
            let slot_map = build_column_slot_map(
                output_exprs,
                desc_tbl,
                sink.tuple_id
                    .ok_or_else(|| "iceberg sink missing tuple_id".to_string())?,
            )?;
            update_partition_expr_slot_refs(&mut partition_exprs, &slot_map, &iceberg_table)?;
        }
        let lowered_partition_exprs =
            lower_partition_exprs(&partition_exprs, &mut arena, layout, last_query_id, fe_addr)?;

        let data_location = resolve_data_location(&sink)?;
        let object_store_s3 = resolve_sink_s3_config(&sink, &data_location)?;
        let file_format = sink
            .file_format
            .clone()
            .ok_or_else(|| "iceberg sink missing file_format".to_string())?;
        if file_format.to_lowercase() != "parquet" {
            return Err(format!(
                "iceberg sink only supports parquet, got {}",
                file_format
            ));
        }

        let plan = IcebergSinkPlan {
            data_location,
            object_store_s3,
            file_format,
            compression: sink
                .compression_type
                .ok_or_else(|| "iceberg sink missing compression_type".to_string())?,
            output_schema,
            output_exprs: lowered_output_exprs,
            partition_exprs: lowered_partition_exprs,
            partition_column_names,
            transform_exprs,
        };

        Ok(Self {
            name: "ICEBERG_TABLE_SINK".to_string(),
            arena: Arc::new(arena),
            plan: Arc::new(plan),
        })
    }
}

impl OperatorFactory for IcebergTableSinkFactory {
    fn name(&self) -> &str {
        &self.name
    }

    fn create(&self, _dop: i32, driver_id: i32) -> Box<dyn Operator> {
        Box::new(IcebergTableSinkOperator {
            name: self.name.clone(),
            arena: Arc::clone(&self.arena),
            plan: Arc::clone(&self.plan),
            driver_id,
            file_seq: 0,
            finished: false,
        })
    }

    fn is_sink(&self) -> bool {
        true
    }
}

struct IcebergTableSinkOperator {
    name: String,
    arena: Arc<ExprArena>,
    plan: Arc<IcebergSinkPlan>,
    driver_id: i32,
    file_seq: u64,
    finished: bool,
}

impl Operator for IcebergTableSinkOperator {
    fn name(&self) -> &str {
        &self.name
    }

    fn as_processor_mut(&mut self) -> Option<&mut dyn ProcessorOperator> {
        Some(self)
    }

    fn as_processor_ref(&self) -> Option<&dyn ProcessorOperator> {
        Some(self)
    }

    fn is_finished(&self) -> bool {
        self.finished
    }
}

impl ProcessorOperator for IcebergTableSinkOperator {
    fn need_input(&self) -> bool {
        !self.finished
    }

    fn has_output(&self) -> bool {
        false
    }

    fn push_chunk(&mut self, state: &RuntimeState, chunk: Chunk) -> Result<(), String> {
        if self.finished {
            return Ok(());
        }
        if chunk.is_empty() {
            return Ok(());
        }

        let output_arrays = eval_exprs(&self.arena, &self.plan.output_exprs, &chunk)?;
        let output_arrays = align_arrays_to_schema(output_arrays, &self.plan.output_schema)?;
        let batch = RecordBatch::try_new(Arc::clone(&self.plan.output_schema), output_arrays)
            .map_err(|e| format!("iceberg sink build batch failed: {e}"))?;

        let mut partition_groups = HashMap::new();
        if self.plan.partition_exprs.is_empty() {
            partition_groups.insert(
                PartitionKey::default(),
                PartitionGroup {
                    indices: (0..batch.num_rows() as u32).collect(),
                },
            );
        } else {
            let partition_arrays = eval_exprs(&self.arena, &self.plan.partition_exprs, &chunk)?;
            for row in 0..batch.num_rows() {
                let (partition, fingerprint) = iceberg_partition_key_for_row(
                    &self.plan.partition_column_names,
                    &self.plan.transform_exprs,
                    &partition_arrays,
                    row,
                )?;
                let key = PartitionKey {
                    path: partition,
                    null_fingerprint: fingerprint,
                };
                partition_groups
                    .entry(key)
                    .or_insert_with(|| PartitionGroup {
                        indices: Vec::new(),
                    })
                    .indices
                    .push(row as u32);
            }
        }

        for (key, group) in partition_groups {
            let indices = UInt32Array::from(group.indices);
            let part_batch = arrow::compute::take_record_batch(&batch, &indices)
                .map_err(|e| format!("iceberg sink take batch failed: {e}"))?;
            if part_batch.num_rows() == 0 {
                continue;
            }
            let (file_path, partition_path) = self.build_file_path(state, &key.path)?;
            let file_size = write_parquet_file(
                &file_path,
                self.plan.object_store_s3.as_ref(),
                Arc::clone(&self.plan.output_schema),
                &part_batch,
                self.plan.compression,
            )?;

            let data_file = types::TIcebergDataFile {
                path: Some(file_path),
                format: Some(self.plan.file_format.clone()),
                record_count: Some(part_batch.num_rows() as i64),
                file_size_in_bytes: Some(file_size as i64),
                partition_path: Some(partition_path),
                split_offsets: None,
                column_stats: None,
                partition_null_fingerprint: Some(key.null_fingerprint),
                file_content: Some(types::TIcebergFileContent::DATA),
                referenced_data_file: None,
            };

            let commit_info = types::TSinkCommitInfo {
                iceberg_data_file: Some(data_file),
                hive_file_info: None,
                is_overwrite: None,
                staging_dir: None,
                is_rewrite: None,
            };
            state.add_sink_commit_info(commit_info);
        }

        Ok(())
    }

    fn pull_chunk(&mut self, _state: &RuntimeState) -> Result<Option<Chunk>, String> {
        Ok(None)
    }

    fn set_finishing(&mut self, _state: &RuntimeState) -> Result<(), String> {
        self.finished = true;
        Ok(())
    }
}

impl IcebergTableSinkOperator {
    fn build_file_path(
        &mut self,
        state: &RuntimeState,
        partition: &str,
    ) -> Result<(String, String), String> {
        let base = normalize_path(&self.plan.data_location)?;
        let base = base.trim_end_matches('/');
        let finst = state
            .fragment_instance_id()
            .map(|id| format!("{:x}_{:x}", id.hi, id.lo))
            .unwrap_or_else(|| "finst_unknown".to_string());
        let file_name = format!(
            "data-{}-driver{}-{}.parquet",
            finst, self.driver_id, self.file_seq
        );
        self.file_seq = self.file_seq.saturating_add(1);

        if partition.is_empty() {
            let path = format!("{base}/{file_name}");
            Ok((path.clone(), base.to_string()))
        } else {
            let partition_path = format!("{base}/{partition}");
            let path = format!("{partition_path}{file_name}");
            Ok((path, partition_path.trim_end_matches('/').to_string()))
        }
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Hash)]
struct PartitionKey {
    path: String,
    null_fingerprint: String,
}

#[derive(Debug)]
struct PartitionGroup {
    indices: Vec<u32>,
}

fn lower_output_exprs(
    output_exprs: &[exprs::TExpr],
    arena: &mut ExprArena,
    layout: &Layout,
    last_query_id: Option<&str>,
    fe_addr: Option<&types::TNetworkAddress>,
) -> Result<Vec<ExprId>, String> {
    if output_exprs.is_empty() {
        return Err("iceberg sink missing output exprs".to_string());
    }
    let mut ids = Vec::with_capacity(output_exprs.len());
    for expr in output_exprs {
        let id = lower_t_expr(expr, arena, layout, last_query_id, fe_addr)?;
        ids.push(id);
    }
    Ok(ids)
}

fn lower_partition_exprs(
    partition_exprs: &[exprs::TExpr],
    arena: &mut ExprArena,
    layout: &Layout,
    last_query_id: Option<&str>,
    fe_addr: Option<&types::TNetworkAddress>,
) -> Result<Vec<ExprId>, String> {
    let mut ids = Vec::with_capacity(partition_exprs.len());
    for expr in partition_exprs {
        let id = lower_t_expr(expr, arena, layout, last_query_id, fe_addr)?;
        ids.push(id);
    }
    Ok(ids)
}

fn eval_exprs(arena: &ExprArena, exprs: &[ExprId], chunk: &Chunk) -> Result<Vec<ArrayRef>, String> {
    let mut out = Vec::with_capacity(exprs.len());
    for expr in exprs {
        out.push(arena.eval(*expr, chunk)?);
    }
    Ok(out)
}

fn align_arrays_to_schema(
    arrays: Vec<ArrayRef>,
    schema: &SchemaRef,
) -> Result<Vec<ArrayRef>, String> {
    if arrays.len() != schema.fields().len() {
        return Err(format!(
            "iceberg sink column count mismatch while aligning arrays: arrays={} schema={}",
            arrays.len(),
            schema.fields().len()
        ));
    }

    arrays
        .into_iter()
        .zip(schema.fields().iter())
        .enumerate()
        .map(|(idx, (array, field))| {
            let target_type = field.data_type();
            if array.data_type() == target_type {
                return Ok(array);
            }

            let casted = cast(array.as_ref(), target_type).map_err(|e| {
                format!(
                    "iceberg sink cast failed at column index {} name={} from {:?} to {:?}: {}",
                    idx,
                    field.name(),
                    array.data_type(),
                    target_type,
                    e
                )
            })?;

            if casted.null_count() > array.null_count() {
                return Err(format!(
                    "iceberg sink cast introduced nulls at column index {} name={} from {:?} to {:?}",
                    idx,
                    field.name(),
                    array.data_type(),
                    target_type
                ));
            }
            Ok(casted)
        })
        .collect()
}

fn resolve_iceberg_table(
    desc_tbl: &descriptors::TDescriptorTable,
    table_id: Option<i64>,
) -> Result<descriptors::TIcebergTable, String> {
    let table_id = table_id.ok_or_else(|| "iceberg sink missing target_table_id".to_string())?;
    let tables = desc_tbl
        .table_descriptors
        .as_ref()
        .ok_or_else(|| "descriptor table missing table_descriptors".to_string())?;
    for table in tables {
        if table.id == table_id {
            let iceberg = table
                .iceberg_table
                .as_ref()
                .ok_or_else(|| "table descriptor missing iceberg_table".to_string())?;
            return Ok(iceberg.clone());
        }
    }
    Err(format!(
        "iceberg table descriptor not found for table_id={table_id}"
    ))
}

fn build_output_schema(iceberg: &descriptors::TIcebergTable) -> Result<SchemaRef, String> {
    let columns = iceberg
        .columns
        .as_ref()
        .ok_or_else(|| "iceberg table missing columns".to_string())?;
    if let Some(schema) = iceberg.iceberg_schema.as_ref() {
        let schema_fields = schema
            .fields
            .as_ref()
            .ok_or_else(|| "iceberg schema missing fields".to_string())?;
        let mut fields = Vec::with_capacity(schema_fields.len());
        for schema_field in schema_fields {
            let name = schema_field
                .name
                .as_ref()
                .ok_or_else(|| "iceberg schema field missing name".to_string())?;
            let col = columns
                .iter()
                .find(|c| &c.column_name == name)
                .ok_or_else(|| {
                    format!("iceberg schema field {} missing column descriptor", name)
                })?;
            let dtype = col
                .type_desc
                .as_ref()
                .and_then(arrow_type_from_desc)
                .ok_or_else(|| format!("iceberg column {} missing type_desc", col.column_name))?;
            let nullable = col.is_allow_null.unwrap_or(true);
            let field = Field::new(col.column_name.clone(), dtype, nullable);
            let field = apply_field_id_recursive(field, schema_field)?;
            fields.push(field);
        }
        return Ok(Arc::new(Schema::new(fields)));
    }

    let mut fields = Vec::with_capacity(columns.len());
    for col in columns {
        let dtype = col
            .type_desc
            .as_ref()
            .and_then(arrow_type_from_desc)
            .ok_or_else(|| format!("iceberg column {} missing type_desc", col.column_name))?;
        let nullable = col.is_allow_null.unwrap_or(true);
        fields.push(Field::new(col.column_name.clone(), dtype, nullable));
    }

    Ok(Arc::new(Schema::new(fields)))
}

fn apply_field_id_recursive(
    field: Field,
    schema_field: &descriptors::TIcebergSchemaField,
) -> Result<Field, String> {
    let field_id = schema_field
        .field_id
        .ok_or_else(|| format!("iceberg schema field {} missing field_id", field.name()))?;
    let mut meta = field.metadata().clone();
    meta.insert(PARQUET_FIELD_ID_META_KEY.to_string(), field_id.to_string());
    let data_type = match field.data_type() {
        DataType::Struct(children) => {
            let schema_children = schema_field
                .children
                .as_ref()
                .ok_or_else(|| format!("iceberg schema field {} missing children", field.name()))?;
            if children.len() != schema_children.len() {
                return Err(format!(
                    "iceberg schema children mismatch for {}: fields={} schema_fields={}",
                    field.name(),
                    children.len(),
                    schema_children.len()
                ));
            }
            let mut new_children = Vec::with_capacity(children.len());
            for (child, schema_child) in children.iter().zip(schema_children.iter()) {
                let new_child = apply_field_id_recursive(child.as_ref().clone(), schema_child)?;
                new_children.push(new_child);
            }
            DataType::Struct(new_children.into())
        }
        DataType::List(child) => {
            let schema_children = schema_field
                .children
                .as_ref()
                .ok_or_else(|| format!("iceberg schema field {} missing children", field.name()))?;
            if schema_children.len() != 1 {
                return Err(format!(
                    "iceberg schema list field {} should have 1 child, got {}",
                    field.name(),
                    schema_children.len()
                ));
            }
            let new_child = apply_field_id_recursive(child.as_ref().clone(), &schema_children[0])?;
            DataType::List(Arc::new(new_child))
        }
        DataType::Map(entries, sorted) => {
            let schema_children = schema_field
                .children
                .as_ref()
                .ok_or_else(|| format!("iceberg schema field {} missing children", field.name()))?;
            if schema_children.len() != 2 {
                return Err(format!(
                    "iceberg schema map field {} should have 2 children, got {}",
                    field.name(),
                    schema_children.len()
                ));
            }
            let entries_field = entries.as_ref();
            let entry_fields = match entries_field.data_type() {
                DataType::Struct(fields) => fields,
                _ => {
                    return Err(format!(
                        "iceberg map field {} has non-struct entries",
                        field.name()
                    ));
                }
            };
            if entry_fields.len() != 2 {
                return Err(format!(
                    "iceberg map field {} entries should have 2 fields",
                    field.name()
                ));
            }
            let key_field =
                apply_field_id_recursive(entry_fields[0].as_ref().clone(), &schema_children[0])?;
            let value_field =
                apply_field_id_recursive(entry_fields[1].as_ref().clone(), &schema_children[1])?;
            let entries_struct = DataType::Struct(vec![key_field, value_field].into());
            let entries_field = Field::new(
                entries_field.name(),
                entries_struct,
                entries_field.is_nullable(),
            );
            DataType::Map(Arc::new(entries_field), *sorted)
        }
        other => other.clone(),
    };
    Ok(Field::new(field.name(), data_type, field.is_nullable()).with_metadata(meta))
}

fn build_partition_exprs(
    iceberg: &descriptors::TIcebergTable,
) -> Result<(Vec<String>, Vec<String>, Vec<exprs::TExpr>), String> {
    let mut partition_column_names = Vec::new();
    let mut transform_exprs = Vec::new();
    let mut exprs = Vec::new();
    if let Some(partition_info) = iceberg.partition_info.as_ref() {
        for info in partition_info {
            let name = info.partition_column_name.clone().ok_or_else(|| {
                "iceberg partition_info missing partition_column_name".to_string()
            })?;
            let transform = info
                .transform_expr
                .clone()
                .ok_or_else(|| "iceberg partition_info missing transform_expr".to_string())?;
            let expr = info
                .partition_expr
                .clone()
                .ok_or_else(|| "iceberg partition_info missing partition_expr".to_string())?;
            partition_column_names.push(name);
            transform_exprs.push(transform);
            exprs.push(expr);
        }
    }
    Ok((partition_column_names, transform_exprs, exprs))
}

fn build_column_slot_map(
    output_exprs: &[exprs::TExpr],
    desc_tbl: &descriptors::TDescriptorTable,
    tuple_id: i32,
) -> Result<HashMap<String, exprs::TExprNode>, String> {
    let slots = desc_tbl
        .slot_descriptors
        .as_ref()
        .ok_or_else(|| "descriptor table missing slot_descriptors".to_string())?;
    let mut tuple_slots = Vec::new();
    for slot in slots {
        if slot.parent == Some(tuple_id) {
            tuple_slots.push(slot);
        }
    }
    if tuple_slots.len() != output_exprs.len() {
        return Err(format!(
            "iceberg sink slot count mismatch: slots={} output_exprs={}",
            tuple_slots.len(),
            output_exprs.len()
        ));
    }

    let mut map = HashMap::new();
    for (slot, expr) in tuple_slots.iter().zip(output_exprs.iter()) {
        let col_name = slot
            .col_name
            .clone()
            .ok_or_else(|| "slot descriptor missing col_name".to_string())?;
        let mut slot_ref = None;
        for node in &expr.nodes {
            if node.node_type == exprs::TExprNodeType::SLOT_REF {
                slot_ref = Some(node.clone());
                break;
            }
        }
        let slot_ref = slot_ref
            .ok_or_else(|| format!("output expr for column {} missing SLOT_REF node", col_name))?;
        map.insert(col_name, slot_ref);
    }
    Ok(map)
}

fn update_partition_expr_slot_refs(
    partition_exprs: &mut [exprs::TExpr],
    column_slot_map: &HashMap<String, exprs::TExprNode>,
    iceberg: &descriptors::TIcebergTable,
) -> Result<(), String> {
    let Some(partition_info) = iceberg.partition_info.as_ref() else {
        return Ok(());
    };
    if partition_exprs.len() != partition_info.len() {
        return Err(format!(
            "partition expr count mismatch: exprs={} partition_info={}",
            partition_exprs.len(),
            partition_info.len()
        ));
    }
    for (expr, info) in partition_exprs.iter_mut().zip(partition_info.iter()) {
        let source_name = info
            .source_column_name
            .as_ref()
            .ok_or_else(|| "partition_info missing source_column_name".to_string())?;
        let slot_ref = column_slot_map.get(source_name).ok_or_else(|| {
            format!(
                "partition source column {} missing slot_ref in output exprs",
                source_name
            )
        })?;
        let mut replaced = false;
        for node in &mut expr.nodes {
            if node.node_type == exprs::TExprNodeType::SLOT_REF {
                *node = slot_ref.clone();
                replaced = true;
                break;
            }
        }
        if !replaced {
            return Err(format!(
                "partition expr for {} missing SLOT_REF node",
                source_name
            ));
        }
    }
    Ok(())
}

fn resolve_data_location(sink: &data_sinks::TIcebergTableSink) -> Result<String, String> {
    if let Some(loc) = sink.data_location.as_ref().filter(|s| !s.is_empty()) {
        return Ok(loc.clone());
    }
    let location = sink
        .location
        .as_ref()
        .ok_or_else(|| "iceberg sink missing table location".to_string())?;
    let base = location.trim_end_matches('/');
    Ok(format!("{base}/data"))
}

fn parse_object_store_bucket_and_root(path: &str) -> Option<(String, String)> {
    for scheme in ["s3://", "oss://"] {
        if let Some(rest) = path.trim().strip_prefix(scheme) {
            let (bucket, key_prefix) = rest.split_once('/').unwrap_or((rest, ""));
            let bucket = bucket.trim();
            if bucket.is_empty() {
                return None;
            }
            return Some((bucket.to_string(), key_prefix.trim_matches('/').to_string()));
        }
    }
    None
}

fn parse_true_false(value: &str) -> Option<bool> {
    let trimmed = value.trim();
    if trimmed.eq_ignore_ascii_case("true") || trimmed == "1" {
        return Some(true);
    }
    if trimmed.eq_ignore_ascii_case("false") || trimmed == "0" {
        return Some(false);
    }
    None
}

fn resolve_sink_s3_config(
    sink: &data_sinks::TIcebergTableSink,
    data_location: &str,
) -> Result<Option<S3StoreConfig>, String> {
    let Some((bucket, root)) = parse_object_store_bucket_and_root(data_location) else {
        return Ok(None);
    };
    let cloud = sink.cloud_configuration.as_ref().ok_or_else(|| {
        format!(
            "iceberg sink object-store path requires cloud_configuration: data_location={data_location}"
        )
    })?;
    let props = cloud
        .cloud_properties
        .as_ref()
        .ok_or_else(|| "iceberg sink cloud_configuration.cloud_properties is empty".to_string())?;

    let endpoint = props
        .get("aws.s3.endpoint")
        .or_else(|| props.get("aws.s3.endpoint_url"))
        .map(|v| v.trim())
        .filter(|v| !v.is_empty())
        .ok_or_else(|| "iceberg sink cloud_properties missing aws.s3.endpoint".to_string())?
        .to_string();
    let access_key_id = props
        .get("aws.s3.accessKeyId")
        .or_else(|| props.get("aws.s3.access_key"))
        .map(|v| v.trim())
        .filter(|v| !v.is_empty())
        .ok_or_else(|| {
            "iceberg sink cloud_properties missing aws.s3.accessKeyId/aws.s3.access_key".to_string()
        })?
        .to_string();
    let access_key_secret = props
        .get("aws.s3.accessKeySecret")
        .or_else(|| props.get("aws.s3.secret_key"))
        .map(|v| v.trim())
        .filter(|v| !v.is_empty())
        .ok_or_else(|| {
            "iceberg sink cloud_properties missing aws.s3.accessKeySecret/aws.s3.secret_key"
                .to_string()
        })?
        .to_string();
    let region = props
        .get("aws.s3.region")
        .map(|v| v.trim())
        .filter(|v| !v.is_empty())
        .map(|v| v.to_string());
    let enable_path_style_access = props
        .get("aws.s3.enable_path_style_access")
        .and_then(|v| parse_true_false(v));

    Ok(Some(S3StoreConfig {
        endpoint,
        bucket,
        root,
        access_key_id,
        access_key_secret,
        region,
        enable_path_style_access,
    }))
}

fn normalize_path(path: &str) -> Result<String, String> {
    if path.starts_with("file:") {
        let url = url::Url::parse(path).map_err(|e| format!("invalid file url: {e}"))?;
        let p = url
            .to_file_path()
            .map_err(|_| "file url is not a valid local path".to_string())?;
        return Ok(p.to_string_lossy().to_string());
    }
    Ok(path.to_string())
}

fn write_parquet_file(
    path: &str,
    s3_config: Option<&S3StoreConfig>,
    schema: SchemaRef,
    batch: &RecordBatch,
    compression: types::TCompressionType,
) -> Result<u64, String> {
    let compression = map_parquet_compression(compression)?;
    let props = WriterProperties::builder()
        .set_compression(compression)
        .build();

    if path.starts_with("oss://") || path.starts_with("s3://") {
        let data = write_parquet_to_bytes(schema, batch, props)?;
        let size = data.len() as u64;
        let s3 = s3_config.ok_or_else(|| {
            format!(
                "iceberg sink missing S3 config for object-store path={path}; \
                expected sink cloud_configuration to provide credentials"
            )
        })?;
        let object_store_cfg = s3.to_object_store_config();
        let op =
            crate::fs::oss::build_oss_operator(&object_store_cfg).map_err(|e| e.to_string())?;
        let rel = crate::fs::oss::normalize_oss_path(
            path,
            &object_store_cfg.bucket,
            &object_store_cfg.root,
        )?;
        data_block_on(op.write(&rel, data))
            .map_err(|e| format!("run object-store write on data runtime failed: {e}"))?
            .map_err(|e| format!("opendal write failed: {e}"))?;
        return Ok(size);
    }

    let path_buf = PathBuf::from(path);
    if let Some(parent) = path_buf.parent() {
        fs::create_dir_all(parent).map_err(|e| format!("create parquet dir failed: {e}"))?;
    }
    let file =
        fs::File::create(&path_buf).map_err(|e| format!("create parquet file failed: {e}"))?;
    let mut writer = ArrowWriter::try_new(file, schema, Some(props))
        .map_err(|e| format!("create parquet writer failed: {e}"))?;
    writer
        .write(batch)
        .map_err(|e| format!("write parquet batch failed: {e}"))?;
    writer
        .close()
        .map_err(|e| format!("close parquet writer failed: {e}"))?;
    let meta = fs::metadata(&path_buf).map_err(|e| format!("stat parquet file failed: {e}"))?;
    Ok(meta.len())
}

fn write_parquet_to_bytes(
    schema: SchemaRef,
    batch: &RecordBatch,
    props: WriterProperties,
) -> Result<Vec<u8>, String> {
    let mut buffer = Vec::new();
    {
        let cursor = Cursor::new(&mut buffer);
        let mut writer = ArrowWriter::try_new(cursor, schema, Some(props))
            .map_err(|e| format!("create parquet writer failed: {e}"))?;
        writer
            .write(batch)
            .map_err(|e| format!("write parquet batch failed: {e}"))?;
        writer
            .close()
            .map_err(|e| format!("close parquet writer failed: {e}"))?;
    }
    Ok(buffer)
}

fn map_parquet_compression(compression: types::TCompressionType) -> Result<Compression, String> {
    use types::TCompressionType as C;
    match compression {
        C::NO_COMPRESSION => Ok(Compression::UNCOMPRESSED),
        C::SNAPPY => Ok(Compression::SNAPPY),
        C::LZ4 | C::LZ4_FRAME => Ok(Compression::LZ4),
        C::ZSTD => Ok(Compression::ZSTD(Default::default())),
        C::GZIP | C::ZLIB | C::DEFLATE => Ok(Compression::GZIP(Default::default())),
        C::BROTLI => Ok(Compression::BROTLI(Default::default())),
        C::LZO => Ok(Compression::LZO),
        other => Err(format!(
            "unsupported compression type for iceberg parquet sink: {:?}",
            other
        )),
    }
}

fn iceberg_partition_key_for_row(
    partition_column_names: &[String],
    transform_exprs: &[String],
    partition_arrays: &[ArrayRef],
    row: usize,
) -> Result<(String, String), String> {
    if partition_column_names.len() != transform_exprs.len()
        || partition_arrays.len() != partition_column_names.len()
    {
        return Err("partition arrays mismatch for iceberg sink".to_string());
    }
    let mut path = String::new();
    let mut nulls = String::with_capacity(partition_column_names.len());
    for i in 0..partition_column_names.len() {
        let transform = transform_exprs[i].to_lowercase();
        let base = transform.split('[').next().unwrap_or(transform.as_str());
        let is_null = base == "void" || partition_arrays[i].is_null(row);
        let value = iceberg_partition_value(base, &partition_arrays[i], row)?;
        nulls.push(if is_null { '1' } else { '0' });
        path.push_str(&partition_column_names[i]);
        path.push('=');
        path.push_str(&value);
        path.push('/');
    }
    Ok((path, nulls))
}

fn iceberg_partition_value(
    transform: &str,
    array: &ArrayRef,
    row: usize,
) -> Result<String, String> {
    if array.is_null(row) || transform == "void" {
        return Ok("null".to_string());
    }
    match transform {
        "year" => {
            let value = array_value_as_i64(array, row)?;
            Ok((value + 1970).to_string())
        }
        "month" => {
            let value = array_value_as_i64(array, row)?;
            let year = 1970 + (value / 12);
            let month = value % 12 + 1;
            Ok(format!("{:04}-{:02}", year, month))
        }
        "day" => {
            let value = array_value_as_i64(array, row)?;
            let days =
                i32::try_from(value).map_err(|_| "day transform value out of range".to_string())?;
            let date = chrono::NaiveDate::from_num_days_from_ce_opt(719_163 + days)
                .ok_or_else(|| "invalid day transform value".to_string())?;
            Ok(date.format("%Y-%m-%d").to_string())
        }
        "hour" => {
            let value = array_value_as_i64(array, row)?;
            let seconds = value * 3600;
            let dt = chrono::DateTime::<chrono::Utc>::from_timestamp(seconds, 0)
                .ok_or_else(|| "invalid hour transform value".to_string())?
                .naive_utc();
            Ok(dt.format("%Y-%m-%d-%H").to_string())
        }
        "truncate" | "bucket" | "identity" => column_value(array, row),
        other => Err(format!("unsupported iceberg partition transform: {other}")),
    }
}

fn array_value_as_i64(array: &ArrayRef, row: usize) -> Result<i64, String> {
    match array.data_type() {
        DataType::Int32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| "expected INT array".to_string())?;
            Ok(i64::from(arr.value(row)))
        }
        DataType::Int64 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| "expected BIGINT array".to_string())?;
            Ok(arr.value(row))
        }
        other => Err(format!(
            "iceberg partition transform expects INT/BIGINT, got {other:?}"
        )),
    }
}

fn column_value(array: &ArrayRef, row: usize) -> Result<String, String> {
    match array.data_type() {
        DataType::Boolean => {
            let arr = array
                .as_any()
                .downcast_ref::<arrow::array::BooleanArray>()
                .ok_or_else(|| "expected BOOLEAN array".to_string())?;
            Ok(if arr.value(row) { "true" } else { "false" }.to_string())
        }
        DataType::Int8 => {
            let arr = array
                .as_any()
                .downcast_ref::<arrow::array::Int8Array>()
                .ok_or_else(|| "expected TINYINT array".to_string())?;
            Ok(arr.value(row).to_string())
        }
        DataType::Int16 => {
            let arr = array
                .as_any()
                .downcast_ref::<arrow::array::Int16Array>()
                .ok_or_else(|| "expected SMALLINT array".to_string())?;
            Ok(arr.value(row).to_string())
        }
        DataType::Int32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| "expected INT array".to_string())?;
            Ok(arr.value(row).to_string())
        }
        DataType::Int64 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| "expected BIGINT array".to_string())?;
            Ok(arr.value(row).to_string())
        }
        DataType::Date32 => {
            let arr = array
                .as_any()
                .downcast_ref::<arrow::array::Date32Array>()
                .ok_or_else(|| "expected DATE array".to_string())?;
            let days = arr.value(row);
            let date = chrono::NaiveDate::from_num_days_from_ce_opt(719_163 + days)
                .ok_or_else(|| "invalid Date32 value".to_string())?;
            Ok(date.format("%Y-%m-%d").to_string())
        }
        DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None) => {
            let arr = array
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .ok_or_else(|| "expected DATETIME array".to_string())?;
            let micros = arr.value(row);
            let secs = micros.div_euclid(1_000_000);
            let rem = micros.rem_euclid(1_000_000);
            let nanos = (rem as u32) * 1000;
            let dt = chrono::DateTime::<chrono::Utc>::from_timestamp(secs, nanos)
                .ok_or_else(|| "invalid DATETIME value".to_string())?
                .naive_utc();
            Ok(url_encode(&format_datetime(dt)))
        }
        DataType::Utf8 => {
            let arr = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| "expected VARCHAR array".to_string())?;
            Ok(url_encode(arr.value(row)))
        }
        DataType::Binary => {
            let arr = array
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| "expected BINARY array".to_string())?;
            let bytes = arr.value(row);
            let encoded = base64::engine::general_purpose::STANDARD.encode(bytes);
            Ok(url_encode(&encoded))
        }
        DataType::Decimal128(_, _) => {
            let arr = array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .ok_or_else(|| "expected DECIMAL array".to_string())?;
            Ok(arr.value_as_string(row))
        }
        other => Err(format!(
            "unsupported iceberg partition column type: {other:?}"
        )),
    }
}

fn url_encode(input: &str) -> String {
    url::form_urlencoded::byte_serialize(input.as_bytes()).collect()
}

fn format_datetime(dt: chrono::NaiveDateTime) -> String {
    let micros = dt.and_utc().timestamp_subsec_micros();
    if micros == 0 {
        dt.format("%Y-%m-%d %H:%M:%S").to_string()
    } else {
        dt.format("%Y-%m-%d %H:%M:%S%.6f").to_string()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Array, ArrayRef, Int32Array, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};

    use super::align_arrays_to_schema;

    #[test]
    fn test_align_arrays_to_schema_casts_int64_to_int32() {
        let schema = Arc::new(Schema::new(vec![Field::new("c0", DataType::Int32, true)]));
        let arrays: Vec<ArrayRef> = vec![Arc::new(Int64Array::from(vec![Some(1), None, Some(2)]))];

        let aligned = align_arrays_to_schema(arrays, &schema).expect("align arrays");
        assert_eq!(aligned.len(), 1);
        assert_eq!(aligned[0].data_type(), &DataType::Int32);

        let out = aligned[0]
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("int32 array");
        assert_eq!(out.len(), 3);
        assert_eq!(out.value(0), 1);
        assert!(out.is_null(1));
        assert_eq!(out.value(2), 2);
    }

    #[test]
    fn test_align_arrays_to_schema_rejects_lossy_cast() {
        let schema = Arc::new(Schema::new(vec![Field::new("c0", DataType::Int32, true)]));
        let arrays: Vec<ArrayRef> = vec![Arc::new(Int64Array::from(vec![Some(i64::MAX)]))];

        let err = align_arrays_to_schema(arrays, &schema).expect_err("should fail");
        assert!(err.contains("introduced nulls"));
    }
}
