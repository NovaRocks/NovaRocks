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
use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{ArrayRef, StringArray};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use csv::{ReaderBuilder, Terminator, Trim};
use serde_json::Value;

use crate::common::ids::SlotId;
use crate::exec::chunk::{Chunk, field_with_slot_id};
use crate::exec::expr::{ExprArena, ExprId};
use crate::exec::node::scan::{RuntimeFilterContext, ScanMorsel, ScanMorsels, ScanNode, ScanOp};
use crate::exec::node::{BoxedExecIter, ExecNode, ExecNodeKind};
use crate::fs::scan_context::FileScanRange;
use crate::lower::expr::lower_t_expr_with_common_slot_map;
use crate::lower::layout::{Layout, layout_from_slot_ids, slot_name_from_desc};
use crate::lower::node::{Lowered, local_rf_waiting_set};
use crate::lower::type_lowering::arrow_type_from_desc;
use crate::service::stream_load;
use crate::{descriptors, internal_service, plan_nodes, types};

#[derive(Clone, Debug)]
struct CsvReadOptions {
    column_delimiter: String,
    row_delimiter: String,
    skip_header: usize,
    trim_space: bool,
    enclose: Option<u8>,
    escape: Option<u8>,
}

#[derive(Clone, Debug)]
struct JsonReadOptions {
    strip_outer_array: bool,
    jsonpaths: Vec<String>,
}

#[derive(Clone, Debug)]
struct FileLoadScanConfig {
    ranges: Vec<FileScanRange>,
    has_more: bool,
    format_type: plan_nodes::TFileFormatType,
    source_slot_ids: Vec<SlotId>,
    output_slot_ids: Vec<SlotId>,
    output_field_names: Vec<String>,
    output_field_types: Vec<DataType>,
    output_exprs: Vec<ExprId>,
    csv: Option<CsvReadOptions>,
    json: Option<JsonReadOptions>,
}

#[derive(Clone, Debug)]
struct FileLoadScanOp {
    cfg: FileLoadScanConfig,
    arena: Arc<ExprArena>,
}

impl FileLoadScanOp {
    fn new(cfg: FileLoadScanConfig, arena: Arc<ExprArena>) -> Self {
        Self { cfg, arena }
    }

    fn parse_csv_source_columns(&self, path: &str) -> Result<Vec<Vec<Option<String>>>, String> {
        let csv = self
            .cfg
            .csv
            .as_ref()
            .ok_or_else(|| "FILE_SCAN missing CSV read options".to_string())?;
        let col_delimiter = single_byte_delimiter(&csv.column_delimiter, "column_separator")?;
        let row_delimiter = single_byte_delimiter(&csv.row_delimiter, "row_delimiter")?;

        let mut builder = ReaderBuilder::new();
        builder
            .has_headers(false)
            .delimiter(col_delimiter)
            .terminator(Terminator::Any(row_delimiter))
            .trim(if csv.trim_space {
                Trim::All
            } else {
                Trim::None
            })
            .flexible(true);
        if let Some(quote) = csv.enclose {
            builder.quoting(true).quote(quote);
        } else {
            builder.quoting(false);
        }
        if let Some(escape) = csv.escape {
            builder.escape(Some(escape));
        }

        let mut reader = builder
            .from_path(path)
            .map_err(|e| format!("FILE_SCAN failed to open csv file `{path}`: {e}"))?;

        let expected_columns = self.cfg.source_slot_ids.len();
        let mut columns: Vec<Vec<Option<String>>> =
            (0..expected_columns).map(|_| Vec::new()).collect();
        for (record_idx, record) in reader.records().enumerate() {
            let record =
                record.map_err(|e| format!("FILE_SCAN failed to read csv row in `{path}`: {e}"))?;
            if record_idx < csv.skip_header {
                continue;
            }
            if record.len() != expected_columns {
                return Err(format!(
                    "FILE_SCAN csv column count mismatch in `{path}`: expected={} actual={} row_index={}",
                    expected_columns,
                    record.len(),
                    record_idx
                ));
            }
            for (idx, field) in record.iter().enumerate() {
                if field == "\\N" {
                    columns[idx].push(None);
                } else {
                    columns[idx].push(Some(field.to_string()));
                }
            }
        }
        Ok(columns)
    }

    fn parse_json_source_columns(&self, path: &str) -> Result<Vec<Vec<Option<String>>>, String> {
        let json = self
            .cfg
            .json
            .as_ref()
            .ok_or_else(|| "FILE_SCAN missing JSON read options".to_string())?;
        let payload = std::fs::read_to_string(path)
            .map_err(|e| format!("FILE_SCAN failed to open json file `{path}`: {e}"))?;
        if payload.trim().is_empty() {
            return Ok((0..self.cfg.source_slot_ids.len())
                .map(|_| Vec::new())
                .collect());
        }

        let rows = parse_json_rows(&payload, json.strip_outer_array)
            .map_err(|e| format!("FILE_SCAN failed to parse json file `{path}`: {e}"))?;

        let expected_columns = self.cfg.source_slot_ids.len();
        if json.jsonpaths.len() != expected_columns {
            return Err(format!(
                "FILE_SCAN jsonpaths count mismatch in `{path}`: expected={} actual={}",
                expected_columns,
                json.jsonpaths.len()
            ));
        }

        let mut columns: Vec<Vec<Option<String>>> = (0..expected_columns)
            .map(|_| Vec::with_capacity(rows.len()))
            .collect();
        for row in &rows {
            for (idx, path_expr) in json.jsonpaths.iter().enumerate() {
                let value = extract_json_path(row, path_expr)
                    .map_err(|e| format!("FILE_SCAN json path `{path_expr}` error: {e}"))?;
                columns[idx].push(json_value_to_field(value));
            }
        }
        Ok(columns)
    }

    fn build_source_chunk(
        &self,
        source_columns: Vec<Vec<Option<String>>>,
    ) -> Result<Chunk, String> {
        let mut fields = Vec::with_capacity(self.cfg.source_slot_ids.len());
        for (idx, slot_id) in self.cfg.source_slot_ids.iter().enumerate() {
            fields.push(field_with_slot_id(
                Field::new(format!("src_col_{idx}"), DataType::Utf8, true),
                *slot_id,
            ));
        }
        let arrays: Vec<ArrayRef> = source_columns
            .into_iter()
            .map(|values| Arc::new(StringArray::from(values)) as ArrayRef)
            .collect();
        let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)
            .map_err(|e| format!("FILE_SCAN build source record batch failed: {e}"))?;
        Chunk::try_new(batch)
    }

    fn build_output_chunk(&self, source: &Chunk) -> Result<Option<Chunk>, String> {
        let mut output_arrays = Vec::with_capacity(self.cfg.output_exprs.len());
        for (idx, expr_id) in self.cfg.output_exprs.iter().enumerate() {
            let mut array = self.arena.eval(*expr_id, source)?;
            let expected =
                self.cfg.output_field_types.get(idx).ok_or_else(|| {
                    format!("FILE_SCAN missing output field type for index={idx}")
                })?;
            if array.data_type() != expected {
                array = cast(array.as_ref(), expected).map_err(|e| {
                    format!(
                        "FILE_SCAN cast projected column failed at index={idx}: from={:?} to={:?} error={e}",
                        array.data_type(),
                        expected
                    )
                })?;
            }
            output_arrays.push(array);
        }
        if output_arrays.is_empty() {
            return Ok(None);
        }
        let row_count = source.len();
        for array in &output_arrays {
            if array.len() != row_count {
                return Err(format!(
                    "FILE_SCAN projected column length mismatch: expected={} actual={}",
                    row_count,
                    array.len()
                ));
            }
        }
        if row_count == 0 {
            return Ok(None);
        }

        let mut fields = Vec::with_capacity(output_arrays.len());
        for ((slot_id, name), array) in self
            .cfg
            .output_slot_ids
            .iter()
            .zip(self.cfg.output_field_names.iter())
            .zip(output_arrays.iter())
        {
            let target_type = self
                .cfg
                .output_field_types
                .get(fields.len())
                .cloned()
                .unwrap_or_else(|| array.data_type().clone());
            fields.push(field_with_slot_id(
                Field::new(name.clone(), target_type, true),
                *slot_id,
            ));
        }

        let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), output_arrays)
            .map_err(|e| format!("FILE_SCAN build output record batch failed: {e}"))?;
        Ok(Some(Chunk::try_new(batch)?))
    }
}

impl ScanOp for FileLoadScanOp {
    fn execute_iter(
        &self,
        morsel: ScanMorsel,
        _profile: Option<crate::runtime::profile::RuntimeProfile>,
        _runtime_filters: Option<&RuntimeFilterContext>,
    ) -> Result<BoxedExecIter, String> {
        let ScanMorsel::FileRange {
            path,
            offset,
            length,
            ..
        } = morsel
        else {
            return Err("FILE_SCAN received unexpected morsel".to_string());
        };

        if offset != 0 || length != 0 {
            return Err(format!(
                "FILE_SCAN supports full-file ranges only, got offset={} length={} path={}",
                offset, length, path
            ));
        }

        let source_columns = match self.cfg.format_type {
            t if t == plan_nodes::TFileFormatType::FORMAT_CSV_PLAIN => {
                self.parse_csv_source_columns(&path)?
            }
            t if t == plan_nodes::TFileFormatType::FORMAT_JSON => {
                self.parse_json_source_columns(&path)?
            }
            _ => {
                return Err(format!(
                    "FILE_SCAN supports FORMAT_CSV_PLAIN/FORMAT_JSON now, got {:?}",
                    self.cfg.format_type
                ));
            }
        };
        let source_chunk = self.build_source_chunk(source_columns)?;
        let Some(chunk) = self.build_output_chunk(&source_chunk)? else {
            return Ok(Box::new(std::iter::empty()));
        };
        Ok(Box::new(std::iter::once(Ok(chunk))))
    }

    fn build_morsels(&self) -> Result<ScanMorsels, String> {
        let mut morsels = Vec::with_capacity(self.cfg.ranges.len());
        for r in &self.cfg.ranges {
            morsels.push(ScanMorsel::FileRange {
                path: r.path.clone(),
                file_len: r.file_len,
                offset: r.offset,
                length: r.length,
                scan_range_id: r.scan_range_id,
                first_row_id: r.first_row_id,
                external_datacache: r.external_datacache.clone(),
            });
        }
        Ok(ScanMorsels::new(morsels, self.cfg.has_more))
    }

    fn profile_name(&self) -> Option<String> {
        Some("FILE_SCAN".to_string())
    }
}

pub(crate) fn lower_file_scan_node(
    node: &plan_nodes::TPlanNode,
    desc_tbl: Option<&descriptors::TDescriptorTable>,
    _tuple_slots: &HashMap<types::TTupleId, Vec<types::TSlotId>>,
    layout_hints: &HashMap<types::TTupleId, Vec<types::TSlotId>>,
    exec_params: Option<&internal_service::TPlanFragmentExecParams>,
    arena: &mut ExprArena,
    mut out_layout: Layout,
) -> Result<Lowered, String> {
    if node.num_children != 0 {
        return Err(format!(
            "FILE_SCAN_NODE expected 0 children, got {}",
            node.num_children
        ));
    }

    let Some(file_scan) = node.file_scan_node.as_ref() else {
        return Err("FILE_SCAN_NODE missing file_scan_node payload".to_string());
    };
    let tuple_id = file_scan.tuple_id;

    if out_layout.order.is_empty() {
        let hint = layout_hints
            .get(&tuple_id)
            .filter(|v| !v.is_empty())
            .ok_or_else(|| {
                format!(
                    "FILE_SCAN_NODE node_id={} missing output layout for tuple_id={}",
                    node.node_id, tuple_id
                )
            })?;
        out_layout = layout_from_slot_ids(tuple_id, hint.iter().copied());
    }
    if out_layout.order.iter().any(|(tid, _)| *tid != tuple_id) {
        return Err(format!(
            "FILE_SCAN_NODE node_id={} has multi-tuple output layout: {:?}",
            node.node_id, out_layout.order
        ));
    }

    let Some(exec_params) = exec_params else {
        return Err("FILE_SCAN_NODE requires exec_params.per_node_scan_ranges".to_string());
    };
    let scan_ranges = exec_params
        .per_node_scan_ranges
        .get(&node.node_id)
        .ok_or_else(|| format!("missing per_node_scan_ranges for node_id={}", node.node_id))?;

    let mut ranges = Vec::new();
    let mut next_scan_range_id: i32 = 0;
    let mut has_more = false;
    let mut params: Option<plan_nodes::TBrokerScanRangeParams> = None;
    let mut format_type: Option<plan_nodes::TFileFormatType> = None;
    let mut json_range_options: Option<(bool, Option<String>)> = None;

    for scan_range_param in scan_ranges {
        if scan_range_param.empty.unwrap_or(false) {
            if scan_range_param.has_more.unwrap_or(false) {
                has_more = true;
            }
            continue;
        }
        if scan_range_param.has_more.unwrap_or(false) {
            has_more = true;
        }

        let broker_scan_range = scan_range_param
            .scan_range
            .broker_scan_range
            .as_ref()
            .ok_or_else(|| {
                format!(
                    "FILE_SCAN_NODE node_id={} missing broker_scan_range in TScanRangeParams",
                    node.node_id
                )
            })?;

        if let Some(existing) = params.as_ref() {
            if existing != &broker_scan_range.params {
                return Err(format!(
                    "FILE_SCAN_NODE node_id={} has inconsistent broker_scan_range params across ranges",
                    node.node_id
                ));
            }
        } else {
            params = Some(broker_scan_range.params.clone());
        }

        for range_desc in &broker_scan_range.ranges {
            let path = match range_desc.file_type {
                t if t == types::TFileType::FILE_LOCAL => {
                    if range_desc.path.trim().is_empty() {
                        return Err(format!(
                            "FILE_SCAN_NODE node_id={} empty local file path in range desc",
                            node.node_id
                        ));
                    }
                    range_desc.path.clone()
                }
                t if t == types::TFileType::FILE_STREAM => {
                    let load_id = range_desc.load_id.as_ref().ok_or_else(|| {
                        format!(
                            "FILE_SCAN_NODE node_id={} FILE_STREAM range is missing load_id",
                            node.node_id
                        )
                    })?;
                    stream_load::resolve_stream_load_file_path(load_id).ok_or_else(|| {
                        format!(
                            "FILE_SCAN_NODE node_id={} has no registered local file for load_id={}:{}",
                            node.node_id, load_id.hi, load_id.lo
                        )
                    })?
                }
                _ => {
                    return Err(format!(
                        "FILE_SCAN_NODE node_id={} unsupported file type {:?}",
                        node.node_id, range_desc.file_type
                    ));
                }
            };
            if let Some(prev) = format_type {
                if prev != range_desc.format_type {
                    return Err(format!(
                        "FILE_SCAN_NODE node_id={} mixed format types: {:?} vs {:?}",
                        node.node_id, prev, range_desc.format_type
                    ));
                }
            } else {
                format_type = Some(range_desc.format_type);
            }

            if range_desc.format_type == plan_nodes::TFileFormatType::FORMAT_JSON {
                let current = (
                    range_desc.strip_outer_array.unwrap_or(false),
                    range_desc.jsonpaths.clone(),
                );
                if let Some(prev) = &json_range_options {
                    if prev != &current {
                        return Err(format!(
                            "FILE_SCAN_NODE node_id={} has inconsistent json range options across ranges",
                            node.node_id
                        ));
                    }
                } else {
                    json_range_options = Some(current);
                }
            }

            if let Some(compression) = range_desc.compression_type
                && compression != types::TCompressionType::NO_COMPRESSION
                && compression != types::TCompressionType::DEFAULT_COMPRESSION
                && compression != types::TCompressionType::UNKNOWN_COMPRESSION
            {
                return Err(format!(
                    "FILE_SCAN_NODE node_id={} unsupported compression type {:?}",
                    node.node_id, compression
                ));
            }

            ranges.push(FileScanRange {
                path,
                file_len: range_desc.file_size.unwrap_or_default().max(0) as u64,
                offset: range_desc.start_offset.max(0) as u64,
                length: if range_desc.size > 0 {
                    range_desc.size as u64
                } else {
                    0
                },
                scan_range_id: next_scan_range_id,
                first_row_id: None,
                external_datacache: None,
            });
            next_scan_range_id = next_scan_range_id.saturating_add(1);
        }
    }
    if has_more {
        return Err(format!(
            "FILE_SCAN_NODE node_id={} incremental scan ranges are not supported",
            node.node_id
        ));
    }

    let format_type = format_type.ok_or_else(|| {
        format!(
            "FILE_SCAN_NODE node_id={} contains no broker range descriptors",
            node.node_id
        )
    })?;
    let params = params.ok_or_else(|| {
        format!(
            "FILE_SCAN_NODE node_id={} missing broker scan range params",
            node.node_id
        )
    })?;

    if format_type != plan_nodes::TFileFormatType::FORMAT_CSV_PLAIN
        && format_type != plan_nodes::TFileFormatType::FORMAT_JSON
    {
        return Err(format!(
            "FILE_SCAN_NODE node_id={} only supports FORMAT_CSV_PLAIN/FORMAT_JSON now, got {:?}",
            node.node_id, format_type
        ));
    }

    let source_layout = layout_from_slot_ids(params.src_tuple_id, params.src_slot_ids.clone());
    let source_slot_ids: Vec<SlotId> = params
        .src_slot_ids
        .iter()
        .map(|slot_id| SlotId::try_from(*slot_id))
        .collect::<Result<_, _>>()?;

    let output_slot_ids: Vec<SlotId> = out_layout
        .order
        .iter()
        .map(|(_, slot_id)| SlotId::try_from(*slot_id))
        .collect::<Result<_, _>>()?;

    let expr_map = params.expr_of_dest_slot.as_ref().ok_or_else(|| {
        format!(
            "FILE_SCAN_NODE node_id={} missing expr_of_dest_slot in broker params",
            node.node_id
        )
    })?;
    let mut output_exprs = Vec::with_capacity(out_layout.order.len());
    for (_, slot_id) in &out_layout.order {
        let expr = expr_map.get(slot_id).ok_or_else(|| {
            format!(
                "FILE_SCAN_NODE node_id={} missing expr_of_dest_slot for slot_id={}",
                node.node_id, slot_id
            )
        })?;
        let expr_id =
            lower_t_expr_with_common_slot_map(expr, arena, &source_layout, None, None, None)?;
        output_exprs.push(expr_id);
    }

    let desc_tbl = desc_tbl.ok_or_else(|| {
        format!(
            "FILE_SCAN_NODE node_id={} requires descriptor table",
            node.node_id
        )
    })?;
    let slot_descs = desc_tbl
        .slot_descriptors
        .as_ref()
        .ok_or_else(|| "missing slot_descriptors in descriptor table".to_string())?;
    let mut output_field_names = Vec::with_capacity(out_layout.order.len());
    let mut output_field_types = Vec::with_capacity(out_layout.order.len());
    for (tuple_id, slot_id) in &out_layout.order {
        let slot_desc = slot_descs
            .iter()
            .find(|slot_desc| slot_desc.parent == Some(*tuple_id) && slot_desc.id == Some(*slot_id))
            .ok_or_else(|| {
                format!(
                    "FILE_SCAN_NODE node_id={} missing slot descriptor for tuple_id={} slot_id={}",
                    node.node_id, tuple_id, slot_id
                )
            })?;
        let field_name = slot_name_from_desc(slot_desc)
            .unwrap_or_else(|| format!("col_{}_{}", tuple_id, slot_id));
        let field_type = slot_desc
            .slot_type
            .as_ref()
            .and_then(arrow_type_from_desc)
            .ok_or_else(|| {
                format!(
                    "FILE_SCAN_NODE node_id={} unsupported slot type for tuple_id={} slot_id={}",
                    node.node_id, tuple_id, slot_id
                )
            })?;
        output_field_names.push(field_name);
        output_field_types.push(field_type);
    }

    let (csv, json) = if format_type == plan_nodes::TFileFormatType::FORMAT_CSV_PLAIN {
        let column_delimiter = params.multi_column_separator.clone().unwrap_or_else(|| {
            String::from_utf8_lossy(&[params.column_separator as u8]).to_string()
        });
        let row_delimiter = params
            .multi_row_delimiter
            .clone()
            .unwrap_or_else(|| String::from_utf8_lossy(&[params.row_delimiter as u8]).to_string());
        let skip_header = params.skip_header.unwrap_or_default();
        if skip_header < 0 {
            return Err(format!(
                "FILE_SCAN_NODE node_id={} invalid negative skip_header={}",
                node.node_id, skip_header
            ));
        }
        (
            Some(CsvReadOptions {
                column_delimiter,
                row_delimiter,
                skip_header: skip_header as usize,
                trim_space: params.trim_space.unwrap_or(false),
                enclose: params.enclose.map(|value| value as u8),
                escape: params.escape.map(|value| value as u8),
            }),
            None,
        )
    } else {
        let (strip_outer_array, jsonpaths_raw) = json_range_options.ok_or_else(|| {
            format!(
                "FILE_SCAN_NODE node_id={} missing json range options for FORMAT_JSON",
                node.node_id
            )
        })?;
        let jsonpaths =
            parse_jsonpaths(jsonpaths_raw.as_deref(), source_slot_ids.len()).map_err(|e| {
                format!(
                    "FILE_SCAN_NODE node_id={} invalid jsonpaths: {e}",
                    node.node_id
                )
            })?;
        (
            None,
            Some(JsonReadOptions {
                strip_outer_array,
                jsonpaths,
            }),
        )
    };

    let config = FileLoadScanConfig {
        ranges,
        has_more,
        format_type,
        source_slot_ids,
        output_slot_ids: output_slot_ids.clone(),
        output_field_names,
        output_field_types,
        output_exprs,
        csv,
        json,
    };

    let limit = node.limit;
    let limit = (limit >= 0).then_some(limit as usize);
    let scan = ScanNode::new(Arc::new(FileLoadScanOp::new(
        config,
        Arc::new(arena.clone()),
    )))
    .with_node_id(node.node_id)
    .with_output_slots(output_slot_ids)
    .with_limit(limit)
    .with_local_rf_waiting_set(local_rf_waiting_set(node));

    Ok(Lowered {
        node: ExecNode {
            kind: ExecNodeKind::Scan(scan),
        },
        layout: out_layout,
    })
}

fn single_byte_delimiter(value: &str, name: &str) -> Result<u8, String> {
    let bytes = value.as_bytes();
    if bytes.len() != 1 {
        return Err(format!(
            "FILE_SCAN only supports single-byte `{name}` now, got `{value}`"
        ));
    }
    Ok(bytes[0])
}

fn parse_jsonpaths(
    jsonpaths: Option<&str>,
    expected_columns: usize,
) -> Result<Vec<String>, String> {
    let raw = jsonpaths.ok_or_else(|| "jsonpaths is required for FORMAT_JSON".to_string())?;
    let paths: Vec<String> =
        serde_json::from_str(raw).map_err(|e| format!("failed to parse jsonpaths array: {e}"))?;
    if paths.len() != expected_columns {
        return Err(format!(
            "jsonpaths count mismatch: expected={} actual={}",
            expected_columns,
            paths.len()
        ));
    }
    if paths.iter().any(|path| path.is_empty()) {
        return Err("jsonpaths cannot contain empty path".to_string());
    }
    Ok(paths)
}

fn parse_json_rows(payload: &str, strip_outer_array: bool) -> Result<Vec<Value>, String> {
    let value: Value =
        serde_json::from_str(payload).map_err(|e| format!("invalid json payload: {e}"))?;
    if strip_outer_array {
        return match value {
            Value::Array(rows) => Ok(rows),
            _ => Err("strip_outer_array=true expects top-level JSON array".to_string()),
        };
    }
    Ok(match value {
        Value::Array(rows) => rows,
        other => vec![other],
    })
}

fn extract_json_path<'a>(root: &'a Value, path: &str) -> Result<Option<&'a Value>, String> {
    if path == "$" {
        return Ok(Some(root));
    }
    let mut rest = path
        .strip_prefix('$')
        .ok_or_else(|| format!("path must start with `$`, got `{path}`"))?;
    let mut current = root;
    while !rest.is_empty() {
        if let Some(stripped) = rest.strip_prefix('.') {
            let mut end = stripped.len();
            for (idx, ch) in stripped.char_indices() {
                if ch == '.' || ch == '[' {
                    end = idx;
                    break;
                }
            }
            let key = &stripped[..end];
            if key.is_empty() {
                return Err(format!("invalid key segment in path `{path}`"));
            }
            let Some(next) = current.get(key) else {
                return Ok(None);
            };
            current = next;
            rest = &stripped[end..];
            continue;
        }
        if rest.starts_with('[') {
            let end = rest
                .find(']')
                .ok_or_else(|| format!("missing `]` in path `{path}`"))?;
            let index_text = &rest[1..end];
            let index = index_text
                .parse::<usize>()
                .map_err(|_| format!("invalid array index `{index_text}` in path `{path}`"))?;
            let Some(array) = current.as_array() else {
                return Ok(None);
            };
            let Some(next) = array.get(index) else {
                return Ok(None);
            };
            current = next;
            rest = &rest[end + 1..];
            continue;
        }
        return Err(format!("invalid token in path `{path}` near `{rest}`"));
    }
    Ok(Some(current))
}

fn json_value_to_field(value: Option<&Value>) -> Option<String> {
    match value {
        None | Some(Value::Null) => None,
        Some(Value::String(v)) => Some(v.clone()),
        Some(Value::Bool(v)) => Some(v.to_string()),
        Some(Value::Number(v)) => Some(v.to_string()),
        Some(other) => Some(other.to_string()),
    }
}
