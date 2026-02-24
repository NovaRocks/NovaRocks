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
use std::io::Cursor;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int32Array, Int64Array, UInt32Array, new_empty_array};
use arrow::compute::{concat, take};
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;

use crate::cache::DataCacheManager;
use crate::common::ids::SlotId;
use crate::descriptors;
use crate::exec::node::scan::RowPositionScanConfig;
use crate::exec::row_position::RowPositionDescriptor;
use crate::formats::{
    FileFormatConfig, build_format_iter, parquet::ParquetReadCachePolicy,
    parquet::ParquetScanConfig,
};
use crate::fs::scan_context::{FileScanContext, FileScanRange};
use crate::lower::type_lowering::{arrow_type_from_desc, primitive_type_from_desc};
use crate::runtime::query_context::{QueryId, query_context_manager};
use crate::types;

#[derive(Clone, Debug)]
pub struct GlobalLateMaterializationContext {
    pub row_source_slot: SlotId,
    pub scan_config: RowPositionScanConfig,
    scan_ranges: HashMap<i32, FileScanRange>,
}

impl GlobalLateMaterializationContext {
    pub fn new(row_source_slot: SlotId, scan_config: RowPositionScanConfig) -> Self {
        Self {
            row_source_slot,
            scan_config,
            scan_ranges: HashMap::new(),
        }
    }

    pub fn register_ranges(&mut self, ranges: Vec<FileScanRange>) {
        for range in ranges {
            self.scan_ranges.entry(range.scan_range_id).or_insert(range);
        }
    }

    pub fn get_scan_range(&self, scan_range_id: i32) -> Option<&FileScanRange> {
        self.scan_ranges.get(&scan_range_id)
    }
}

#[derive(Clone, Debug)]
struct SlotMeta {
    name: String,
    primitive: types::TPrimitiveType,
    arrow_type: arrow::datatypes::DataType,
}

fn build_slot_meta_map(
    desc_tbl: &descriptors::TDescriptorTable,
) -> Result<HashMap<SlotId, SlotMeta>, String> {
    let slot_descs = desc_tbl
        .slot_descriptors
        .as_ref()
        .ok_or_else(|| "missing slot_descriptors in desc_tbl".to_string())?;
    let mut map = HashMap::new();
    for s in slot_descs {
        let (Some(id), Some(slot_type)) = (s.id, s.slot_type.as_ref()) else {
            continue;
        };
        let Some(name) = crate::lower::layout::slot_name_from_desc(s) else {
            continue;
        };
        let primitive =
            primitive_type_from_desc(slot_type).unwrap_or(types::TPrimitiveType::INVALID_TYPE);
        let arrow_type = arrow_type_from_desc(slot_type)
            .ok_or_else(|| format!("unsupported slot_type for slot_id={id}"))?;
        map.insert(
            SlotId::try_from(id)?,
            SlotMeta {
                name,
                primitive,
                arrow_type,
            },
        );
    }
    Ok(map)
}

fn lookup_output_slots(
    desc_tbl: &descriptors::TDescriptorTable,
    tuple_id: i32,
    row_pos_desc: &RowPositionDescriptor,
) -> Result<Vec<SlotId>, String> {
    let slot_descs = desc_tbl
        .slot_descriptors
        .as_ref()
        .ok_or_else(|| "missing slot_descriptors in desc_tbl".to_string())?;
    let mut ignore: HashSet<SlotId> = HashSet::new();
    ignore.insert(row_pos_desc.row_source_slot);
    for slot in &row_pos_desc.fetch_ref_slots {
        ignore.insert(*slot);
    }
    for slot in &row_pos_desc.lookup_ref_slots {
        ignore.insert(*slot);
    }
    let mut slots = Vec::new();
    for s in slot_descs {
        let (Some(id), Some(parent)) = (s.id, s.parent) else {
            continue;
        };
        if parent != tuple_id {
            continue;
        }
        let slot_id = SlotId::try_from(id)?;
        if ignore.contains(&slot_id) {
            continue;
        }
        slots.push(slot_id);
    }
    Ok(slots)
}

pub fn encode_column_ipc(array: &ArrayRef) -> Result<Vec<u8>, String> {
    let field = arrow::datatypes::Field::new("col", array.data_type().clone(), true);
    let schema = Arc::new(arrow::datatypes::Schema::new(vec![field]));
    let batch =
        RecordBatch::try_new(schema.clone(), vec![array.clone()]).map_err(|e| e.to_string())?;
    let mut buf = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buf, &schema).map_err(|e| e.to_string())?;
        writer.write(&batch).map_err(|e| e.to_string())?;
        writer.finish().map_err(|e| e.to_string())?;
    }
    Ok(buf)
}

pub fn decode_column_ipc(data: &[u8]) -> Result<ArrayRef, String> {
    let mut reader = StreamReader::try_new(Cursor::new(data), None).map_err(|e| e.to_string())?;
    let batch = reader
        .next()
        .ok_or_else(|| "empty ipc stream".to_string())?
        .map_err(|e| e.to_string())?;
    if batch.num_columns() != 1 {
        return Err(format!(
            "expected 1 column in ipc stream, got {}",
            batch.num_columns()
        ));
    }
    Ok(batch.column(0).clone())
}

pub(crate) fn execute_lookup_request(
    query_id: QueryId,
    tuple_id: i32,
    request_columns: HashMap<SlotId, ArrayRef>,
) -> Result<Vec<(SlotId, ArrayRef)>, String> {
    let mgr = query_context_manager();
    let row_pos_desc = mgr
        .row_pos_desc(query_id, tuple_id)
        .ok_or_else(|| format!("row position descriptor missing for tuple_id={}", tuple_id))?;
    if row_pos_desc.row_position_type != descriptors::TRowPositionType::ICEBERG_V3_ROW_POSITION {
        return Err(format!(
            "unsupported row position type: {:?}",
            row_pos_desc.row_position_type
        ));
    }
    let desc_tbl = mgr
        .desc_tbl(query_id)
        .ok_or_else(|| "descriptor table missing for lookup".to_string())?;
    let cache_options = mgr
        .cache_options(query_id)
        .ok_or_else(|| "cache options missing for lookup".to_string())?;
    let slot_meta = build_slot_meta_map(&desc_tbl)?;
    let lookup_slots = lookup_output_slots(&desc_tbl, tuple_id, &row_pos_desc)?;

    let fetch_ref_slots = &row_pos_desc.fetch_ref_slots;
    if fetch_ref_slots.len() != 2 {
        return Err(format!(
            "iceberg row position expects 2 fetch_ref_slots, got {}",
            fetch_ref_slots.len()
        ));
    }
    let scan_range_slot = fetch_ref_slots[0];
    let row_id_slot = fetch_ref_slots[1];
    let scan_range_col = request_columns
        .get(&scan_range_slot)
        .ok_or_else(|| format!("missing scan_range_id column {}", scan_range_slot))?;
    let row_id_col = request_columns
        .get(&row_id_slot)
        .ok_or_else(|| format!("missing row_id column {}", row_id_slot))?;

    let scan_range_ids = scan_range_col
        .as_any()
        .downcast_ref::<Int32Array>()
        .ok_or_else(|| "scan_range_id column must be Int32".to_string())?;
    let row_ids = row_id_col
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| "row_id column must be Int64".to_string())?;

    if scan_range_ids.len() != row_ids.len() {
        return Err("scan_range_id and row_id columns length mismatch".to_string());
    }
    let request_len = scan_range_ids.len();
    if request_len == 0 {
        let mut out = Vec::new();
        for slot in lookup_slots {
            let meta = slot_meta
                .get(&slot)
                .ok_or_else(|| format!("missing slot meta for slot {}", slot))?;
            let empty = new_empty_array(&meta.arrow_type);
            out.push((slot, empty));
        }
        return Ok(out);
    }

    let mut range_to_positions: HashMap<i32, HashMap<i64, VecDeque<usize>>> = HashMap::new();
    for idx in 0..request_len {
        let scan_range_id = scan_range_ids.value(idx);
        let row_id = row_ids.value(idx);
        range_to_positions
            .entry(scan_range_id)
            .or_default()
            .entry(row_id)
            .or_default()
            .push_back(idx);
    }

    let scan_cfg = mgr
        .glm_scan_config(query_id, row_pos_desc.row_source_slot)
        .ok_or_else(|| "missing glm scan config".to_string())?;

    let mut column_chunks: HashMap<SlotId, Vec<ArrayRef>> = HashMap::new();
    let mut response_positions: Vec<usize> = Vec::with_capacity(request_len);

    for (scan_range_id, mut positions_map) in range_to_positions {
        let scan_range = mgr
            .glm_scan_range(query_id, row_pos_desc.row_source_slot, scan_range_id)
            .ok_or_else(|| format!("scan_range_id {} not registered", scan_range_id))?;
        let first_row_id = scan_range.first_row_id.ok_or_else(|| {
            format!(
                "scan_range_id {} missing first_row_id for lookup",
                scan_range_id
            )
        })?;

        let lookup_metas = lookup_slots
            .iter()
            .map(|slot| {
                slot_meta
                    .get(slot)
                    .cloned()
                    .ok_or_else(|| format!("missing slot meta for slot {}", slot))
            })
            .collect::<Result<Vec<_>, _>>()?;

        let columns = lookup_metas.iter().map(|m| m.name.clone()).collect();
        let slot_ids = lookup_slots.clone();
        let slot_types = lookup_metas.iter().map(|m| m.primitive).collect();

        let parquet_cfg = ParquetScanConfig {
            columns,
            slot_ids,
            slot_types,
            case_sensitive: scan_cfg.case_sensitive,
            enable_page_index: false,
            min_max_predicates: Vec::new(),
            batch_size: scan_cfg.batch_size,
            datacache: DataCacheManager::instance().external_context(cache_options.clone()),
            cache_policy: ParquetReadCachePolicy::with_flags(
                scan_cfg.enable_file_metacache,
                scan_cfg.enable_file_pagecache,
                u32::try_from(cache_options.datacache_evict_probability).ok(),
            ),
            profile_label: None,
        };
        let format = match scan_cfg.file_format {
            descriptors::THdfsFileFormat::PARQUET => FileFormatConfig::Parquet(parquet_cfg),
            other => {
                return Err(format!("lookup only supports PARQUET, got {:?}", other));
            }
        };

        let scan = FileScanContext::build(vec![scan_range.clone()], None)?;
        let mut iter = build_format_iter(scan, format, None, None, None)?;

        let mut row_offset = 0i64;
        while let Some(next) = iter.next() {
            let chunk = next?;
            let row_count = chunk.len();
            if row_count == 0 {
                continue;
            }
            let mut indices = Vec::new();
            let mut positions = Vec::new();
            for row_idx in 0..row_count {
                let row_id = first_row_id + row_offset + row_idx as i64;
                if let Some(queue) = positions_map.get_mut(&row_id) {
                    while let Some(pos) = queue.pop_front() {
                        indices.push(row_idx as u32);
                        positions.push(pos);
                    }
                }
            }
            row_offset = row_offset.saturating_add(row_count as i64);
            if indices.is_empty() {
                continue;
            }
            let indices_array = UInt32Array::from(indices);
            for slot in lookup_slots.iter() {
                let column = chunk.column_by_slot_id(*slot)?;
                let taken =
                    take(column.as_ref(), &indices_array, None).map_err(|e| e.to_string())?;
                column_chunks.entry(*slot).or_default().push(taken);
            }
            response_positions.extend(positions);
        }

        for (row_id, queue) in positions_map {
            if !queue.is_empty() {
                return Err(format!(
                    "lookup failed to materialize row_id {} ({} pending)",
                    row_id,
                    queue.len()
                ));
            }
        }
    }

    if response_positions.len() != request_len {
        return Err(format!(
            "lookup response size mismatch: expected {} got {}",
            request_len,
            response_positions.len()
        ));
    }

    let mut response_indices = vec![u32::MAX; request_len];
    for (resp_idx, req_pos) in response_positions.iter().enumerate() {
        if response_indices[*req_pos] != u32::MAX {
            return Err(format!("duplicate response position {}", req_pos));
        }
        response_indices[*req_pos] = resp_idx as u32;
    }
    if response_indices.iter().any(|v| *v == u32::MAX) {
        return Err("lookup response missing positions".to_string());
    }
    let response_indices_array = UInt32Array::from(response_indices);

    let mut out = Vec::new();
    for slot in lookup_slots {
        let chunks = column_chunks
            .get(&slot)
            .ok_or_else(|| format!("missing lookup column {}", slot))?;
        let chunk_refs: Vec<&dyn arrow::array::Array> = chunks.iter().map(|c| c.as_ref()).collect();
        let full = concat(&chunk_refs).map_err(|e| e.to_string())?;
        let ordered = take(&full, &response_indices_array, None).map_err(|e| e.to_string())?;
        out.push((slot, ordered));
    }
    Ok(out)
}
