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
//! Fetch processor for late materialization and lookup joins.
//!
//! Responsibilities:
//! - Resolves deferred slot values by fetching referenced rows from stored scan artifacts.
//! - Merges fetched columns with probe-side chunks while preserving row ordering.
//!
//! Key exported interfaces:
//! - Types: `FetchProcessorFactory`.
//!
//! Current limitations:
//! - Implements only the execution semantics currently wired by novarocks plan lowering and pipeline builder.
//! - Unsupported states should be surfaced as explicit runtime errors instead of fallback behavior.

use std::collections::{HashMap, HashSet};

use arrow::array::{Array, ArrayRef, Int32Array, UInt32Array};
use arrow::compute::{concat, take};
use arrow::datatypes::{Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;

use crate::common::ids::SlotId;
use crate::descriptors;
use crate::exec::chunk::{Chunk, field_with_slot_id};
use crate::exec::pipeline::operator::{Operator, ProcessorOperator};
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::exec::row_position::RowPositionDescriptor;
use crate::runtime::lookup::{decode_column_ipc, encode_column_ipc, execute_lookup_request};
use crate::runtime::query_context::{QueryId, query_context_manager};
use crate::runtime::runtime_state::RuntimeState;
use crate::service::grpc_client;

/// Factory for fetch processors that resolve deferred row/slot materialization.
pub struct FetchProcessorFactory {
    name: String,
    node_id: i32,
    target_node_id: i32,
    row_pos_descs: HashMap<i32, RowPositionDescriptor>,
    nodes_info: Option<descriptors::TNodesInfo>,
    output_slots: Vec<SlotId>,
}

impl FetchProcessorFactory {
    pub fn new(
        node_id: i32,
        target_node_id: i32,
        row_pos_descs: HashMap<i32, RowPositionDescriptor>,
        nodes_info: Option<descriptors::TNodesInfo>,
        output_slots: Vec<SlotId>,
    ) -> Self {
        Self {
            name: format!("FETCH (id={})", node_id),
            node_id,
            target_node_id,
            row_pos_descs,
            nodes_info,
            output_slots,
        }
    }
}

impl OperatorFactory for FetchProcessorFactory {
    fn name(&self) -> &str {
        &self.name
    }

    fn create(&self, _dop: i32, _driver_id: i32) -> Box<dyn Operator> {
        Box::new(FetchProcessor {
            name: self.name.clone(),
            node_id: self.node_id,
            target_node_id: self.target_node_id,
            row_pos_descs: self.row_pos_descs.clone(),
            nodes_info: self.nodes_info.clone(),
            output_slots: self.output_slots.clone(),
            pending_output: None,
            finishing: false,
            output_schema: None,
        })
    }
}

struct FetchProcessor {
    name: String,
    node_id: i32,
    target_node_id: i32,
    row_pos_descs: HashMap<i32, RowPositionDescriptor>,
    nodes_info: Option<descriptors::TNodesInfo>,
    output_slots: Vec<SlotId>,
    pending_output: Option<Chunk>,
    finishing: bool,
    output_schema: Option<SchemaRef>,
}

impl Operator for FetchProcessor {
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
        self.finishing && self.pending_output.is_none()
    }
}

impl ProcessorOperator for FetchProcessor {
    fn need_input(&self) -> bool {
        self.pending_output.is_none() && !self.finishing
    }

    fn has_output(&self) -> bool {
        self.pending_output.is_some()
    }

    fn push_chunk(&mut self, state: &RuntimeState, chunk: Chunk) -> Result<(), String> {
        let output = self.process_chunk(state, chunk)?;
        self.pending_output = Some(output);
        Ok(())
    }

    fn pull_chunk(&mut self, _state: &RuntimeState) -> Result<Option<Chunk>, String> {
        Ok(self.pending_output.take())
    }

    fn set_finishing(&mut self, _state: &RuntimeState) -> Result<(), String> {
        self.finishing = true;
        Ok(())
    }
}

impl FetchProcessor {
    fn process_chunk(&mut self, state: &RuntimeState, chunk: Chunk) -> Result<Chunk, String> {
        let Some(query_id) = state.query_id() else {
            return Err("FETCH_NODE requires query_id".to_string());
        };
        let desc_tbl = query_context_manager()
            .desc_tbl(query_id)
            .ok_or_else(|| "descriptor table missing for fetch".to_string())?;
        let output_schema = self
            .output_schema
            .get_or_insert(schema_for_slots(&desc_tbl, &self.output_slots)?)
            .clone();

        let mut fetched_columns: HashMap<SlotId, ArrayRef> = HashMap::new();
        for (tuple_id, row_pos_desc) in &self.row_pos_descs {
            let lookup_slots = lookup_output_slots(&desc_tbl, *tuple_id, row_pos_desc)?;
            if lookup_slots.is_empty() {
                continue;
            }
            let fetch_ref_slots = &row_pos_desc.fetch_ref_slots;
            if fetch_ref_slots.len() != 2 {
                return Err(format!(
                    "FETCH_NODE node_id={} expects 2 fetch_ref_slots, got {}",
                    self.node_id,
                    fetch_ref_slots.len()
                ));
            }

            let row_source_col = chunk.column_by_slot_id(row_pos_desc.row_source_slot)?;
            let row_source_col = row_source_col
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| "row_source_id column must be Int32".to_string())?;
            if row_source_col.null_count() > 0 {
                return Err("row_source_id column contains nulls".to_string());
            }

            let scan_range_col = chunk.column_by_slot_id(fetch_ref_slots[0])?;
            let row_id_col = chunk.column_by_slot_id(fetch_ref_slots[1])?;

            let groups = group_positions_by_backend(row_source_col)?;
            let mut group_results: Vec<HashMap<SlotId, ArrayRef>> = Vec::new();
            for (backend_id, positions) in &groups {
                let indices =
                    UInt32Array::from(positions.iter().map(|v| *v as u32).collect::<Vec<_>>());
                let scan_range =
                    take(&scan_range_col, &indices, None).map_err(|e| e.to_string())?;
                let row_id = take(&row_id_col, &indices, None).map_err(|e| e.to_string())?;

                let mut request_columns = HashMap::new();
                request_columns.insert(fetch_ref_slots[0], scan_range);
                request_columns.insert(fetch_ref_slots[1], row_id);

                let response_columns = if self.is_local_backend(*backend_id)? {
                    execute_lookup_request(query_id, *tuple_id, request_columns)?
                } else {
                    self.lookup_remote(query_id, *tuple_id, &request_columns, *backend_id)?
                };

                let mut response_map = HashMap::new();
                for (slot, array) in response_columns {
                    response_map.insert(slot, array);
                }
                for slot in &lookup_slots {
                    if !response_map.contains_key(slot) {
                        return Err(format!("lookup response missing slot {}", slot));
                    }
                }
                group_results.push(response_map);
            }

            let scatter_indices = build_scatter_indices(&groups, chunk.len());
            let scatter_array = UInt32Array::from(scatter_indices);

            for slot in &lookup_slots {
                let mut chunks = Vec::new();
                for result in &group_results {
                    let array = result
                        .get(slot)
                        .ok_or_else(|| format!("missing lookup column {}", slot))?;
                    chunks.push(array.clone());
                }
                let chunk_refs: Vec<&dyn arrow::array::Array> =
                    chunks.iter().map(|c| c.as_ref()).collect();
                let concat_array = concat(&chunk_refs).map_err(|e| e.to_string())?;
                let reordered =
                    take(&concat_array, &scatter_array, None).map_err(|e| e.to_string())?;
                fetched_columns.insert(*slot, reordered);
            }
        }

        let mut output_columns = Vec::with_capacity(self.output_slots.len());
        for slot in &self.output_slots {
            if let Ok(column) = chunk.column_by_slot_id(*slot) {
                output_columns.push(column);
            } else if let Some(column) = fetched_columns.remove(slot) {
                output_columns.push(column);
            } else {
                return Err(format!("missing output slot {}", slot));
            }
        }
        let batch =
            RecordBatch::try_new(output_schema, output_columns).map_err(|e| e.to_string())?;
        Chunk::try_new(batch)
    }

    fn is_local_backend(&self, backend_id: i32) -> Result<bool, String> {
        let local = crate::runtime::backend_id::backend_id()
            .ok_or_else(|| "backend_id is not initialized".to_string())?;
        let local = i32::try_from(local)
            .map_err(|_| format!("backend_id {} does not fit in int32", local))?;
        Ok(local == backend_id)
    }

    fn lookup_remote(
        &self,
        query_id: QueryId,
        tuple_id: i32,
        request_columns: &HashMap<SlotId, ArrayRef>,
        backend_id: i32,
    ) -> Result<Vec<(SlotId, ArrayRef)>, String> {
        let node_info = self
            .nodes_info
            .as_ref()
            .and_then(|info| find_node(info, backend_id))
            .ok_or_else(|| format!("node info not found for backend_id {}", backend_id))?;
        let mut req = grpc_client::proto::starrocks::PLookUpRequest::default();
        req.query_id = Some(grpc_client::proto::starrocks::PUniqueId {
            hi: query_id.hi,
            lo: query_id.lo,
        });
        req.lookup_node_id = Some(self.target_node_id);
        req.request_tuple_id = Some(tuple_id);
        for (slot_id, array) in request_columns {
            let data = encode_column_ipc(array)?;
            req.request_columns
                .push(grpc_client::proto::starrocks::PColumn {
                    slot_id: Some(slot_id.as_u32() as i32),
                    data_size: Some(data.len() as i64),
                    data: Some(data),
                });
        }
        let resp = grpc_client::lookup(&node_info.host, node_info.async_internal_port as u16, req)?;
        if let Some(status) = resp.status.as_ref() {
            if status.status_code != 0 {
                return Err(format!("lookup failed: {:?}", status.error_msgs));
            }
        }
        let mut out = Vec::new();
        for col in resp.columns {
            let Some(slot_id) = col.slot_id else {
                return Err("lookup response column missing slot_id".to_string());
            };
            let slot_id = SlotId::try_from(slot_id)?;
            let data = col
                .data
                .as_ref()
                .ok_or_else(|| "lookup response column missing data".to_string())?;
            let array = decode_column_ipc(data)?;
            out.push((slot_id, array));
        }
        Ok(out)
    }
}

fn group_positions_by_backend(row_source: &Int32Array) -> Result<Vec<(i32, Vec<usize>)>, String> {
    let mut groups = Vec::new();
    let mut index_map: HashMap<i32, usize> = HashMap::new();
    for row_idx in 0..row_source.len() {
        if row_source.is_null(row_idx) {
            return Err("row_source_id column contains null".to_string());
        }
        let backend_id = row_source.value(row_idx);
        let idx = match index_map.get(&backend_id) {
            Some(v) => *v,
            None => {
                let pos = groups.len();
                groups.push((backend_id, Vec::new()));
                index_map.insert(backend_id, pos);
                pos
            }
        };
        groups[idx].1.push(row_idx);
    }
    Ok(groups)
}

fn build_scatter_indices(groups: &[(i32, Vec<usize>)], total_rows: usize) -> Vec<u32> {
    let mut out = vec![0u32; total_rows];
    let mut offset = 0u32;
    for (_, positions) in groups {
        for (idx, row_pos) in positions.iter().enumerate() {
            out[*row_pos] = offset + idx as u32;
        }
        offset = offset.saturating_add(positions.len() as u32);
    }
    out
}

fn find_node(
    nodes_info: &descriptors::TNodesInfo,
    backend_id: i32,
) -> Option<&descriptors::TNodeInfo> {
    nodes_info
        .nodes
        .iter()
        .find(|node| node.id == backend_id as i64)
}

fn schema_for_slots(
    desc_tbl: &descriptors::TDescriptorTable,
    slots: &[SlotId],
) -> Result<SchemaRef, String> {
    let slot_descs = desc_tbl
        .slot_descriptors
        .as_ref()
        .ok_or_else(|| "missing slot_descriptors in desc_tbl".to_string())?;
    let mut map: HashMap<SlotId, (Field, bool)> = HashMap::new();
    for s in slot_descs {
        let (Some(id), Some(slot_type)) = (s.id, s.slot_type.as_ref()) else {
            continue;
        };
        let Some(name) = crate::lower::layout::slot_name_from_desc(s) else {
            continue;
        };
        let dt = crate::lower::type_lowering::arrow_type_from_desc(slot_type)
            .ok_or_else(|| format!("unsupported slot_type for slot_id={id}"))?;
        let nullable = s.is_nullable.unwrap_or(true);
        let field = field_with_slot_id(Field::new(name, dt, nullable), SlotId::try_from(id)?);
        map.insert(SlotId::try_from(id)?, (field, nullable));
    }
    let mut fields = Vec::with_capacity(slots.len());
    for slot in slots {
        let (field, _) = map
            .get(slot)
            .ok_or_else(|| format!("missing slot descriptor for slot {}", slot))?;
        fields.push(field.clone());
    }
    Ok(SchemaRef::new(Schema::new(fields)))
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
