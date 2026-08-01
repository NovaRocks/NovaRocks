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

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Int32Array, UInt32Array};
use arrow::compute::{concat, take};

use crate::common::ids::SlotId;
use crate::exec::chunk::{Chunk, ChunkSchemaRef};
use crate::exec::pipeline::operator::{Operator, ProcessorOperator};
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::exec::row_position::RowPositionDescriptor;
use crate::runtime::descriptor_snapshot::LookupNodesInfo;
use crate::runtime::descriptor_snapshot::is_lake_row_position;
use crate::runtime::fragment::io::{
    FragmentLookupClient, LookupColumn, LookupKind, LookupRequest, LookupTarget,
};
use crate::runtime::runtime_state::RuntimeState;

/// Factory for fetch processors that resolve deferred row/slot materialization.
pub struct FetchProcessorFactory {
    name: String,
    node_id: i32,
    target_node_id: i32,
    row_pos_descs: HashMap<i32, RowPositionDescriptor>,
    output_slots_by_tuple: HashMap<i32, Vec<SlotId>>,
    nodes_info: Option<LookupNodesInfo>,
    output_chunk_schema: ChunkSchemaRef,
    lookup_client: Arc<dyn FragmentLookupClient>,
}

impl FetchProcessorFactory {
    pub fn new(
        node_id: i32,
        target_node_id: i32,
        row_pos_descs: HashMap<i32, RowPositionDescriptor>,
        output_slots_by_tuple: HashMap<i32, Vec<SlotId>>,
        nodes_info: Option<LookupNodesInfo>,
        output_chunk_schema: ChunkSchemaRef,
        lookup_client: Arc<dyn FragmentLookupClient>,
    ) -> Self {
        Self {
            name: format!("FETCH (id={})", node_id),
            node_id,
            target_node_id,
            row_pos_descs,
            output_slots_by_tuple,
            nodes_info,
            output_chunk_schema,
            lookup_client,
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
            output_slots_by_tuple: self.output_slots_by_tuple.clone(),
            nodes_info: self.nodes_info.clone(),
            output_chunk_schema: self.output_chunk_schema.clone(),
            lookup_client: Arc::clone(&self.lookup_client),
            pending_output: None,
            finishing: false,
        })
    }
}

struct FetchProcessor {
    name: String,
    node_id: i32,
    target_node_id: i32,
    row_pos_descs: HashMap<i32, RowPositionDescriptor>,
    output_slots_by_tuple: HashMap<i32, Vec<SlotId>>,
    nodes_info: Option<LookupNodesInfo>,
    output_chunk_schema: ChunkSchemaRef,
    lookup_client: Arc<dyn FragmentLookupClient>,
    pending_output: Option<Chunk>,
    finishing: bool,
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
        let output_chunk_schema = self.output_chunk_schema.clone();

        let mut fetched_columns: HashMap<SlotId, ArrayRef> = HashMap::new();
        for (tuple_id, row_pos_desc) in &self.row_pos_descs {
            let output_slots = self
                .output_slots_by_tuple
                .get(tuple_id)
                .cloned()
                .unwrap_or_default();
            if output_slots.is_empty() {
                continue;
            }
            let fetch_ref_slots = &row_pos_desc.fetch_ref_slots;
            let is_lake = is_lake_row_position(row_pos_desc.row_position_type);
            if is_lake {
                return Err(
                    "lake late-materialization lookup is retired; row-position virtual columns are not part of the fragment kernel"
                        .to_string(),
                );
            }
            let expected_ref_slots = if is_lake { 3 } else { 2 };
            if fetch_ref_slots.len() != expected_ref_slots {
                return Err(format!(
                    "FETCH_NODE node_id={} expects {} fetch_ref_slots, got {}",
                    self.node_id,
                    expected_ref_slots,
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

            let groups = group_positions_by_backend(row_source_col)?;
            let mut group_results: Vec<HashMap<SlotId, ArrayRef>> = Vec::new();
            for (backend_id, positions) in &groups {
                let indices =
                    UInt32Array::from(positions.iter().map(|v| *v as u32).collect::<Vec<_>>());

                let mut request_columns = HashMap::new();
                for slot_id in fetch_ref_slots {
                    let col = chunk.column_by_slot_id(*slot_id)?;
                    let taken = take(col.as_ref(), &indices, None).map_err(|e| e.to_string())?;
                    request_columns.insert(*slot_id, taken);
                }

                let request = LookupRequest::new(
                    query_id,
                    self.target_node_id,
                    *tuple_id,
                    LookupKind::PrimaryKey,
                    LookupTarget::new(*backend_id, self.lookup_endpoint(*backend_id)?),
                    request_columns
                        .into_iter()
                        .map(|(slot_id, values)| LookupColumn::new(slot_id, values))
                        .collect(),
                );
                let response_columns = self
                    .lookup_client
                    .lookup(request)
                    .map_err(|error| error.to_string())?
                    .columns()
                    .iter()
                    .map(|column| (column.slot_id(), column.values().clone()))
                    .collect::<Vec<_>>();

                let mut response_map = HashMap::new();
                for (slot, array) in response_columns {
                    response_map.insert(slot, array);
                }
                for slot in &output_slots {
                    if !response_map.contains_key(slot) {
                        return Err(format!("lookup response missing slot {}", slot));
                    }
                }
                group_results.push(response_map);
            }

            let scatter_indices = build_scatter_indices(&groups, chunk.len());
            let scatter_array = UInt32Array::from(scatter_indices);

            for slot in &output_slots {
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

        let mut output_columns = Vec::with_capacity(self.output_chunk_schema.slot_ids().len());
        for slot in self.output_chunk_schema.slot_ids() {
            if let Ok(column) = chunk.column_by_slot_id(*slot) {
                output_columns.push(column);
            } else if let Some(column) = fetched_columns.remove(slot) {
                output_columns.push(column);
            } else {
                return Err(format!("missing output slot {}", slot));
            }
        }
        Chunk::try_new_with_columns(output_chunk_schema, output_columns)
    }

    fn lookup_endpoint(
        &self,
        backend_id: i32,
    ) -> Result<Option<crate::runtime::endpoint::RuntimeEndpoint>, String> {
        self.nodes_info
            .as_ref()
            .and_then(|info| info.nodes.iter().find(|node| node.id == backend_id as i64))
            .map(|node| {
                crate::runtime::endpoint::RuntimeEndpoint::new(
                    &node.host,
                    i32::from(node.async_internal_port),
                )
            })
            .transpose()
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
