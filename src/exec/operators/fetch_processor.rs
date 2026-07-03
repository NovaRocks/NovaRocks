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

use arrow::array::{Array, ArrayRef, Int32Array, UInt32Array};
use arrow::compute::{concat, take};

use crate::common::ids::SlotId;
use crate::exec::chunk::{Chunk, ChunkSchemaRef};
use crate::exec::pipeline::operator::{Operator, ProcessorOperator};
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::exec::row_position::RowPositionDescriptor;
use crate::proto;
use crate::runtime::descriptor_snapshot_thrift::{
    LookupNodeInfo, LookupNodesInfo, is_lake_row_position,
};
use crate::runtime::lookup::{
    decode_column_ipc, encode_column_ipc, execute_lake_lookup_request, execute_lookup_request,
};
use crate::runtime::query_context::{QueryId, query_context_manager};
use crate::runtime::runtime_state::RuntimeState;

/// Factory for fetch processors that resolve deferred row/slot materialization.
pub struct FetchProcessorFactory {
    name: String,
    node_id: i32,
    target_node_id: i32,
    row_pos_descs: HashMap<i32, RowPositionDescriptor>,
    nodes_info: Option<LookupNodesInfo>,
    output_chunk_schema: ChunkSchemaRef,
}

impl FetchProcessorFactory {
    pub fn new(
        node_id: i32,
        target_node_id: i32,
        row_pos_descs: HashMap<i32, RowPositionDescriptor>,
        nodes_info: Option<LookupNodesInfo>,
        output_chunk_schema: ChunkSchemaRef,
    ) -> Self {
        Self {
            name: format!("FETCH (id={})", node_id),
            node_id,
            target_node_id,
            row_pos_descs,
            nodes_info,
            output_chunk_schema,
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
            output_chunk_schema: self.output_chunk_schema.clone(),
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
    nodes_info: Option<LookupNodesInfo>,
    output_chunk_schema: ChunkSchemaRef,
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
        let snapshot = query_context_manager()
            .descriptor_snapshot(query_id)
            .ok_or_else(|| "descriptor snapshot missing for fetch".to_string())?;
        let output_chunk_schema = self.output_chunk_schema.clone();

        let mut fetched_columns: HashMap<SlotId, ArrayRef> = HashMap::new();
        for (tuple_id, row_pos_desc) in &self.row_pos_descs {
            let output_slots = snapshot.lookup_output_slots(*tuple_id, row_pos_desc);
            if output_slots.is_empty() {
                continue;
            }
            let fetch_ref_slots = &row_pos_desc.fetch_ref_slots;
            let is_lake = is_lake_row_position(row_pos_desc.row_position_type);
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

                let response_columns = if is_lake {
                    execute_lake_lookup_request(query_id, *tuple_id, request_columns)?
                } else if self.is_local_backend(*backend_id)? {
                    execute_lookup_request(query_id, *tuple_id, request_columns)?
                } else {
                    self.lookup_remote(query_id, *tuple_id, &request_columns, *backend_id)?
                };

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
        #[allow(unused_variables)] // used when feature = "compat" is enabled
        let node_info = self
            .nodes_info
            .as_ref()
            .and_then(|info| find_node(info, backend_id))
            .ok_or_else(|| format!("node info not found for backend_id {}", backend_id))?;
        let mut req = proto::filter::LookupRequest {
            query_id: Some(proto::common::UniqueId {
                hi: query_id.hi,
                lo: query_id.lo,
            }),
            lookup_node_id: self.target_node_id,
            request_tuple_id: tuple_id,
            request_columns: Vec::with_capacity(request_columns.len()),
        };
        for (slot_id, array) in request_columns {
            let data = encode_column_ipc(array)?;
            req.request_columns.push(proto::filter::Column {
                slot_id: slot_id.as_u32() as i32,
                data_size: data.len() as i64,
                data,
            });
        }
        let port = lookup_async_internal_port(node_info)?;

        #[cfg(not(feature = "compat"))]
        let resp = crate::service::grpc_client::lookup(&node_info.host, port, req)?;
        #[cfg(feature = "compat")]
        let resp = crate::service::internal_rpc_client::lookup(&node_info.host, port, req)?;

        if let Some(status) = resp.status.as_ref()
            && status.code != 0
        {
            return Err(format!("lookup failed: {}", status.message));
        }
        let mut out = Vec::new();
        for col in resp.columns {
            let slot_id = SlotId::try_from(col.slot_id)?;
            if col.data.is_empty() {
                return Err("lookup response column missing data".to_string());
            }
            let array = decode_column_ipc(&col.data)?;
            out.push((slot_id, array));
        }
        Ok(out)
    }
}

#[cfg(all(test, feature = "compat"))]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use super::FetchProcessor;
    use crate::proto;
    use crate::runtime::descriptor_snapshot_thrift::test_lookup_nodes_info;
    use crate::runtime::query_context::QueryId;
    use crate::service::internal_rpc_client;

    #[test]
    fn test_lookup_remote_uses_nodes_info_async_internal_port() {
        let _hook_guard = internal_rpc_client::test_hook_lock();
        internal_rpc_client::clear_test_hooks();

        let captured = std::sync::Arc::new(std::sync::Mutex::new(None));
        let captured_hook = std::sync::Arc::clone(&captured);
        internal_rpc_client::set_lookup_hook(move |host, port, req| {
            *captured_hook.lock().expect("captured lock") = Some((host.to_string(), port, req));
            Ok(proto::filter::LookupResponse {
                status: Some(proto::common::Status {
                    code: 0,
                    message: String::new(),
                }),
                columns: Vec::new(),
            })
        });

        let processor = FetchProcessor {
            name: "FETCH (test)".to_string(),
            node_id: 1,
            target_node_id: 2,
            row_pos_descs: HashMap::new(),
            nodes_info: Some(test_lookup_nodes_info(9, "remote-host", 9911)),
            pending_output: None,
            finishing: false,
            output_chunk_schema: Arc::new(crate::exec::chunk::ChunkSchema::empty()),
        };

        let result = processor.lookup_remote(QueryId { hi: 1, lo: 2 }, 3, &HashMap::new(), 9);
        assert!(result.is_ok());
        let captured = captured.lock().expect("captured lock");
        let (host, port, req) = captured.as_ref().expect("captured lookup request");
        assert_eq!(host, "remote-host");
        assert_eq!(*port, 9911);
        assert_eq!(req.query_id, Some(proto::common::UniqueId { hi: 1, lo: 2 }));
        assert_eq!(req.lookup_node_id, 2);
        assert_eq!(req.request_tuple_id, 3);
        internal_rpc_client::clear_test_hooks();
    }
}

#[cfg(test)]
#[cfg(not(feature = "compat"))]
mod tests {
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    use arrow::array::{ArrayRef, Int32Array};

    use super::FetchProcessor;
    use crate::common::ids::SlotId;
    use crate::proto;
    use crate::runtime::query_context::QueryId;
    use crate::service::grpc_client;
    use crate::thrift::descriptors;

    #[test]
    fn test_lookup_remote_uses_native_grpc_lookup_in_pure_mode() {
        let _hook_guard = grpc_client::test_hook_lock();
        grpc_client::clear_test_hooks();

        let captured = Arc::new(Mutex::new(None));
        let captured_hook = Arc::clone(&captured);
        grpc_client::set_lookup_hook(move |host, port, req| {
            *captured_hook.lock().expect("captured lock") = Some((host.to_string(), port, req));
            Ok(proto::filter::LookupResponse {
                status: Some(proto::common::Status {
                    code: 0,
                    message: String::new(),
                }),
                columns: Vec::new(),
            })
        });

        let processor = FetchProcessor {
            name: "FETCH (test)".to_string(),
            node_id: 1,
            target_node_id: 2,
            row_pos_descs: HashMap::new(),
            nodes_info: Some(descriptors::TNodesInfo::new(
                1,
                vec![descriptors::TNodeInfo::new(
                    9,
                    0,
                    "remote-host".to_string(),
                    9911,
                )],
            )),
            pending_output: None,
            finishing: false,
            output_chunk_schema: Arc::new(crate::exec::chunk::ChunkSchema::empty()),
        };
        let request_columns = HashMap::from([(
            SlotId::new(2),
            Arc::new(Int32Array::from(vec![9])) as ArrayRef,
        )]);

        let result = processor.lookup_remote(QueryId { hi: 1, lo: 2 }, 3, &request_columns, 9);

        assert!(result.is_ok());
        let captured = captured.lock().expect("captured lock");
        let (host, port, req) = captured.as_ref().expect("captured lookup request");
        assert_eq!(host, "remote-host");
        assert_eq!(*port, 9911);
        assert_eq!(req.query_id, Some(proto::common::UniqueId { hi: 1, lo: 2 }));
        assert_eq!(req.lookup_node_id, 2);
        assert_eq!(req.request_tuple_id, 3);
        assert_eq!(req.request_columns.len(), 1);
        assert_eq!(req.request_columns[0].slot_id, 2);
        assert!(!req.request_columns[0].data.is_empty());
        grpc_client::clear_test_hooks();
    }
}

#[cfg(test)]
mod common_tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use super::FetchProcessor;
    use crate::runtime::query_context::QueryId;
    use crate::thrift::descriptors;

    #[test]
    fn test_lookup_remote_rejects_async_internal_port_out_of_u16_range() {
        let processor = FetchProcessor {
            name: "FETCH (test)".to_string(),
            node_id: 1,
            target_node_id: 2,
            row_pos_descs: HashMap::new(),
            nodes_info: Some(descriptors::TNodesInfo::new(
                1,
                vec![descriptors::TNodeInfo::new(
                    9,
                    0,
                    "remote-host".to_string(),
                    70_000,
                )],
            )),
            pending_output: None,
            finishing: false,
            output_chunk_schema: Arc::new(crate::exec::chunk::ChunkSchema::empty()),
        };

        let err = processor
            .lookup_remote(QueryId { hi: 1, lo: 2 }, 3, &HashMap::new(), 9)
            .expect_err("out-of-range async_internal_port must fail before lookup rpc");

        assert!(
            err.contains("async_internal_port") && err.contains("out of u16 range"),
            "unexpected error: {err}"
        );
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

fn find_node(nodes_info: &LookupNodesInfo, backend_id: i32) -> Option<&LookupNodeInfo> {
    nodes_info
        .nodes
        .iter()
        .find(|node| node.id == backend_id as i64)
}

fn lookup_async_internal_port(node_info: &LookupNodeInfo) -> Result<u16, String> {
    u16::try_from(node_info.async_internal_port).map_err(|_| {
        format!(
            "lookup async_internal_port {} for backend_id {} is out of u16 range",
            node_info.async_internal_port, node_info.id
        )
    })
}
