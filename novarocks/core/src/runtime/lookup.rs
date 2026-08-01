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
use std::collections::{HashMap, VecDeque};
use std::io::Cursor;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{ArrayRef, Int32Array, Int64Array, UInt32Array, new_empty_array};
use arrow::compute::{concat, take};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;

use crate::common::ids::SlotId;
use crate::connector::starrocks::scan::read_starrocks_batches;
use crate::exec::chunk::{ChunkSchema, ChunkSlotSchema};
use crate::novarocks_connectors::StarRocksScanConfig;
use crate::runtime::descriptor_snapshot::{DescriptorSlot, DescriptorSnapshot};
use crate::runtime::descriptor_snapshot::{is_iceberg_v3_row_position, is_lake_row_position};
use crate::runtime::query_context::{QueryId, query_context_manager};
use novarocks_spi::connector::{
    ConnectorBatchBudget, ConnectorBatchReader, ConnectorCancellation, ConnectorOpenReaderRequest,
    ConnectorRequestContext, MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES, MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
};

struct LookupCancellation {
    query_id: QueryId,
}

impl ConnectorCancellation for LookupCancellation {
    fn is_cancelled(&self) -> bool {
        query_context_manager().is_query_canceled(self.query_id)
    }
}

fn lookup_slot_meta<'a>(
    snapshot: &'a DescriptorSnapshot,
    tuple_id: i32,
    slots: &[SlotId],
) -> Result<HashMap<SlotId, &'a DescriptorSlot>, String> {
    let mut map = HashMap::with_capacity(slots.len());
    for slot in slots {
        let meta = snapshot
            .slot(tuple_id, *slot)
            .ok_or_else(|| format!("missing slot meta for tuple_id={} slot {}", tuple_id, slot))?;
        map.insert(*slot, meta);
    }
    Ok(map)
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

fn execute_connector_lookup_request(
    query_id: QueryId,
    row_source_slot: SlotId,
    lookup_slots: &[SlotId],
    slot_meta: &HashMap<SlotId, &DescriptorSlot>,
    mut range_to_positions: HashMap<i32, HashMap<i64, VecDeque<usize>>>,
    request_len: usize,
) -> Result<Vec<(SlotId, ArrayRef)>, String> {
    let lookup_metas = lookup_slots
        .iter()
        .map(|slot| {
            slot_meta
                .get(slot)
                .copied()
                .ok_or_else(|| format!("missing slot meta for slot {slot}"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let row_id_index = lookup_metas
        .iter()
        .position(|meta| crate::exec::row_position::is_row_id(&meta.name));
    let mut provider_fields = lookup_metas
        .iter()
        .map(|meta| meta.field.as_ref().clone())
        .collect::<Vec<_>>();
    let row_id_index = match row_id_index {
        Some(index) => index,
        None => {
            provider_fields.push(Field::new("_row_id", DataType::Int64, false));
            provider_fields.len() - 1
        }
    };
    let provider_schema = Arc::new(Schema::new(provider_fields));
    let context = ConnectorRequestContext::try_new(
        Instant::now() + Duration::from_secs(3600),
        Arc::new(LookupCancellation { query_id }),
        MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
        MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
    )
    .map_err(|error| error.to_string())?;
    let request = ConnectorOpenReaderRequest {
        expected_schema: Arc::clone(&provider_schema),
        batch: ConnectorBatchBudget {
            max_rows: NonZeroUsize::new(4096).expect("constant is nonzero"),
            max_bytes: NonZeroUsize::new(64 * 1024 * 1024).expect("constant is nonzero"),
        },
        context,
    };

    let mut column_chunks: HashMap<SlotId, Vec<ArrayRef>> = HashMap::new();
    let mut response_positions = Vec::with_capacity(request_len);
    for (scan_range_id, mut positions_map) in range_to_positions.drain() {
        let (binding, split) = query_context_manager()
            .connector_glm_split(query_id, row_source_slot, scan_range_id)
            .ok_or_else(|| {
                format!(
                    "connector late-materialization split {} is not registered",
                    scan_range_id
                )
            })?;
        let mut reader = binding
            .read()
            .ok_or_else(|| "connector lookup binding has no read capability".to_string())?
            .open_reader(&split, request.clone())
            .map_err(|error| error.to_string())?;
        let read_result = (|| -> Result<(), String> {
            while let Some(batch) = reader.next_batch().map_err(|error| error.to_string())? {
                let row_ids = batch
                    .column(row_id_index)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| "connector lookup _row_id must be Int64".to_string())?;
                for row_idx in 0..batch.num_rows() {
                    let row_id = row_ids.value(row_idx);
                    let Some(queue) = positions_map.get_mut(&row_id) else {
                        continue;
                    };
                    while let Some(position) = queue.pop_front() {
                        for (column_index, slot) in lookup_slots.iter().enumerate() {
                            let column = batch.column(column_index);
                            let taken = take(
                                column.as_ref(),
                                &UInt32Array::from(vec![row_idx as u32]),
                                None,
                            )
                            .map_err(|error| error.to_string())?;
                            column_chunks.entry(*slot).or_default().push(taken);
                        }
                        response_positions.push(position);
                    }
                }
            }
            for (row_id, queue) in positions_map {
                if !queue.is_empty() {
                    return Err(format!(
                        "connector lookup failed to materialize row_id {} ({} pending)",
                        row_id,
                        queue.len()
                    ));
                }
            }
            Ok(())
        })();
        let close_result = reader.close().map_err(|error| error.to_string());
        match (read_result, close_result) {
            (Ok(()), Ok(())) => {}
            (Ok(()), Err(cleanup)) => return Err(cleanup),
            (Err(primary), Ok(())) => return Err(primary),
            (Err(primary), Err(cleanup)) => {
                return Err(format!("{primary} (cleanup: {cleanup})"));
            }
        }
    }
    if response_positions.len() != request_len {
        return Err(format!(
            "connector lookup response size mismatch: expected {} got {}",
            request_len,
            response_positions.len()
        ));
    }
    let mut response_indices = vec![u32::MAX; request_len];
    for (response_index, request_index) in response_positions.iter().enumerate() {
        if response_indices[*request_index] != u32::MAX {
            return Err(format!(
                "duplicate connector lookup response position {request_index}"
            ));
        }
        response_indices[*request_index] = response_index as u32;
    }
    if response_indices.contains(&u32::MAX) {
        return Err("connector lookup response is missing positions".to_string());
    }
    let response_indices = UInt32Array::from(response_indices);
    lookup_slots
        .iter()
        .map(|slot| {
            let chunks = column_chunks
                .get(slot)
                .ok_or_else(|| format!("missing connector lookup column {slot}"))?;
            let refs = chunks
                .iter()
                .map(|chunk| chunk.as_ref())
                .collect::<Vec<_>>();
            let full = concat(&refs).map_err(|error| error.to_string())?;
            take(&full, &response_indices, None)
                .map(|array| (*slot, array))
                .map_err(|error| error.to_string())
        })
        .collect()
}

pub fn execute_lookup_request(
    query_id: QueryId,
    tuple_id: i32,
    request_columns: HashMap<SlotId, ArrayRef>,
) -> Result<Vec<(SlotId, ArrayRef)>, String> {
    let mgr = query_context_manager();
    let row_pos_desc = mgr
        .row_pos_desc(query_id, tuple_id)
        .ok_or_else(|| format!("row position descriptor missing for tuple_id={}", tuple_id))?;
    if !is_iceberg_v3_row_position(row_pos_desc.row_position_type) {
        return Err(format!(
            "unsupported row position type: {:?}",
            row_pos_desc.row_position_type
        ));
    }
    let snapshot = mgr
        .descriptor_snapshot(query_id)
        .ok_or_else(|| "descriptor snapshot missing for lookup".to_string())?;
    let lookup_slots = snapshot.lookup_output_slots(tuple_id, &row_pos_desc);
    let slot_meta = lookup_slot_meta(&snapshot, tuple_id, &lookup_slots)?;

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
            let empty = new_empty_array(meta.field.data_type());
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

    for scan_range_id in range_to_positions.keys() {
        if mgr
            .connector_glm_split(query_id, row_pos_desc.row_source_slot, *scan_range_id)
            .is_none()
        {
            return Err(format!(
                "Iceberg late-materialization split {} is not bound to a connector instance",
                scan_range_id
            ));
        }
    }
    execute_connector_lookup_request(
        query_id,
        row_pos_desc.row_source_slot,
        &lookup_slots,
        &slot_meta,
        range_to_positions,
        request_len,
    )
}

pub(crate) fn execute_position_lookup_request(
    query_id: QueryId,
    tuple_id: i32,
    request_columns: HashMap<SlotId, ArrayRef>,
) -> Result<Vec<(SlotId, ArrayRef)>, String> {
    let row_position_type = query_context_manager()
        .row_pos_desc(query_id, tuple_id)
        .ok_or_else(|| format!("row position descriptor missing for tuple_id={}", tuple_id))?
        .row_position_type;
    if is_lake_row_position(row_position_type) {
        return Err("lake late-materialization lookup is retired".to_string());
    }

    execute_lookup_request(query_id, tuple_id, request_columns)
}
