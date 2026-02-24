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
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use arrow::array::{Array, ArrayRef, RecordBatch};
use arrow::buffer::Buffer;
use arrow::datatypes::{Schema, SchemaRef};

use crate::common::ids::SlotId;
use crate::runtime::mem_tracker::MemTracker;

/// A chunk of data, consisting of multiple rows.
/// Phase 2: Wrapper around Arrow RecordBatch.
#[derive(Debug, Clone)]
pub struct Chunk {
    pub batch: RecordBatch,
    slot_id_to_index: Arc<HashMap<SlotId, usize>>,
    accounting: Option<Arc<ChunkAccounting>>,
}

impl Chunk {
    pub fn try_new(batch: RecordBatch) -> Result<Self, String> {
        let slot_id_to_index = slot_id_to_index_from_schema(batch.schema().as_ref())?;
        Ok(Self {
            batch,
            slot_id_to_index: Arc::new(slot_id_to_index),
            accounting: None,
        })
    }

    pub fn try_new_strict(batch: RecordBatch) -> Result<Self, String> {
        let slot_id_to_index = slot_id_to_index_from_schema(batch.schema().as_ref())?;
        Ok(Self {
            batch,
            slot_id_to_index: Arc::new(slot_id_to_index),
            accounting: None,
        })
    }

    pub fn new(batch: RecordBatch) -> Self {
        match Self::try_new(batch) {
            Ok(v) => v,
            Err(e) => panic!("{e}"),
        }
    }

    pub fn schema(&self) -> SchemaRef {
        self.batch.schema()
    }

    pub fn slot_id_to_index(&self) -> &HashMap<SlotId, usize> {
        &self.slot_id_to_index
    }

    pub fn column_by_slot_id(&self, slot_id: SlotId) -> Result<ArrayRef, String> {
        let idx = self
            .slot_id_to_index
            .get(&slot_id)
            .copied()
            .ok_or_else(|| {
                format!(
                    "slot id {} not found in chunk (num_columns={}, slot_ids={:?})",
                    slot_id,
                    self.batch.num_columns(),
                    self.slot_id_to_index.keys().collect::<Vec<_>>()
                )
            })?;
        self.batch
            .columns()
            .get(idx)
            .cloned()
            .ok_or_else(|| format!("slot id {} mapped to invalid index {}", slot_id, idx))
    }

    pub fn len(&self) -> usize {
        self.batch.num_rows()
    }

    pub fn slice(&self, offset: usize, length: usize) -> Self {
        let mut out = Self {
            batch: self.batch.slice(offset, length),
            slot_id_to_index: Arc::clone(&self.slot_id_to_index),
            accounting: None,
        };
        if let Some(accounting) = self.accounting.as_ref() {
            let tracker = accounting.tracker();
            out.transfer_to(&tracker);
        }
        out
    }

    pub fn is_empty(&self) -> bool {
        self.batch.num_rows() == 0
    }

    pub fn columns(&self) -> &[ArrayRef] {
        self.batch.columns()
    }

    pub fn estimated_bytes(&self) -> usize {
        self.batch.get_array_memory_size()
    }

    pub fn logical_bytes(&self) -> usize {
        record_batch_bytes(&self.batch)
    }

    pub fn transfer_to(&mut self, tracker: &Arc<MemTracker>) {
        if let Some(accounting) = self.accounting.as_ref() {
            accounting.transfer_to(tracker);
            return;
        }
        let bytes = chunk_bytes_i64(&self.batch);
        if bytes <= 0 {
            return;
        }
        self.accounting = Some(Arc::new(ChunkAccounting::new(bytes, tracker)));
    }
}

pub const FIELD_META_SLOT_ID: &str = "novarocks.slot_id";

pub fn field_with_slot_id(
    field: arrow::datatypes::Field,
    slot_id: SlotId,
) -> arrow::datatypes::Field {
    let mut meta = field.metadata().clone();
    meta.insert(FIELD_META_SLOT_ID.to_string(), slot_id.to_string());
    field.with_metadata(meta)
}

pub fn field_slot_id(field: &arrow::datatypes::Field) -> Result<Option<SlotId>, String> {
    let Some(v) = field.metadata().get(FIELD_META_SLOT_ID) else {
        return Ok(None);
    };
    Ok(Some(v.parse::<SlotId>()?))
}

fn slot_id_to_index_from_schema(schema: &Schema) -> Result<HashMap<SlotId, usize>, String> {
    let mut map = HashMap::new();
    for (idx, f) in schema.fields().iter().enumerate() {
        let slot_id = field_slot_id(f.as_ref())?.ok_or_else(|| {
            format!(
                "missing {} in chunk schema field at index {} (name={})",
                FIELD_META_SLOT_ID,
                idx,
                f.name()
            )
        })?;
        if map.insert(slot_id, idx).is_some() {
            // Slot id collision in a single chunk is a logic error and would make expression evaluation ambiguous.
            let mut slots = Vec::new();
            for (i, ff) in schema.fields().iter().enumerate() {
                slots.push((
                    i,
                    ff.name().to_string(),
                    field_slot_id(ff.as_ref())?.map(|v| v.to_string()),
                ));
            }
            return Err(format!(
                "duplicate slot id {} in chunk schema: fields={:?}",
                slot_id, slots
            ));
        }
    }
    if !schema.fields().is_empty() && map.len() != schema.fields().len() {
        return Err(format!(
            "chunk schema slot id map is incomplete: fields={} mapped={}",
            schema.fields().len(),
            map.len()
        ));
    }
    Ok(map)
}

/// Estimate RecordBatch size by summing unique buffers inside the batch.
///
/// NOTE: Scheme S (per-batch accounting).
/// We de-duplicate buffers only within a single RecordBatch.
/// Shared buffers across batches (e.g., slices/dictionaries) will be double-counted.
/// TODO: Upgrade to Scheme R with global buffer refcount to avoid cross-batch double counting.
pub fn record_batch_bytes(batch: &RecordBatch) -> usize {
    let mut seen = HashSet::new();
    let mut total = 0usize;
    for column in batch.columns() {
        total = total.saturating_add(array_data_bytes(&column.to_data(), &mut seen));
    }
    total
}

fn array_data_bytes(data: &arrow::array::ArrayData, seen: &mut HashSet<usize>) -> usize {
    let mut total = 0usize;
    for buffer in data.buffers() {
        total = total.saturating_add(buffer_bytes(buffer, seen));
    }
    if let Some(nulls) = data.nulls() {
        total = total.saturating_add(buffer_bytes(nulls.buffer(), seen));
    }
    for child in data.child_data() {
        total = total.saturating_add(array_data_bytes(child, seen));
    }
    total
}

fn buffer_bytes(buffer: &Buffer, seen: &mut HashSet<usize>) -> usize {
    let ptr = buffer.data_ptr().as_ptr() as usize;
    if !seen.insert(ptr) {
        return 0;
    }
    buffer.capacity().max(buffer.len())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field};
    use std::sync::Arc;

    #[test]
    fn strict_requires_slot_id_metadata_for_all_fields() {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, true)]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1, 2]))])
            .expect("record batch");
        let err = Chunk::try_new_strict(batch).expect_err("expected strict error");
        assert!(err.contains(FIELD_META_SLOT_ID), "err={}", err);
    }

    #[test]
    fn strict_rejects_duplicate_slot_id() {
        let schema = Arc::new(Schema::new(vec![
            field_with_slot_id(Field::new("a", DataType::Int32, true), SlotId::new(1)),
            field_with_slot_id(Field::new("b", DataType::Int32, true), SlotId::new(1)),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(Int32Array::from(vec![3, 4])),
            ],
        )
        .expect("record batch");
        let err = Chunk::try_new_strict(batch).expect_err("expected duplicate error");
        assert!(err.contains("duplicate slot id"), "err={}", err);
    }
}

impl Default for Chunk {
    fn default() -> Self {
        Self {
            batch: RecordBatch::new_empty(Arc::new(Schema::empty())),
            slot_id_to_index: Arc::new(HashMap::new()),
            accounting: None,
        }
    }
}

#[derive(Debug)]
struct ChunkAccounting {
    bytes: i64,
    tracker: Mutex<Arc<MemTracker>>,
}

impl ChunkAccounting {
    fn new(bytes: i64, tracker: &Arc<MemTracker>) -> Self {
        tracker.consume(bytes);
        Self {
            bytes,
            tracker: Mutex::new(Arc::clone(tracker)),
        }
    }

    fn transfer_to(&self, tracker: &Arc<MemTracker>) {
        let mut guard = self.tracker.lock().unwrap_or_else(|e| e.into_inner());
        if Arc::ptr_eq(&guard, tracker) {
            return;
        }
        guard.release(self.bytes);
        tracker.consume(self.bytes);
        *guard = Arc::clone(tracker);
    }

    fn tracker(&self) -> Arc<MemTracker> {
        let guard = self.tracker.lock().unwrap_or_else(|e| e.into_inner());
        Arc::clone(&guard)
    }
}

impl Drop for ChunkAccounting {
    fn drop(&mut self) {
        let guard = self.tracker.lock().unwrap_or_else(|e| e.into_inner());
        guard.release(self.bytes);
    }
}

fn chunk_bytes_i64(batch: &RecordBatch) -> i64 {
    i64::try_from(record_batch_bytes(batch)).unwrap_or(i64::MAX)
}
