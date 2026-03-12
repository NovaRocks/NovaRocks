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

use arrow::array::{ArrayRef, RecordBatch};
use arrow::datatypes::{Schema, SchemaRef};

use crate::common::ids::SlotId;
use crate::runtime::mem_tracker::MemTracker;

use super::memory::{ChunkAccounting, chunk_bytes_i64, record_batch_bytes};
use super::schema::{ChunkSchema, ChunkSchemaRef, validate_chunk_schema_against_batch};

/// A chunk of data, consisting of multiple rows.
/// Phase 2: Wrapper around Arrow RecordBatch.
#[derive(Debug, Clone)]
pub struct Chunk {
    pub batch: RecordBatch,
    chunk_schema: ChunkSchemaRef,
    accounting: Option<Arc<ChunkAccounting>>,
}

impl Chunk {
    pub fn try_new_with_columns(
        chunk_schema: ChunkSchemaRef,
        columns: Vec<ArrayRef>,
    ) -> Result<Self, String> {
        let batch = RecordBatch::try_new(chunk_schema.arrow_schema_ref(), columns)
            .map_err(|e| format!("build chunk record batch failed: {e}"))?;
        Self::try_new_with_chunk_schema(batch, chunk_schema)
    }

    pub fn try_new_like(batch: RecordBatch, source: &Chunk) -> Result<Self, String> {
        Self::try_new_with_chunk_schema(batch, source.chunk_schema_ref())
    }

    pub fn new_like(batch: RecordBatch, source: &Chunk) -> Self {
        match Self::try_new_like(batch, source) {
            Ok(v) => v,
            Err(e) => panic!("{e}"),
        }
    }

    pub fn try_new_with_chunk_schema(
        batch: RecordBatch,
        chunk_schema: ChunkSchemaRef,
    ) -> Result<Self, String> {
        validate_chunk_schema_against_batch(&batch, chunk_schema.as_ref())?;
        Ok(Self {
            batch,
            chunk_schema,
            accounting: None,
        })
    }

    pub fn new_with_chunk_schema(batch: RecordBatch, chunk_schema: ChunkSchemaRef) -> Self {
        match Self::try_new_with_chunk_schema(batch, chunk_schema) {
            Ok(v) => v,
            Err(e) => panic!("{e}"),
        }
    }

    pub fn schema(&self) -> SchemaRef {
        self.batch.schema()
    }

    pub fn chunk_schema(&self) -> &ChunkSchema {
        self.chunk_schema.as_ref()
    }

    pub fn chunk_schema_ref(&self) -> ChunkSchemaRef {
        Arc::clone(&self.chunk_schema)
    }

    pub fn slot_id_to_index(&self) -> &HashMap<SlotId, usize> {
        self.chunk_schema.index_by_slot()
    }

    pub fn column_by_slot_id(&self, slot_id: SlotId) -> Result<ArrayRef, String> {
        let idx = self
            .chunk_schema
            .index_by_slot()
            .get(&slot_id)
            .copied()
            .ok_or_else(|| {
                format!(
                    "slot id {} not found in chunk (num_columns={}, slot_ids={:?})",
                    slot_id,
                    self.batch.num_columns(),
                    self.chunk_schema.index_by_slot().keys().collect::<Vec<_>>()
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
            chunk_schema: Arc::clone(&self.chunk_schema),
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

impl Default for Chunk {
    fn default() -> Self {
        Self {
            batch: RecordBatch::new_empty(Arc::new(Schema::empty())),
            chunk_schema: Arc::new(ChunkSchema::empty()),
            accounting: None,
        }
    }
}
