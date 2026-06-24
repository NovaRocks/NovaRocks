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
//! Single build-side chunk store for hash join.

use std::sync::Arc;

use arrow::compute::concat_batches;
use arrow::record_batch::RecordBatch;

use crate::exec::chunk::{Chunk, ChunkSchemaRef};
use crate::runtime::mem_tracker::MemTracker;

#[derive(Debug, Clone)]
pub(crate) struct BuildStore {
    chunk: Arc<Chunk>,
}

impl BuildStore {
    pub(crate) fn new(chunk: Chunk) -> Self {
        Self {
            chunk: Arc::new(chunk),
        }
    }

    pub(crate) fn chunk(&self) -> Arc<Chunk> {
        Arc::clone(&self.chunk)
    }

    pub(crate) fn len(&self) -> usize {
        self.chunk.len()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.chunk.is_empty()
    }

    pub(crate) fn transfer_to(&mut self, tracker: &Arc<MemTracker>) {
        Arc::make_mut(&mut self.chunk).transfer_to(tracker);
    }
}

pub(crate) struct BuildStoreBuilder {
    schema: Option<ChunkSchemaRef>,
    batches: Vec<RecordBatch>,
    row_count: usize,
}

impl BuildStoreBuilder {
    pub(crate) fn new() -> Self {
        Self {
            schema: None,
            batches: Vec::new(),
            row_count: 0,
        }
    }

    pub(crate) fn push_chunk(&mut self, chunk: &Chunk) -> Result<(), String> {
        if chunk.is_empty() {
            return Ok(());
        }
        if let Some(schema) = self.schema.as_ref() {
            validate_chunk_schema(schema, chunk)?;
        } else {
            self.schema = Some(chunk.chunk_schema_ref());
        }
        self.row_count = self
            .row_count
            .checked_add(chunk.len())
            .ok_or_else(|| "join build store row count overflow".to_string())?;
        self.batches.push(chunk.batch.clone());
        Ok(())
    }

    pub(crate) fn row_count(&self) -> usize {
        self.row_count
    }

    pub(crate) fn finish(self) -> Result<Option<BuildStore>, String> {
        let Some(chunk_schema) = self.schema else {
            return Ok(None);
        };
        if self.batches.is_empty() {
            return Ok(None);
        }
        let arrow_schema = chunk_schema.arrow_schema_ref();
        let batch = if self.batches.len() == 1 {
            self.batches.into_iter().next().expect("one build batch")
        } else {
            concat_batches(&arrow_schema, &self.batches).map_err(|e| e.to_string())?
        };
        let chunk = Chunk::try_new_with_chunk_schema(batch, chunk_schema)?;
        Ok(Some(BuildStore::new(chunk)))
    }
}

fn validate_chunk_schema(expected: &ChunkSchemaRef, chunk: &Chunk) -> Result<(), String> {
    let actual = chunk.chunk_schema();
    if expected.slots() == actual.slots() {
        return Ok(());
    }
    if expected.slots().len() != actual.slots().len() {
        return Err(format!(
            "join build store schema slot count mismatch: expected={} actual={}",
            expected.slots().len(),
            actual.slots().len()
        ));
    }
    for (idx, (expected_slot, actual_slot)) in expected
        .slots()
        .iter()
        .zip(actual.slots().iter())
        .enumerate()
    {
        if expected_slot.slot_id() != actual_slot.slot_id() {
            return Err(format!(
                "join build store schema slot mismatch at index {idx}: expected={} actual={}",
                expected_slot.slot_id(),
                actual_slot.slot_id()
            ));
        }
        if expected_slot.field() != actual_slot.field() {
            return Err(format!(
                "join build store schema field mismatch at slot {}: expected={:?} actual={:?}",
                expected_slot.slot_id(),
                expected_slot.field(),
                actual_slot.field()
            ));
        }
    }
    Err("join build store schema mismatch".to_string())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Array, ArrayRef, Int32Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    use crate::common::ids::SlotId;
    use crate::exec::chunk::{Chunk, ChunkSchema};
    use crate::runtime::mem_tracker::MemTracker;

    use super::*;

    fn one_column_chunk_with_slot(slot_id: SlotId, name: &str, values: Vec<i32>) -> Chunk {
        let schema = Arc::new(Schema::new(vec![Field::new(name, DataType::Int32, false)]));
        let array = Arc::new(Int32Array::from(values)) as ArrayRef;
        let batch = RecordBatch::try_new(schema, vec![array]).expect("record batch");
        let chunk_schema =
            ChunkSchema::try_ref_from_schema_and_slot_ids(batch.schema().as_ref(), &[slot_id])
                .expect("chunk schema");
        Chunk::new_with_chunk_schema(batch, chunk_schema)
    }

    fn one_column_chunk(values: Vec<i32>) -> Chunk {
        one_column_chunk_with_slot(SlotId(1), "k", values)
    }

    #[test]
    fn build_store_builder_merges_chunks_into_single_chunk() {
        let mut builder = BuildStoreBuilder::new();

        builder
            .push_chunk(&one_column_chunk(vec![1, 2]))
            .expect("push");
        assert_eq!(builder.row_count(), 2);
        builder
            .push_chunk(&one_column_chunk(vec![3]))
            .expect("push");
        let store = builder.finish().expect("finish").expect("store");

        assert_eq!(store.len(), 3);
        assert!(!store.is_empty());
        assert_eq!(store.chunk().batch.num_columns(), 1);
        let chunk = store.chunk();
        let values = chunk
            .batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("int32 values");
        assert_eq!(values.value(0), 1);
        assert_eq!(values.value(1), 2);
        assert_eq!(values.value(2), 3);
    }

    #[test]
    fn build_store_builder_rejects_slot_schema_mismatch() {
        let mut builder = BuildStoreBuilder::new();

        builder
            .push_chunk(&one_column_chunk_with_slot(SlotId(1), "k", vec![1]))
            .expect("push first chunk");
        let err = builder
            .push_chunk(&one_column_chunk_with_slot(SlotId(2), "k", vec![2]))
            .expect_err("slot mismatch");

        assert!(err.contains("join build store schema slot mismatch"));
    }

    #[test]
    fn build_store_tracks_concatenated_batch_memory() {
        let mut builder = BuildStoreBuilder::new();

        builder
            .push_chunk(&one_column_chunk(vec![1, 2]))
            .expect("push");
        builder
            .push_chunk(&one_column_chunk(vec![3, 4]))
            .expect("push");
        let mut store = builder.finish().expect("finish").expect("store");
        let tracker = MemTracker::new_root("build-store-test");

        store.transfer_to(&tracker);

        assert!(tracker.current() > 0);
        drop(store);
        assert_eq!(tracker.current(), 0);
    }

    #[test]
    fn build_store_tracks_single_batch_memory() {
        let mut builder = BuildStoreBuilder::new();

        builder
            .push_chunk(&one_column_chunk(vec![1, 2]))
            .expect("push");
        let mut store = builder.finish().expect("finish").expect("store");
        let tracker = MemTracker::new_root("build-store-single-batch-test");

        store.transfer_to(&tracker);

        assert!(tracker.current() > 0);
        drop(store);
        assert_eq!(tracker.current(), 0);
    }
}
