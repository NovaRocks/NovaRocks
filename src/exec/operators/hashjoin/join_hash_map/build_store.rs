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
            if schema.slots().len() != chunk.chunk_schema().slots().len() {
                return Err(format!(
                    "join build store schema slot mismatch: expected={} actual={}",
                    schema.slots().len(),
                    chunk.chunk_schema().slots().len()
                ));
            }
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Array, ArrayRef, Int32Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    use crate::common::ids::SlotId;
    use crate::exec::chunk::{Chunk, ChunkSchema};

    use super::*;

    fn one_column_chunk(values: Vec<i32>) -> Chunk {
        let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int32, false)]));
        let array = Arc::new(Int32Array::from(values)) as ArrayRef;
        let batch = RecordBatch::try_new(schema, vec![array]).expect("record batch");
        let chunk_schema =
            ChunkSchema::try_ref_from_schema_and_slot_ids(batch.schema().as_ref(), &[SlotId(1)])
                .expect("chunk schema");
        Chunk::new_with_chunk_schema(batch, chunk_schema)
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
}
