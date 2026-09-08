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
use std::sync::{Arc, Mutex};

use arrow::array::{ArrayRef, RecordBatch, RecordBatchOptions};
use arrow::datatypes::{Schema, SchemaRef};

use crate::runtime::mem_tracker::MemTracker;
use novarocks_spi::connector::ConnectorOutputMemoryToken;
use novarocks_types::SlotId;

use super::memory::{ChunkAccounting, ChunkMemoryLease, chunk_bytes_i64, record_batch_bytes};
use super::schema::{
    ChunkSchema, ChunkSchemaRef, align_chunk_schema_to_batch, align_chunk_schema_to_columns,
};
use super::type_compatibility::retag_column;

/// A chunk of data, consisting of multiple rows.
/// Phase 2: Wrapper around Arrow RecordBatch.
#[derive(Debug, Clone)]
pub struct Chunk {
    pub batch: RecordBatch,
    chunk_schema: ChunkSchemaRef,
    accounting: Option<Arc<ChunkAccounting>>,
    /// A provider reservation that already charges the Arrow buffers in this
    /// chunk to the admitted fragment hierarchy.
    ///
    /// The mutex makes the opaque SPI lease shareable with zero-copy `Chunk`
    /// clones. While it is present, `transfer_to` must not create a second
    /// charge for the same buffers. The last chunk owner releases the original
    /// provider charge.
    connector_output_memory: Option<Arc<Mutex<ConnectorOutputMemoryToken>>>,
}

impl Chunk {
    pub fn try_new_with_columns(
        chunk_schema: ChunkSchemaRef,
        columns: Vec<ArrayRef>,
    ) -> Result<Self, String> {
        let row_count = columns.first().map(|col| col.len()).unwrap_or(0);
        let chunk_schema = align_chunk_schema_to_columns(&columns, chunk_schema.as_ref())?;
        let columns = retag_columns_to_chunk_schema(&columns, chunk_schema.as_ref())?;
        let batch = build_record_batch(chunk_schema.arrow_schema_ref(), columns, row_count)?;
        Ok(Self {
            batch,
            chunk_schema,
            accounting: None,
            connector_output_memory: None,
        })
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
        let arrow_schema = chunk_schema.arrow_schema_ref();
        if batch.schema().as_ref() == arrow_schema.as_ref() {
            return Ok(Self {
                batch,
                chunk_schema,
                accounting: None,
                connector_output_memory: None,
            });
        }
        let row_count = batch.num_rows();
        let chunk_schema = align_chunk_schema_to_batch(&batch, chunk_schema.as_ref())?;
        let columns = retag_columns_to_chunk_schema(batch.columns(), chunk_schema.as_ref())?;
        let batch = build_record_batch(chunk_schema.arrow_schema_ref(), columns, row_count)?;
        Ok(Self {
            batch,
            chunk_schema,
            accounting: None,
            connector_output_memory: None,
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
            connector_output_memory: self.connector_output_memory.clone(),
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
        if self.connector_output_memory.is_some() {
            return;
        }
        let bytes = chunk_bytes_i64(&self.batch);
        if bytes <= 0 {
            return;
        }
        self.accounting = Some(Arc::new(ChunkAccounting::new(bytes, tracker)));
    }

    /// Transfers this chunk's complete accounting lease while enforcing the
    /// destination tracker limit. A failed first charge remains attached to
    /// the chunk so dropping it releases the live bytes instead of leaking an
    /// exceeded counter.
    pub fn try_transfer_to(&mut self, tracker: &Arc<MemTracker>) -> Result<(), String> {
        if let Some(accounting) = self.accounting.as_ref() {
            return accounting.try_transfer_to(tracker);
        }
        if self.connector_output_memory.is_some() {
            return Ok(());
        }
        let bytes = chunk_bytes_i64(&self.batch);
        if bytes <= 0 {
            return Ok(());
        }
        let result = tracker.consume_and_check_limit(bytes);
        self.accounting = Some(Arc::new(ChunkAccounting::from_charged(bytes, tracker)));
        result
    }

    #[cfg(test)]
    pub(crate) fn memory_lease(&self) -> Option<ChunkMemoryLease> {
        self.accounting
            .as_ref()
            .map(|accounting| ChunkMemoryLease::native(Arc::clone(accounting)))
    }

    /// Moves this chunk's accounting owner without cloning it. Consumers that
    /// retain only a zero-copy projection can then split the projected bytes
    /// from the unprojected remainder exactly.
    pub(crate) fn take_memory_lease(&mut self) -> Option<ChunkMemoryLease> {
        if let Some(accounting) = self.accounting.take() {
            return Some(ChunkMemoryLease::native(accounting));
        }
        let output_memory = self.connector_output_memory.take()?;
        if Arc::strong_count(&output_memory) == 1 {
            return Some(ChunkMemoryLease::connector(output_memory));
        }
        // A clone or slice still owns the same provider reservation. Keep this
        // chunk attached as well so the shared charge cannot disappear while
        // either Arrow view remains live.
        self.connector_output_memory = Some(output_memory);
        None
    }

    /// Move an existing connector output reservation onto this chunk without
    /// charging its Arrow buffers again.
    pub(crate) fn attach_connector_output_memory(
        &mut self,
        output_memory: ConnectorOutputMemoryToken,
    ) -> Result<(), String> {
        if self.accounting.is_some() || self.connector_output_memory.is_some() {
            return Err("chunk output memory already has an accounting owner".to_string());
        }
        self.connector_output_memory = Some(Arc::new(Mutex::new(output_memory)));
        Ok(())
    }
}

fn retag_columns_to_chunk_schema(
    columns: &[ArrayRef],
    chunk_schema: &ChunkSchema,
) -> Result<Vec<ArrayRef>, String> {
    columns
        .iter()
        .zip(chunk_schema.slots())
        .enumerate()
        .map(|(idx, (column, slot))| {
            retag_column(column, slot.data_type()).map_err(|e| {
                format!(
                    "retag chunk column {} to target descriptor type {:?} failed: {:?}",
                    idx,
                    slot.data_type(),
                    e
                )
            })
        })
        .collect()
}

fn build_record_batch(
    schema: SchemaRef,
    columns: Vec<ArrayRef>,
    row_count: usize,
) -> Result<RecordBatch, String> {
    let result = if columns.is_empty() {
        let options = RecordBatchOptions::new().with_row_count(Some(row_count));
        RecordBatch::try_new_with_options(schema, columns, &options)
    } else {
        RecordBatch::try_new(schema, columns)
    };
    result.map_err(|e| format!("build chunk record batch failed: {e}"))
}

impl Default for Chunk {
    fn default() -> Self {
        Self {
            batch: RecordBatch::new_empty(Arc::new(Schema::empty())),
            chunk_schema: Arc::new(ChunkSchema::empty()),
            accounting: None,
            connector_output_memory: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Array, ArrayRef, DictionaryArray, LargeStringDictionaryBuilder};
    use arrow::datatypes::{DataType, Field, Int32Type};

    use super::{Chunk, ChunkSchema};
    use crate::exec::chunk::ChunkSlotSchema;
    use novarocks_types::SlotId;

    fn dict_utf8() -> ArrayRef {
        Arc::new(
            vec!["PAID", "NEW", "PAID"]
                .into_iter()
                .collect::<DictionaryArray<Int32Type>>(),
        )
    }

    fn dict_large_utf8() -> ArrayRef {
        let mut builder = LargeStringDictionaryBuilder::<Int32Type>::new();
        builder.append_value("PAID");
        builder.append_value("NEW");
        builder.append_value("PAID");
        Arc::new(builder.finish())
    }

    #[test]
    fn chunk_string_slot_can_carry_dictionary_int32_string_column() {
        for (slot_type, column) in [
            (DataType::Utf8, dict_utf8()),
            (DataType::LargeUtf8, dict_large_utf8()),
        ] {
            let dict_type = column.data_type().clone();
            assert!(matches!(
                dict_type,
                DataType::Dictionary(ref key, ref value)
                    if key.as_ref() == &DataType::Int32 && value.as_ref() == &slot_type
            ));

            let chunk_schema = Arc::new(
                ChunkSchema::try_new(vec![ChunkSlotSchema::new_with_field(
                    SlotId::new(3),
                    Field::new("status", slot_type, false),
                    None,
                    None,
                )])
                .expect("chunk schema"),
            );

            let chunk =
                Chunk::try_new_with_columns(chunk_schema, vec![column]).expect("dictionary chunk");

            assert_eq!(chunk.len(), 3);
            assert_eq!(chunk.columns()[0].data_type(), &dict_type);
            assert_eq!(
                chunk
                    .chunk_schema()
                    .slot(SlotId::new(3))
                    .expect("slot schema")
                    .data_type(),
                &dict_type
            );
            assert_eq!(
                chunk
                    .column_by_slot_id(SlotId::new(3))
                    .expect("column by slot")
                    .data_type(),
                &dict_type
            );
        }
    }
}
