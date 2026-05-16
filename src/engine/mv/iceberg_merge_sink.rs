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
//! IVM-A1 merge sink: routes mixed +/- chunks to data-file writer or
//! A9 target locator, accumulating `WrittenFile`s and `PositionDeleteGroup`s
//! into a shared `IcebergCommitCollector`. Commit dispatch is owned by the
//! refresh driver (not this sink) per design §3 / §5.

use std::sync::Arc;

use arrow::array::Int8Array;
use arrow::record_batch::RecordBatch;
use iceberg::spec::DataFile;

use crate::connector::iceberg::commit::IcebergCommitCollector;
use crate::connector::iceberg::data_writer::IcebergStreamingDataFileWriter;
use crate::engine::iceberg_writer::data_file_to_written_file;
use crate::exec::change_op::{CHANGE_OP_COLUMN, CHANGE_OP_DELETE, CHANGE_OP_INSERT};
use crate::exec::chunk::Chunk;
use crate::exec::pipeline::operator::{Operator, ProcessorOperator};
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::runtime::global_async_runtime::data_block_on;
use crate::runtime::runtime_state::RuntimeState;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ApplyKeyValueType {
    Int64,
    Utf8,
}

pub struct IcebergMergeSinkPlan {
    pub target_table: iceberg::table::Table,
    pub collector: Arc<IcebergCommitCollector>,
    pub locator_state: Option<TargetLocatorState>,
    pub apply_key_column: String,
    pub apply_key_value_type: ApplyKeyValueType,
}

pub struct TargetLocatorState {
    pub existing_deletes_by_file: crate::engine::delete_flow::ExistingDeleteVisibilityByDataFile,
    pub referenced_data_file_partitions: crate::engine::delete_flow::ReferencedDataFilePartitions,
}

pub struct IcebergMergeSinkFactory {
    name: String,
    plan: Arc<IcebergMergeSinkPlan>,
}

impl IcebergMergeSinkFactory {
    pub fn new(plan: IcebergMergeSinkPlan) -> Self {
        let ident = plan.target_table.identifier();
        Self {
            name: format!(
                "IcebergMergeSink ({}.{})",
                ident.namespace().to_url_string(),
                ident.name(),
            ),
            plan: Arc::new(plan),
        }
    }
}

impl OperatorFactory for IcebergMergeSinkFactory {
    fn name(&self) -> &str {
        &self.name
    }

    fn create(&self, _dop: i32, driver_id: i32) -> Box<dyn Operator> {
        // A1 single-driver: only driver 0 owns the writer. Other drivers
        // produce no-op sinks. Multi-driver morsel allocation is deferred
        // to a later A1 phase.
        let writer = if driver_id == 0 {
            match IcebergStreamingDataFileWriter::new(self.plan.target_table.clone()) {
                Ok(w) => Some(w),
                Err(e) => {
                    return Box::new(FailedSinkOperator {
                        name: self.name.clone(),
                        error: e,
                    });
                }
            }
        } else {
            None
        };
        Box::new(IcebergMergeSinkOperator {
            name: self.name.clone(),
            plan: Arc::clone(&self.plan),
            writer,
            driver_id,
            finished: false,
        })
    }

    fn is_sink(&self) -> bool {
        true
    }
}

struct IcebergMergeSinkOperator {
    name: String,
    plan: Arc<IcebergMergeSinkPlan>,
    writer: Option<IcebergStreamingDataFileWriter>,
    driver_id: i32,
    finished: bool,
}

impl Operator for IcebergMergeSinkOperator {
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
        self.finished
    }
}

impl ProcessorOperator for IcebergMergeSinkOperator {
    fn need_input(&self) -> bool {
        !self.finished
    }

    fn has_output(&self) -> bool {
        false
    }

    fn push_chunk(&mut self, _state: &RuntimeState, chunk: Chunk) -> Result<(), String> {
        if self.driver_id != 0 {
            return Ok(());
        }
        let (insert_batch, delete_batch) = partition_chunk_by_change_op(&chunk)?;
        if let Some(batch) = insert_batch {
            let writer = self
                .writer
                .as_mut()
                .ok_or_else(|| "merge sink: writer missing on driver 0".to_string())?;
            data_block_on(writer.write_record_batch(strip_change_op(batch)?))??;
        }
        if let Some(batch) = delete_batch {
            self.handle_delete_batch(batch)?;
        }
        Ok(())
    }

    fn pull_chunk(&mut self, _state: &RuntimeState) -> Result<Option<Chunk>, String> {
        Err("merge sink does not produce output".to_string())
    }

    fn set_finishing(&mut self, _state: &RuntimeState) -> Result<(), String> {
        if let Some(writer) = self.writer.take() {
            let data_files: Vec<DataFile> = data_block_on(writer.finish())??;
            let partition_spec_id = self
                .plan
                .target_table
                .metadata()
                .default_partition_spec_id();
            for df in data_files {
                let wf = data_file_to_written_file(&df, partition_spec_id)?;
                self.plan.collector.inject_written_file(wf);
            }
        }
        self.finished = true;
        Ok(())
    }
}

impl IcebergMergeSinkOperator {
    fn handle_delete_batch(&self, batch: RecordBatch) -> Result<(), String> {
        let locator_state = self.plan.locator_state.as_ref().ok_or_else(|| {
            "merge sink: DELETE chunk arrived but no locator preloaded (refresh driver must call \
             load_target_apply_locator_inputs when has_deletes)"
                .to_string()
        })?;
        let groups = match self.plan.apply_key_value_type {
            ApplyKeyValueType::Int64 => {
                validate_i64_apply_key_column(&self.plan.apply_key_column)?;
                let apply_keys = extract_i64_apply_key_values_from_record_batch(
                    &batch,
                    &self.plan.apply_key_column,
                )?;
                if apply_keys.is_empty() {
                    return Ok(());
                }
                data_block_on(
                    crate::engine::mv::iceberg_target_apply::locate_target_rows_by_apply_key(
                        &self.plan.target_table,
                        &apply_keys,
                        &locator_state.existing_deletes_by_file,
                        &locator_state.referenced_data_file_partitions,
                    ),
                )??
            }
            ApplyKeyValueType::Utf8 => {
                let apply_keys = extract_utf8_apply_key_values_from_record_batch(
                    &batch,
                    &self.plan.apply_key_column,
                )?;
                if apply_keys.is_empty() {
                    return Ok(());
                }
                data_block_on(
                    crate::engine::mv::iceberg_target_apply::locate_target_rows_by_string_apply_key(
                        &self.plan.target_table,
                        &self.plan.apply_key_column,
                        &apply_keys,
                        &locator_state.existing_deletes_by_file,
                        &locator_state.referenced_data_file_partitions,
                    ),
                )??
            }
        };
        for group in groups {
            self.plan.collector.inject_delete_group(group);
        }
        Ok(())
    }
}

fn validate_i64_apply_key_column(apply_key_column: &str) -> Result<(), String> {
    if apply_key_column != crate::engine::mv::iceberg_target_apply::ICEBERG_MV_APPLY_KEY_COLUMN {
        return Err(format!(
            "merge sink: Int64 apply-key column must be {}, got {apply_key_column}",
            crate::engine::mv::iceberg_target_apply::ICEBERG_MV_APPLY_KEY_COLUMN
        ));
    }
    Ok(())
}

struct FailedSinkOperator {
    name: String,
    error: String,
}

impl Operator for FailedSinkOperator {
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
        false
    }
}

impl ProcessorOperator for FailedSinkOperator {
    fn need_input(&self) -> bool {
        true
    }
    fn has_output(&self) -> bool {
        false
    }
    fn push_chunk(&mut self, _state: &RuntimeState, _chunk: Chunk) -> Result<(), String> {
        Err(format!("merge sink failed to initialize: {}", self.error))
    }
    fn pull_chunk(&mut self, _state: &RuntimeState) -> Result<Option<Chunk>, String> {
        Err(format!("merge sink failed to initialize: {}", self.error))
    }
    fn set_finishing(&mut self, _state: &RuntimeState) -> Result<(), String> {
        Err(format!("merge sink failed to initialize: {}", self.error))
    }
}

fn partition_chunk_by_change_op(
    chunk: &Chunk,
) -> Result<(Option<RecordBatch>, Option<RecordBatch>), String> {
    let batch = &chunk.batch;
    let col_idx = batch
        .schema()
        .index_of(CHANGE_OP_COLUMN)
        .map_err(|_| format!("merge sink: chunk missing column {CHANGE_OP_COLUMN}"))?;
    let arr = batch
        .column(col_idx)
        .as_any()
        .downcast_ref::<Int8Array>()
        .ok_or_else(|| format!("merge sink: column {CHANGE_OP_COLUMN} must be Int8"))?;

    let mut insert_indices = Vec::new();
    let mut delete_indices = Vec::new();
    for (i, value) in arr.iter().enumerate() {
        match value {
            Some(CHANGE_OP_INSERT) => insert_indices.push(i),
            Some(CHANGE_OP_DELETE) => delete_indices.push(i),
            Some(other) => {
                return Err(format!(
                    "merge sink: unexpected {CHANGE_OP_COLUMN} value {other}"
                ));
            }
            None => return Err(format!("merge sink: null {CHANGE_OP_COLUMN}")),
        }
    }

    let take = |indices: &[usize]| -> Result<Option<RecordBatch>, String> {
        if indices.is_empty() {
            return Ok(None);
        }
        let index_arr =
            arrow::array::UInt32Array::from_iter_values(indices.iter().map(|&i| i as u32));
        let mut taken_columns = Vec::with_capacity(batch.num_columns());
        for col in batch.columns() {
            let taken = arrow::compute::take(col.as_ref(), &index_arr, None)
                .map_err(|e| format!("merge sink take: {e}"))?;
            taken_columns.push(taken);
        }
        let new_batch = RecordBatch::try_new(batch.schema(), taken_columns)
            .map_err(|e| format!("merge sink rebuild batch: {e}"))?;
        Ok(Some(new_batch))
    };

    Ok((take(&insert_indices)?, take(&delete_indices)?))
}

fn strip_change_op(batch: RecordBatch) -> Result<RecordBatch, String> {
    let schema = batch.schema();
    let Some(idx) = schema
        .fields()
        .iter()
        .position(|f| f.name() == CHANGE_OP_COLUMN)
    else {
        return Ok(batch);
    };
    let mut fields: Vec<arrow::datatypes::Field> =
        schema.fields().iter().map(|f| f.as_ref().clone()).collect();
    fields.remove(idx);
    let mut columns: Vec<arrow::array::ArrayRef> = batch.columns().to_vec();
    columns.remove(idx);
    let new_schema = Arc::new(arrow::datatypes::Schema::new(fields));
    RecordBatch::try_new(new_schema, columns)
        .map_err(|e| format!("merge sink strip __change_op: {e}"))
}

fn extract_i64_apply_key_values_from_record_batch(
    batch: &RecordBatch,
    apply_key_column: &str,
) -> Result<Vec<i64>, String> {
    let idx = batch.schema().index_of(apply_key_column).map_err(|_| {
        format!("merge sink: DELETE batch missing apply-key column {apply_key_column}")
    })?;
    let arr = batch
        .column(idx)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .ok_or_else(|| format!("merge sink: apply-key column {apply_key_column} must be Int64"))?;
    arr.iter()
        .map(|v| {
            v.ok_or_else(|| {
                format!("merge sink: null value in apply-key column {apply_key_column}")
            })
        })
        .collect()
}

fn extract_utf8_apply_key_values_from_record_batch(
    batch: &RecordBatch,
    apply_key_column: &str,
) -> Result<Vec<String>, String> {
    let idx = batch.schema().index_of(apply_key_column).map_err(|_| {
        format!("merge sink: DELETE batch missing apply-key column {apply_key_column}")
    })?;
    let arr = batch
        .column(idx)
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .ok_or_else(|| format!("merge sink: apply-key column {apply_key_column} must be Utf8"))?;
    arr.iter()
        .map(|v| {
            v.map(str::to_string).ok_or_else(|| {
                format!("merge sink: null value in apply-key column {apply_key_column}")
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, Int8Array, Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn chunk_with(batch: RecordBatch) -> Chunk {
        let schema = batch.schema();
        let slots = schema
            .fields()
            .iter()
            .enumerate()
            .map(|(i, f)| {
                crate::exec::chunk::ChunkSlotSchema::from_field(
                    crate::common::ids::SlotId::new(i as u32),
                    f.as_ref(),
                    None,
                )
                .unwrap()
            })
            .collect::<Vec<_>>();
        let chunk_schema = crate::exec::chunk::ChunkSchema::try_new(slots).unwrap();
        Chunk::try_new_with_chunk_schema(batch, Arc::new(chunk_schema)).unwrap()
    }

    #[test]
    fn partition_pure_insert_chunk() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("v", DataType::Int32, false),
            crate::exec::change_op::change_op_field(),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef,
                Arc::new(Int8Array::from(vec![CHANGE_OP_INSERT; 3])) as ArrayRef,
            ],
        )
        .unwrap();
        let chunk = chunk_with(batch);
        let (ins, del) = partition_chunk_by_change_op(&chunk).unwrap();
        assert_eq!(ins.unwrap().num_rows(), 3);
        assert!(del.is_none());
    }

    #[test]
    fn partition_mixed_chunk() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("v", DataType::Int32, false),
            crate::exec::change_op::change_op_field(),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3, 4])) as ArrayRef,
                Arc::new(Int8Array::from(vec![1, -1, 1, -1])) as ArrayRef,
            ],
        )
        .unwrap();
        let chunk = chunk_with(batch);
        let (ins, del) = partition_chunk_by_change_op(&chunk).unwrap();
        assert_eq!(ins.unwrap().num_rows(), 2);
        assert_eq!(del.unwrap().num_rows(), 2);
    }

    #[test]
    fn partition_rejects_unexpected_change_op_value() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("v", DataType::Int32, false),
            crate::exec::change_op::change_op_field(),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef,
                Arc::new(Int8Array::from(vec![CHANGE_OP_INSERT, 5])) as ArrayRef,
            ],
        )
        .unwrap();
        let chunk = chunk_with(batch);
        let err = partition_chunk_by_change_op(&chunk).unwrap_err();
        assert!(err.contains("unexpected"));
    }

    #[test]
    fn extract_apply_key_values_rejects_missing_column() {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int32Array::from(vec![1])) as ArrayRef],
        )
        .unwrap();
        let err = extract_i64_apply_key_values_from_record_batch(&batch, "__nova_base_row_id")
            .unwrap_err();
        assert!(err.contains("missing apply-key column"));
    }

    #[test]
    fn extract_utf8_apply_key_values_accepts_strings() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "__row_id__",
            DataType::Utf8,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(vec!["g1", "g2"])) as ArrayRef],
        )
        .unwrap();

        let values = extract_utf8_apply_key_values_from_record_batch(&batch, "__row_id__")
            .expect("utf8 keys");

        assert_eq!(values, vec!["g1".to_string(), "g2".to_string()]);
    }

    #[test]
    fn int64_apply_key_column_rejects_non_base_row_id_column() {
        let err = validate_i64_apply_key_column("__some_other_i64").unwrap_err();

        assert!(err.contains("__some_other_i64"), "err={err}");
        assert!(err.contains("__nova_base_row_id"), "err={err}");
    }
}
