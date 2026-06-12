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
//! A9 target locator, accumulating writer-reported files and
//! `PositionDeleteGroup`s into a shared `IcebergCommitCollector`. Commit
//! dispatch is owned by the refresh driver (not this sink) per design §3 / §5.

use std::sync::Arc;

use arrow::array::Int8Array;
use arrow::record_batch::RecordBatch;
use iceberg::spec::DataFile;

use crate::connector::iceberg::commit::IcebergCommitCollector;
use crate::connector::iceberg::data_writer::{
    IcebergStreamingDataFileWriter, written_file_to_sink_commit_info_for_metadata,
};
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
    BranchInt64,
    BranchUtf8,
}

pub struct IcebergMergeSinkPlan {
    pub target_table: iceberg::table::Table,
    pub collector: Arc<IcebergCommitCollector>,
    pub locator_state: Option<TargetLocatorState>,
    pub apply_key_column: String,
    pub apply_key_value_type: ApplyKeyValueType,
    /// Partition allow-list for the delete-side locator. `None` = no pruning
    /// (join / union / unpartitioned / NotDerived). Derived from the refresh
    /// context's `affected_partitions` at construction; when it is an
    /// `AllowList`, the locator skips target files whose partition key is not
    /// in the set, mirroring the target-state read-side pruning.
    pub partition_filter: crate::engine::mv::partition::TargetPartitionFilter,
    /// Target-visible partition derivation used when plan-time affected
    /// partitions are not available, for example join-side row movement.
    pub(crate) partition_derivation: Option<BoundTargetPartitionDerivation>,
}

pub struct TargetLocatorState {
    pub existing_deletes_by_file: crate::engine::delete_flow::ExistingDeleteVisibilityByDataFile,
    pub referenced_data_file_partitions: crate::engine::delete_flow::ReferencedDataFilePartitions,
}

#[derive(Clone, Debug)]
pub(crate) struct BoundTargetPartitionDerivation {
    pub(crate) target_spec_id: i32,
    pub(crate) bound_fields: Vec<crate::engine::mv::partition::BoundPartitionField>,
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
            let metadata = self.plan.target_table.metadata();
            let partition_spec_id = metadata.default_partition_spec_id();
            let sink_commit_infos = data_files
                .into_iter()
                .map(|df| {
                    let wf = data_file_to_written_file(&df, partition_spec_id)?;
                    written_file_to_sink_commit_info_for_metadata(&wf, metadata)
                })
                .collect::<Result<Vec<_>, _>>()?;
            self.plan
                .collector
                .inject_sink_commit_infos(sink_commit_infos)?;
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
        let partition_filter = delete_batch_partition_filter(
            &self.plan.partition_filter,
            self.plan.partition_derivation.as_ref(),
            &batch,
        )?;
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
                        &partition_filter,
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
                        &partition_filter,
                    ),
                )??
            }
            ApplyKeyValueType::BranchInt64 => {
                let apply_keys = extract_branch_i64_apply_key_values_from_record_batch(&batch)?;
                if apply_keys.is_empty() {
                    return Ok(());
                }
                data_block_on(
                    crate::engine::mv::iceberg_target_apply::locate_target_rows_by_branch_apply_key(
                        &self.plan.target_table,
                        &apply_keys,
                        &locator_state.existing_deletes_by_file,
                        &locator_state.referenced_data_file_partitions,
                        &partition_filter,
                    ),
                )??
            }
            ApplyKeyValueType::BranchUtf8 => {
                let apply_keys = extract_branch_utf8_apply_key_values_from_record_batch(
                    &batch,
                    &self.plan.apply_key_column,
                )?;
                if apply_keys.is_empty() {
                    return Ok(());
                }
                data_block_on(
                    crate::engine::mv::iceberg_target_apply::locate_target_rows_by_branch_string_apply_key(
                        &self.plan.target_table,
                        &self.plan.apply_key_column,
                        &apply_keys,
                        &locator_state.existing_deletes_by_file,
                        &locator_state.referenced_data_file_partitions,
                        &partition_filter,
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

fn delete_batch_partition_filter(
    plan_filter: &crate::engine::mv::partition::TargetPartitionFilter,
    partition_derivation: Option<&BoundTargetPartitionDerivation>,
    batch: &RecordBatch,
) -> Result<crate::engine::mv::partition::TargetPartitionFilter, String> {
    if plan_filter.is_allow_list() {
        return Ok(plan_filter.clone());
    }

    let Some(derivation) = partition_derivation else {
        return Ok(crate::engine::mv::partition::TargetPartitionFilter::None);
    };

    let partitions = crate::engine::mv::partition::evaluate_partition_spec_record_batch(
        derivation.target_spec_id,
        &derivation.bound_fields,
        batch,
    )
    .map_err(|err| format!("merge sink partition derivation: {err}"))?;

    Ok(crate::engine::mv::partition::TargetPartitionFilter::AllowList(partitions))
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
    // The IMV rewrite pipeline propagates every internal column from the
    // delta-bound scan to the root projection: `__change_op` (consumed by
    // `partition_chunk_by_change_op` above) and `_row_id` (consumed by
    // `InjectApplyKeyProjectRule` to derive `__nova_base_row_id`). Both are
    // optimizer-internal and must not flow into the iceberg target file.
    // `__nova_base_row_id` is the only IMV-added column the target schema
    // expects, so it is preserved.
    let internal_names = [
        CHANGE_OP_COLUMN,
        crate::exec::row_position::ICEBERG_ROW_ID_COL,
    ];
    let schema = batch.schema();
    let drop_indices: Vec<usize> = schema
        .fields()
        .iter()
        .enumerate()
        .filter_map(|(idx, f)| {
            if internal_names.iter().any(|n| f.name() == *n) {
                Some(idx)
            } else {
                None
            }
        })
        .collect();
    if drop_indices.is_empty() {
        return Ok(batch);
    }
    let mut fields: Vec<arrow::datatypes::Field> =
        schema.fields().iter().map(|f| f.as_ref().clone()).collect();
    let mut columns: Vec<arrow::array::ArrayRef> = batch.columns().to_vec();
    // Remove from highest index to lowest to keep remaining indices valid.
    for idx in drop_indices.iter().rev() {
        fields.remove(*idx);
        columns.remove(*idx);
    }
    let new_schema = Arc::new(arrow::datatypes::Schema::new(fields));
    RecordBatch::try_new(new_schema, columns)
        .map_err(|e| format!("merge sink strip internal columns: {e}"))
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

fn extract_branch_i64_apply_key_values_from_record_batch(
    batch: &RecordBatch,
) -> Result<Vec<crate::engine::mv::iceberg_target_apply::BranchApplyKey>, String> {
    let branch_column = crate::engine::mv::iceberg_target_apply::ICEBERG_MV_BRANCH_ID_COLUMN;
    let key_column = crate::engine::mv::iceberg_target_apply::ICEBERG_MV_APPLY_KEY_COLUMN;
    let schema = batch.schema();
    let branch_idx = schema.index_of(branch_column).map_err(|_| {
        format!("merge sink: DELETE batch missing branch-id column {branch_column}")
    })?;
    let key_idx = schema
        .index_of(key_column)
        .map_err(|_| format!("merge sink: DELETE batch missing apply-key column {key_column}"))?;
    let branches = batch
        .column(branch_idx)
        .as_any()
        .downcast_ref::<arrow::array::Int32Array>()
        .ok_or_else(|| format!("merge sink: branch-id column {branch_column} must be Int32"))?;
    let keys = batch
        .column(key_idx)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .ok_or_else(|| format!("merge sink: apply-key column {key_column} must be Int64"))?;

    branches
        .iter()
        .zip(keys.iter())
        .map(|(branch_id, base_row_id)| {
            let branch_id = branch_id.ok_or_else(|| {
                format!("merge sink: null value in branch-id column {branch_column}")
            })?;
            let base_row_id = base_row_id.ok_or_else(|| {
                format!("merge sink: null value in apply-key column {key_column}")
            })?;
            Ok(crate::engine::mv::iceberg_target_apply::BranchApplyKey {
                branch_id,
                base_row_id,
            })
        })
        .collect()
}

fn extract_branch_utf8_apply_key_values_from_record_batch(
    batch: &RecordBatch,
    apply_key_column: &str,
) -> Result<Vec<crate::engine::mv::iceberg_target_apply::BranchStringApplyKey>, String> {
    let branch_column = crate::engine::mv::iceberg_target_apply::ICEBERG_MV_BRANCH_ID_COLUMN;
    let schema = batch.schema();
    let branch_idx = schema.index_of(branch_column).map_err(|_| {
        format!("merge sink: DELETE batch missing branch-id column {branch_column}")
    })?;
    let key_idx = schema.index_of(apply_key_column).map_err(|_| {
        format!("merge sink: DELETE batch missing apply-key column {apply_key_column}")
    })?;
    let branches = batch
        .column(branch_idx)
        .as_any()
        .downcast_ref::<arrow::array::Int32Array>()
        .ok_or_else(|| format!("merge sink: branch-id column {branch_column} must be Int32"))?;
    let keys = batch
        .column(key_idx)
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .ok_or_else(|| format!("merge sink: apply-key column {apply_key_column} must be Utf8"))?;

    branches
        .iter()
        .zip(keys.iter())
        .map(|(branch_id, key)| {
            let branch_id = branch_id.ok_or_else(|| {
                format!("merge sink: null value in branch-id column {branch_column}")
            })?;
            let key = key.ok_or_else(|| {
                format!("merge sink: null value in apply-key column {apply_key_column}")
            })?;
            Ok(
                crate::engine::mv::iceberg_target_apply::BranchStringApplyKey {
                    branch_id,
                    key: key.to_string(),
                },
            )
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, Int8Array, Int32Array, Int64Array, StringArray};
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

    fn partition_key(value: &str) -> crate::engine::mv::partition::MvPartitionKey {
        crate::engine::mv::partition::MvPartitionKey::new(
            7,
            vec![crate::engine::mv::partition::MvPartitionKeyField::new(
                "region".to_string(),
                crate::engine::mv::partition::MvPartitionValue::String(value.to_string()),
            )],
        )
    }

    fn partition_batch<const N: usize>(values: [&str; N]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "region",
            DataType::Utf8,
            false,
        )]));
        RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(values.to_vec())) as ArrayRef],
        )
        .unwrap()
    }

    fn bound_partition_derivation() -> BoundTargetPartitionDerivation {
        BoundTargetPartitionDerivation {
            target_spec_id: 7,
            bound_fields: vec![crate::engine::mv::partition::BoundPartitionField {
                partition_field_name: "region".to_string(),
                column_name: "region".to_string(),
                transform: iceberg::spec::Transform::Identity,
            }],
        }
    }

    #[test]
    fn delete_batch_partition_filter_prefers_plan_time_allow_list() {
        let plan_filter = crate::engine::mv::partition::TargetPartitionFilter::AllowList(
            [partition_key("planned")].into_iter().collect(),
        );
        let batch = partition_batch(["batch"]);

        let filter = delete_batch_partition_filter(
            &plan_filter,
            Some(&bound_partition_derivation()),
            &batch,
        )
        .expect("filter");

        assert_eq!(filter, plan_filter);
    }

    #[test]
    fn delete_batch_partition_filter_derives_batch_allow_list_when_plan_filter_is_none() {
        let batch = partition_batch(["west", "east", "west"]);

        let filter = delete_batch_partition_filter(
            &crate::engine::mv::partition::TargetPartitionFilter::None,
            Some(&bound_partition_derivation()),
            &batch,
        )
        .expect("filter");

        assert_eq!(
            filter,
            crate::engine::mv::partition::TargetPartitionFilter::AllowList(
                [partition_key("east"), partition_key("west")]
                    .into_iter()
                    .collect(),
            )
        );
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

    #[test]
    fn extract_branch_i64_apply_key_values_accepts_pairs() {
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                crate::engine::mv::iceberg_target_apply::ICEBERG_MV_BRANCH_ID_COLUMN,
                DataType::Int32,
                false,
            ),
            Field::new(
                crate::engine::mv::iceberg_target_apply::ICEBERG_MV_APPLY_KEY_COLUMN,
                DataType::Int64,
                false,
            ),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![0, 1])) as ArrayRef,
                Arc::new(Int64Array::from(vec![42, 42])) as ArrayRef,
            ],
        )
        .unwrap();

        let values = extract_branch_i64_apply_key_values_from_record_batch(&batch)
            .expect("branch apply keys");

        assert_eq!(
            values,
            vec![
                crate::engine::mv::iceberg_target_apply::BranchApplyKey {
                    branch_id: 0,
                    base_row_id: 42
                },
                crate::engine::mv::iceberg_target_apply::BranchApplyKey {
                    branch_id: 1,
                    base_row_id: 42
                },
            ]
        );
    }

    #[test]
    fn extract_branch_i64_apply_key_values_rejects_missing_branch_column() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            crate::engine::mv::iceberg_target_apply::ICEBERG_MV_APPLY_KEY_COLUMN,
            DataType::Int64,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(vec![42])) as ArrayRef],
        )
        .unwrap();

        let err = extract_branch_i64_apply_key_values_from_record_batch(&batch).unwrap_err();

        assert!(
            err.contains(crate::engine::mv::iceberg_target_apply::ICEBERG_MV_BRANCH_ID_COLUMN),
            "err={err}"
        );
        assert!(err.contains("missing"), "err={err}");
    }

    #[test]
    fn extract_branch_i64_apply_key_values_rejects_null_branch_or_key() {
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                crate::engine::mv::iceberg_target_apply::ICEBERG_MV_BRANCH_ID_COLUMN,
                DataType::Int32,
                true,
            ),
            Field::new(
                crate::engine::mv::iceberg_target_apply::ICEBERG_MV_APPLY_KEY_COLUMN,
                DataType::Int64,
                true,
            ),
        ]));
        let null_branch_batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![Some(0), None])) as ArrayRef,
                Arc::new(Int64Array::from(vec![Some(42), Some(43)])) as ArrayRef,
            ],
        )
        .unwrap();
        let null_key_batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![Some(0), Some(1)])) as ArrayRef,
                Arc::new(Int64Array::from(vec![Some(42), None])) as ArrayRef,
            ],
        )
        .unwrap();

        let branch_err =
            extract_branch_i64_apply_key_values_from_record_batch(&null_branch_batch).unwrap_err();
        assert!(branch_err.contains("null"), "err={branch_err}");
        assert!(
            branch_err
                .contains(crate::engine::mv::iceberg_target_apply::ICEBERG_MV_BRANCH_ID_COLUMN),
            "err={branch_err}"
        );

        let key_err =
            extract_branch_i64_apply_key_values_from_record_batch(&null_key_batch).unwrap_err();
        assert!(key_err.contains("null"), "err={key_err}");
        assert!(
            key_err.contains(crate::engine::mv::iceberg_target_apply::ICEBERG_MV_APPLY_KEY_COLUMN),
            "err={key_err}"
        );
    }

    #[test]
    fn extract_branch_utf8_apply_key_values_accepts_pairs() {
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                crate::engine::mv::iceberg_target_apply::ICEBERG_MV_BRANCH_ID_COLUMN,
                DataType::Int32,
                false,
            ),
            Field::new("__row_id__", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![0, 1])) as ArrayRef,
                Arc::new(StringArray::from(vec!["group-1", "group-1"])) as ArrayRef,
            ],
        )
        .unwrap();

        let values = extract_branch_utf8_apply_key_values_from_record_batch(&batch, "__row_id__")
            .expect("branch string apply keys");

        assert_eq!(
            values,
            vec![
                crate::engine::mv::iceberg_target_apply::BranchStringApplyKey {
                    branch_id: 0,
                    key: "group-1".to_string(),
                },
                crate::engine::mv::iceberg_target_apply::BranchStringApplyKey {
                    branch_id: 1,
                    key: "group-1".to_string(),
                },
            ]
        );
    }

    #[test]
    fn extract_branch_utf8_apply_key_values_rejects_missing_branch_column() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "__row_id__",
            DataType::Utf8,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(vec!["group-1"])) as ArrayRef],
        )
        .unwrap();

        let err = extract_branch_utf8_apply_key_values_from_record_batch(&batch, "__row_id__")
            .unwrap_err();

        assert!(
            err.contains(crate::engine::mv::iceberg_target_apply::ICEBERG_MV_BRANCH_ID_COLUMN),
            "err={err}"
        );
        assert!(err.contains("missing"), "err={err}");
    }

    #[test]
    fn extract_branch_utf8_apply_key_values_rejects_null_branch_or_key() {
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                crate::engine::mv::iceberg_target_apply::ICEBERG_MV_BRANCH_ID_COLUMN,
                DataType::Int32,
                true,
            ),
            Field::new("__row_id__", DataType::Utf8, true),
        ]));
        let null_branch_batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![Some(0), None])) as ArrayRef,
                Arc::new(StringArray::from(vec![Some("group-1"), Some("group-2")])) as ArrayRef,
            ],
        )
        .unwrap();
        let null_key_batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![Some(0), Some(1)])) as ArrayRef,
                Arc::new(StringArray::from(vec![Some("group-1"), None])) as ArrayRef,
            ],
        )
        .unwrap();

        let branch_err = extract_branch_utf8_apply_key_values_from_record_batch(
            &null_branch_batch,
            "__row_id__",
        )
        .unwrap_err();
        assert!(branch_err.contains("null"), "err={branch_err}");
        assert!(
            branch_err
                .contains(crate::engine::mv::iceberg_target_apply::ICEBERG_MV_BRANCH_ID_COLUMN),
            "err={branch_err}"
        );

        let key_err =
            extract_branch_utf8_apply_key_values_from_record_batch(&null_key_batch, "__row_id__")
                .unwrap_err();
        assert!(key_err.contains("null"), "err={key_err}");
        assert!(key_err.contains("__row_id__"), "err={key_err}");
    }

    #[test]
    fn strip_change_op_preserves_branch_and_apply_key_columns() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("v", DataType::Int32, false),
            crate::exec::change_op::change_op_field(),
            Field::new(
                crate::exec::row_position::ICEBERG_ROW_ID_COL,
                DataType::Int64,
                false,
            ),
            Field::new(
                crate::engine::mv::iceberg_target_apply::ICEBERG_MV_BRANCH_ID_COLUMN,
                DataType::Int32,
                false,
            ),
            Field::new(
                crate::engine::mv::iceberg_target_apply::ICEBERG_MV_APPLY_KEY_COLUMN,
                DataType::Int64,
                false,
            ),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![10])) as ArrayRef,
                Arc::new(Int8Array::from(vec![CHANGE_OP_INSERT])) as ArrayRef,
                Arc::new(Int64Array::from(vec![9001])) as ArrayRef,
                Arc::new(Int32Array::from(vec![1])) as ArrayRef,
                Arc::new(Int64Array::from(vec![42])) as ArrayRef,
            ],
        )
        .unwrap();

        let stripped = strip_change_op(batch).expect("strip internal columns");
        let stripped_schema = stripped.schema();
        let names = stripped_schema
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>();

        assert_eq!(
            names,
            vec![
                "v",
                crate::engine::mv::iceberg_target_apply::ICEBERG_MV_BRANCH_ID_COLUMN,
                crate::engine::mv::iceberg_target_apply::ICEBERG_MV_APPLY_KEY_COLUMN,
            ]
        );
    }
}
