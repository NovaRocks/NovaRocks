use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use arrow::array::{Array, ArrayRef, Int8Array, Int64Array, StringArray, UInt32Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

#[derive(Clone, Debug)]
struct CoalescedPayload {
    net: i32,
    payload: RecordBatch,
}

#[derive(Clone, Debug, Default)]
struct CoalescedRow {
    payloads: Vec<CoalescedPayload>,
}

pub(crate) struct JoinCoalesceFlushOutcome {
    pub(crate) added_rows: i64,
    pub(crate) deleted_rows: i64,
}

pub(crate) struct JoinDeltaCoalescer {
    left_table_uuid: String,
    right_table_uuid: String,
    max_keys: usize,
    rows: Mutex<BTreeMap<String, CoalescedRow>>,
}

impl JoinDeltaCoalescer {
    pub(crate) fn new(
        left_table_uuid: String,
        right_table_uuid: String,
        max_keys: usize,
    ) -> Arc<Self> {
        Arc::new(Self {
            left_table_uuid,
            right_table_uuid,
            max_keys,
            rows: Mutex::new(BTreeMap::new()),
        })
    }

    pub(crate) fn push_batch(&self, batch: RecordBatch) -> Result<(), String> {
        let hidden_indices = hidden_column_indices(batch.schema().as_ref())?;
        let [op_idx, left_idx, right_idx] = hidden_indices;
        let ops = batch
            .column(op_idx)
            .as_any()
            .downcast_ref::<Int8Array>()
            .ok_or_else(|| "join coalesce __change_op must be Int8".to_string())?;
        let left_ids = batch
            .column(left_idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| "join coalesce left row id must be Int64".to_string())?;
        let right_ids = batch
            .column(right_idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| "join coalesce right row id must be Int64".to_string())?;

        let mut rows = self.rows.lock().expect("join coalescer lock");
        for row in 0..batch.num_rows() {
            if ops.is_null(row) {
                return Err("join coalesce null __change_op".to_string());
            }
            if left_ids.is_null(row) {
                return Err("join coalesce null left row id".to_string());
            }
            if right_ids.is_null(row) {
                return Err("join coalesce null right row id".to_string());
            }
            let delta = match ops.value(row) {
                crate::exec::change_op::CHANGE_OP_INSERT => 1,
                crate::exec::change_op::CHANGE_OP_DELETE => -1,
                other => return Err(format!("join coalesce unexpected __change_op {other}")),
            };
            let key = stable_join_row_key(
                &self.left_table_uuid,
                left_ids.value(row),
                &self.right_table_uuid,
                right_ids.value(row),
            );
            let payload = take_one_row_without_hidden_columns(&batch, row, &hidden_indices)?;
            let entry = rows.entry(key.clone()).or_default();
            entry.push_delta(delta, payload)?;
            if entry.is_empty() {
                rows.remove(&key);
            }
            if rows.len() > self.max_keys {
                return Err(format!(
                    "join coalesce exceeded max key budget {}; use full refresh or split the delta",
                    self.max_keys
                ));
            }
        }
        Ok(())
    }

    pub(crate) fn flush_to_iceberg_commit_collector(
        &self,
        target_table: &iceberg::table::Table,
        collector: Arc<crate::connector::iceberg::commit::IcebergCommitCollector>,
        locator_inputs: Option<(
            crate::engine::delete_flow::ExistingDeleteVisibilityByDataFile,
            crate::engine::delete_flow::ReferencedDataFilePartitions,
        )>,
    ) -> Result<JoinCoalesceFlushOutcome, String> {
        let (insert_batches, delete_keys) = {
            let rows = self.rows.lock().expect("join coalescer lock");
            let mut insert_batches = Vec::new();
            let mut delete_keys = Vec::new();
            for (key, row) in rows.iter() {
                for payload in row.pending_payloads(key)? {
                    match payload.net {
                        1 => insert_batches.push(append_join_apply_key(&payload.payload, key)?),
                        -1 => delete_keys.push(key.clone()),
                        0 => {}
                        other => {
                            return Err(format!("join coalesce unsupported net change_op {other}"));
                        }
                    }
                }
            }
            (insert_batches, delete_keys)
        };

        let added_rows = count_insert_rows(&insert_batches)?;
        let deleted_rows = i64::try_from(delete_keys.len())
            .map_err(|_| "join coalesce delete key count exceeds i64".to_string())?;
        let delete_groups = if !delete_keys.is_empty() {
            let (existing_deletes_by_file, referenced_data_file_partitions) = locator_inputs
                .ok_or_else(|| {
                    "join coalesce needs target locator inputs for DELETE rows".to_string()
                })?;
            crate::runtime::global_async_runtime::data_block_on(
                crate::engine::mv::iceberg_target_apply::locate_target_rows_by_apply_key_string(
                    target_table,
                    &delete_keys,
                    &existing_deletes_by_file,
                    &referenced_data_file_partitions,
                    &crate::engine::mv::partition::TargetPartitionFilter::None,
                ),
            )??
        } else {
            Vec::new()
        };

        if !insert_batches.is_empty() {
            let data_files = crate::runtime::global_async_runtime::data_block_on(
                crate::connector::iceberg::data_writer::write_record_batches_as_data_files(
                    target_table,
                    insert_batches,
                ),
            )??;
            let partition_spec_id = target_table.metadata().default_partition_spec_id();
            for data_file in data_files {
                let written = crate::engine::iceberg_writer::data_file_to_written_file(
                    &data_file,
                    partition_spec_id,
                )?;
                collector.inject_written_file(written);
            }
        }

        for group in delete_groups {
            collector.inject_delete_group(group);
        }
        Ok(JoinCoalesceFlushOutcome {
            added_rows,
            deleted_rows,
        })
    }

    pub(crate) fn pending_change_counts(&self) -> Result<JoinCoalesceFlushOutcome, String> {
        let rows = self.rows.lock().expect("join coalescer lock");
        let mut added_rows = 0_i64;
        let mut deleted_rows = 0_i64;
        for (key, row) in rows.iter() {
            for payload in row.pending_payloads(key)? {
                match payload.net {
                    1 => {
                        let rows = i64::try_from(payload.payload.num_rows()).map_err(|_| {
                            "join coalesce insert row count exceeds i64".to_string()
                        })?;
                        added_rows = added_rows
                            .checked_add(rows)
                            .ok_or_else(|| "join coalesce insert row count overflow".to_string())?;
                    }
                    -1 => {
                        deleted_rows = deleted_rows
                            .checked_add(1)
                            .ok_or_else(|| "join coalesce delete row count overflow".to_string())?;
                    }
                    0 => {}
                    other => {
                        return Err(format!("join coalesce unsupported net change_op {other}"));
                    }
                }
            }
        }
        Ok(JoinCoalesceFlushOutcome {
            added_rows,
            deleted_rows,
        })
    }

    #[cfg(test)]
    fn finish_for_test(&self) -> Result<Vec<(String, i32)>, String> {
        let rows = self.rows.lock().expect("join coalescer lock");
        let mut out = Vec::new();
        for (key, row) in rows.iter() {
            let net = row.total_net(key)?;
            if net != 0 {
                out.push((key.clone(), net));
            }
        }
        Ok(out)
    }
}

impl CoalescedRow {
    fn push_delta(&mut self, delta: i32, payload: RecordBatch) -> Result<(), String> {
        if let Some(pos) = self.payloads.iter().position(|existing| {
            existing.net == -delta && payloads_equal(&existing.payload, &payload)
        }) {
            let existing = &mut self.payloads[pos];
            existing.net += delta;
            if existing.net == 0 {
                self.payloads.remove(pos);
            }
            return Ok(());
        }
        if let Some(existing) = self
            .payloads
            .iter_mut()
            .find(|existing| payloads_equal(&existing.payload, &payload))
        {
            existing.net += delta;
            return Ok(());
        }
        self.payloads.push(CoalescedPayload {
            net: delta,
            payload,
        });
        Ok(())
    }

    fn pending_payloads(&self, key: &str) -> Result<Vec<&CoalescedPayload>, String> {
        self.validate_pending_shape(key)?;
        Ok(self
            .payloads
            .iter()
            .filter(|payload| payload.net != 0)
            .collect())
    }

    #[cfg(test)]
    fn total_net(&self, key: &str) -> Result<i32, String> {
        self.validate_pending_shape(key)?;
        Ok(self.payloads.iter().map(|payload| payload.net).sum())
    }

    fn is_empty(&self) -> bool {
        self.payloads.is_empty()
    }

    fn validate_pending_shape(&self, key: &str) -> Result<(), String> {
        let mut inserts = 0;
        let mut deletes = 0;
        for payload in &self.payloads {
            match payload.net {
                1 => inserts += 1,
                -1 => deletes += 1,
                0 => {}
                other => {
                    return Err(format!("join coalesce unsupported net change_op {other}"));
                }
            }
        }
        if inserts > 1 || deletes > 1 {
            return Err(format!(
                "join coalesce multiple pending payloads for key {key}: inserts={inserts}, deletes={deletes}"
            ));
        }
        Ok(())
    }
}

fn payloads_equal(left: &RecordBatch, right: &RecordBatch) -> bool {
    left.num_rows() == 1
        && right.num_rows() == 1
        && left.schema() == right.schema()
        && left
            .columns()
            .iter()
            .zip(right.columns())
            .all(|(left, right)| left.to_data() == right.to_data())
}

fn append_join_apply_key(batch: &RecordBatch, key: &str) -> Result<RecordBatch, String> {
    let apply_key_column =
        crate::engine::mv::iceberg_target_apply::ICEBERG_MV_JOIN_APPLY_KEY_COLUMN;
    if batch
        .schema()
        .fields()
        .iter()
        .any(|field| field.name().eq_ignore_ascii_case(apply_key_column))
    {
        return Err(format!(
            "join coalesce payload already contains reserved column {apply_key_column}"
        ));
    }

    let mut fields: Vec<Field> = batch
        .schema()
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect();
    fields.push(Field::new(apply_key_column, DataType::Utf8, false));
    let mut columns = batch.columns().to_vec();
    columns.push(Arc::new(StringArray::from(vec![
        key.to_string();
        batch.num_rows()
    ])));
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
        .map_err(|e| format!("join coalesce append apply key: {e}"))
}

fn count_insert_rows(batches: &[RecordBatch]) -> Result<i64, String> {
    batches.iter().try_fold(0_i64, |acc, batch| {
        let rows = i64::try_from(batch.num_rows())
            .map_err(|_| "join coalesce insert row count exceeds i64".to_string())?;
        acc.checked_add(rows)
            .ok_or_else(|| "join coalesce insert row count overflow".to_string())
    })
}

pub(crate) fn stable_join_row_key(
    left_uuid: &str,
    left_row_id: i64,
    right_uuid: &str,
    right_row_id: i64,
) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(left_uuid.as_bytes());
    hasher.update([0]);
    hasher.update(left_row_id.to_be_bytes());
    hasher.update([0]);
    hasher.update(right_uuid.as_bytes());
    hasher.update([0]);
    hasher.update(right_row_id.to_be_bytes());
    let digest = hasher.finalize();
    format!("v1:{}", hex::encode(&digest[..16]))
}

fn hidden_column_indices(schema: &Schema) -> Result<[usize; 3], String> {
    Ok([
        find_unique_hidden_column(schema, crate::exec::change_op::CHANGE_OP_COLUMN)?,
        find_unique_hidden_column(
            schema,
            crate::engine::mv::iceberg_join_branch::JOIN_LEFT_ROW_ID_COLUMN,
        )?,
        find_unique_hidden_column(
            schema,
            crate::engine::mv::iceberg_join_branch::JOIN_RIGHT_ROW_ID_COLUMN,
        )?,
    ])
}

fn find_unique_hidden_column(schema: &Schema, name: &str) -> Result<usize, String> {
    let mut exact_idx = None;
    for (idx, field) in schema.fields().iter().enumerate() {
        if field.name() == name {
            if exact_idx.replace(idx).is_some() {
                return Err(format!(
                    "join coalesce hidden column {name} appears more than once"
                ));
            }
        } else if field.name().eq_ignore_ascii_case(name) {
            return Err(format!(
                "join coalesce hidden column {name} collides with field {}",
                field.name()
            ));
        }
    }
    exact_idx.ok_or_else(|| format!("join coalesce batch missing {name}"))
}

fn take_one_row_without_hidden_columns(
    batch: &RecordBatch,
    row: usize,
    hidden_indices: &[usize; 3],
) -> Result<RecordBatch, String> {
    let row_u32 = u32::try_from(row).map_err(|_| format!("row index {row} exceeds u32"))?;
    let indices = UInt32Array::from(vec![row_u32]);
    let schema = batch.schema();
    let mut fields = Vec::new();
    let mut columns: Vec<ArrayRef> = Vec::new();
    for (idx, field) in schema.fields().iter().enumerate() {
        if hidden_indices.contains(&idx) {
            continue;
        }
        fields.push(field.as_ref().clone());
        let taken = arrow::compute::take(batch.column(idx).as_ref(), &indices, None)
            .map_err(|e| format!("join coalesce take one row: {e}"))?;
        columns.push(taken);
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
        .map_err(|e| format!("join coalesce rebuild one-row batch: {e}"))
}

pub(crate) struct IcebergJoinCoalesceSinkFactory {
    name: String,
    coalescer: Arc<JoinDeltaCoalescer>,
}

impl IcebergJoinCoalesceSinkFactory {
    pub(crate) fn new(coalescer: Arc<JoinDeltaCoalescer>) -> Self {
        Self {
            name: "IcebergJoinCoalesceSink".to_string(),
            coalescer,
        }
    }
}

impl crate::exec::pipeline::operator_factory::OperatorFactory for IcebergJoinCoalesceSinkFactory {
    fn name(&self) -> &str {
        &self.name
    }

    fn create(
        &self,
        _dop: i32,
        _driver_id: i32,
    ) -> Box<dyn crate::exec::pipeline::operator::Operator> {
        Box::new(IcebergJoinCoalesceSinkOperator {
            name: self.name.clone(),
            coalescer: Arc::clone(&self.coalescer),
            finished: false,
        })
    }

    fn is_sink(&self) -> bool {
        true
    }
}

struct IcebergJoinCoalesceSinkOperator {
    name: String,
    coalescer: Arc<JoinDeltaCoalescer>,
    finished: bool,
}

impl crate::exec::pipeline::operator::Operator for IcebergJoinCoalesceSinkOperator {
    fn name(&self) -> &str {
        &self.name
    }

    fn as_processor_mut(
        &mut self,
    ) -> Option<&mut dyn crate::exec::pipeline::operator::ProcessorOperator> {
        Some(self)
    }

    fn as_processor_ref(&self) -> Option<&dyn crate::exec::pipeline::operator::ProcessorOperator> {
        Some(self)
    }

    fn is_finished(&self) -> bool {
        self.finished
    }
}

impl crate::exec::pipeline::operator::ProcessorOperator for IcebergJoinCoalesceSinkOperator {
    fn need_input(&self) -> bool {
        !self.finished
    }

    fn has_output(&self) -> bool {
        false
    }

    fn push_chunk(
        &mut self,
        _state: &crate::runtime::runtime_state::RuntimeState,
        chunk: crate::exec::chunk::Chunk,
    ) -> Result<(), String> {
        self.coalescer.push_batch(chunk.batch)
    }

    fn pull_chunk(
        &mut self,
        _state: &crate::runtime::runtime_state::RuntimeState,
    ) -> Result<Option<crate::exec::chunk::Chunk>, String> {
        Err("join coalesce sink does not produce output".to_string())
    }

    fn set_finishing(
        &mut self,
        _state: &crate::runtime::runtime_state::RuntimeState,
    ) -> Result<(), String> {
        self.finished = true;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int8Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    fn batch(op: i8, left: i64, right: i64, value: &str) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("value", DataType::Utf8, false),
                Field::new(
                    crate::exec::change_op::CHANGE_OP_COLUMN,
                    DataType::Int8,
                    false,
                ),
                Field::new(
                    crate::engine::mv::iceberg_join_branch::JOIN_LEFT_ROW_ID_COLUMN,
                    DataType::Int64,
                    false,
                ),
                Field::new(
                    crate::engine::mv::iceberg_join_branch::JOIN_RIGHT_ROW_ID_COLUMN,
                    DataType::Int64,
                    false,
                ),
            ])),
            vec![
                Arc::new(StringArray::from(vec![value])),
                Arc::new(Int8Array::from(vec![op])),
                Arc::new(Int64Array::from(vec![left])),
                Arc::new(Int64Array::from(vec![right])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn coalescer_cancels_insert_and_delete() {
        let coalescer =
            JoinDeltaCoalescer::new("left-uuid".to_string(), "right-uuid".to_string(), 10_000);
        coalescer
            .push_batch(batch(crate::exec::change_op::CHANGE_OP_INSERT, 1, 2, "a"))
            .unwrap();
        coalescer
            .push_batch(batch(crate::exec::change_op::CHANGE_OP_DELETE, 1, 2, "a"))
            .unwrap();
        let rows = coalescer.finish_for_test().unwrap();
        assert!(rows.is_empty());
    }

    #[test]
    fn coalescer_preserves_same_key_payload_replacement() {
        let coalescer =
            JoinDeltaCoalescer::new("left-uuid".to_string(), "right-uuid".to_string(), 10_000);
        coalescer
            .push_batch(batch(crate::exec::change_op::CHANGE_OP_DELETE, 1, 2, "old"))
            .unwrap();
        coalescer
            .push_batch(batch(crate::exec::change_op::CHANGE_OP_INSERT, 1, 2, "new"))
            .unwrap();

        let pending = coalescer.pending_change_counts().unwrap();
        assert_eq!(pending.added_rows, 1);
        assert_eq!(pending.deleted_rows, 1);
    }

    #[test]
    fn coalescer_cancels_intermediate_payload_for_same_key() {
        let coalescer =
            JoinDeltaCoalescer::new("left-uuid".to_string(), "right-uuid".to_string(), 10_000);
        coalescer
            .push_batch(batch(crate::exec::change_op::CHANGE_OP_DELETE, 1, 2, "old"))
            .unwrap();
        coalescer
            .push_batch(batch(
                crate::exec::change_op::CHANGE_OP_INSERT,
                1,
                2,
                "middle",
            ))
            .unwrap();
        coalescer
            .push_batch(batch(
                crate::exec::change_op::CHANGE_OP_DELETE,
                1,
                2,
                "middle",
            ))
            .unwrap();
        coalescer
            .push_batch(batch(crate::exec::change_op::CHANGE_OP_INSERT, 1, 2, "new"))
            .unwrap();

        let pending = coalescer.pending_change_counts().unwrap();
        assert_eq!(pending.added_rows, 1);
        assert_eq!(pending.deleted_rows, 1);
    }

    #[test]
    fn coalescer_allows_transient_multiple_inserts_before_matching_delete() {
        let coalescer =
            JoinDeltaCoalescer::new("left-uuid".to_string(), "right-uuid".to_string(), 10_000);
        coalescer
            .push_batch(batch(crate::exec::change_op::CHANGE_OP_INSERT, 1, 2, "old"))
            .unwrap();
        coalescer
            .push_batch(batch(crate::exec::change_op::CHANGE_OP_INSERT, 1, 2, "new"))
            .unwrap();
        coalescer
            .push_batch(batch(crate::exec::change_op::CHANGE_OP_DELETE, 1, 2, "old"))
            .unwrap();

        let pending = coalescer.pending_change_counts().unwrap();
        assert_eq!(pending.added_rows, 1);
        assert_eq!(pending.deleted_rows, 0);
    }

    #[test]
    fn coalescer_rejects_unbalanced_duplicate_same_sign_payload_at_finish() {
        let coalescer =
            JoinDeltaCoalescer::new("left-uuid".to_string(), "right-uuid".to_string(), 10_000);
        coalescer
            .push_batch(batch(crate::exec::change_op::CHANGE_OP_INSERT, 1, 2, "a"))
            .unwrap();
        coalescer
            .push_batch(batch(crate::exec::change_op::CHANGE_OP_INSERT, 1, 2, "a"))
            .unwrap();

        let err = match coalescer.pending_change_counts() {
            Ok(_) => panic!("expected unbalanced duplicate same-sign payload"),
            Err(err) => err,
        };
        assert!(err.contains("unsupported net change_op 2"), "err={err}");
    }

    #[test]
    fn coalescer_balances_duplicate_same_payload_telescope_events() {
        let coalescer =
            JoinDeltaCoalescer::new("left-uuid".to_string(), "right-uuid".to_string(), 10_000);
        coalescer
            .push_batch(batch(crate::exec::change_op::CHANGE_OP_INSERT, 1, 2, "a"))
            .unwrap();
        coalescer
            .push_batch(batch(crate::exec::change_op::CHANGE_OP_INSERT, 1, 2, "a"))
            .unwrap();
        coalescer
            .push_batch(batch(crate::exec::change_op::CHANGE_OP_DELETE, 1, 2, "a"))
            .unwrap();
        coalescer
            .push_batch(batch(crate::exec::change_op::CHANGE_OP_DELETE, 1, 2, "a"))
            .unwrap();

        let pending = coalescer.pending_change_counts().unwrap();
        assert_eq!(pending.added_rows, 0);
        assert_eq!(pending.deleted_rows, 0);
    }

    #[test]
    fn coalescer_drops_zero_net_keys_before_budget_check() {
        let coalescer =
            JoinDeltaCoalescer::new("left-uuid".to_string(), "right-uuid".to_string(), 1);
        for key in 0..16 {
            coalescer
                .push_batch(batch(crate::exec::change_op::CHANGE_OP_INSERT, key, 2, "a"))
                .unwrap();
            coalescer
                .push_batch(batch(crate::exec::change_op::CHANGE_OP_DELETE, key, 2, "a"))
                .unwrap();
        }
        let rows = coalescer.finish_for_test().unwrap();
        assert!(rows.is_empty());
    }

    #[test]
    fn coalescer_rejects_case_insensitive_hidden_column_collision() {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("value", DataType::Utf8, false),
                Field::new(
                    crate::exec::change_op::CHANGE_OP_COLUMN,
                    DataType::Int8,
                    false,
                ),
                Field::new("__CHANGE_OP", DataType::Utf8, false),
                Field::new(
                    crate::engine::mv::iceberg_join_branch::JOIN_LEFT_ROW_ID_COLUMN,
                    DataType::Int64,
                    false,
                ),
                Field::new(
                    crate::engine::mv::iceberg_join_branch::JOIN_RIGHT_ROW_ID_COLUMN,
                    DataType::Int64,
                    false,
                ),
            ])),
            vec![
                Arc::new(StringArray::from(vec!["a"])),
                Arc::new(Int8Array::from(vec![
                    crate::exec::change_op::CHANGE_OP_INSERT,
                ])),
                Arc::new(StringArray::from(vec!["visible"])),
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(Int64Array::from(vec![2])),
            ],
        )
        .unwrap();
        let coalescer =
            JoinDeltaCoalescer::new("left-uuid".to_string(), "right-uuid".to_string(), 10_000);
        let err = coalescer
            .push_batch(batch)
            .expect_err("hidden column collision");
        assert!(err.contains("collides with field __CHANGE_OP"), "err={err}");
    }
}
