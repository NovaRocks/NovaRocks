use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use arrow::array::{Array, ArrayRef, Int8Array, Int64Array, UInt32Array};
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;

#[derive(Clone, Debug)]
struct CoalescedRow {
    net: i32,
    payload: Option<RecordBatch>,
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
        let op_idx = batch
            .schema()
            .index_of(crate::exec::change_op::CHANGE_OP_COLUMN)
            .map_err(|_| "join coalesce batch missing __change_op".to_string())?;
        let left_idx = batch
            .schema()
            .index_of(crate::engine::mv::iceberg_join_branch::JOIN_LEFT_ROW_ID_COLUMN)
            .map_err(|_| "join coalesce batch missing left row id".to_string())?;
        let right_idx = batch
            .schema()
            .index_of(crate::engine::mv::iceberg_join_branch::JOIN_RIGHT_ROW_ID_COLUMN)
            .map_err(|_| "join coalesce batch missing right row id".to_string())?;
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
            let payload = take_one_row_without_hidden_columns(&batch, row)?;
            {
                let entry = rows.entry(key).or_insert(CoalescedRow {
                    net: 0,
                    payload: None,
                });
                entry.net += delta;
                if delta > 0 {
                    if let Some(existing) = &entry.payload {
                        if !record_batch_single_row_equal(existing, &payload)? {
                            return Err("join coalesce payload mismatch for the same join row key"
                                .to_string());
                        }
                    }
                    entry.payload = Some(payload);
                }
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

    #[cfg(test)]
    fn finish_for_test(&self) -> Result<Vec<(String, i32)>, String> {
        let rows = self.rows.lock().expect("join coalescer lock");
        let mut out = Vec::new();
        for (key, row) in rows.iter() {
            if row.net.abs() > 1 {
                return Err(format!(
                    "join coalesce net change_op {} for key {key}",
                    row.net
                ));
            }
            if row.net != 0 {
                out.push((key.clone(), row.net));
            }
        }
        Ok(out)
    }
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

fn take_one_row_without_hidden_columns(
    batch: &RecordBatch,
    row: usize,
) -> Result<RecordBatch, String> {
    let hidden = [
        crate::exec::change_op::CHANGE_OP_COLUMN,
        crate::engine::mv::iceberg_join_branch::JOIN_LEFT_ROW_ID_COLUMN,
        crate::engine::mv::iceberg_join_branch::JOIN_RIGHT_ROW_ID_COLUMN,
    ];
    let row_u32 = u32::try_from(row).map_err(|_| format!("row index {row} exceeds u32"))?;
    let indices = UInt32Array::from(vec![row_u32]);
    let schema = batch.schema();
    let mut fields = Vec::new();
    let mut columns: Vec<ArrayRef> = Vec::new();
    for (idx, field) in schema.fields().iter().enumerate() {
        if hidden
            .iter()
            .any(|name| field.name().eq_ignore_ascii_case(name))
        {
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

fn record_batch_single_row_equal(left: &RecordBatch, right: &RecordBatch) -> Result<bool, String> {
    if left.num_rows() != 1 || right.num_rows() != 1 {
        return Err("join coalesce payload comparison requires single-row batches".to_string());
    }
    if left.schema() != right.schema() {
        return Ok(false);
    }
    for idx in 0..left.num_columns() {
        if left.column(idx).to_data() != right.column(idx).to_data() {
            return Ok(false);
        }
    }
    Ok(true)
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
    fn coalescer_rejects_abs_net_greater_than_one() {
        let coalescer =
            JoinDeltaCoalescer::new("left-uuid".to_string(), "right-uuid".to_string(), 10_000);
        coalescer
            .push_batch(batch(crate::exec::change_op::CHANGE_OP_INSERT, 1, 2, "a"))
            .unwrap();
        coalescer
            .push_batch(batch(crate::exec::change_op::CHANGE_OP_INSERT, 1, 2, "a"))
            .unwrap();
        let err = coalescer.finish_for_test().expect_err("net > 1");
        assert!(err.contains("net change_op"), "err={err}");
    }
}
