use crate::connector::iceberg::changes::{
    ChangeError, IcebergChangeBatch, MaterializedChanges, plan_changes,
};
use crate::engine::QueryResult;

// Compatibility wrapper for the older two-branch materialized change stream.
#[allow(dead_code)]
pub(crate) struct IvmChangeStream {
    pub(crate) previous_snapshot_id: i64,
    pub(crate) current_snapshot_id: i64,
    pub(crate) inserts: QueryResult,
    pub(crate) deletes: QueryResult,
}

#[allow(dead_code)]
impl IvmChangeStream {
    pub(crate) fn from_materialized(changes: MaterializedChanges) -> Self {
        Self {
            previous_snapshot_id: changes.previous_snapshot_id,
            current_snapshot_id: changes.current_snapshot_id,
            inserts: changes.inserts,
            deletes: changes.deletes,
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.inserts.row_count() == 0 && self.deletes.row_count() == 0
    }

    pub(crate) fn into_results(self) -> (QueryResult, QueryResult) {
        (self.inserts, self.deletes)
    }
}

pub(crate) fn validate_change_batch_current_snapshot(
    batch: &IcebergChangeBatch,
    expected_current_snapshot_id: i64,
) -> Result<(), String> {
    if batch.current_snapshot_id != expected_current_snapshot_id {
        return Err(format!(
            "iceberg change batch current snapshot mismatch: expected {expected_current_snapshot_id}, got {}",
            batch.current_snapshot_id
        ));
    }
    Ok(())
}

pub(crate) fn plan_iceberg_change_batch_for_ivm(
    base_table: &iceberg::table::Table,
    previous_snapshot_id: i64,
    expected_current_snapshot_id: i64,
    pk_columns: &[String],
) -> Result<IcebergChangeBatch, ChangeError> {
    let batch = plan_changes(base_table, previous_snapshot_id, pk_columns)?;
    validate_change_batch_current_snapshot(&batch, expected_current_snapshot_id)
        .map_err(ChangeError::InternalInconsistency)?;
    Ok(batch)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    use crate::connector::iceberg::changes::{IcebergChangeBatch, MaterializedChanges};
    use crate::engine::{QueryResult, QueryResultColumn, record_batch_to_chunk};

    use super::{IvmChangeStream, validate_change_batch_current_snapshot};

    fn one_row_result(value: i32) -> QueryResult {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![value]))])
            .expect("record batch");
        QueryResult {
            columns: vec![QueryResultColumn {
                name: "id".to_string(),
                data_type: DataType::Int32,
                nullable: false,
                logical_type: None,
            }],
            chunks: vec![record_batch_to_chunk(batch).expect("chunk")],
        }
    }

    #[test]
    fn validate_change_batch_current_snapshot_rejects_mismatch() {
        let batch = IcebergChangeBatch {
            previous_snapshot_id: 10,
            current_snapshot_id: 12,
            inserts: Vec::new(),
            deletes: Vec::new(),
            equality_deletes: Vec::new(),
            deleted_data_files: Vec::new(),
        };

        let err = validate_change_batch_current_snapshot(&batch, 13).expect_err("mismatch");

        assert_eq!(
            err,
            "iceberg change batch current snapshot mismatch: expected 13, got 12"
        );
    }

    #[test]
    fn materialized_changes_becomes_ivm_stream_results() {
        let changes = MaterializedChanges {
            previous_snapshot_id: 10,
            current_snapshot_id: 12,
            inserts: one_row_result(1),
            deletes: one_row_result(2),
        };

        let stream = IvmChangeStream::from_materialized(changes);

        assert_eq!(stream.previous_snapshot_id, 10);
        assert_eq!(stream.current_snapshot_id, 12);
        assert!(!stream.is_empty());
        let (inserts, deletes) = stream.into_results();
        assert_eq!(inserts.row_count(), 1);
        assert_eq!(deletes.row_count(), 1);
    }
}
