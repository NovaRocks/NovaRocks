use std::sync::Arc;

use crate::connector::iceberg::changes::{
    ChangeAction, IcebergChangeBatch, MaterializedChanges, materialize_changes, plan_changes,
};
use crate::connector::starrocks::managed::store::IcebergTableRef;
use crate::engine::{QueryResult, StandaloneState};

pub(crate) struct IvmChangeBranch<'a> {
    pub(crate) action: ChangeAction,
    pub(crate) result: &'a QueryResult,
}

pub(crate) struct IvmChangeStream {
    pub(crate) previous_snapshot_id: i64,
    pub(crate) current_snapshot_id: i64,
    pub(crate) inserts: QueryResult,
    pub(crate) deletes: QueryResult,
}

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

    pub(crate) fn non_empty_branches(&self) -> Vec<IvmChangeBranch<'_>> {
        let mut branches = Vec::with_capacity(2);
        if self.inserts.row_count() > 0 {
            branches.push(IvmChangeBranch {
                action: ChangeAction::Insert,
                result: &self.inserts,
            });
        }
        if self.deletes.row_count() > 0 {
            branches.push(IvmChangeBranch {
                action: ChangeAction::Delete,
                result: &self.deletes,
            });
        }
        branches
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
) -> Result<IcebergChangeBatch, String> {
    let batch =
        plan_changes(base_table, previous_snapshot_id, pk_columns).map_err(|e| e.to_string())?;
    validate_change_batch_current_snapshot(&batch, expected_current_snapshot_id)?;
    Ok(batch)
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn materialize_iceberg_change_stream(
    state: &Arc<StandaloneState>,
    current_database: &str,
    select_sql: &str,
    base_ref: &IcebergTableRef,
    base_table: &iceberg::table::Table,
    previous_snapshot_id: i64,
    expected_current_snapshot_id: i64,
    pk_columns: &[String],
) -> Result<IvmChangeStream, String> {
    let batch = plan_iceberg_change_batch_for_ivm(
        base_table,
        previous_snapshot_id,
        expected_current_snapshot_id,
        pk_columns,
    )?;
    let materialized = materialize_changes(
        state,
        current_database,
        select_sql,
        base_ref,
        base_table,
        batch,
        pk_columns,
    )?;
    Ok(IvmChangeStream::from_materialized(materialized))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    use crate::connector::iceberg::changes::{
        ChangeAction, IcebergChangeBatch, MaterializedChanges,
    };
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
        };

        let err = validate_change_batch_current_snapshot(&batch, 13).expect_err("mismatch");

        assert_eq!(
            err,
            "iceberg change batch current snapshot mismatch: expected 13, got 12"
        );
    }

    #[test]
    fn materialized_changes_becomes_ordered_ivm_branches() {
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
        let branches = stream.non_empty_branches();
        assert_eq!(branches.len(), 2);
        assert_eq!(branches[0].action, ChangeAction::Insert);
        assert_eq!(branches[0].result.row_count(), 1);
        assert_eq!(branches[1].action, ChangeAction::Delete);
        assert_eq!(branches[1].result.row_count(), 1);
    }
}
