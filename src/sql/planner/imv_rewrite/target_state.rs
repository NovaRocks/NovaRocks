use crate::sql::catalog::{ColumnDef, IcebergMvTargetStateScan, ScanSource};

pub(crate) fn build_target_state_scan_source(
    catalog: String,
    database: String,
    table: String,
    target_table_uuid: String,
    target_snapshot_id: Option<i64>,
    aggregate_state_layout_version: u16,
    columns: Vec<ColumnDef>,
    group_key_names: Vec<String>,
    aggregate_state_names: Vec<String>,
    physical_column_names: Vec<String>,
    row_id_column_name: String,
    row_filter: crate::sql::catalog::IcebergMvTargetStateRowFilter,
    partition_constraint: crate::sql::catalog::IcebergMvTargetStatePartitionConstraint,
) -> ScanSource {
    ScanSource::IcebergMvTargetState(IcebergMvTargetStateScan {
        catalog,
        database,
        table,
        target_table_uuid,
        target_snapshot_id,
        aggregate_state_layout_version,
        columns,
        group_key_names,
        aggregate_state_names,
        physical_column_names,
        row_id_column_name,
        row_filter,
        partition_constraint,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::DataType;

    #[test]
    fn build_target_state_scan_source_carries_target_state_metadata() {
        let columns = vec![ColumnDef {
            name: "k".to_string(),
            data_type: DataType::Int64,
            nullable: false,
            write_default: None,
            logical_type: None,
        }];

        let source = build_target_state_scan_source(
            "ice".to_string(),
            "db".to_string(),
            "mv_target".to_string(),
            "target-uuid".to_string(),
            Some(123),
            1,
            columns.clone(),
            vec!["k".to_string()],
            vec!["sum_v_state".to_string()],
            vec![
                "k".to_string(),
                "sum_v".to_string(),
                "sum_v_state".to_string(),
            ],
            "__row_id__".to_string(),
            crate::sql::catalog::IcebergMvTargetStateRowFilter::DeltaInputRowIds {
                row_id_column_name: "__row_id__".to_string(),
                branch_scope: None,
            },
            crate::sql::catalog::IcebergMvTargetStatePartitionConstraint::Unpartitioned,
        );

        let ScanSource::IcebergMvTargetState(scan) = source else {
            panic!("expected IcebergMvTargetState scan source");
        };

        assert_eq!(scan.fqn(), "ice.db.mv_target");
        assert_eq!(scan.target_table_uuid, "target-uuid");
        assert_eq!(scan.target_snapshot_id, Some(123));
        assert_eq!(scan.aggregate_state_layout_version, 1);
        assert_eq!(scan.columns, columns);
        assert_eq!(scan.group_key_names, vec!["k"]);
        assert_eq!(scan.aggregate_state_names, vec!["sum_v_state"]);
        assert_eq!(
            scan.physical_column_names,
            vec!["k", "sum_v", "sum_v_state"]
        );
        assert_eq!(scan.row_id_column_name, "__row_id__");
        assert!(matches!(
            scan.row_filter,
            crate::sql::catalog::IcebergMvTargetStateRowFilter::DeltaInputRowIds {
                branch_scope: None,
                ..
            }
        ));
        assert!(matches!(
            scan.partition_constraint,
            crate::sql::catalog::IcebergMvTargetStatePartitionConstraint::Unpartitioned
        ));
    }
}
