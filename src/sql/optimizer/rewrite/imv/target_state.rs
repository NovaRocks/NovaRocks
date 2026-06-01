use crate::sql::catalog::{ColumnDef, IcebergMvTargetStateScan, ScanSource};

pub(crate) fn build_target_state_scan_source(
    catalog: String,
    database: String,
    table: String,
    columns: Vec<ColumnDef>,
    group_key_names: Vec<String>,
    aggregate_state_names: Vec<String>,
    row_id_column_name: String,
) -> ScanSource {
    ScanSource::IcebergMvTargetState(IcebergMvTargetStateScan {
        catalog,
        database,
        table,
        columns,
        group_key_names,
        aggregate_state_names,
        row_id_column_name,
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
            columns.clone(),
            vec!["k".to_string()],
            vec!["sum_v_state".to_string()],
            "__row_id__".to_string(),
        );

        let ScanSource::IcebergMvTargetState(scan) = source else {
            panic!("expected IcebergMvTargetState scan source");
        };

        assert_eq!(scan.fqn(), "ice.db.mv_target");
        assert_eq!(scan.columns, columns);
        assert_eq!(scan.group_key_names, vec!["k"]);
        assert_eq!(scan.aggregate_state_names, vec!["sum_v_state"]);
        assert_eq!(scan.row_id_column_name, "__row_id__");
    }
}
