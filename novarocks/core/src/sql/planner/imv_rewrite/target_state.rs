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

use crate::sql::planner::table::{IcebergMvTargetStateScan, ScanSource};
use novarocks_catalog::schema::ColumnDef;

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
    row_filter: crate::sql::planner::table::IcebergMvTargetStateRowFilter,
    partition_constraint: crate::sql::planner::table::IcebergMvTargetStatePartitionConstraint,
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
            crate::sql::planner::table::IcebergMvTargetStateRowFilter::DeltaInputRowIds {
                row_id_column_name: "__row_id__".to_string(),
                branch_scope: None,
            },
            crate::sql::planner::table::IcebergMvTargetStatePartitionConstraint::Unpartitioned,
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
            crate::sql::planner::table::IcebergMvTargetStateRowFilter::DeltaInputRowIds {
                branch_scope: None,
                ..
            }
        ));
        assert!(matches!(
            scan.partition_constraint,
            crate::sql::planner::table::IcebergMvTargetStatePartitionConstraint::Unpartitioned
        ));
    }
}
