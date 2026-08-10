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

use super::*;

#[test]
fn projected_required_column_merge_preserves_unicode_case_deduplication() {
    let merged = super::super::projection::merge_required_columns_with_projected(
        Some(vec!["äpfel".to_string()]),
        &["ÄPFEL".to_string()],
    );

    assert_eq!(merged, vec!["ÄPFEL"]);
}

#[test]
fn version_scan_without_required_columns_rejects_unmaterializable_planner_output() {
    let mut root = scan_node(37, IcebergDataFileBinding::ExplicitFiles);
    let DistributedNodeKind::Scan(scan) = &mut root.payload else {
        panic!("test root must be a scan");
    };
    scan.required_columns = None;
    scan.columns
        .push(column(99, "stale_planner_only", DataType::Utf8, true));
    scan.table.source = crate::sql::planner::table::test_sql_scan_source(
        crate::sql::planner::table::SqlScanKind::Data {
            version: crate::sql::planner::table::SqlTableVersionSelector::Snapshot(6),
        },
    );
    let plan = plan(root);
    let before = format!("{plan:#?}");
    let error = match prepare_scan_bindings(
        &plan,
        &registry(vec![data_file("s3://bucket/version-6.parquet")]),
        None,
    ) {
        Ok(_) => panic!("unmaterializable planner column must fail before submission"),
        Err(error) => error,
    };

    assert!(error.contains("node_id=37"), "{error}");
    assert!(error.contains("stale_planner_only"), "{error}");
    assert_eq!(format!("{plan:#?}"), before);
}

#[test]
fn target_locator_projection_preserves_planner_ids_and_metadata_contract() {
    use novarocks_execution::exec::row_position::{
        ICEBERG_FILE_PATH_COL, ICEBERG_LAST_UPDATED_SEQ_COL, ICEBERG_ROW_ID_COL,
        ICEBERG_ROW_POS_COL,
    };

    let mut root = scan_node(37, IcebergDataFileBinding::ExplicitFiles);
    let DistributedNodeKind::Scan(scan) = &mut root.payload else {
        panic!("test root must be a scan");
    };
    scan.table
        .columns
        .push(source_column("extra", DataType::Utf8, true));
    scan.table.iceberg_row_lineage_metadata_columns = vec![
        source_column(ICEBERG_FILE_PATH_COL, DataType::Utf8, false),
        source_column(ICEBERG_ROW_POS_COL, DataType::Int64, false),
        source_column(ICEBERG_ROW_ID_COL, DataType::Int64, false),
        source_column(ICEBERG_LAST_UPDATED_SEQ_COL, DataType::Int64, true),
    ];
    scan.columns = vec![
        column(1, "id", DataType::Int32, false),
        column(2, "extra", DataType::Utf8, true),
        column(11, ICEBERG_FILE_PATH_COL, DataType::Utf8, false),
        column(12, ICEBERG_ROW_POS_COL, DataType::Int64, false),
        column(13, ICEBERG_ROW_ID_COL, DataType::Int64, false),
        column(14, ICEBERG_LAST_UPDATED_SEQ_COL, DataType::Int64, true),
    ];
    scan.table.source = crate::sql::planner::table::test_sql_scan_source(
        crate::sql::planner::table::SqlScanKind::MvTargetLocator {
            facts: crate::sql::planner::table::SqlMvTargetLocatorScan {
                target_table_uuid: "00000000-0000-0000-0000-000000000001".to_string(),
                target_snapshot_id: Some(6),
                apply_key_column: "id".to_string(),
                branch_id_column: None,
            },
        },
    );
    let (registry, seen_column_names) =
        recording_registry(vec![data_file("s3://bucket/target-6.parquet")]);
    let bindings =
        prepare_scan_bindings(&plan(root), &registry, None).expect("prepare target locator scan");
    let binding = bindings.binding(37).expect("binding");
    let physical = &binding.physical_columns;

    assert_eq!(
        physical
            .iter()
            .map(|column| (column.source.name.as_str(), column.planner.column_id))
            .collect::<Vec<_>>(),
        vec![
            ("id", ColumnId::new_for_test(1)),
            (ICEBERG_FILE_PATH_COL, ColumnId::new_for_test(11)),
            (ICEBERG_ROW_POS_COL, ColumnId::new_for_test(12)),
            (ICEBERG_ROW_ID_COL, ColumnId::new_for_test(13)),
            (ICEBERG_LAST_UPDATED_SEQ_COL, ColumnId::new_for_test(14)),
        ]
    );
    assert_eq!(
        binding
            .required_reads
            .iter()
            .map(|read| read.source.name.as_str())
            .collect::<Vec<_>>(),
        vec![
            "id",
            ICEBERG_FILE_PATH_COL,
            ICEBERG_ROW_POS_COL,
            ICEBERG_ROW_ID_COL,
            ICEBERG_LAST_UPDATED_SEQ_COL,
        ]
    );
    assert_eq!(
        seen_column_names
            .lock()
            .expect("seen column names lock")
            .last()
            .cloned(),
        Some(vec![0, 6, 7, 8, 9])
    );
}

#[test]
fn target_state_projection_keeps_declared_columns_and_row_lineage_ids() {
    use novarocks_execution::exec::row_position::{
        ICEBERG_FILE_PATH_COL, ICEBERG_LAST_UPDATED_SEQ_COL, ICEBERG_ROW_ID_COL,
        ICEBERG_ROW_POS_COL,
    };

    let mut root = scan_node(38, IcebergDataFileBinding::ExplicitFiles);
    let DistributedNodeKind::Scan(scan) = &mut root.payload else {
        panic!("test root must be a scan");
    };
    scan.table.columns.extend([
        source_column("agg", DataType::Binary, true),
        source_column("extra", DataType::Utf8, true),
    ]);
    scan.table.iceberg_row_lineage_metadata_columns = vec![
        source_column(ICEBERG_FILE_PATH_COL, DataType::Utf8, false),
        source_column(ICEBERG_ROW_POS_COL, DataType::Int64, false),
        source_column(ICEBERG_ROW_ID_COL, DataType::Int64, false),
        source_column(ICEBERG_LAST_UPDATED_SEQ_COL, DataType::Int64, true),
    ];
    scan.columns = vec![
        column(1, "id", DataType::Int32, false),
        column(3, "agg", DataType::Binary, true),
        column(4, "extra", DataType::Utf8, true),
        column(11, ICEBERG_FILE_PATH_COL, DataType::Utf8, false),
        column(12, ICEBERG_ROW_POS_COL, DataType::Int64, false),
        column(13, ICEBERG_ROW_ID_COL, DataType::Int64, false),
        column(14, ICEBERG_LAST_UPDATED_SEQ_COL, DataType::Int64, true),
    ];
    scan.table.source = crate::sql::planner::table::test_sql_scan_source(
        crate::sql::planner::table::SqlScanKind::MvTargetState {
            facts: crate::sql::planner::table::SqlMvTargetStateScan {
                target_table_uuid: "00000000-0000-0000-0000-000000000001".to_string(),
                target_snapshot_id: Some(6),
                aggregate_state_layout_version: 1,
                columns: scan.table.columns.clone(),
                group_key_names: vec!["id".to_string()],
                aggregate_state_names: vec!["agg".to_string()],
                physical_column_names: vec!["id".to_string(), "agg".to_string()],
                row_id_column_name: ICEBERG_ROW_ID_COL.to_string(),
                row_filter:
                    crate::sql::planner::table::SqlMvTargetStateRowFilter::DeltaInputRowIds {
                        row_id_column_name: ICEBERG_ROW_ID_COL.to_string(),
                        branch_scope: None,
                    },
                partition_constraint:
                    crate::sql::planner::table::SqlMvTargetStatePartitionConstraint::Unpartitioned,
            },
        },
    );
    let bindings = prepare_scan_bindings(
        &plan(root),
        &registry(vec![data_file("s3://bucket/target-state-6.parquet")]),
        None,
    )
    .expect("prepare target-state scan");
    let physical = &bindings.binding(38).expect("binding").physical_columns;

    assert_eq!(
        physical
            .iter()
            .map(|column| (column.source.name.as_str(), column.planner.column_id))
            .collect::<Vec<_>>(),
        vec![
            (ICEBERG_ROW_ID_COL, ColumnId::new_for_test(13)),
            ("id", ColumnId::new_for_test(1)),
            ("agg", ColumnId::new_for_test(3)),
            (ICEBERG_FILE_PATH_COL, ColumnId::new_for_test(11)),
            (ICEBERG_ROW_POS_COL, ColumnId::new_for_test(12)),
            (ICEBERG_LAST_UPDATED_SEQ_COL, ColumnId::new_for_test(14)),
        ]
    );
}

#[test]
fn hidden_equality_key_remains_provider_owned_without_plan_mutation() {
    let mut root = scan_node(10, IcebergDataFileBinding::CurrentSnapshot);
    let DistributedNodeKind::Scan(scan) = &mut root.payload else {
        panic!("test root must be a scan");
    };
    scan.table
        .columns
        .push(source_column("category", DataType::Utf8, true));
    let plan = plan(root);
    let before = format!("{plan:#?}");
    let mut file = data_file("s3://bucket/data.parquet");
    file.delete_files = vec![equality_delete_file(Vec::new(), vec![3])];

    let (registry, seen_column_names) = recording_registry(vec![file]);
    let bindings =
        prepare_scan_bindings(&plan, &registry, None).expect("prepare equality-delete scan");
    let binding = bindings.binding(10).expect("binding");

    assert_eq!(format!("{plan:#?}"), before);
    assert_eq!(binding.required_reads.len(), 1);
    assert_eq!(
        binding.required_reads[0].planner_column_id,
        Some(ColumnId::new_for_test(1))
    );
    assert_eq!(
        binding.required_reads[0].reason,
        ResolvedReadReason::PlannerRequiredOrOutput
    );
    assert!(bindings.scan_ranges(0, 10).expect("ranges").is_empty());
    let planned = bindings
        .connector_read(0, 10)
        .expect("opaque connector read");
    let file =
        crate::connector::iceberg::provider::planned_split_data_file_for_test(&planned.splits[0])
            .expect("decode test Iceberg split");
    assert_eq!(file.delete_files.len(), 1);
    assert_eq!(
        file.delete_files[0].file_content,
        novarocks_connector_iceberg::scan_model::IcebergDeleteFileContent::Equality
    );
    assert_eq!(
        seen_column_names
            .lock()
            .expect("seen column names lock")
            .last()
            .cloned(),
        Some(vec![0])
    );
}

#[test]
fn variant_synthetic_output_is_not_prepared_as_a_physical_column() {
    let mut root = scan_node(10, IcebergDataFileBinding::ExplicitFiles);
    let DistributedNodeKind::Scan(scan) = &mut root.payload else {
        panic!("test root must be a scan");
    };
    scan.table.columns = vec![source_column("v", DataType::LargeBinary, false)];
    scan.columns = vec![
        column(1, "v", DataType::LargeBinary, false),
        OutputColumn {
            column_id: ColumnId::new_for_test(2),
            name: "__nr_var_v_0".to_string(),
            data_type: DataType::Int64,
            nullable: false,
            is_internal: true,
        },
    ];
    scan.required_columns = Some(vec!["__nr_var_v_0".to_string()]);
    scan.variant_columns = vec![crate::sql::common::ScanVariantColumn {
        source_column_id: ColumnId::new_for_test(1),
        source_column: "v".to_string(),
        synthetic_column_id: ColumnId::new_for_test(2),
        synthetic_column: "__nr_var_v_0".to_string(),
        canonical_path: "$.a.b".to_string(),
        requested_type: DataType::Int64,
        strict: true,
    }];

    let (registry, seen_column_names) =
        recording_registry(vec![data_file("s3://bucket/variant.parquet")]);
    let bindings =
        prepare_scan_bindings(&plan(root), &registry, None).expect("prepare bound VARIANT scan");
    let binding = bindings.binding(10).expect("binding");
    assert_eq!(binding.physical_columns.len(), 1);
    assert_eq!(binding.physical_columns[0].source.name, "v");
    assert!(binding.required_reads.is_empty());
    assert_eq!(
        seen_column_names
            .lock()
            .expect("seen column names lock")
            .last()
            .cloned(),
        Some(vec![2])
    );
}

#[test]
fn equality_key_already_in_planner_output_keeps_column_id() {
    let mut root = scan_node(10, IcebergDataFileBinding::CurrentSnapshot);
    let DistributedNodeKind::Scan(scan) = &mut root.payload else {
        panic!("test root must be a scan");
    };
    scan.table
        .columns
        .push(source_column("category", DataType::Utf8, true));
    scan.columns
        .push(column(3, "category", DataType::Utf8, true));
    scan.required_columns = Some(vec!["id".to_string(), "category".to_string()]);
    let mut file = data_file("s3://bucket/data.parquet");
    file.delete_files = vec![equality_delete_file(Vec::new(), vec![3])];

    let bindings = prepare_scan_bindings(&plan(root), &registry(vec![file]), None)
        .expect("prepare equality-delete output scan");
    let category = bindings
        .binding(10)
        .expect("binding")
        .required_reads
        .iter()
        .find(|read| read.source.name == "category")
        .expect("category read");

    assert_eq!(category.planner_column_id, Some(ColumnId::new_for_test(3)));
    assert_eq!(category.reason, ResolvedReadReason::PlannerRequiredOrOutput);
}

#[test]
fn physical_projection_missing_type_and_nullability_mismatches_fail_fast() {
    let mut missing = scan_node(43, IcebergDataFileBinding::ExplicitFiles);
    let DistributedNodeKind::Scan(scan) = &mut missing.payload else {
        panic!("test root must be a scan");
    };
    scan.columns[0].name = "missing".to_string();

    let mut type_mismatch = scan_node(44, IcebergDataFileBinding::ExplicitFiles);
    let DistributedNodeKind::Scan(scan) = &mut type_mismatch.payload else {
        panic!("test root must be a scan");
    };
    scan.columns[0].data_type = DataType::Int64;

    let mut nullability_mismatch = scan_node(45, IcebergDataFileBinding::ExplicitFiles);
    let DistributedNodeKind::Scan(scan) = &mut nullability_mismatch.payload else {
        panic!("test root must be a scan");
    };
    scan.columns[0].nullable = true;

    for (root, expected) in [
        (missing, "cannot resolve planner physical column 'missing'"),
        (type_mismatch, "type mismatch"),
        (nullability_mismatch, "nullability mismatch"),
    ] {
        let err = match prepare_scan_bindings(
            &plan(root),
            &registry(vec![data_file("s3://bucket/data.parquet")]),
            None,
        ) {
            Ok(_) => panic!("physical projection mismatch must fail: {expected}"),
            Err(err) => err,
        };
        assert!(err.contains(expected), "{err}");
        assert!(err.contains("node_id="), "{err}");
    }
}

#[test]
fn invalid_equality_identity_fails_fast_with_scan_node_context() {
    for (delete, expected) in [
        (
            equality_delete_file(Vec::new(), vec![99]),
            "unknown field id 99",
        ),
        (
            equality_delete_file(Vec::new(), vec![3, 3]),
            "duplicate equality field id 3",
        ),
        (
            equality_delete_file(vec!["category", "CATEGORY"], Vec::new()),
            "duplicate equality column name",
        ),
        (
            equality_delete_file(vec!["missing"], Vec::new()),
            "unknown equality column missing",
        ),
        (
            equality_delete_file(vec!["id"], vec![3]),
            "field id/name mismatch",
        ),
    ] {
        let mut root = scan_node(46, IcebergDataFileBinding::CurrentSnapshot);
        let DistributedNodeKind::Scan(scan) = &mut root.payload else {
            panic!("test root must be a scan");
        };
        scan.table
            .columns
            .push(source_column("category", DataType::Utf8, true));
        let mut file = data_file("s3://bucket/data.parquet");
        file.delete_files = vec![delete];

        let err = match prepare_scan_bindings(&plan(root), &registry(vec![file]), None) {
            Ok(_) => panic!("invalid equality identity must fail: {expected}"),
            Err(err) => err,
        };

        assert!(err.contains(expected), "{err}");
        assert!(err.contains("node_id=46"), "{err}");
    }
}
