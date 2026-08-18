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
use novarocks_sql::plan_read::ColumnId;

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
    let plan = native_scan_plan(NativeScanFixture::VersionSnapshotWithStaleOutput)
        .expect("sealed stale-output fixture");
    let before = format!("{plan:#?}");
    let error = match prepare_scan_bindings(
        &plan,
        &registry(vec![data_file("s3://bucket/version-6.parquet")]),
        None,
    ) {
        Ok(_) => panic!("unmaterializable planner column must fail before submission"),
        Err(error) => error,
    };

    assert!(error.contains("node_id=10"), "{error}");
    assert!(error.contains("stale_planner_only"), "{error}");
    assert_eq!(format!("{plan:#?}"), before);
}

#[test]
fn target_locator_projection_preserves_planner_ids_and_metadata_contract() {
    use novarocks_execution::exec::row_position::{
        ICEBERG_FILE_PATH_COL, ICEBERG_LAST_UPDATED_SEQ_COL, ICEBERG_ROW_ID_COL,
        ICEBERG_ROW_POS_COL,
    };

    let (registry, seen_column_names) =
        recording_registry(vec![data_file("s3://bucket/target-6.parquet")]);
    let plan = native_scan_plan(NativeScanFixture::TargetLocatorProjection)
        .expect("sealed target-locator projection fixture");
    let bindings =
        prepare_scan_bindings(&plan, &registry, None).expect("prepare target locator scan");
    let binding = bindings.binding(10).expect("binding");
    let physical = &binding.physical_columns;

    assert_eq!(
        physical
            .iter()
            .map(|column| (column.source.name.as_str(), column.planner.column_id))
            .collect::<Vec<_>>(),
        vec![
            ("id", ColumnId(1)),
            (ICEBERG_FILE_PATH_COL, ColumnId(11)),
            (ICEBERG_ROW_POS_COL, ColumnId(12)),
            (ICEBERG_ROW_ID_COL, ColumnId(13)),
            (ICEBERG_LAST_UPDATED_SEQ_COL, ColumnId(14)),
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

    let plan = native_scan_plan(NativeScanFixture::TargetStateProjection)
        .expect("sealed target-state projection fixture");
    let bindings = prepare_scan_bindings(
        &plan,
        &registry(vec![data_file("s3://bucket/target-state-6.parquet")]),
        None,
    )
    .expect("prepare target-state scan");
    let physical = &bindings.binding(10).expect("binding").physical_columns;

    assert_eq!(
        physical
            .iter()
            .map(|column| (column.source.name.as_str(), column.planner.column_id))
            .collect::<Vec<_>>(),
        vec![
            (ICEBERG_ROW_ID_COL, ColumnId(13)),
            ("id", ColumnId(1)),
            ("agg", ColumnId(3)),
            (ICEBERG_FILE_PATH_COL, ColumnId(11)),
            (ICEBERG_ROW_POS_COL, ColumnId(12)),
            (ICEBERG_LAST_UPDATED_SEQ_COL, ColumnId(14)),
        ]
    );
}

#[test]
fn hidden_equality_key_remains_provider_owned_without_plan_mutation() {
    let plan = native_scan_plan(NativeScanFixture::EqualityKeyHidden)
        .expect("sealed hidden-equality fixture");
    let before = format!("{plan:#?}");
    let mut file = data_file("s3://bucket/data.parquet");
    file.deletes = vec![equality_delete_file(Vec::new(), vec![3])];

    let (registry, seen_column_names) = recording_registry(vec![file]);
    let bindings =
        prepare_scan_bindings(&plan, &registry, None).expect("prepare equality-delete scan");
    let binding = bindings.binding(10).expect("binding");

    assert_eq!(format!("{plan:#?}"), before);
    assert_eq!(binding.required_reads.len(), 1);
    assert_eq!(
        binding.required_reads[0].planner_column_id,
        Some(ColumnId(1))
    );
    assert_eq!(
        binding.required_reads[0].reason,
        ResolvedReadReason::PlannerRequiredOrOutput
    );
    assert!(bindings.scan_ranges(0, 10).expect("ranges").is_empty());
    let planned = bindings
        .connector_read(0, 10)
        .expect("opaque connector read");
    let file = novarocks::connector::scan_model::planned_split_file_for_test(&planned.splits[0])
        .expect("decode fixture split");
    assert_eq!(file.deletes.len(), 1);
    assert_eq!(
        file.deletes[0].kind,
        novarocks::connector::scan_model::FixtureDeleteKind::Equality
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
    let (registry, seen_column_names) =
        recording_registry(vec![data_file("s3://bucket/variant.parquet")]);
    let bindings = prepare_scan_bindings(
        &native_scan_plan(NativeScanFixture::VariantProjection).expect("sealed VARIANT fixture"),
        &registry,
        None,
    )
    .expect("prepare bound VARIANT scan");
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
    let mut file = data_file("s3://bucket/data.parquet");
    file.deletes = vec![equality_delete_file(Vec::new(), vec![3])];

    let bindings = prepare_scan_bindings(
        &native_scan_plan(NativeScanFixture::EqualityKeyProjected)
            .expect("sealed projected-equality fixture"),
        &registry(vec![file]),
        None,
    )
    .expect("prepare equality-delete output scan");
    let category = bindings
        .binding(10)
        .expect("binding")
        .required_reads
        .iter()
        .find(|read| read.source.name == "category")
        .expect("category read");

    assert_eq!(category.planner_column_id, Some(ColumnId(3)));
    assert_eq!(category.reason, ResolvedReadReason::PlannerRequiredOrOutput);
}

#[test]
fn physical_projection_missing_type_and_nullability_mismatches_fail_fast() {
    for (fixture, expected) in [
        (
            NativeScanFixture::ProjectionMissingColumn,
            "cannot resolve planner physical column 'missing'",
        ),
        (NativeScanFixture::ProjectionTypeMismatch, "type mismatch"),
        (
            NativeScanFixture::ProjectionNullabilityMismatch,
            "nullability mismatch",
        ),
    ] {
        let err = match prepare_scan_bindings(
            &native_scan_plan(fixture).expect("sealed mismatched-projection fixture"),
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
