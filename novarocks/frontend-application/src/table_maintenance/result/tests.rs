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
fn add_files_returns_trino_metric_name_and_bigint_count() {
    for count in [0, 2] {
        let result = add_files_result(count).unwrap();
        assert_eq!(result.columns.len(), 2);
        assert_eq!(result.columns[0].name(), "metric_name");
        assert_eq!(result.columns[0].data_type(), &DataType::Utf8);
        assert_eq!(result.columns[1].name(), "metric_value");
        assert_eq!(result.columns[1].data_type(), &DataType::Int64);
        let batch = &result.batches[0];
        assert_eq!(batch.num_rows(), 1);
        let name = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let value = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(name.value(0), "added_data_files");
        assert_eq!(value.value(0), i64::from(count));
    }
}

#[test]
fn explicit_maintenance_keeps_metric_schema_without_rows() {
    let result = procedure_metrics_result(None).unwrap();
    assert_eq!(result.row_count(), 0);
    assert_eq!(
        result
            .columns
            .iter()
            .map(|column| (column.name(), column.data_type()))
            .collect::<Vec<_>>(),
        vec![
            ("metric_name", &DataType::Utf8),
            ("metric_value", &DataType::Int64)
        ],
    );
    let result = procedure_metrics_result(Some(
        ProcedureMetrics::from_counts([("scanned_files_count", 9), ("deleted_files_count", 1)])
            .unwrap(),
    ))
    .unwrap();
    assert_eq!(result.row_count(), 2);
    let batch = &result.batches[0];
    let names = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let values = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(names.value(0), "scanned_files_count");
    assert_eq!(names.value(1), "deleted_files_count");
    assert_eq!(values.values().as_ref(), &[9, 1]);
}
