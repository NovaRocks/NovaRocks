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

use crate::query_execution::maintenance::MaintenanceStatementResult;
use arrow::array::{ArrayRef, Int32Array, Int64Array, StringArray};
use arrow::datatypes::DataType;
use novarocks_query_application::api::{
    QueryResult, ResultField as QueryResultColumn, build_arrow_query_result,
};
use std::sync::Arc;

use novarocks_table_maintenance::{MaintenanceActionOutcome, ProcedureMetrics};

pub fn action_result(
    outcome: MaintenanceActionOutcome,
) -> Result<MaintenanceStatementResult, String> {
    let result = match outcome {
        MaintenanceActionOutcome::Procedure { metrics } => procedure_metrics_result(metrics)?,
        MaintenanceActionOutcome::RewriteDataFiles {
            rewritten_data_files_count,
            added_data_files_count,
            rewritten_bytes_count,
            failed_data_files_count,
            removed_delete_files_count,
            ..
        } => build_query_result(
            vec![
                column("rewritten_data_files_count", DataType::Int32, false),
                column("added_data_files_count", DataType::Int32, true),
                column("rewritten_bytes_count", DataType::Int64, false),
                column("failed_data_files_count", DataType::Int32, false),
                column("removed_delete_files_count", DataType::Int32, false),
            ],
            vec![
                Arc::new(Int32Array::from(vec![rewritten_data_files_count])) as ArrayRef,
                Arc::new(Int32Array::from(vec![added_data_files_count])) as ArrayRef,
                Arc::new(Int64Array::from(vec![rewritten_bytes_count])) as ArrayRef,
                Arc::new(Int32Array::from(vec![failed_data_files_count])) as ArrayRef,
                Arc::new(Int32Array::from(vec![removed_delete_files_count])) as ArrayRef,
            ],
            "build Iceberg maintenance result",
        )?,
        MaintenanceActionOutcome::RewriteManifests {
            rewritten_manifests_count,
            added_manifests_count,
        } => build_query_result(
            vec![
                column("rewritten_manifests_count", DataType::Int32, false),
                column("added_manifests_count", DataType::Int32, false),
            ],
            vec![
                Arc::new(Int32Array::from(vec![rewritten_manifests_count])) as ArrayRef,
                Arc::new(Int32Array::from(vec![added_manifests_count])) as ArrayRef,
            ],
            "build Iceberg maintenance result",
        )?,
        MaintenanceActionOutcome::ExpireSnapshots {
            deleted_data_files_count,
            deleted_position_delete_files_count,
            deleted_equality_delete_files_count,
            deleted_manifest_files_count,
            deleted_manifest_lists_count,
            deleted_statistics_files_count,
        } => {
            let names = [
                "deleted_data_files_count",
                "deleted_position_delete_files_count",
                "deleted_equality_delete_files_count",
                "deleted_manifest_files_count",
                "deleted_manifest_lists_count",
                "deleted_statistics_files_count",
            ];
            let values = [
                deleted_data_files_count,
                deleted_position_delete_files_count,
                deleted_equality_delete_files_count,
                deleted_manifest_files_count,
                deleted_manifest_lists_count,
                deleted_statistics_files_count,
            ];
            build_query_result(
                names
                    .iter()
                    .map(|name| column(name, DataType::Int64, true))
                    .collect(),
                values
                    .into_iter()
                    .map(|value| Arc::new(Int64Array::from(vec![value])) as ArrayRef)
                    .collect(),
                "build Iceberg maintenance result",
            )?
        }
        MaintenanceActionOutcome::RemoveOrphanFiles {
            orphan_file_locations,
        } => build_query_result(
            vec![column("orphan_file_location", DataType::Utf8, false)],
            vec![Arc::new(StringArray::from(orphan_file_locations)) as ArrayRef],
            "build Iceberg maintenance result",
        )?,
        MaintenanceActionOutcome::RewritePositionDeleteFiles {
            rewritten_delete_files_count,
            added_delete_files_count,
            rewritten_bytes_count,
            added_bytes_count,
        } => build_query_result(
            vec![
                column("rewritten_delete_files_count", DataType::Int32, false),
                column("added_delete_files_count", DataType::Int32, true),
                column("rewritten_bytes_count", DataType::Int64, false),
                column("added_bytes_count", DataType::Int64, false),
            ],
            vec![
                Arc::new(Int32Array::from(vec![rewritten_delete_files_count])) as ArrayRef,
                Arc::new(Int32Array::from(vec![added_delete_files_count])) as ArrayRef,
                Arc::new(Int64Array::from(vec![rewritten_bytes_count])) as ArrayRef,
                Arc::new(Int64Array::from(vec![added_bytes_count])) as ArrayRef,
            ],
            "build Iceberg maintenance result",
        )?,
    };
    Ok(MaintenanceStatementResult::Query(result))
}

/// The optimize table procedure returns Trino's fixed three metric rows.
pub(crate) fn optimize_result(
    outcome: MaintenanceActionOutcome,
) -> Result<MaintenanceStatementResult, String> {
    let MaintenanceActionOutcome::RewriteDataFiles {
        rewritten_data_files_count,
        removed_delete_files_count,
        added_data_files_count,
        ..
    } = outcome
    else {
        return Err("OPTIMIZE requires data rewrite outcome facts".into());
    };
    build_query_result(
        vec![
            column("metric_name", DataType::Utf8, false),
            column("metric_value", DataType::Int64, true),
        ],
        vec![
            Arc::new(StringArray::from(vec![
                "rewritten_data_files_count",
                "removed_delete_files_count",
                "added_data_files_count",
            ])) as ArrayRef,
            Arc::new(Int64Array::from(vec![
                Some(i64::from(rewritten_data_files_count)),
                Some(i64::from(removed_delete_files_count)),
                added_data_files_count.map(i64::from),
            ])) as ArrayRef,
        ],
        "build Iceberg optimize metrics",
    )
    .map(MaintenanceStatementResult::Query)
}

pub(crate) fn procedure_metrics_result(
    metrics: Option<ProcedureMetrics>,
) -> Result<QueryResult, String> {
    let (names, values): (Vec<_>, Vec<_>) = metrics
        .map(ProcedureMetrics::into_rows)
        .unwrap_or_default()
        .into_iter()
        .unzip();
    build_query_result(
        vec![
            column("metric_name", DataType::Utf8, false),
            column("metric_value", DataType::Int64, false),
        ],
        vec![
            Arc::new(StringArray::from(names)) as ArrayRef,
            Arc::new(Int64Array::from(values)) as ArrayRef,
        ],
        "build Iceberg procedure metrics",
    )
}

fn build_query_result(
    columns: Vec<QueryResultColumn>,
    arrays: Vec<ArrayRef>,
    context: &str,
) -> Result<QueryResult, String> {
    build_arrow_query_result(columns, arrays).map_err(|error| format!("{context} failed: {error}"))
}

fn column(name: &str, data_type: DataType, nullable: bool) -> QueryResultColumn {
    QueryResultColumn::new(
        name,
        novarocks_type_contract::FunctionValueType::new(data_type, nullable),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
