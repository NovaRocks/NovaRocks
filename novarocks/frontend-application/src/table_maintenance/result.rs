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
use arrow::array::{ArrayRef, Int64Array, StringArray};
use arrow::datatypes::DataType;
use novarocks_query_application::api::{
    QueryResult, ResultField as QueryResultColumn, build_arrow_query_result,
};
use std::sync::Arc;

use novarocks_table_maintenance::{MaintenanceActionOutcome, ProcedureMetrics};

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
    table_execute_metrics(
        &[
            (
                "rewritten_data_files_count",
                Some(i64::from(rewritten_data_files_count)),
            ),
            (
                "removed_delete_files_count",
                Some(i64::from(removed_delete_files_count)),
            ),
            (
                "added_data_files_count",
                added_data_files_count.map(i64::from),
            ),
        ],
        "build Iceberg optimize metrics",
    )
    .map(MaintenanceStatementResult::Query)
}

pub(crate) fn add_files_result(file_count: u32) -> Result<QueryResult, String> {
    table_execute_metrics(
        &[("added_data_files", Some(i64::from(file_count)))],
        "build Iceberg ADD FILES metrics",
    )
}

fn table_execute_metrics(
    rows: &[(&str, Option<i64>)],
    context: &str,
) -> Result<QueryResult, String> {
    build_query_result(
        vec![
            column("metric_name", DataType::Utf8, false),
            column("metric_value", DataType::Int64, true),
        ],
        vec![
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|(name, _)| *name),
            )) as ArrayRef,
            Arc::new(Int64Array::from_iter(rows.iter().map(|(_, value)| *value))) as ArrayRef,
        ],
        context,
    )
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
mod tests;
