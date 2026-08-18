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

use std::sync::Arc;

use crate::query_execution::maintenance::{MaintenanceActionOutcome, MaintenanceStatementResult};
use crate::runtime::query_result::{QueryResult, QueryResultColumn};
use arrow::array::{ArrayRef, Int32Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use novarocks_execution::exec::chunk::{Chunk, ChunkSchema};
use novarocks_types::SlotId;

use super::model::{OptimizeJob, OptimizeJobOutcome};

pub fn action_result(
    outcome: MaintenanceActionOutcome,
) -> Result<MaintenanceStatementResult, String> {
    let result = match outcome {
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
                column("added_data_files_count", DataType::Int32, false),
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
                column("added_delete_files_count", DataType::Int32, false),
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

pub fn optimize_jobs_result(jobs: Vec<OptimizeJob>) -> Result<MaintenanceStatementResult, String> {
    let column_names = [
        "JobId",
        "TableName",
        "State",
        "CreateTime",
        "FinishTime",
        "Msg",
        "BaseSnapshotId",
        "TargetSnapshotId",
        "InputDataFiles",
        "OutputDataFiles",
        "InputDeleteFiles",
        "OutputDeleteFiles",
    ];
    let mut values = column_names
        .iter()
        .map(|_| Vec::with_capacity(jobs.len()))
        .collect::<Vec<Vec<String>>>();
    for job in jobs {
        let outcome = job.outcome.as_ref();
        values[0].push(job.job_id.to_string());
        values[1].push(job.target.table);
        values[2].push(job.state.as_str().to_string());
        values[3].push(job.created_at_ms.to_string());
        values[4].push(
            job.finished_at_ms
                .map(|value| value.to_string())
                .unwrap_or_default(),
        );
        values[5].push(
            job.error_message
                .unwrap_or_else(|| outcome.map(optimize_outcome_message).unwrap_or_default()),
        );
        values[6].push(job.base_snapshot_id.to_string());
        values[7].push(
            outcome
                .and_then(|value| value.target_snapshot_id)
                .map(|value| value.to_string())
                .unwrap_or_default(),
        );
        values[8].push(
            outcome
                .map(|value| value.rewritten_data_files.to_string())
                .unwrap_or_default(),
        );
        values[9].push(
            outcome
                .map(|value| value.added_data_files.to_string())
                .unwrap_or_default(),
        );
        values[10].push(
            outcome
                .map(|value| value.deleted_data_files.to_string())
                .unwrap_or_default(),
        );
        values[11].push(outcome.map(|_| "0".to_string()).unwrap_or_default());
    }
    let result = build_query_result(
        column_names
            .iter()
            .map(|name| column(name, DataType::Utf8, false))
            .collect(),
        values
            .into_iter()
            .map(|column| Arc::new(StringArray::from(column)) as ArrayRef)
            .collect(),
        "build SHOW ALTER TABLE OPTIMIZE result",
    )?;
    Ok(MaintenanceStatementResult::Query(result))
}

fn optimize_outcome_message(outcome: &OptimizeJobOutcome) -> String {
    format!(
        "rewrote {} data files and {} delete files into {} data files ({} rows)",
        outcome.rewritten_data_files,
        outcome.deleted_data_files,
        outcome.added_data_files,
        outcome.output_record_count
    )
}

fn build_query_result(
    columns: Vec<QueryResultColumn>,
    arrays: Vec<ArrayRef>,
    context: &str,
) -> Result<QueryResult, String> {
    let fields = columns
        .iter()
        .map(|column| Field::new(&column.name, column.data_type.clone(), column.nullable))
        .collect::<Vec<_>>();
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)
        .map_err(|error| format!("{context} failed: {error}"))?;
    let slot_ids = (1..=batch.num_columns())
        .map(|index| {
            u32::try_from(index)
                .map(SlotId::new)
                .map_err(|_| "too many output columns".to_string())
        })
        .collect::<Result<Vec<_>, _>>()?;
    let chunk_schema =
        ChunkSchema::try_ref_from_schema_and_slot_ids(batch.schema().as_ref(), &slot_ids)?;
    let chunk = Chunk::try_new_with_chunk_schema(batch, chunk_schema)?;
    Ok(QueryResult {
        columns,
        chunks: vec![chunk],
    })
}

fn column(name: &str, data_type: DataType, nullable: bool) -> QueryResultColumn {
    QueryResultColumn {
        name: name.to_string(),
        data_type,
        nullable,
        logical_type: None,
    }
}
