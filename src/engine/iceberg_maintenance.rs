use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int32Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use crate::engine::{QueryResult, QueryResultColumn, record_batch_to_chunk};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum MaintenanceActionSource {
    SparkProcedure,
    LegacyAlter,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum MaintenanceActionKind {
    RewriteDataFiles,
    RewriteManifests,
    ExpireSnapshots,
    RemoveOrphanFiles,
    RewritePositionDeleteFiles,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct MaintenanceActionOptions {
    pub(crate) values: BTreeMap<String, String>,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct MaintenanceActionRequest {
    pub(crate) source: MaintenanceActionSource,
    pub(crate) kind: MaintenanceActionKind,
    pub(crate) catalog: String,
    pub(crate) namespace: String,
    pub(crate) table: String,
    pub(crate) options: MaintenanceActionOptions,
    pub(crate) older_than_ms: Option<i64>,
    pub(crate) retain_last: Option<u32>,
    pub(crate) use_caching: Option<bool>,
    pub(crate) spec_id: Option<i32>,
    pub(crate) branch: Option<String>,
    pub(crate) where_clause: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum MaintenanceActionOutcome {
    RewriteManifests {
        rewritten_manifests_count: i32,
        added_manifests_count: i32,
    },
    ExpireSnapshots {
        deleted_data_files_count: Option<i64>,
        deleted_position_delete_files_count: Option<i64>,
        deleted_equality_delete_files_count: Option<i64>,
        deleted_manifest_files_count: Option<i64>,
        deleted_manifest_lists_count: Option<i64>,
        deleted_statistics_files_count: Option<i64>,
    },
    RemoveOrphanFiles {
        orphan_file_locations: Vec<String>,
    },
    RewriteDataFiles {
        rewritten_data_files_count: i32,
        added_data_files_count: i32,
        rewritten_bytes_count: i64,
        failed_data_files_count: i32,
        removed_delete_files_count: i32,
    },
    RewritePositionDeleteFiles {
        rewritten_delete_files_count: i32,
        added_delete_files_count: i32,
        rewritten_bytes_count: i64,
        added_bytes_count: i64,
    },
}

impl MaintenanceActionOutcome {
    pub(crate) fn to_spark_query_result(&self) -> Result<QueryResult, String> {
        match self {
            Self::RewriteManifests {
                rewritten_manifests_count,
                added_manifests_count,
            } => build_rewrite_manifests_result(*rewritten_manifests_count, *added_manifests_count),
            Self::ExpireSnapshots {
                deleted_data_files_count,
                deleted_position_delete_files_count,
                deleted_equality_delete_files_count,
                deleted_manifest_files_count,
                deleted_manifest_lists_count,
                deleted_statistics_files_count,
            } => build_expire_snapshots_result([
                *deleted_data_files_count,
                *deleted_position_delete_files_count,
                *deleted_equality_delete_files_count,
                *deleted_manifest_files_count,
                *deleted_manifest_lists_count,
                *deleted_statistics_files_count,
            ]),
            Self::RemoveOrphanFiles {
                orphan_file_locations,
            } => build_string_rows_result("orphan_file_location", orphan_file_locations),
            Self::RewriteDataFiles {
                rewritten_data_files_count,
                added_data_files_count,
                rewritten_bytes_count,
                failed_data_files_count,
                removed_delete_files_count,
            } => build_rewrite_data_files_result(
                *rewritten_data_files_count,
                *added_data_files_count,
                *rewritten_bytes_count,
                *failed_data_files_count,
                *removed_delete_files_count,
            ),
            Self::RewritePositionDeleteFiles {
                rewritten_delete_files_count,
                added_delete_files_count,
                rewritten_bytes_count,
                added_bytes_count,
            } => build_rewrite_position_delete_files_result(
                *rewritten_delete_files_count,
                *added_delete_files_count,
                *rewritten_bytes_count,
                *added_bytes_count,
            ),
        }
    }
}

fn build_rewrite_manifests_result(
    rewritten_manifests_count: i32,
    added_manifests_count: i32,
) -> Result<QueryResult, String> {
    build_query_result(
        vec![
            column("rewritten_manifests_count", DataType::Int32, false),
            column("added_manifests_count", DataType::Int32, false),
        ],
        vec![
            Arc::new(Int32Array::from(vec![rewritten_manifests_count])) as ArrayRef,
            Arc::new(Int32Array::from(vec![added_manifests_count])) as ArrayRef,
        ],
    )
}

fn build_expire_snapshots_result(values: [Option<i64>; 6]) -> Result<QueryResult, String> {
    let names = [
        "deleted_data_files_count",
        "deleted_position_delete_files_count",
        "deleted_equality_delete_files_count",
        "deleted_manifest_files_count",
        "deleted_manifest_lists_count",
        "deleted_statistics_files_count",
    ];
    let columns = names
        .iter()
        .map(|name| column(name, DataType::Int64, true))
        .collect::<Vec<_>>();
    let arrays = values
        .iter()
        .map(|value| Arc::new(Int64Array::from(vec![*value])) as ArrayRef)
        .collect::<Vec<_>>();
    build_query_result(columns, arrays)
}

fn build_string_rows_result(column_name: &str, rows: &[String]) -> Result<QueryResult, String> {
    build_query_result(
        vec![column(column_name, DataType::Utf8, false)],
        vec![Arc::new(StringArray::from(rows.to_vec())) as ArrayRef],
    )
}

fn build_rewrite_data_files_result(
    rewritten_data_files_count: i32,
    added_data_files_count: i32,
    rewritten_bytes_count: i64,
    failed_data_files_count: i32,
    removed_delete_files_count: i32,
) -> Result<QueryResult, String> {
    build_query_result(
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
    )
}

fn build_rewrite_position_delete_files_result(
    rewritten_delete_files_count: i32,
    added_delete_files_count: i32,
    rewritten_bytes_count: i64,
    added_bytes_count: i64,
) -> Result<QueryResult, String> {
    build_query_result(
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
    )
}

fn build_query_result(
    columns: Vec<QueryResultColumn>,
    arrays: Vec<ArrayRef>,
) -> Result<QueryResult, String> {
    let fields = columns
        .iter()
        .map(|column| Field::new(&column.name, column.data_type.clone(), column.nullable))
        .collect::<Vec<_>>();
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)
        .map_err(|e| format!("build Iceberg maintenance result failed: {e}"))?;
    Ok(QueryResult {
        columns,
        chunks: vec![record_batch_to_chunk(batch)?],
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rewrite_position_delete_files_schema_matches_spark() {
        let outcome = MaintenanceActionOutcome::RewritePositionDeleteFiles {
            rewritten_delete_files_count: 2,
            added_delete_files_count: 1,
            rewritten_bytes_count: 128,
            added_bytes_count: 96,
        };
        let result = outcome.to_spark_query_result().unwrap();
        let names = result
            .columns
            .iter()
            .map(|c| c.name.as_str())
            .collect::<Vec<_>>();
        assert_eq!(
            names,
            vec![
                "rewritten_delete_files_count",
                "added_delete_files_count",
                "rewritten_bytes_count",
                "added_bytes_count"
            ]
        );
        assert_eq!(result.row_count(), 1);
    }

    #[test]
    fn remove_orphan_files_returns_one_row_per_location() {
        let outcome = MaintenanceActionOutcome::RemoveOrphanFiles {
            orphan_file_locations: vec![
                "s3://bucket/table/data/a.parquet".to_string(),
                "s3://bucket/table/metadata/old.avro".to_string(),
            ],
        };
        let result = outcome.to_spark_query_result().unwrap();
        assert_eq!(result.columns[0].name, "orphan_file_location");
        assert_eq!(result.row_count(), 2);
    }
}
