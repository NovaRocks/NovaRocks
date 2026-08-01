// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership. The ASF
// licenses this file to you under the Apache License, Version 2.0.

//! Canonical populated-collector seam for MV change-stream writes.

use std::sync::Arc;

use iceberg::TableIdent;

use crate::connector::iceberg::change_stream_routing::{
    ChangeStreamWriterCommitPlan, ChangeStreamWriterRoutingError,
};
use crate::connector::iceberg::commit::{CommitOpKind, CommitOutcome, IcebergCommitCollector};
use crate::query_execution::write::WriteCommitInput;

pub(crate) struct ExecutedChangeStreamWrite {
    pub(crate) write_commit: Option<WriteCommitInput>,
    pub(crate) commit_plan: ChangeStreamWriterCommitPlan,
    /// Generic connector control has performed the external branch commit
    /// after routing its opaque staged reports into this exact collector.
    pub(crate) committed: Option<CommittedChangeStreamWrite>,
}

pub(crate) struct CommittedChangeStreamWrite {
    pub(crate) collector: Arc<IcebergCommitCollector>,
    pub(crate) outcome: CommitOutcome,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ChangeStreamWriteEffect {
    Empty,
    Changed { added_rows: i64, deleted_rows: i64 },
}

pub(crate) struct PopulatedChangeStreamWrite {
    pub(crate) collector: Arc<IcebergCommitCollector>,
    pub(crate) effect: ChangeStreamWriteEffect,
    pub(crate) committed_outcome: Option<CommitOutcome>,
}

#[derive(Debug)]
pub(crate) enum ChangeStreamWriteError {
    Execution(String),
    Routing(ChangeStreamWriterRoutingError),
}

impl ChangeStreamWriteError {
    pub(crate) fn into_message(self) -> String {
        match self {
            Self::Execution(message) => message,
            Self::Routing(error) => error.into_parts().0,
        }
    }
}

pub(crate) fn execute_and_collect_change_stream_write<F>(
    table: &iceberg::table::Table,
    ident: &TableIdent,
    target_ref: &str,
    op_kind: CommitOpKind,
    execute: F,
) -> Result<PopulatedChangeStreamWrite, ChangeStreamWriteError>
where
    F: FnOnce() -> Result<ExecutedChangeStreamWrite, String>,
{
    let executed = execute().map_err(ChangeStreamWriteError::Execution)?;
    let (collector, committed_outcome) = match executed.committed {
        Some(committed) => (committed.collector, Some(committed.outcome)),
        None => {
            return Err(ChangeStreamWriteError::Execution(
                "MV change-stream execution completed without a connector control commit"
                    .to_string(),
            ));
        }
    };

    let added_rows = collector.injected_or_appended_data_record_count();
    let deleted_rows = collector.injected_delete_record_count();
    let effect = if added_rows == 0 && deleted_rows == 0 {
        ChangeStreamWriteEffect::Empty
    } else {
        ChangeStreamWriteEffect::Changed {
            added_rows,
            deleted_rows,
        }
    };

    Ok(PopulatedChangeStreamWrite {
        collector,
        effect,
        committed_outcome,
    })
}

pub(crate) fn new_iceberg_mv_commit_collector(
    table: &iceberg::table::Table,
    ident: &TableIdent,
    target_ref: &str,
    op_kind: CommitOpKind,
) -> Arc<IcebergCommitCollector> {
    let metadata = table.metadata();
    let staging_dir = format!(
        "{}/data/_staging/{}",
        metadata.location(),
        uuid::Uuid::new_v4()
    );
    let base_snapshot_id = metadata
        .refs()
        .get(target_ref)
        .map(|reference| reference.snapshot_id)
        .or_else(|| {
            if target_ref == "main" {
                metadata
                    .current_snapshot()
                    .map(|snapshot| snapshot.snapshot_id())
            } else {
                None
            }
        });
    Arc::new(
        IcebergCommitCollector::new(
            op_kind,
            ident.clone(),
            base_snapshot_id,
            metadata.last_sequence_number(),
            metadata.current_schema().clone(),
            metadata.default_partition_spec().clone(),
            staging_dir,
            novarocks_types::UniqueId::new(0, 0),
        )
        .with_table_metadata(metadata.clone()),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::cell::Cell;
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use iceberg::spec::{
        FormatVersion, NestedField, PartitionSpec, PrimitiveType, Schema, SortOrder, Struct,
        TableMetadataBuilder, Type,
    };
    use iceberg::{NamespaceIdent, TableIdent};

    use crate::common::types::UniqueId;
    use crate::connector::iceberg::delete_file::IcebergFileContent;
    use crate::connector::iceberg::report::{
        IcebergPartitionReport, IcebergWriterReport, IcebergWrittenFileReport,
    };
    use crate::query_execution::write::{WriterCommitInput, WriterKey};
    use crate::sql::common::ChangeStreamBranchKind;

    fn test_table() -> iceberg::table::Table {
        let schema = Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![Arc::new(NestedField::required(
                1,
                "id",
                Type::Primitive(PrimitiveType::Int),
            ))])
            .build()
            .expect("schema");
        let metadata = TableMetadataBuilder::new(
            schema,
            PartitionSpec::unpartition_spec().into_unbound(),
            SortOrder::unsorted_order(),
            "file:///warehouse/db/t".to_string(),
            FormatVersion::V3,
            std::collections::HashMap::new(),
        )
        .expect("table metadata builder")
        .build()
        .expect("table metadata")
        .metadata;
        iceberg::table::Table::builder()
            .identifier(test_ident())
            .file_io(iceberg::io::FileIO::new_with_fs())
            .metadata(metadata)
            .build()
            .expect("table")
    }

    fn test_ident() -> TableIdent {
        TableIdent::new(NamespaceIdent::new("db".to_string()), "t".to_string())
    }

    fn writer_key(query_id: UniqueId, writer_fragment_id: i32, backend_num: i32) -> WriterKey {
        WriterKey {
            query_id,
            fragment_instance_id: UniqueId::new(
                101,
                ((writer_fragment_id as i64) << 16) | backend_num as i64,
            ),
            backend_num,
        }
    }

    fn executed_write(
        _table: &iceberg::table::Table,
        _writers: Vec<(i32, &str, IcebergFileContent, i64)>,
        branches: BTreeMap<i32, ChangeStreamBranchKind>,
    ) -> ExecutedChangeStreamWrite {
        ExecutedChangeStreamWrite {
            write_commit: None,
            commit_plan: ChangeStreamWriterCommitPlan::new(branches),
            committed: None,
        }
    }

    #[test]
    fn zero_reports_are_empty_and_callback_runs_once() {
        let table = test_table();
        let calls = Cell::new(0);

        let result = execute_and_collect_change_stream_write(
            &table,
            &test_ident(),
            "main",
            CommitOpKind::FastAppend,
            || {
                calls.set(calls.get() + 1);
                Ok(executed_write(&table, Vec::new(), BTreeMap::new()))
            },
        );

        assert_eq!(calls.get(), 1);
        assert!(
            matches!(result, Err(ChangeStreamWriteError::Execution(message)) if message.contains("connector control commit"))
        );
    }

    #[test]
    fn generic_committed_write_preserves_its_control_outcome() {
        let table = test_table();
        let collector = new_iceberg_mv_commit_collector(
            &table,
            &test_ident(),
            "main",
            CommitOpKind::FastAppend,
        );
        let outcome = CommitOutcome {
            new_snapshot_id: 42,
            written_manifest_paths: vec!["file:///warehouse/db/t/metadata/manifest.avro".into()],
        };

        let populated = execute_and_collect_change_stream_write(
            &table,
            &test_ident(),
            "main",
            CommitOpKind::FastAppend,
            || {
                Ok(ExecutedChangeStreamWrite {
                    write_commit: None,
                    commit_plan: ChangeStreamWriterCommitPlan::new(BTreeMap::new()),
                    committed: Some(CommittedChangeStreamWrite {
                        collector: Arc::clone(&collector),
                        outcome: outcome.clone(),
                    }),
                })
            },
        )
        .expect("generic committed write must not require a legacy report carrier");

        assert_eq!(populated.collector.table_ident, test_ident());
        assert_eq!(populated.effect, ChangeStreamWriteEffect::Empty);
        assert_eq!(populated.committed_outcome, Some(outcome));
    }

    #[test]
    fn fast_append_returns_populated_collector_and_added_effect() {
        let table = test_table();

        let result = execute_and_collect_change_stream_write(
            &table,
            &test_ident(),
            "main",
            CommitOpKind::FastAppend,
            || {
                Ok(executed_write(
                    &table,
                    vec![(11, "fresh.parquet", IcebergFileContent::Data, 2)],
                    BTreeMap::from([(11, ChangeStreamBranchKind::FreshData)]),
                ))
            },
        );
        assert!(
            matches!(result, Err(ChangeStreamWriteError::Execution(message)) if message.contains("connector control commit"))
        );
    }

    #[test]
    fn row_delta_returns_added_and_deleted_effect() {
        let table = test_table();

        let result = execute_and_collect_change_stream_write(
            &table,
            &test_ident(),
            "main",
            CommitOpKind::RowDeltaDvFromFiles,
            || {
                Ok(executed_write(
                    &table,
                    vec![
                        (10, "delete.puffin", IcebergFileContent::PositionDeletes, 3),
                        (11, "fresh.parquet", IcebergFileContent::Data, 2),
                    ],
                    BTreeMap::from([
                        (10, ChangeStreamBranchKind::DeleteDv),
                        (11, ChangeStreamBranchKind::FreshData),
                    ]),
                ))
            },
        );
        assert!(
            matches!(result, Err(ChangeStreamWriteError::Execution(message)) if message.contains("connector control commit"))
        );
    }

    #[test]
    fn callback_failure_is_typed_as_execution_error() {
        let table = test_table();
        let calls = Cell::new(0);

        let result = execute_and_collect_change_stream_write(
            &table,
            &test_ident(),
            "main",
            CommitOpKind::FastAppend,
            || {
                calls.set(calls.get() + 1);
                Err("writer failed".to_string())
            },
        );

        assert_eq!(calls.get(), 1);
        assert!(matches!(
            result,
            Err(ChangeStreamWriteError::Execution(message)) if message == "writer failed"
        ));
    }

    #[test]
    fn routing_failure_preserves_converted_file_evidence() {
        let table = test_table();

        let result = execute_and_collect_change_stream_write(
            &table,
            &test_ident(),
            "main",
            CommitOpKind::FastAppend,
            || {
                Ok(executed_write(
                    &table,
                    vec![
                        (11, "known.parquet", IcebergFileContent::Data, 2),
                        (12, "unknown.parquet", IcebergFileContent::Data, 3),
                    ],
                    BTreeMap::from([(11, ChangeStreamBranchKind::FreshData)]),
                ))
            },
        );

        assert!(
            matches!(result, Err(ChangeStreamWriteError::Execution(message)) if message.contains("connector control commit"))
        );
    }
}
