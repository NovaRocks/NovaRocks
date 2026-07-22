// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership. The ASF
// licenses this file to you under the Apache License, Version 2.0.

//! Canonical payload and write adapters for MV repartition.

use std::collections::BTreeMap;

use iceberg::TableIdent;

use crate::connector::iceberg::commit::CommitOpKind;
use crate::exec::chunk::Chunk;
use crate::mv::aggregate_state::aggregate_sql_calls::AggregateSqlCalls;
use crate::mv::refresh::aggregate_first_refresh::{
    AggregateStateRead, prepare_aggregate_first_refresh_chunks,
};
use crate::mv::refresh::capabilities::{RefreshCapabilities, RefreshIdentity};
use crate::mv::refresh::change_stream_write::{
    ChangeStreamWriteError, ExecutedChangeStreamWrite, PopulatedChangeStreamWrite,
    execute_and_collect_change_stream_write,
};
use crate::mv::refresh::join_first_refresh::{
    JoinFirstRefreshLogicalInput, JoinFirstRefreshLogicalPlan,
    build_join_first_refresh_logical_plan,
};
use crate::mv::refresh::pin::RefreshSnapshotPin;
use crate::mv::refresh::projection_first_refresh::{
    prepare_projection_first_refresh_chunks, prepare_union_projection_first_refresh_chunks,
};
use crate::mv::refresh::snapshot::BaseSnapshotPolicy;
use crate::mv::rewrite::context::IcebergMvRewriteContext;
use novarocks_catalog::identifier::TableIdentity;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum RepartitionShape {
    ProjectionFilterSingleBase,
    AggregateSingleBase,
    JoinProjectionFilter,
    JoinAggregate,
    FanInAggregate,
    UnionProjectionFilter,
}

impl RepartitionShape {
    pub(crate) fn label(&self) -> &'static str {
        match self {
            Self::ProjectionFilterSingleBase => "projection/filter single-base",
            Self::AggregateSingleBase => "aggregate single-base",
            Self::JoinProjectionFilter => "join projection/filter",
            Self::JoinAggregate => "join aggregate",
            Self::FanInAggregate => "fan-in aggregate",
            Self::UnionProjectionFilter => "UNION ALL projection/filter",
        }
    }
}

pub(crate) fn select_repartition_shape(
    capabilities: &RefreshCapabilities,
) -> Result<RepartitionShape, String> {
    match (
        &capabilities.snapshot_policy,
        capabilities.has_agg_state,
        &capabilities.identity,
    ) {
        (BaseSnapshotPolicy::SingleBase, false, RefreshIdentity::BaseRowId) => {
            Ok(RepartitionShape::ProjectionFilterSingleBase)
        }
        (BaseSnapshotPolicy::SingleBase, true, RefreshIdentity::GroupRowId) => {
            Ok(RepartitionShape::AggregateSingleBase)
        }
        (BaseSnapshotPolicy::JoinPairPartialInitialSkip, false, RefreshIdentity::JoinRowKey) => {
            Ok(RepartitionShape::JoinProjectionFilter)
        }
        (BaseSnapshotPolicy::JoinPairPartialInitialSkip, true, RefreshIdentity::GroupRowId) => {
            Ok(RepartitionShape::JoinAggregate)
        }
        (BaseSnapshotPolicy::AllBasesRequired, true, RefreshIdentity::GroupRowId) => {
            Ok(RepartitionShape::FanInAggregate)
        }
        (BaseSnapshotPolicy::AllBasesRequired, false, RefreshIdentity::BranchScoped(inner))
            if matches!(inner.as_ref(), RefreshIdentity::BaseRowId) =>
        {
            Ok(RepartitionShape::UnionProjectionFilter)
        }
        _ => Err(format!(
            "UnsupportedRepartitionShape: ALTER MATERIALIZED VIEW ... REPARTITION does not support identity={:?}, snapshot_policy={:?}, aggregate_state={}; supported shapes are projection/filter single-base, aggregate single-base, join projection/filter, join aggregate, fan-in aggregate, and UNION ALL projection/filter",
            capabilities.identity, capabilities.snapshot_policy, capabilities.has_agg_state
        )),
    }
}

pub(crate) struct RepartitionChunkPayload {
    chunks: Vec<Chunk>,
    base_snapshots: BTreeMap<String, i64>,
    base_table_uuids: BTreeMap<String, String>,
}

impl RepartitionChunkPayload {
    pub(crate) fn from_chunks(chunks: Vec<Chunk>, pin: &RefreshSnapshotPin) -> Self {
        Self {
            chunks,
            base_snapshots: pin.to_snapshot_map(),
            base_table_uuids: pin.to_table_uuid_map(),
        }
    }

    #[cfg(test)]
    pub(crate) fn chunk_count(&self) -> usize {
        self.chunks.len()
    }

    pub(crate) fn total_rows(&self) -> i64 {
        self.chunks
            .iter()
            .map(|chunk| chunk.batch.num_rows() as i64)
            .sum()
    }

    pub(crate) fn base_snapshots(&self) -> &BTreeMap<String, i64> {
        &self.base_snapshots
    }

    pub(crate) fn base_table_uuids(&self) -> &BTreeMap<String, String> {
        &self.base_table_uuids
    }

    pub(crate) fn into_parts(
        self,
    ) -> (Vec<Chunk>, BTreeMap<String, i64>, BTreeMap<String, String>) {
        (self.chunks, self.base_snapshots, self.base_table_uuids)
    }
}

pub(crate) struct PreparedRepartitionChunkWrite {
    pub(crate) data_files: Vec<iceberg::spec::DataFile>,
    pub(crate) total_rows: i64,
    pub(crate) base_snapshots: BTreeMap<String, i64>,
    pub(crate) base_table_uuids: BTreeMap<String, String>,
}

pub(crate) async fn write_repartition_chunk_payload(
    table: &iceberg::table::Table,
    payload: RepartitionChunkPayload,
) -> Result<PreparedRepartitionChunkWrite, String> {
    let total_rows = payload.total_rows();
    let (chunks, base_snapshots, base_table_uuids) = payload.into_parts();
    let data_files = if total_rows == 0 {
        Vec::new()
    } else {
        crate::connector::iceberg::data_writer::write_record_batches_as_data_files(
            table,
            chunks.into_iter().map(|chunk| chunk.batch),
        )
        .await?
    };
    Ok(PreparedRepartitionChunkWrite {
        data_files,
        total_rows,
        base_snapshots,
        base_table_uuids,
    })
}

fn join_repartition_commit_kind() -> CommitOpKind {
    CommitOpKind::Overwrite
}

fn execute_prepared_join_repartition_write<T, F>(
    table: &iceberg::table::Table,
    ident: &TableIdent,
    target_ref: &str,
    logical: T,
    execute: F,
) -> Result<PopulatedChangeStreamWrite, ChangeStreamWriteError>
where
    F: FnOnce(T) -> Result<ExecutedChangeStreamWrite, String>,
{
    execute_and_collect_change_stream_write(
        table,
        ident,
        target_ref,
        join_repartition_commit_kind(),
        || execute(logical),
    )
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn execute_join_repartition_write<F>(
    table: &iceberg::table::Table,
    ident: &TableIdent,
    target_ref: &str,
    rewrite: &IcebergMvRewriteContext,
    left_ref: &TableIdentity,
    right_ref: &TableIdentity,
    input: JoinFirstRefreshLogicalInput,
    execute: F,
) -> Result<PopulatedChangeStreamWrite, ChangeStreamWriteError>
where
    F: FnOnce(JoinFirstRefreshLogicalPlan) -> Result<ExecutedChangeStreamWrite, String>,
{
    let logical = build_join_first_refresh_logical_plan(rewrite, left_ref, right_ref, input)
        .map_err(ChangeStreamWriteError::Execution)?;
    execute_prepared_join_repartition_write(table, ident, target_ref, logical, execute)
}

pub(crate) fn prepare_projection_repartition_payload<F>(
    select_sql: &str,
    pin: &RefreshSnapshotPin,
    current_catalog: Option<&str>,
    current_database: &str,
    read: &mut F,
) -> Result<RepartitionChunkPayload, String>
where
    F: FnMut(&str) -> Result<Vec<Chunk>, String>,
{
    let chunks = prepare_projection_first_refresh_chunks(
        select_sql,
        pin,
        current_catalog,
        current_database,
        read,
    )?;
    Ok(RepartitionChunkPayload::from_chunks(chunks, pin))
}

pub(crate) fn prepare_union_projection_repartition_payload<F>(
    select_sql: &str,
    branch_count: usize,
    pin: &RefreshSnapshotPin,
    current_catalog: Option<&str>,
    current_database: &str,
    read: &mut F,
) -> Result<RepartitionChunkPayload, String>
where
    F: FnMut(&str) -> Result<Vec<Chunk>, String>,
{
    let chunks = prepare_union_projection_first_refresh_chunks(
        select_sql,
        branch_count,
        pin,
        current_catalog,
        current_database,
        read,
    )?;
    Ok(RepartitionChunkPayload::from_chunks(chunks, pin))
}

pub(crate) fn prepare_aggregate_repartition_payload<F>(
    select_sql: &str,
    calls: &AggregateSqlCalls,
    pin: &RefreshSnapshotPin,
    current_catalog: Option<&str>,
    current_database: &str,
    read: &mut F,
) -> Result<RepartitionChunkPayload, String>
where
    F: FnMut(&str, &AggregateSqlCalls, sqlparser::ast::Query) -> Result<AggregateStateRead, String>,
{
    let chunks = prepare_aggregate_first_refresh_chunks(
        select_sql,
        calls,
        pin,
        current_catalog,
        current_database,
        read,
    )?;
    Ok(RepartitionChunkPayload::from_chunks(chunks, pin))
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::cell::Cell;
    use std::sync::Arc;

    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use iceberg::spec::{
        FormatVersion, NestedField, PartitionSpec, PrimitiveType, Schema as IcebergSchema,
        SortOrder, TableMetadataBuilder, Type,
    };
    use iceberg::{NamespaceIdent, TableIdent};

    use crate::mv::refresh::apply_key::ApplyKeyValueType;
    use crate::mv::refresh::capabilities::{
        PartitionPruningPolicy, RefreshCapabilities, RefreshIdentity,
    };
    use crate::mv::refresh::snapshot::BaseSnapshotPolicy;
    use crate::runtime::query_result::record_batch_to_chunk;

    fn capabilities(
        snapshot_policy: BaseSnapshotPolicy,
        has_agg_state: bool,
        identity: RefreshIdentity,
    ) -> RefreshCapabilities {
        RefreshCapabilities {
            snapshot_policy,
            has_agg_state,
            identity,
            apply_key_column: "apply_key".to_string(),
            apply_key_value_type: ApplyKeyValueType::Utf8,
            partition_pruning: PartitionPruningPolicy::BestEffort,
        }
    }

    fn chunk(values: Vec<i64>) -> crate::exec::chunk::Chunk {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
            vec![Arc::new(Int64Array::from(values))],
        )
        .expect("record batch");
        record_batch_to_chunk(batch).expect("chunk")
    }

    fn test_table() -> iceberg::table::Table {
        let schema = IcebergSchema::builder()
            .with_schema_id(1)
            .with_fields(vec![Arc::new(NestedField::required(
                1,
                "id",
                Type::Primitive(PrimitiveType::Long),
            ))])
            .build()
            .expect("schema");
        let metadata = TableMetadataBuilder::new(
            schema,
            PartitionSpec::unpartition_spec().into_unbound(),
            SortOrder::unsorted_order(),
            "file:///warehouse/db/mv".to_string(),
            FormatVersion::V3,
            std::collections::HashMap::new(),
        )
        .expect("table metadata builder")
        .build()
        .expect("table metadata")
        .metadata;
        iceberg::table::Table::builder()
            .identifier(TableIdent::new(
                NamespaceIdent::new("db".to_string()),
                "mv".to_string(),
            ))
            .file_io(iceberg::io::FileIO::new_with_fs())
            .metadata(metadata)
            .build()
            .expect("table")
    }

    fn test_ident() -> TableIdent {
        TableIdent::new(NamespaceIdent::new("db".to_string()), "mv".to_string())
    }

    #[test]
    fn selects_supported_shapes() {
        let cases = [
            (
                capabilities(
                    BaseSnapshotPolicy::SingleBase,
                    false,
                    RefreshIdentity::BaseRowId,
                ),
                RepartitionShape::ProjectionFilterSingleBase,
            ),
            (
                capabilities(
                    BaseSnapshotPolicy::SingleBase,
                    true,
                    RefreshIdentity::GroupRowId,
                ),
                RepartitionShape::AggregateSingleBase,
            ),
            (
                capabilities(
                    BaseSnapshotPolicy::JoinPairPartialInitialSkip,
                    false,
                    RefreshIdentity::JoinRowKey,
                ),
                RepartitionShape::JoinProjectionFilter,
            ),
            (
                capabilities(
                    BaseSnapshotPolicy::JoinPairPartialInitialSkip,
                    true,
                    RefreshIdentity::GroupRowId,
                ),
                RepartitionShape::JoinAggregate,
            ),
            (
                capabilities(
                    BaseSnapshotPolicy::AllBasesRequired,
                    true,
                    RefreshIdentity::GroupRowId,
                ),
                RepartitionShape::FanInAggregate,
            ),
            (
                capabilities(
                    BaseSnapshotPolicy::AllBasesRequired,
                    false,
                    RefreshIdentity::BranchScoped(Box::new(RefreshIdentity::BaseRowId)),
                ),
                RepartitionShape::UnionProjectionFilter,
            ),
        ];

        for (capabilities, expected) in cases {
            assert_eq!(
                select_repartition_shape(&capabilities).expect("supported shape"),
                expected
            );
        }
    }

    #[test]
    fn rejects_unsupported_shape_with_capability_evidence() {
        let capabilities = capabilities(
            BaseSnapshotPolicy::AllBasesRequired,
            false,
            RefreshIdentity::JoinRowKey,
        );

        let error = select_repartition_shape(&capabilities).expect_err("unsupported shape");

        assert!(error.contains("UnsupportedRepartitionShape"));
        assert!(error.contains("JoinRowKey"));
        assert!(error.contains("AllBasesRequired"));
        assert!(error.contains("aggregate_state=false"));
    }

    #[test]
    fn chunk_payload_preserves_pin_lineage_and_row_count() {
        let pin = crate::mv::refresh::pin::RefreshSnapshotPin::from_entries_for_tests(&[
            ("ice.db.left", 101, "left-uuid"),
            ("ice.db.right", 202, "right-uuid"),
        ]);

        let payload =
            RepartitionChunkPayload::from_chunks(vec![chunk(vec![1, 2]), chunk(vec![3])], &pin);

        assert_eq!(payload.chunk_count(), 2);
        assert_eq!(payload.total_rows(), 3);
        assert_eq!(payload.base_snapshots().get("ice.db.left"), Some(&101));
        assert_eq!(
            payload
                .base_table_uuids()
                .get("ice.db.right")
                .map(String::as_str),
            Some("right-uuid")
        );
    }

    #[test]
    fn projection_preparation_builds_typed_payload() {
        let pin = crate::mv::refresh::pin::RefreshSnapshotPin::from_entries_for_tests(&[(
            "ice.db.base",
            101,
            "base-uuid",
        )]);
        let calls = Cell::new(0);
        let mut read = |physical_sql: &str| {
            calls.set(calls.get() + 1);
            assert!(physical_sql.contains("VERSION AS OF 101"));
            Ok(vec![chunk(vec![1, 2])])
        };

        let payload = prepare_projection_repartition_payload(
            "SELECT id FROM ice.db.base",
            &pin,
            Some("ice"),
            "db",
            &mut read,
        )
        .expect("projection payload");

        assert_eq!(calls.get(), 1);
        assert_eq!(payload.total_rows(), 2);
        assert_eq!(payload.base_snapshots().get("ice.db.base"), Some(&101));
    }

    #[test]
    fn empty_chunk_write_preserves_payload_metadata_without_files() {
        let pin = crate::mv::refresh::pin::RefreshSnapshotPin::from_entries_for_tests(&[(
            "ice.db.base",
            101,
            "base-uuid",
        )]);
        let payload = RepartitionChunkPayload::from_chunks(Vec::new(), &pin);

        let prepared = crate::runtime::global_async_runtime::data_block_on(
            write_repartition_chunk_payload(&test_table(), payload),
        )
        .expect("runtime")
        .expect("prepared write");

        assert!(prepared.data_files.is_empty());
        assert_eq!(prepared.total_rows, 0);
        assert_eq!(prepared.base_snapshots.get("ice.db.base"), Some(&101));
        assert_eq!(
            prepared
                .base_table_uuids
                .get("ice.db.base")
                .map(String::as_str),
            Some("base-uuid")
        );
    }

    #[test]
    fn join_write_adapter_uses_overwrite_and_invokes_callback_once() {
        let calls = Cell::new(0);

        let error = match execute_prepared_join_repartition_write(
            &test_table(),
            &test_ident(),
            "staging",
            (),
            |()| {
                calls.set(calls.get() + 1);
                Err("sentinel repartition execution failure".to_string())
            },
        ) {
            Ok(_) => panic!("callback failure must cross the repartition write seam"),
            Err(error) => error,
        };

        assert_eq!(
            join_repartition_commit_kind(),
            crate::connector::iceberg::commit::CommitOpKind::Overwrite
        );
        assert_eq!(calls.get(), 1);
        assert_eq!(
            error.into_message(),
            "sentinel repartition execution failure"
        );
    }
}
