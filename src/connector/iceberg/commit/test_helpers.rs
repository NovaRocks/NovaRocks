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

//! Shared test fixtures for the commit-action unit tests.
//!
//! Used by `truncate.rs::tests` and `overwrite_partitions.rs::tests`.
//! Kept in its own module so the pattern can be reused for any future
//! commit-action whose unit tests need a `MemoryCatalog`-backed table fixture.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
use iceberg::spec::{
    DataContentType, DataFileFormat, FormatVersion, Literal, NestedField, PrimitiveLiteral,
    PrimitiveType, Schema, Struct, Transform, Type, UnboundPartitionSpec,
};
use iceberg::table::Table;
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent};
use uuid::Uuid;

use super::action::{CommitCtx, IcebergCommitAction};
use super::collector::IcebergCommitCollector;
use super::fast_append::FastAppendCommit;
use super::overwrite_partitions::OverwritePartitionsCommit;
use super::types::{CommitOpKind, CommitOutcome, WrittenFile};

/// A minimal in-memory iceberg fixture: a `MemoryCatalog`, the freshly
/// `create_table`-ed `Table`, and the matching `TableIdent`. The catalog is
/// `Arc`-wrapped so it can be cloned cheaply for use as both the
/// `commit()` argument and the `reload after commit` handle.
#[derive(Clone)]
pub(crate) struct IcebergTestFixture {
    pub catalog: Arc<dyn Catalog>,
    pub table: Table,
    pub table_ident: TableIdent,
}

/// Build a `MemoryCatalog`-backed v3 iceberg table with a single `id long`
/// column, no partitioning, and no current snapshot. Suitable as the base for
/// the empty-table TRUNCATE test; for tests that need actual data files,
/// drive a `FastAppendCommit` through `run_iceberg_commit` against the
/// returned catalog before the action under test.
pub(crate) async fn empty_v3_iceberg_table() -> IcebergTestFixture {
    let warehouse = format!("memory://test-warehouse-{}", Uuid::new_v4());
    let catalog: Arc<dyn Catalog> = Arc::new(
        MemoryCatalogBuilder::default()
            .load(
                "memory",
                HashMap::from([(MEMORY_CATALOG_WAREHOUSE.to_string(), warehouse)]),
            )
            .await
            .expect("MemoryCatalog::load"),
    );

    let namespace = NamespaceIdent::new("db".to_string());
    catalog
        .create_namespace(&namespace, HashMap::new())
        .await
        .expect("create_namespace");

    let schema = Schema::builder()
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
        ])
        .build()
        .expect("build schema");

    let table_ident = TableIdent::new(namespace.clone(), "t".to_string());
    // Force format-version = V3 so we exercise the V3 manifest writer path.
    let table = catalog
        .create_table(
            &namespace,
            TableCreation::builder()
                .name("t".to_string())
                .schema(schema)
                .format_version(FormatVersion::V3)
                .build(),
        )
        .await
        .expect("create_table");

    IcebergTestFixture {
        catalog,
        table,
        table_ident,
    }
}

/// Drive a single commit-action through the same pieces `run_iceberg_commit`
/// does, but with a minimal collector — no pipeline, no Operator-backed
/// abort cleanup, no sidecar threading. Suitable for direct unit testing of
/// commit-action logic.
pub(crate) async fn run_commit_with<A>(
    action: A,
    op_kind: CommitOpKind,
    fixture: IcebergTestFixture,
    target_ref: &str,
) -> Result<CommitOutcome, String>
where
    A: IcebergCommitAction + 'static,
{
    let metadata = fixture.table.metadata();
    let staging_dir = format!("{}/staging", metadata.location());
    let collector = Arc::new(IcebergCommitCollector::new(
        op_kind,
        fixture.table_ident.clone(),
        metadata.current_snapshot().map(|s| s.snapshot_id()),
        metadata.last_sequence_number(),
        metadata.current_schema().clone(),
        metadata.default_partition_spec().clone(),
        staging_dir,
        crate::common::types::UniqueId { hi: 0, lo: 0 },
    ));

    let file_io = fixture.table.file_io().clone();
    let abort_handle = collector.abort_log.clone();
    let snapshot_properties = BTreeMap::new();
    let ctx = CommitCtx {
        collector: &collector,
        table: &fixture.table,
        catalog: fixture.catalog.as_ref(),
        file_io: &file_io,
        commit_uuid: Uuid::new_v4(),
        abort_handle,
        target_ref,
        snapshot_properties: &snapshot_properties,
    };

    action.commit(ctx).await
}

/// Build a `MemoryCatalog`-backed v3 iceberg table seeded with `n` synthetic
/// data files via a single `FastAppendCommit`. Each file claims
/// `record_count = 10` and a unique synthetic path under
/// `<table-location>/data/`. The actual Parquet bytes are NOT written —
/// `FastAppendCommit` only encodes the `WrittenFile` metadata into a manifest
/// entry, and `TruncateCommit` only ever reads manifest entries (not the
/// underlying data files), so the synthetic paths are sufficient.
pub(crate) async fn v3_table_with_n_data_files(n: usize) -> IcebergTestFixture {
    let mut fixture = empty_v3_iceberg_table().await;

    let table_location = fixture.table.metadata().location().to_string();
    let mut written = Vec::with_capacity(n);
    for idx in 0..n {
        written.push(WrittenFile {
            path: format!("{table_location}/data/file-{idx}.parquet"),
            format: DataFileFormat::Parquet,
            content: DataContentType::Data,
            partition_values: Struct::empty(),
            partition_spec_id: 0,
            record_count: 10,
            file_size_in_bytes: 1024,
            split_offsets: vec![],
            column_sizes: HashMap::new(),
            value_counts: HashMap::new(),
            null_value_counts: HashMap::new(),
            key_metadata: None,
            referenced_data_file: None,
            equality_ids: None,
            first_row_id: None,
        });
    }

    // Seed the collector with synthetic written-files so the FastAppend path
    // picks them up via `take_written_files` rather than draining
    // `runtime::sink_commit`.
    let metadata = fixture.table.metadata();
    let collector = Arc::new(IcebergCommitCollector::new(
        CommitOpKind::FastAppend,
        fixture.table_ident.clone(),
        metadata.current_snapshot().map(|s| s.snapshot_id()),
        metadata.last_sequence_number(),
        metadata.current_schema().clone(),
        metadata.default_partition_spec().clone(),
        format!("{table_location}/staging"),
        crate::common::types::UniqueId { hi: 0, lo: 0 },
    ));
    for wf in written {
        collector.inject_written_file(wf);
    }
    let file_io = fixture.table.file_io().clone();
    let abort_handle = collector.abort_log.clone();
    let snapshot_properties = BTreeMap::new();
    let ctx = CommitCtx {
        collector: &collector,
        table: &fixture.table,
        catalog: fixture.catalog.as_ref(),
        file_io: &file_io,
        commit_uuid: Uuid::new_v4(),
        abort_handle,
        target_ref: "main",
        snapshot_properties: &snapshot_properties,
    };
    FastAppendCommit
        .commit(ctx)
        .await
        .expect("FastAppendCommit succeeds in fixture setup");

    // Refresh the table handle to pick up the new snapshot.
    fixture.table = fixture
        .catalog
        .load_table(&fixture.table_ident)
        .await
        .expect("reload table after fixture FastAppend");
    fixture
}

/// Build a `MemoryCatalog`-backed v3 iceberg table seeded with multiple
/// sequential `FastAppendCommit` batches. Each element of `batches` gives the
/// number of synthetic data files for that commit. File paths are assigned a
/// globally unique index across all batches to avoid path collisions.
///
/// Returns the fixture after all commits; the table has `batches.len()`
/// snapshots in its history. Useful for testing snapshot-lifecycle helpers that
/// need a multi-snapshot history.
pub(crate) async fn v3_table_with_multi_batch_appends(batches: &[usize]) -> IcebergTestFixture {
    let mut fixture = empty_v3_iceberg_table().await;
    let mut global_idx: usize = 0;

    for &n in batches {
        let table_location = fixture.table.metadata().location().to_string();
        let metadata = fixture.table.metadata();
        let collector = Arc::new(IcebergCommitCollector::new(
            CommitOpKind::FastAppend,
            fixture.table_ident.clone(),
            metadata.current_snapshot().map(|s| s.snapshot_id()),
            metadata.last_sequence_number(),
            metadata.current_schema().clone(),
            metadata.default_partition_spec().clone(),
            format!("{table_location}/staging"),
            crate::common::types::UniqueId { hi: 0, lo: 0 },
        ));
        for _ in 0..n {
            collector.inject_written_file(WrittenFile {
                path: format!("{table_location}/data/file-{global_idx}.parquet"),
                format: DataFileFormat::Parquet,
                content: DataContentType::Data,
                partition_values: Struct::empty(),
                partition_spec_id: 0,
                record_count: 10,
                file_size_in_bytes: 1024,
                split_offsets: vec![],
                column_sizes: HashMap::new(),
                value_counts: HashMap::new(),
                null_value_counts: HashMap::new(),
                key_metadata: None,
                referenced_data_file: None,
                equality_ids: None,
                first_row_id: None,
            });
            global_idx += 1;
        }
        let file_io = fixture.table.file_io().clone();
        let abort_handle = collector.abort_log.clone();
        let snapshot_properties = BTreeMap::new();
        let ctx = CommitCtx {
            collector: &collector,
            table: &fixture.table,
            catalog: fixture.catalog.as_ref(),
            file_io: &file_io,
            commit_uuid: Uuid::new_v4(),
            abort_handle,
            target_ref: "main",
            snapshot_properties: &snapshot_properties,
        };
        FastAppendCommit
            .commit(ctx)
            .await
            .expect("FastAppendCommit succeeds in v3_table_with_multi_batch_appends");
        fixture.table = fixture
            .catalog
            .load_table(&fixture.table_ident)
            .await
            .expect("reload table after batch append");
    }
    fixture
}

/// Build a `MemoryCatalog`-backed v3 iceberg table with an identity partition
/// on the `region` column, pre-seeded with:
/// - 2 data files in partition `region=us`
/// - 1 data file in partition `region=eu`
///
/// Used by `overwrite_partitions.rs::tests` to validate that OVERWRITE
/// PARTITIONS replaces only the touched partition while preserving others.
pub(crate) async fn v3_partitioned_table_with_data() -> IcebergTestFixture {
    let warehouse = format!("memory://test-warehouse-{}", Uuid::new_v4());
    let catalog: Arc<dyn Catalog> = Arc::new(
        MemoryCatalogBuilder::default()
            .load(
                "memory",
                HashMap::from([(MEMORY_CATALOG_WAREHOUSE.to_string(), warehouse)]),
            )
            .await
            .expect("MemoryCatalog::load"),
    );

    let namespace = NamespaceIdent::new("db".to_string());
    catalog
        .create_namespace(&namespace, HashMap::new())
        .await
        .expect("create_namespace");

    // Schema: id (long), region (string).
    let schema = Schema::builder()
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
            NestedField::optional(2, "region", Type::Primitive(PrimitiveType::String)).into(),
        ])
        .build()
        .expect("build schema");

    // Partition spec: identity(region) — field_id=2 as per the schema above.
    let partition_spec = UnboundPartitionSpec::builder()
        .add_partition_field(2, "region", Transform::Identity)
        .expect("add partition field")
        .build();

    let table_ident = TableIdent::new(namespace.clone(), "t".to_string());
    let table = catalog
        .create_table(
            &namespace,
            TableCreation::builder()
                .name("t".to_string())
                .schema(schema)
                .partition_spec(partition_spec)
                .format_version(FormatVersion::V3)
                .build(),
        )
        .await
        .expect("create_table");

    let mut fixture = IcebergTestFixture {
        catalog,
        table,
        table_ident,
    };

    let table_location = fixture.table.metadata().location().to_string();
    let partition_spec_id = fixture.table.metadata().default_partition_spec().spec_id();

    // Helper: build a partition Struct for an identity-partitioned region.
    let region_partition = |val: &str| {
        Struct::from_iter([Some(Literal::Primitive(PrimitiveLiteral::String(
            val.to_string(),
        )))])
    };

    // Seed with FastAppendCommit: 2 files in us, 1 file in eu.
    let written = vec![
        WrittenFile {
            path: format!("{table_location}/data/us-0.parquet"),
            format: DataFileFormat::Parquet,
            content: DataContentType::Data,
            partition_values: region_partition("us"),
            partition_spec_id,
            record_count: 10,
            file_size_in_bytes: 1024,
            split_offsets: vec![],
            column_sizes: HashMap::new(),
            value_counts: HashMap::new(),
            null_value_counts: HashMap::new(),
            key_metadata: None,
            referenced_data_file: None,
            equality_ids: None,
            first_row_id: None,
        },
        WrittenFile {
            path: format!("{table_location}/data/us-1.parquet"),
            format: DataFileFormat::Parquet,
            content: DataContentType::Data,
            partition_values: region_partition("us"),
            partition_spec_id,
            record_count: 10,
            file_size_in_bytes: 1024,
            split_offsets: vec![],
            column_sizes: HashMap::new(),
            value_counts: HashMap::new(),
            null_value_counts: HashMap::new(),
            key_metadata: None,
            referenced_data_file: None,
            equality_ids: None,
            first_row_id: None,
        },
        WrittenFile {
            path: format!("{table_location}/data/eu-0.parquet"),
            format: DataFileFormat::Parquet,
            content: DataContentType::Data,
            partition_values: region_partition("eu"),
            partition_spec_id,
            record_count: 10,
            file_size_in_bytes: 1024,
            split_offsets: vec![],
            column_sizes: HashMap::new(),
            value_counts: HashMap::new(),
            null_value_counts: HashMap::new(),
            key_metadata: None,
            referenced_data_file: None,
            equality_ids: None,
            first_row_id: None,
        },
    ];

    let metadata = fixture.table.metadata();
    let collector = Arc::new(IcebergCommitCollector::new(
        CommitOpKind::FastAppend,
        fixture.table_ident.clone(),
        metadata.current_snapshot().map(|s| s.snapshot_id()),
        metadata.last_sequence_number(),
        metadata.current_schema().clone(),
        metadata.default_partition_spec().clone(),
        format!("{table_location}/staging"),
        crate::common::types::UniqueId { hi: 0, lo: 0 },
    ));
    for wf in written {
        collector.inject_written_file(wf);
    }
    let file_io = fixture.table.file_io().clone();
    let abort_handle = collector.abort_log.clone();
    let snapshot_properties = BTreeMap::new();
    let ctx = CommitCtx {
        collector: &collector,
        table: &fixture.table,
        catalog: fixture.catalog.as_ref(),
        file_io: &file_io,
        commit_uuid: Uuid::new_v4(),
        abort_handle,
        target_ref: "main",
        snapshot_properties: &snapshot_properties,
    };
    FastAppendCommit
        .commit(ctx)
        .await
        .expect("FastAppendCommit succeeds in v3_partitioned_table_with_data fixture");

    fixture.table = fixture
        .catalog
        .load_table(&fixture.table_ident)
        .await
        .expect("reload table after fixture FastAppend");
    fixture
}

/// Run `OverwritePartitionsCommit` against the given fixture with the
/// supplied written files pre-loaded into the collector.
pub(crate) async fn run_overwrite_partitions_commit(
    fixture: IcebergTestFixture,
    written: Vec<WrittenFile>,
) -> Result<CommitOutcome, String> {
    let metadata = fixture.table.metadata();
    let staging_dir = format!("{}/staging", metadata.location());
    let collector = Arc::new(IcebergCommitCollector::new(
        CommitOpKind::OverwritePartitions,
        fixture.table_ident.clone(),
        metadata.current_snapshot().map(|s| s.snapshot_id()),
        metadata.last_sequence_number(),
        metadata.current_schema().clone(),
        metadata.default_partition_spec().clone(),
        staging_dir,
        crate::common::types::UniqueId { hi: 0, lo: 0 },
    ));
    for wf in written {
        collector.inject_written_file(wf);
    }
    let file_io = fixture.table.file_io().clone();
    let abort_handle = collector.abort_log.clone();
    let snapshot_properties = BTreeMap::new();
    let ctx = CommitCtx {
        collector: &collector,
        table: &fixture.table,
        catalog: fixture.catalog.as_ref(),
        file_io: &file_io,
        commit_uuid: Uuid::new_v4(),
        abort_handle,
        target_ref: "main",
        snapshot_properties: &snapshot_properties,
    };
    OverwritePartitionsCommit.commit(ctx).await
}
