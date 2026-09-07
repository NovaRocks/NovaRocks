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

//! This module contains transaction api.
//!
//! The transaction API enables changes to be made to an existing table.
//!
//! Note that this may also have side effects, such as producing new manifest
//! files.
//!
//! Below is a basic example using the "fast-append" action:
//!
//! ```ignore
//! use iceberg::transaction::{ApplyTransactionAction, Transaction};
//! use iceberg::Catalog;
//!
//! // Create a transaction.
//! let tx = Transaction::new(my_table);
//!
//! // Create a `FastAppendAction` which will not rewrite or append
//! // to existing metadata. This will create a new manifest.
//! let action = tx.fast_append().add_data_files(my_data_files);
//!
//! // Apply the fast-append action to the given transaction, returning
//! // the newly updated `Transaction`.
//! let tx = action.apply(tx).await.unwrap();
//!
//!
//! // End the transaction by committing to an `iceberg::Catalog`
//! // implementation. This will cause a table update to occur.
//! let table = tx
//!     .commit(&some_catalog_impl)
//!     .await
//!     .unwrap();
//! ```

/// The `ApplyTransactionAction` trait provides an `apply` method
/// that allows users to apply a transaction action to a `Transaction`.
mod action;

pub use action::*;
mod append;
mod snapshot;
mod sort_order;
mod update_location;
mod update_properties;
mod update_statistics;
mod upgrade_format_version;

use std::sync::Arc;

use crate::error::Result;
use crate::spec::Snapshot;
use crate::table::Table;
use crate::transaction::append::FastAppendAction;
use crate::transaction::sort_order::ReplaceSortOrderAction;
use crate::transaction::update_location::UpdateLocationAction;
use crate::transaction::update_properties::UpdatePropertiesAction;
use crate::transaction::update_statistics::UpdateStatisticsAction;
use crate::transaction::upgrade_format_version::UpgradeFormatVersionAction;
use crate::{Catalog, TableCommit, TableRequirement, TableUpdate};

/// Table transaction.
pub struct Transaction {
    base_table: Table,
    table: Table,
    updates: Vec<TableUpdate>,
    external_requirements: Vec<TableRequirement>,
}

impl Transaction {
    /// Creates a new transaction.
    pub fn new(table: &Table) -> Self {
        Self {
            base_table: table.clone(),
            table: table.clone(),
            updates: vec![],
            external_requirements: vec![],
        }
    }

    /// Returns the transaction-local table after every successfully staged action.
    pub fn staged_table(&self) -> &Table {
        &self.table
    }

    /// Returns the current transaction-local snapshot, if one has been staged.
    pub fn staged_snapshot(&self) -> Option<&Snapshot> {
        self.table.metadata().current_snapshot().map(AsRef::as_ref)
    }

    fn update_table_metadata(table: Table, updates: &[TableUpdate]) -> Result<Table> {
        let mut metadata_builder = table.metadata().clone().into_builder(None);
        for update in updates {
            metadata_builder = update.clone().apply(metadata_builder)?;
        }

        Ok(table.with_metadata(Arc::new(metadata_builder.build()?.metadata)))
    }

    /// Applies an [`ActionCommit`] to the local table and accumulates its catalog commit data.
    fn stage_commit(&mut self, mut action_commit: ActionCommit) -> Result<()> {
        let updates = action_commit.take_updates();
        let requirements = action_commit.take_requirements();

        for requirement in &requirements {
            requirement.check(Some(self.table.metadata()))?;
            let external_requirement = self.requirement_against_base(requirement);
            if !self.external_requirements.contains(&external_requirement) {
                self.external_requirements.push(external_requirement);
            }
        }

        self.table = Self::update_table_metadata(self.table.clone(), &updates)?;

        self.updates.extend(updates);

        Ok(())
    }

    /// Eagerly stage an already assembled action commit.
    ///
    /// Connector-owned composite mutations use this to place provider updates
    /// ahead of a snapshot action while retaining transaction-local
    /// requirement evaluation and one exported catalog payload.
    pub fn stage_action_commit(mut self, action_commit: ActionCommit) -> Result<Self> {
        self.stage_commit(action_commit)?;
        Ok(self)
    }

    /// Converts a stage-local precondition into the equivalent external OCC
    /// requirement against the table state at transaction start.
    fn requirement_against_base(&self, requirement: &TableRequirement) -> TableRequirement {
        let metadata = self.base_table.metadata();
        match requirement {
            TableRequirement::NotExist => TableRequirement::NotExist,
            TableRequirement::UuidMatch { .. } => TableRequirement::UuidMatch {
                uuid: metadata.uuid(),
            },
            TableRequirement::RefSnapshotIdMatch { r#ref, .. } => {
                TableRequirement::RefSnapshotIdMatch {
                    r#ref: r#ref.clone(),
                    snapshot_id: metadata
                        .snapshot_for_ref(r#ref)
                        .map(|snapshot| snapshot.snapshot_id()),
                }
            }
            TableRequirement::LastAssignedFieldIdMatch { .. } => {
                TableRequirement::LastAssignedFieldIdMatch {
                    last_assigned_field_id: metadata.last_column_id(),
                }
            }
            TableRequirement::CurrentSchemaIdMatch { .. } => {
                TableRequirement::CurrentSchemaIdMatch {
                    current_schema_id: metadata.current_schema_id(),
                }
            }
            TableRequirement::LastAssignedPartitionIdMatch { .. } => {
                TableRequirement::LastAssignedPartitionIdMatch {
                    last_assigned_partition_id: metadata.last_partition_id(),
                }
            }
            TableRequirement::DefaultSpecIdMatch { .. } => TableRequirement::DefaultSpecIdMatch {
                default_spec_id: metadata.default_partition_spec_id(),
            },
            TableRequirement::DefaultSortOrderIdMatch { .. } => {
                TableRequirement::DefaultSortOrderIdMatch {
                    default_sort_order_id: metadata.default_sort_order_id(),
                }
            }
        }
    }

    /// Eagerly evaluates a custom transaction action against the current
    /// transaction-local table and accumulates its commit payload.
    ///
    /// This is public for connector-owned actions that need to compose with
    /// built-in actions before one external catalog dispatch.
    pub async fn stage_action(mut self, action: Arc<dyn TransactionAction>) -> Result<Self> {
        let action_commit = action.commit(&self.table).await?;
        self.stage_commit(action_commit)?;
        Ok(self)
    }

    /// Sets table to a new version.
    pub fn upgrade_table_version(&self) -> UpgradeFormatVersionAction {
        UpgradeFormatVersionAction::new()
    }

    /// Update table's property.
    pub fn update_table_properties(&self) -> UpdatePropertiesAction {
        UpdatePropertiesAction::new()
    }

    /// Creates a fast append action.
    pub fn fast_append(&self) -> FastAppendAction {
        FastAppendAction::new()
    }

    /// Creates replace sort order action.
    pub fn replace_sort_order(&self) -> ReplaceSortOrderAction {
        ReplaceSortOrderAction::new()
    }

    /// Set the location of table
    pub fn update_location(&self) -> UpdateLocationAction {
        UpdateLocationAction::new()
    }

    /// Update the statistics of table
    pub fn update_statistics(&self) -> UpdateStatisticsAction {
        UpdateStatisticsAction::new()
    }

    /// Consumes this transaction and exports the complete catalog commit.
    ///
    /// Exporting is side-effect free: it performs no catalog I/O, refresh, or retry.
    pub fn into_table_commit(self) -> TableCommit {
        TableCommit::builder()
            .ident(self.table.identifier().to_owned())
            .updates(self.updates)
            .requirements(self.external_requirements)
            .build()
    }

    /// Commit transaction with exactly one catalog dispatch.
    ///
    /// Actions have already been evaluated against the transaction-local table while
    /// they were applied. This method never refreshes, replays, or retries them.
    pub async fn commit(self, catalog: &dyn Catalog) -> Result<Table> {
        if self.updates.is_empty() && self.external_requirements.is_empty() {
            // nothing to commit
            return Ok(self.table);
        }
        catalog.update_table(self.into_table_commit()).await
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::fs::File;
    use std::io::BufReader;
    use std::sync::Arc;

    use crate::catalog::MockCatalog;
    use crate::io::FileIO;
    use crate::spec::{BlobMetadata, StatisticsFile, TableMetadata};
    use crate::table::Table;
    use crate::transaction::{ApplyTransactionAction, Transaction};
    use crate::{Catalog, Error, ErrorKind, TableCreation, TableIdent, TableUpdate};

    pub fn make_v1_table() -> Table {
        let file = File::open(format!(
            "{}/testdata/table_metadata/{}",
            env!("CARGO_MANIFEST_DIR"),
            "TableMetadataV1Valid.json"
        ))
        .unwrap();
        let reader = BufReader::new(file);
        let resp = serde_json::from_reader::<_, TableMetadata>(reader).unwrap();

        Table::builder()
            .metadata(resp)
            .metadata_location("s3://bucket/test/location/metadata/v1.json".to_string())
            .identifier(TableIdent::from_strs(["ns1", "test1"]).unwrap())
            .file_io(FileIO::new_with_memory())
            .build()
            .unwrap()
    }

    pub fn make_v2_table() -> Table {
        let file = File::open(format!(
            "{}/testdata/table_metadata/{}",
            env!("CARGO_MANIFEST_DIR"),
            "TableMetadataV2Valid.json"
        ))
        .unwrap();
        let reader = BufReader::new(file);
        let resp = serde_json::from_reader::<_, TableMetadata>(reader).unwrap();

        Table::builder()
            .metadata(resp)
            .metadata_location("s3://bucket/test/location/metadata/v1.json".to_string())
            .identifier(TableIdent::from_strs(["ns1", "test1"]).unwrap())
            .file_io(FileIO::new_with_memory())
            .build()
            .unwrap()
    }

    pub fn make_v2_minimal_table() -> Table {
        let file = File::open(format!(
            "{}/testdata/table_metadata/{}",
            env!("CARGO_MANIFEST_DIR"),
            "TableMetadataV2ValidMinimal.json"
        ))
        .unwrap();
        let reader = BufReader::new(file);
        let resp = serde_json::from_reader::<_, TableMetadata>(reader).unwrap();

        Table::builder()
            .metadata(resp)
            .metadata_location("s3://bucket/test/location/metadata/v1.json".to_string())
            .identifier(TableIdent::from_strs(["ns1", "test1"]).unwrap())
            .file_io(FileIO::new_with_memory())
            .build()
            .unwrap()
    }

    pub(crate) async fn make_v3_minimal_table_in_catalog(catalog: &impl Catalog) -> Table {
        let table_ident =
            TableIdent::from_strs([format!("ns1-{}", uuid::Uuid::new_v4()), "test1".to_string()])
                .unwrap();

        catalog
            .create_namespace(table_ident.namespace(), HashMap::new())
            .await
            .unwrap();

        let file = File::open(format!(
            "{}/testdata/table_metadata/{}",
            env!("CARGO_MANIFEST_DIR"),
            "TableMetadataV3ValidMinimal.json"
        ))
        .unwrap();
        let reader = BufReader::new(file);
        let base_metadata = serde_json::from_reader::<_, TableMetadata>(reader).unwrap();

        let table_creation = TableCreation::builder()
            .schema((**base_metadata.current_schema()).clone())
            .partition_spec((**base_metadata.default_partition_spec()).clone())
            .sort_order((**base_metadata.default_sort_order()).clone())
            .name(table_ident.name().to_string())
            .format_version(crate::spec::FormatVersion::V3)
            .build();

        catalog
            .create_table(table_ident.namespace(), table_creation)
            .await
            .unwrap()
    }

    /// Helper function to create a test table with retry properties
    pub(super) fn setup_test_table(num_retries: &str) -> Table {
        let table = make_v2_table();

        // Set retry properties
        let mut props = HashMap::new();
        props.insert("commit.retry.min-wait-ms".to_string(), "10".to_string());
        props.insert("commit.retry.max-wait-ms".to_string(), "100".to_string());
        props.insert(
            "commit.retry.total-timeout-ms".to_string(),
            "1000".to_string(),
        );
        props.insert(
            "commit.retry.num-retries".to_string(),
            num_retries.to_string(),
        );

        // Update table properties
        let metadata = table
            .metadata()
            .clone()
            .into_builder(None)
            .set_properties(props)
            .unwrap()
            .build()
            .unwrap()
            .metadata;

        table.with_metadata(Arc::new(metadata))
    }

    /// Helper function to create a transaction with a simple update action.
    async fn create_test_transaction(table: &Table) -> Transaction {
        let tx = Transaction::new(table);
        tx.update_table_properties()
            .set("test.key".to_string(), "test.value".to_string())
            .apply(tx)
            .await
            .unwrap()
    }

    /// Helper function to set up a mock catalog with one update result.
    fn setup_mock_catalog_with_error(error_kind: ErrorKind, retryable: bool) -> MockCatalog {
        let mut mock_catalog = MockCatalog::new();
        mock_catalog.expect_load_table().times(0);
        mock_catalog
            .expect_update_table()
            .times(1)
            .returning_st(move |_| {
                Box::pin(async move {
                    Err(Error::new(error_kind, "Commit failed").with_retryable(retryable))
                })
            });
        mock_catalog
    }

    #[tokio::test]
    async fn test_retryable_commit_error_is_dispatched_once_without_refresh() {
        let table = setup_test_table("3");
        let tx = create_test_transaction(&table).await;
        let mock_catalog = setup_mock_catalog_with_error(ErrorKind::CatalogCommitConflicts, true);
        let result = tx.commit(&mock_catalog).await;
        let error = result.expect_err("retryable failure must be returned to the caller");
        assert_eq!(error.kind(), ErrorKind::CatalogCommitConflicts);
        assert!(error.retryable());
    }

    #[tokio::test]
    async fn test_commit_non_retryable_error() {
        let table = setup_test_table("3");
        let tx = create_test_transaction(&table).await;
        let mock_catalog = setup_mock_catalog_with_error(ErrorKind::Unexpected, false);
        let result = tx.commit(&mock_catalog).await;
        assert!(result.is_err(), "Transaction should fail immediately");
        if let Err(err) = result {
            assert_eq!(err.kind(), ErrorKind::Unexpected);
            assert_eq!(err.message(), "Commit failed");
            assert!(!err.retryable(), "Error should not be retryable");
        }
    }

    #[tokio::test]
    async fn export_is_side_effect_free_and_contains_staged_changes() {
        let table = make_v2_table();
        let tx = create_test_transaction(&table).await;
        let mut commit = tx.into_table_commit();
        assert_eq!(commit.identifier(), table.identifier());
        assert!(!commit.take_updates().is_empty());
    }

    #[tokio::test]
    async fn later_action_reads_snapshot_created_by_eager_append() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);
        let tx = tx
            .fast_append()
            .set_snapshot_properties(HashMap::from([("test".to_string(), "true".to_string())]))
            .apply(tx)
            .await
            .unwrap();
        let snapshot = tx.staged_snapshot().expect("append must be staged eagerly");
        let snapshot_id = snapshot.snapshot_id();
        let sequence_number = snapshot.sequence_number();
        let statistics = StatisticsFile {
            snapshot_id,
            statistics_path: "s3://bucket/test/location/metadata/stats.puffin".to_string(),
            file_size_in_bytes: 10,
            file_footer_size_in_bytes: 5,
            key_metadata: None,
            blob_metadata: vec![BlobMetadata {
                r#type: "ndv".to_string(),
                snapshot_id,
                sequence_number,
                fields: vec![1],
                properties: HashMap::new(),
            }],
        };
        let tx = tx
            .update_statistics()
            .set_statistics(statistics.clone())
            .apply(tx)
            .await
            .unwrap();
        assert_eq!(
            tx.staged_table()
                .metadata()
                .statistics_for_snapshot(snapshot_id),
            Some(&statistics)
        );

        let mut commit = tx.into_table_commit();
        let updates = commit.take_updates();
        let requirements = commit.take_requirements();
        assert!(updates.iter().any(
            |update| matches!(update, TableUpdate::AddSnapshot { snapshot } if snapshot.snapshot_id() == snapshot_id)
        ));
        assert!(updates.iter().any(
            |update| matches!(update, TableUpdate::SetStatistics { statistics: value } if value == &statistics)
        ));
        assert!(
            !requirements.is_empty(),
            "append requirements must be accumulated for publication"
        );
    }
}

#[cfg(test)]
mod test_row_lineage {
    use crate::TableRequirement;
    use crate::memory::tests::new_memory_catalog;
    use crate::spec::{
        DataContentType, DataFile, DataFileBuilder, DataFileFormat, Literal, MAIN_BRANCH, Struct,
    };
    use crate::transaction::tests::make_v3_minimal_table_in_catalog;
    use crate::transaction::{ApplyTransactionAction, Transaction};

    // Helper function to create a data file with specified number of rows.
    fn file_with_rows(record_count: u64) -> DataFile {
        DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(format!("test/{record_count}.parquet"))
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(record_count)
            .partition(Struct::from_iter([Some(Literal::long(0))]))
            .partition_spec_id(0)
            .build()
            .unwrap()
    }

    #[tokio::test]
    async fn test_fast_append_with_row_lineage() {
        let catalog = new_memory_catalog().await;

        let table = make_v3_minimal_table_in_catalog(&catalog).await;

        // Check initial state - next_row_id should be 0
        assert_eq!(table.metadata().next_row_id(), 0);

        // First fast append with 30 rows
        let tx = Transaction::new(&table);
        let data_file_30 = file_with_rows(30);
        let action = tx.fast_append().add_data_files(vec![data_file_30]);
        let tx = action.apply(tx).await.unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        // Check snapshot and table state after first append
        let snapshot = table.metadata().current_snapshot().unwrap();
        assert_eq!(snapshot.first_row_id(), Some(0));
        assert_eq!(table.metadata().next_row_id(), 30);

        // Check written manifest for first_row_id
        let manifest_list = table
            .metadata()
            .current_snapshot()
            .unwrap()
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap();

        assert_eq!(manifest_list.entries().len(), 1);
        let manifest_file = &manifest_list.entries()[0];
        assert_eq!(manifest_file.first_row_id, Some(0));

        // Second fast append with 17 and 11 rows
        let tx = Transaction::new(&table);
        let data_file_17 = file_with_rows(17);
        let data_file_11 = file_with_rows(11);
        let action = tx
            .fast_append()
            .add_data_files(vec![data_file_17, data_file_11]);
        let tx = action.apply(tx).await.unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        // Check snapshot and table state after second append
        let snapshot = table.metadata().current_snapshot().unwrap();
        assert_eq!(snapshot.first_row_id(), Some(30));
        assert_eq!(table.metadata().next_row_id(), 30 + 17 + 11);

        // Check written manifest for first_row_id
        let manifest_list = table
            .metadata()
            .current_snapshot()
            .unwrap()
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap();
        assert_eq!(manifest_list.entries().len(), 2);
        let manifest_file = &manifest_list.entries()[1];
        assert_eq!(manifest_file.first_row_id, Some(30));
    }

    #[tokio::test]
    async fn two_appends_share_the_original_external_ref_requirement() {
        let catalog = new_memory_catalog().await;
        let table = make_v3_minimal_table_in_catalog(&catalog).await;
        let base_snapshot_id = table.metadata().current_snapshot_id();

        let tx = Transaction::new(&table);
        let tx = tx
            .fast_append()
            .add_data_files([file_with_rows(30)])
            .apply(tx)
            .await
            .unwrap();
        let first_snapshot_id = tx.staged_snapshot().unwrap().snapshot_id();

        let tx = tx
            .fast_append()
            .add_data_files([file_with_rows(17)])
            .apply(tx)
            .await
            .unwrap();
        let second_snapshot_id = tx.staged_snapshot().unwrap().snapshot_id();
        assert_ne!(first_snapshot_id, second_snapshot_id);

        let ref_requirements: Vec<_> = tx
            .external_requirements
            .iter()
            .filter(|requirement| {
                matches!(
                    requirement,
                    TableRequirement::RefSnapshotIdMatch { r#ref, snapshot_id }
                        if r#ref == MAIN_BRANCH && *snapshot_id == base_snapshot_id
                )
            })
            .collect();
        assert_eq!(ref_requirements.len(), 1);

        let committed = tx.commit(&catalog).await.unwrap();
        let current = committed.metadata().current_snapshot().unwrap();
        assert_eq!(current.snapshot_id(), second_snapshot_id);
        assert_eq!(current.parent_snapshot_id(), Some(first_snapshot_id));
        assert_eq!(committed.metadata().next_row_id(), 47);
    }
}
