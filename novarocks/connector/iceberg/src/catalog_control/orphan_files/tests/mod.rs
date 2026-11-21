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

use std::{
    fs::FileTimes,
    path::{Path, PathBuf},
    task::{Context, Poll},
    time::{Duration, Instant, UNIX_EPOCH},
};

use novarocks_failure_contract::connector_reason::{
    ICEBERG_FILESYSTEM_ERROR, ICEBERG_INVALID_METADATA,
};
use novarocks_spi::connector::{
    ConnectorInstanceId, ConnectorMaintenanceDuration, ConnectorOriginalResultScopeCheck,
    ConnectorProviderId, ConnectorStopOwner, OriginalResultCheckError, ProviderMetadataCpuInput,
};

use super::*;
use crate::catalog::SnapshotLoading;
use crate::catalog_config::parse_catalog_configuration;
use crate::catalog_control::IcebergCatalogControlState;
use crate::iceberg::spec::{
    DataContentType, DataFile, DataFileBuilder, DataFileFormat, FormatVersion, ManifestFile,
    ManifestListWriter, ManifestWriterBuilder, NestedField, Operation, PartitionStatisticsFile,
    PrimitiveType, Schema, Snapshot, SnapshotReference, SnapshotRetention, StatisticsFile, Struct,
    Summary, TableMetadata, TableMetadataBuilder, Type,
};
use crate::iceberg::{NamespaceIdent, TableCreation, TableIdent};
use crate::metadata_context::IcebergMetadataContext;
use crate::resources::IcebergMetadataResources;

const CUTOFF: i64 = 1_750_000_000_000;
const EARLY: i64 = CUTOFF - 1;

struct Scope(Instant);

impl ConnectorOriginalResultScopeCheck for Scope {
    fn original_deadline(&self) -> Instant {
        self.0
    }

    fn check_active(&self) -> Result<(), OriginalResultCheckError> {
        Ok(())
    }
}

struct Cpu;

impl ProviderMetadataCpuPort for Cpu {
    fn submit_metadata_in(
        &self,
        input: &mut Option<ProviderMetadataCpuInput>,
    ) -> Result<(), ConnectorError> {
        input
            .take()
            .expect("original metadata decode work")
            .run_original_phase();
        Ok(())
    }

    fn poll_finished(&self, _: &mut Context<'_>) -> Poll<Result<(), ConnectorError>> {
        Poll::Ready(Ok(()))
    }

    fn poll_closed(&self, _: &mut Context<'_>) -> Poll<Result<(), ConnectorError>> {
        Poll::Ready(Ok(()))
    }

    fn local_failure(&self) -> Option<Arc<dyn std::error::Error + Send + Sync>> {
        None
    }
}

struct Fixture {
    _warehouse: tempfile::TempDir,
    runtime: IcebergMetadataContext,
    binding: IcebergReadBinding,
    descriptor: ConnectorInstanceDescriptor,
    cpu: Arc<dyn ProviderMetadataCpuPort>,
    scope: ConnectorOriginalResultScope,
    stop: ConnectorStopOwner,
    context: ConnectorRequestContext,
    admission: ListingAdmission,
    deletions: Deletions,
    table_root: PathBuf,
}

impl Fixture {
    fn new() -> Self {
        let warehouse = tempfile::tempdir().unwrap();
        let table_root = warehouse.path().join("n/t");
        std::fs::create_dir_all(table_root.join("metadata")).unwrap();
        let table_root = std::fs::canonicalize(table_root).unwrap();
        let handle = tokio::runtime::Handle::current();
        let config = parse_catalog_configuration(
            "ice",
            &[(
                "iceberg.catalog.warehouse".into(),
                warehouse.path().display().to_string(),
            )],
        )
        .unwrap();
        let template = IcebergReadBinding::new(
            None,
            novarocks_fs::FsAccessResolver::new(),
            Arc::new(novarocks_fs::TokioFileIoRuntime::new(handle.clone())),
            novarocks_fs::TokioRefreshExecutor::shared(handle.clone()),
            Arc::new(novarocks_fs::TokioFileIoSpawner::new(handle.clone())),
        );
        let runtime = IcebergMetadataContext::try_new(
            IcebergCatalogControlState::new(config),
            IcebergMetadataResources::new(template, handle),
        )
        .unwrap();
        let deadline = Instant::now() + Duration::from_secs(60);
        let stop = ConnectorStopOwner::new();
        let context = ConnectorRequestContext::try_new(deadline, stop.view(), 1024, 4096).unwrap();
        let binding = runtime
            .resources()
            .planning_binding()
            .for_request(context.clone());
        Self {
            _warehouse: warehouse,
            runtime,
            binding,
            descriptor: ConnectorInstanceDescriptor {
                provider_id: ConnectorProviderId::parse("iceberg").unwrap(),
                instance_id: ConnectorInstanceId::parse("ice").unwrap(),
            },
            cpu: Arc::new(Cpu),
            scope: ConnectorOriginalResultScope::from_original(Arc::new(Scope(deadline))),
            stop,
            context,
            admission: ListingAdmission::default(),
            deletions: Deletions::default(),
            table_root,
        }
    }

    fn scan(&self) -> OrphanScan<'_> {
        OrphanScan {
            context: &self.context,
            scope: &self.scope,
            binding: &self.binding,
            admission: &self.admission,
            cpu: &self.cpu,
            descriptor: &self.descriptor,
            incarnation: ProviderBindingEpoch::from_bytes([1; 16]),
            table_name: "n.t",
            retention: ConnectorMaintenanceRetention {
                requested: ConnectorMaintenanceDuration::parse("7d").unwrap(),
                session_minimum: None,
            },
            configuration: self.runtime.control_state().configuration().maintenance,
            deletions: &self.deletions,
        }
    }

    fn path(&self, name: &str) -> String {
        self.table_root.join(name).display().to_string()
    }

    fn io(&self) -> FileIO {
        crate::fs_io::build_file_io_for_location(&self.path(""), self.binding.clone())
    }

    async fn storage(&self) -> ScanStorage<'_> {
        let access = resolve_request_access_for_location(&self.path(""), &self.binding)
            .await
            .unwrap();
        ScanStorage::new(access, "n.t", &mut Budget::new(&self.context))
            .await
            .unwrap()
    }

    fn table(&self, metadata: TableMetadata, current: &str) -> Table {
        Table::builder()
            .identifier(TableIdent::new(NamespaceIdent::new("n".into()), "t".into()))
            .metadata(Arc::new(metadata))
            .metadata_location(self.path(current))
            .file_io(self.io())
            .runtime(
                self.runtime
                    .resources()
                    .catalog_runtime()
                    .iceberg_runtime()
                    .clone(),
            )
            .build()
            .unwrap()
    }

    fn base(&self, gc_enabled: bool) -> TableMetadata {
        let creation = TableCreation::builder()
            .name("t".into())
            .location(self.path(""))
            .schema(
                Schema::builder()
                    .with_fields(vec![Arc::new(NestedField::required(
                        1,
                        "id",
                        Type::Primitive(PrimitiveType::Long),
                    ))])
                    .build()
                    .unwrap(),
            )
            .format_version(FormatVersion::V2)
            .properties([("gc.enabled".into(), gc_enabled.to_string())])
            .build();
        TableMetadataBuilder::from_table_creation(creation)
            .unwrap()
            .build()
            .unwrap()
            .metadata
    }

    fn write(&self, name: &str, bytes: &[u8], modified: i64) {
        let path = self.table_root.join(name);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, bytes).unwrap();
        set_mtime(&path, modified);
    }

    fn exists(&self, name: &str) -> bool {
        self.table_root.join(name).exists()
    }

    async fn manifest_list(&self, name: &str, id: i64, entries: Vec<ManifestFile>) {
        let mut writer = ManifestListWriter::v2(
            self.io()
                .new_output(self.path(name))
                .unwrap()
                .writer()
                .await
                .unwrap(),
            id,
            (id > 1).then_some(id - 1),
            id,
        );
        writer.add_manifests(entries.into_iter()).unwrap();
        writer.close().await.unwrap();
        set_mtime(&self.table_root.join(name), EARLY);
    }
}

fn set_mtime(path: &Path, time_ms: i64) {
    std::fs::File::options()
        .write(true)
        .open(path)
        .unwrap()
        .set_times(
            FileTimes::new()
                .set_modified(UNIX_EPOCH + Duration::from_millis(time_ms.try_into().unwrap())),
        )
        .unwrap();
}

fn snapshot(id: i64, path: String) -> Snapshot {
    Snapshot::builder()
        .with_snapshot_id(id)
        .with_parent_snapshot_id((id > 1).then_some(id - 1))
        .with_sequence_number(id)
        .with_timestamp_ms(
            std::time::SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis()
                .try_into()
                .unwrap(),
        )
        .with_manifest_list(path)
        .with_schema_id(0)
        .with_summary(Summary {
            operation: Operation::Append,
            additional_properties: Default::default(),
        })
        .build()
}

fn reference(id: i64) -> SnapshotReference {
    SnapshotReference::new(id, SnapshotRetention::branch(None, None, None))
}

fn data_file(path: String, content: DataContentType) -> DataFile {
    DataFileBuilder::default()
        .content(content)
        .file_path(path)
        .file_format(DataFileFormat::Parquet)
        .partition(Struct::empty())
        .partition_spec_id(0)
        .record_count(1)
        .file_size_in_bytes(1)
        .build()
        .unwrap()
}

#[tokio::test]
async fn rest_style_metadata_protects_all_snapshot_facts_and_uses_strict_mtime() {
    let fixture = Fixture::new();
    // REST tables use immutable UUID-style metadata names and log previous
    // pointers. The same returned metadata, manifest lists and files drive the scan.
    let base = fixture.base(false);
    let history = "metadata/8d79-history.avro";
    let deletes = "metadata/587c-deletes.avro";
    let older_list = "metadata/snap-1.avro";
    let current_list = "metadata/snap-2.avro";
    let previous = "metadata/00001-8ed3.metadata.json";
    let current = "metadata/00002-b0f4.metadata.json";
    let mut history_writer = ManifestWriterBuilder::new(
        fixture.io().new_output(fixture.path(history)).unwrap(),
        Some(1),
        base.current_schema().clone(),
        base.default_partition_spec().as_ref().clone(),
    )
    .build_v2_data();
    history_writer
        .add_file(
            data_file(
                fixture.path("data/historical.parquet"),
                DataContentType::Data,
            ),
            1,
        )
        .unwrap();
    history_writer
        .add_delete_file(
            data_file(
                fixture.path("data/deleted-entry.parquet"),
                DataContentType::Data,
            ),
            1,
            Some(1),
        )
        .unwrap();
    let old_manifest = history_writer.write_manifest_file().await.unwrap();
    let mut delete_writer = ManifestWriterBuilder::new(
        fixture.io().new_output(fixture.path(deletes)).unwrap(),
        Some(2),
        base.current_schema().clone(),
        base.default_partition_spec().as_ref().clone(),
    )
    .build_v2_deletes();
    delete_writer
        .add_file(
            data_file(
                fixture.path("data/equality-delete.parquet"),
                DataContentType::EqualityDeletes,
            ),
            2,
        )
        .unwrap();
    let delete_manifest = delete_writer.write_manifest_file().await.unwrap();
    fixture
        .manifest_list(older_list, 1, vec![old_manifest])
        .await;
    fixture
        .manifest_list(current_list, 2, vec![delete_manifest])
        .await;
    for name in [history, deletes] {
        set_mtime(&fixture.table_root.join(name), EARLY);
    }

    let metadata = base
        .into_builder(Some(fixture.path(previous)))
        .add_snapshot(snapshot(1, fixture.path(older_list)))
        .unwrap()
        .add_snapshot(snapshot(2, fixture.path(current_list)))
        .unwrap()
        .set_ref("main", reference(2))
        .unwrap()
        .set_statistics(StatisticsFile {
            snapshot_id: 1,
            statistics_path: fixture.path("metadata/historical.puffin"),
            file_size_in_bytes: 1,
            file_footer_size_in_bytes: 1,
            key_metadata: None,
            blob_metadata: Vec::new(),
        })
        .set_partition_statistics(PartitionStatisticsFile {
            snapshot_id: 2,
            statistics_path: fixture.path("metadata/partition-stats.parquet"),
            file_size_in_bytes: 1,
        })
        .build()
        .unwrap()
        .metadata;
    let protected = [
        previous,
        current,
        "metadata/historical.puffin",
        "metadata/partition-stats.parquet",
        "data/historical.parquet",
        "extra/historical.parquet",
        "data/equality-delete.parquet",
        "metadata/version-hint.text",
    ];
    for name in protected {
        fixture.write(name, b"x", EARLY);
    }
    let orphans = [
        ("metadata/00000-b15a.metadata.json", b"abc".as_slice()),
        ("data/deleted-entry.parquet", b"xx".as_slice()),
        ("data/early.parquet", b"".as_slice()),
        ("data/v42.metadata.json", b"x".as_slice()),
    ];
    for (name, bytes) in orphans {
        fixture.write(name, bytes, EARLY);
    }
    fixture.write("data/equal.parquet", b"x", CUTOFF);
    fixture.write("data/late.parquet", b"x", CUTOFF + 1);

    let table = fixture.table(metadata, current);
    let metrics = fixture
        .scan()
        .execute(&table, &object_id(&table), CUTOFF)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        metrics,
        ConnectorOrphanCleanupMetrics {
            processed_manifests: 2,
            active_files: 10,
            scanned_files: 18,
            deleted_files: 4,
            deleted_bytes: 6,
        }
    );
    assert!(
        !table
            .metadata()
            .properties()
            .get("gc.enabled")
            .unwrap()
            .parse::<bool>()
            .unwrap()
    );
    for name in protected.into_iter().chain([
        history,
        deletes,
        older_list,
        current_list,
        "data/equal.parquet",
        "data/late.parquet",
    ]) {
        assert!(fixture.exists(name), "{name} must survive");
    }
    for (name, _) in orphans {
        assert!(!fixture.exists(name), "{name} must be deleted");
    }
}

#[tokio::test]
async fn hadoop_stale_hint_can_still_discover_latest_after_cleanup() {
    let fixture = Fixture::new();
    let list = "metadata/snap-1.avro";
    fixture.manifest_list(list, 1, Vec::new()).await;
    // The current document has no log entry for v1. Its discovery hint still
    // points to v1, which must remain probing-visible even when older than cutoff.
    let old = fixture.base(true);
    let current = old
        .clone()
        .into_builder(None)
        .add_snapshot(snapshot(1, fixture.path(list)))
        .unwrap()
        .set_ref("main", reference(1))
        .unwrap()
        .build()
        .unwrap()
        .metadata;
    fixture.write(
        "metadata/v1.metadata.json",
        &serde_json::to_vec(&old).unwrap(),
        EARLY,
    );
    fixture.write(
        "metadata/v2.metadata.json",
        &serde_json::to_vec(&current).unwrap(),
        EARLY,
    );
    fixture.write("metadata/version-hint.text", b"1\n", EARLY);
    fixture.write("metadata/not-in-log.metadata.json", b"x", EARLY);
    let load = || {
        fixture
            .runtime
            .observe_table_original_for_request_with_snapshots_async(
                "n",
                "t",
                &fixture.context,
                SnapshotLoading::All,
            )
    };
    let before = load().await.unwrap();
    assert!(before.table.metadata().metadata_log().is_empty());
    let metrics = fixture
        .scan()
        .execute(&before.table, &object_id(&before.table), CUTOFF)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(metrics.deleted_files, 1);
    assert!(fixture.exists("metadata/v1.metadata.json"));
    assert!(!fixture.exists("metadata/not-in-log.metadata.json"));
    let after = load().await.unwrap();
    assert_eq!(after.table.metadata().current_snapshot_id(), Some(1));
    assert!(
        after
            .table
            .metadata_location()
            .unwrap()
            .ends_with("v2.metadata.json")
    );
}

#[tokio::test]
async fn empty_table_never_dispatches() {
    let fixture = Fixture::new();
    fixture.write("candidate", b"x", EARLY);
    let table = fixture.table(fixture.base(false), "metadata/current.metadata.json");
    assert_eq!(
        fixture
            .scan()
            .execute(&table, &object_id(&table), CUTOFF)
            .await
            .unwrap(),
        None
    );
    assert!(fixture.exists("candidate"));
    assert_eq!(fixture.deletions.publication_fact(), (None, false));
}

#[tokio::test]
async fn missing_manifest_fails_before_any_delete() {
    let fixture = Fixture::new();
    let base = fixture.base(true);
    let manifest = "metadata/missing.avro";
    let list = "metadata/snap-1.avro";
    let mut writer = ManifestWriterBuilder::new(
        fixture.io().new_output(fixture.path(manifest)).unwrap(),
        Some(1),
        base.current_schema().clone(),
        base.default_partition_spec().as_ref().clone(),
    )
    .build_v2_data();
    writer
        .add_file(
            data_file(fixture.path("data/live.parquet"), DataContentType::Data),
            1,
        )
        .unwrap();
    fixture
        .manifest_list(list, 1, vec![writer.write_manifest_file().await.unwrap()])
        .await;
    std::fs::remove_file(fixture.table_root.join(manifest)).unwrap();
    fixture.write("candidate", b"x", EARLY);
    let metadata = base
        .into_builder(None)
        .add_snapshot(snapshot(1, fixture.path(list)))
        .unwrap()
        .set_ref("main", reference(1))
        .unwrap()
        .build()
        .unwrap()
        .metadata;
    let table = fixture.table(metadata, "metadata/current.metadata.json");
    let error = fixture
        .scan()
        .execute(&table, &object_id(&table), CUTOFF)
        .await
        .unwrap_err();
    assert_eq!(error.failure_fact().reason(), ICEBERG_INVALID_METADATA);
    assert_eq!(
        error.public_failure_detail(),
        Some(format!("Manifest file does not exist: {}", fixture.path(manifest)).as_str())
    );
    assert!(fixture.exists("candidate"));
    assert_eq!(fixture.deletions.publication_fact(), (None, false));
}

#[tokio::test]
async fn second_batch_failure_preserves_first_confirmation() {
    let fixture = Fixture::new();
    let storage = fixture.storage().await;
    let scan = fixture.scan();
    scan.delete(&storage, DELETE_BATCH_SIZE as u64, 1007, async { Ok(()) })
        .await
        .unwrap();
    let error = scan
        .delete(&storage, 3, 17, async {
            Err(StorageError::new(
                crate::opendal::ErrorKind::PermissionDenied,
                "second request failed",
            ))
        })
        .await
        .unwrap_err();
    assert_eq!(error.failure_fact().reason(), ICEBERG_FILESYSTEM_ERROR);
    assert_eq!(fixture.deletions.counts(), (1000, 1007));
    assert_eq!(
        fixture.deletions.publication_fact(),
        (Some(LakePublicationDisposition::CommitUnknown), true)
    );
}

#[tokio::test]
async fn cancellation_waits_for_dispatched_delete_and_records_its_receipt() {
    let fixture = Fixture::new();
    let storage = fixture.storage().await;
    let scan = fixture.scan();
    let (settle, settlement) = tokio::sync::oneshot::channel();
    let mut original = std::pin::pin!(scan.delete(&storage, 1, 17, async {
        settlement.await.unwrap();
        Ok(())
    }));
    assert!(futures::poll!(&mut original).is_pending());
    assert_eq!(
        fixture.deletions.publication_fact(),
        (Some(LakePublicationDisposition::CommitUnknown), false)
    );
    fixture.stop.request_stop();
    assert!(futures::poll!(&mut original).is_pending());
    settle.send(()).unwrap();
    let error = original.await.unwrap_err();
    assert_eq!(error.kind(), ConnectorErrorKind::Cancelled);
    assert_eq!(fixture.deletions.counts(), (1, 17));
    assert_eq!(
        fixture.deletions.publication_fact(),
        (Some(LakePublicationDisposition::KnownCommitted), true)
    );
    let error = scan
        .delete(&storage, 1, 2, async {
            panic!("never dispatch after cancellation")
        })
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ConnectorErrorKind::Cancelled);
}

fn object_id(table: &Table) -> ConnectorTableObjectId {
    crate::document_storage::observation::table_object_id(table.metadata()).unwrap()
}

#[tokio::test]
async fn replaced_guarded_object_is_refused_before_listing_or_deleting() {
    let fixture = Fixture::new();
    fixture.write("candidate", b"x", EARLY);
    let table = fixture.table(fixture.base(true), "metadata/current.metadata.json");
    let original = ConnectorTableObjectId::try_new(Bytes::from_static(b"old table uuid")).unwrap();
    let error = fixture
        .scan()
        .execute(&table, &original, CUTOFF)
        .await
        .unwrap_err();
    assert_eq!(
        error.table_object_binding_failure(),
        Some(novarocks_spi::connector::ConnectorTableObjectBindingFailure::Replaced),
    );
    assert!(fixture.exists("candidate"));
    assert_eq!(fixture.deletions.publication_fact(), (None, false));
}

async fn table_with_empty_manifest_list(fixture: &Fixture) -> Table {
    let list = "metadata/snap-1.avro";
    fixture.manifest_list(list, 1, Vec::new()).await;
    let metadata = fixture
        .base(true)
        .into_builder(None)
        .add_snapshot(snapshot(1, fixture.path(list)))
        .unwrap()
        .set_ref("main", reference(1))
        .unwrap()
        .build()
        .unwrap()
        .metadata;
    fixture.table(metadata, "metadata/current.metadata.json")
}

#[tokio::test]
async fn listing_reaches_the_tail_after_a_thousand_entry_batch() {
    let fixture = Fixture::new();
    let table = table_with_empty_manifest_list(&fixture).await;
    for i in 0..(DELETE_BATCH_SIZE + 1) {
        fixture.write(&format!("data/part-{i}.parquet"), b"x", EARLY);
    }
    let requests = Arc::new(Mutex::new(Vec::new()));
    let metrics = crate::fs_io::TEST_STORAGE_REQUESTS
        .scope(requests.clone(), async {
            fixture
                .scan()
                .execute(&table, &object_id(&table), CUTOFF)
                .await
        })
        .await
        .unwrap()
        .unwrap();
    assert_eq!(metrics.deleted_files, 1001);
    assert_eq!(metrics.deleted_bytes, 1001);
    assert_eq!(
        requests
            .lock()
            .unwrap()
            .iter()
            .filter(|r| r.operation == "orphan_delete_batch")
            .count(),
        2,
    );
}

#[cfg(unix)]
#[tokio::test]
async fn denied_child_directory_is_a_filesystem_failure() {
    use std::os::unix::fs::PermissionsExt;

    // Root can traverse a directory even with mode 000, so this fixture
    // requires the same denial semantics as the native process.
    let fixture = Fixture::new();
    let table = table_with_empty_manifest_list(&fixture).await;
    let denied = fixture.table_root.join("denied");
    std::fs::create_dir(&denied).unwrap();
    std::fs::set_permissions(&denied, std::fs::Permissions::from_mode(0o000)).unwrap();
    let can_open = std::fs::read_dir(&denied).is_ok();
    let result = fixture
        .scan()
        .execute(&table, &object_id(&table), CUTOFF)
        .await;
    std::fs::set_permissions(&denied, std::fs::Permissions::from_mode(0o700)).unwrap();
    if can_open {
        return;
    }
    let error = result.unwrap_err();
    assert_eq!(error.failure_fact().reason(), ICEBERG_FILESYSTEM_ERROR);
    assert_eq!(
        error.public_failure_detail(),
        Some("Failed removing orphan files for table: n.t")
    );
    assert_eq!(fixture.deletions.publication_fact(), (None, false));
}

#[test]
fn listed_file_without_modification_time_is_rejected() {
    let incomplete =
        crate::opendal::Metadata::new(crate::opendal::EntryMode::FILE).with_content_length(1);
    let error = required_file_facts(&incomplete, "n.t").unwrap_err();
    assert_eq!(error.failure_fact().reason(), ICEBERG_FILESYSTEM_ERROR);
}
