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

//! White-box verification of registered stage inputs, their payments and Avro output.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Mutex;

use crate::commit::fast_append::CompactFastAppendPreparer;
use crate::commit::model::{ArtifactWriter, OperationToken};
use crate::commit::operation::OperationLimits;
use crate::commit::staging::{StagingBase, StagingEngine};
use crate::iceberg::spec::{
    DataContentType, DataFileBuilder, DataFileFormat, Datum, NestedField, PrimitiveType, Schema,
    Type,
};
use novarocks_spi::connector::ConnectorMutationOperationId;

use super::*;

pub(in crate::commit) fn fixture() -> Fixture {
    let mut fixture = Fixture::new();
    let runtime = tokio::runtime::Handle::current();
    let binding = crate::access_binding::IcebergReadBinding::new(
        None,
        novarocks_fs::FsAccessResolver::new(),
        Arc::new(novarocks_fs::TokioFileIoRuntime::new(runtime.clone())),
        novarocks_fs::TokioRefreshExecutor::shared(runtime.clone()),
        Arc::new(novarocks_fs::TokioFileIoSpawner::new(runtime)),
    );
    fixture.operation = IcebergCommitOperation::new(
        OperationToken::from_mutation(ConnectorMutationOperationId::from_bytes([5; 16])),
        format!("file://{}", fixture.directory.path().display()),
        binding,
        fixture.operation.request_context().clone(),
        fixture.operation.runtime().clone(),
        OperationLimits::default(),
    )
    .unwrap();
    fixture
}

pub(in crate::commit) struct Installed {
    pub(in crate::commit) external: Vec<PathBuf>,
    pub(in crate::commit) paid_files: Arc<AtomicUsize>,
    pub(in crate::commit) paid_paths: Arc<AtomicUsize>,
    pub(in crate::commit) paid_stage: Arc<AtomicUsize>,
}

struct OriginalFileReceipt {
    original: Option<ConnectorPayloadRetentionGuard>,
    exits_at: Arc<Mutex<Vec<usize>>>,
    trace: Arc<Mutex<Vec<crate::fs_io::TestStorageRequest>>>,
}

impl Drop for OriginalFileReceipt {
    fn drop(&mut self) {
        let writes = self
            .trace
            .lock()
            .unwrap()
            .iter()
            .filter(|event| event.operation == "write" && event.path.ends_with(".avro"))
            .count();
        self.exits_at.lock().unwrap().push(writes);
        drop(self.original.take());
    }
}

pub(in crate::commit) fn install(
    fixture: &Fixture,
    metadata: &TableMetadata,
    count: usize,
) -> Installed {
    install_observed(fixture, metadata, count, |receipt| receipt)
}

fn install_observed(
    fixture: &Fixture,
    metadata: &TableMetadata,
    count: usize,
    observe_file: impl Fn(ConnectorPayloadRetentionGuard) -> ConnectorPayloadRetentionGuard,
) -> Installed {
    let file_window = Arc::new(Window::default());
    let path_window = Arc::new(Window::default());
    let stage_window = Arc::new(Window::default());
    let paid_files = file_window.0.clone();
    let paid_paths = path_window.0.clone();
    let paid_stage = stage_window.0.clone();
    let file_author = ConnectorPayloadRetentionGuard::from_byte_budget(file_window);
    let path_author = ConnectorPayloadRetentionGuard::from_byte_budget(path_window);
    let author = ConnectorPayloadRetentionGuard::from_byte_budget(stage_window);
    let entries_receipt = author
        .reserve_owned_bytes(count * size_of::<RegisteredFile>())
        .unwrap();
    let mut entries = Vec::with_capacity(count);
    let data = fixture.directory.path().join("data");
    std::fs::create_dir_all(&data).unwrap();
    let mut external = Vec::with_capacity(count);
    for ordinal in 0..count {
        let path = data.join(format!("external-{ordinal:05}.parquet"));
        // This owner writes only the metadata. Pre-phase validates Parquet.
        std::fs::write(&path, b"unchanged external input").unwrap();
        let location = format!("file://{}", path.display());
        let ids = metadata
            .current_schema()
            .as_struct()
            .fields()
            .iter()
            .map(|field| field.id)
            .collect::<Vec<_>>();
        let file_receipt = observe_file(file_author.reserve_owned_bytes(location.len()).unwrap());
        let path_receipt = path_author.reserve_owned_bytes(location.len()).unwrap();
        let object = ObjectIdentity::from_shared_path(Arc::from(location.as_str())).unwrap();
        let file = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_format(DataFileFormat::Parquet)
            .file_path(location)
            .record_count(2)
            .file_size_in_bytes(20)
            .column_sizes(ids.iter().map(|&id| (id, 100000)).collect())
            .value_counts(ids.iter().map(|&id| (id, 2)).collect())
            .null_value_counts(ids.iter().map(|&id| (id, 0)).collect())
            .lower_bounds(
                ids.iter()
                    .map(|&id| (id, Datum::long(i64::from(id))))
                    .collect(),
            )
            .upper_bounds(
                ids.iter()
                    .map(|&id| (id, Datum::long(i64::from(id) + 1)))
                    .collect(),
            )
            .build()
            .unwrap();
        entries.push(RegisteredFile::try_new(file, object, path_receipt, file_receipt).unwrap());
        external.push(path);
    }
    let files =
        RegisteredNewFiles::try_new(Arc::new(metadata.clone()), entries, entries_receipt, author)
            .unwrap();
    assert_eq!(files.counts().data.files, count as u64);
    assert_eq!(files.counts().data.rows, 2 * count as u64);
    assert_eq!(files.counts().data.bytes, 20 * count as u64);
    fixture
        .operation
        .install_registered_new_files(0, files)
        .unwrap();
    Installed {
        external,
        paid_files,
        paid_paths,
        paid_stage,
    }
}

fn wide_metrics(metadata: TableMetadata) -> TableMetadata {
    let mut fields = metadata.current_schema().as_struct().fields().to_vec();
    fields.extend((2..=64).map(|id| {
        Arc::new(NestedField::optional(
            id,
            format!("metric_{id}"),
            Type::Primitive(PrimitiveType::Long),
        ))
    }));
    let schema = Schema::builder().with_fields(fields).build().unwrap();
    metadata
        .into_builder(None)
        .add_current_schema(schema)
        .unwrap()
        .set_properties(HashMap::from([
            ("commit.manifest.target-size-bytes".into(), "512".into()),
            ("commit.manifest-merge.enabled".into(), "false".into()),
        ]))
        .unwrap()
        .build()
        .unwrap()
        .metadata
}

#[tokio::test]
async fn wide_registration_rolls_before_the_second_entry_and_refunds_each_prefix() {
    let fixture = fixture();
    let metadata = wide_metrics(fixture.metadata(FormatVersion::V2));
    let trace = Arc::new(Mutex::new(Vec::new()));
    let exits_at = Arc::new(Mutex::new(Vec::new()));
    let installed = install_observed(&fixture, &metadata, 5, |receipt| {
        ConnectorPayloadRetentionGuard::new(OriginalFileReceipt {
            original: Some(receipt),
            exits_at: exits_at.clone(),
            trace: trace.clone(),
        })
    });
    let intent = fixture
        .intent(&metadata, "main", Vec::new())
        .with_compact_stage(0)
        .unwrap();
    let attempt = fixture.operation.begin_attempt().unwrap();
    let mut engine = StagingEngine::begin(
        StagingBase::Existing {
            metadata,
            metadata_location: "file:///previous.metadata.json".into(),
        },
        &intent,
        &attempt,
    )
    .unwrap();
    crate::fs_io::TEST_STORAGE_REQUESTS
        .scope(trace.clone(), async {
            engine
                .stage(&CompactFastAppendPreparer(fixture.operation.clone()))
                .await
                .unwrap();
        })
        .await;
    assert_eq!(*exits_at.lock().unwrap(), [1, 2, 3, 4, 5]);
    assert_eq!(installed.paid_files.load(Ordering::SeqCst), 0);
    assert!(installed.paid_paths.load(Ordering::SeqCst) > 0);
    assert!(installed.paid_stage.load(Ordering::SeqCst) > 0);
    let committed = engine.metadata();
    let snapshot = committed.current_snapshot().unwrap();
    let list = crate::manifest::load_manifest_list(snapshot, attempt.file_io(), committed)
        .await
        .unwrap();
    let first = &list.entries()[0];
    assert_eq!(list.entries().len(), 5);
    assert_eq!(first.added_files_count, Some(1));
    let source = ManifestSource::pending(snapshot.snapshot_id(), first);
    let raw = source.load_sized(attempt.file_io()).await.unwrap();
    let file = raw.entries()[0].data_file();
    assert_eq!(file.lower_bounds().len(), 64);
    assert_eq!(file.column_sizes().len(), 64);
    engine.freeze().unwrap();
    fixture.operation.release_registered_paths();
    fixture.operation.release_registered_paths();
    assert_eq!(installed.paid_paths.load(Ordering::SeqCst), 0);
    assert_eq!(installed.paid_stage.load(Ordering::SeqCst), 0);
    assert!(installed.external.iter().all(|file| file.exists()));
}

#[tokio::test]
async fn no_registered_inputs_still_produce_an_empty_append_snapshot() {
    let fixture = fixture();
    let metadata = fixture.metadata(FormatVersion::V1);
    let installed = install(&fixture, &metadata, 0);
    let intent = fixture
        .intent(&metadata, "main", Vec::new())
        .with_compact_stage(0)
        .unwrap();
    let attempt = fixture.operation.begin_attempt().unwrap();
    let mut engine = StagingEngine::begin(
        StagingBase::Existing {
            metadata,
            metadata_location: "file:///previous.metadata.json".into(),
        },
        &intent,
        &attempt,
    )
    .unwrap();
    engine
        .stage(&CompactFastAppendPreparer(fixture.operation.clone()))
        .await
        .unwrap();
    let committed = engine.metadata();
    let snapshot = committed.current_snapshot().unwrap();
    let list = crate::manifest::load_manifest_list(snapshot, attempt.file_io(), committed)
        .await
        .unwrap();
    assert!(list.entries().is_empty());
    assert_eq!(
        snapshot.summary().operation,
        crate::iceberg::spec::Operation::Append
    );
    engine.freeze().unwrap();
    fixture.operation.release_registered_paths();
    assert_eq!(installed.paid_stage.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn actual_added_coverage_rejects_a_repeated_path() {
    let fixture = fixture();
    let metadata = fixture.metadata(FormatVersion::V2);
    let installed = install(&fixture, &metadata, 2);
    let attempt = fixture.operation.begin_attempt().unwrap();
    let snapshot = attempt.stage_snapshot_id(0, &metadata).unwrap();
    let first = format!("file://{}", installed.external[0].display());
    let mut observed = attempt
        .observe_registered_manifest(snapshot, &[])
        .unwrap()
        .unwrap();
    observed.push_added(&first).unwrap();
    assert!(observed.push_added(&first).is_err());
    assert!(
        attempt
            .observe_registered_manifest(snapshot + 1, &[])
            .unwrap()
            .is_none()
    );
    fixture.operation.release_registered_paths();
    // Actual observation backing still owns the path alias.
    assert!(installed.paid_paths.load(Ordering::SeqCst) > 0);
    drop(observed);
    // The undispatched stage still holds the original paid inputs.
    assert!(installed.paid_files.load(Ordering::SeqCst) > 0);
    let cleanup = fixture
        .operation
        .cleanup(crate::commit::model::CleanupScope::EntireOperation)
        .await;
    assert!(matches!(
        cleanup,
        crate::commit::model::IcebergCleanupReport::Complete { deleted: 0 }
    ));
    assert_eq!(installed.paid_files.load(Ordering::SeqCst), 0);
    assert_eq!(installed.paid_paths.load(Ordering::SeqCst), 0);
    assert!(installed.external.iter().all(|file| file.exists()));
}
