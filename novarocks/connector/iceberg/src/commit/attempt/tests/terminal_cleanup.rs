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

//! Observes physical object inventory, dispatches and original receipts at terminal cuts.

use super::*;
use crate::commit::fast_append::CompactFastAppendPreparer;
use crate::commit::new_manifests::tests::install_appends;
use crate::commit::write_stack::domain::{
    IcebergCreationReceipt, IcebergCreationScope, IcebergWriteBranch,
};
use crate::commit::write_stack::test_support::{
    paid_fragments_counted, received_write_proof, write_query_identity,
};
use crate::commit::write_stack::tests::{
    FrozenRewriteLake, dv_session, fixed_dv_fragment, request_context as write_request_context,
};
use novarocks_spi::connector::write_stack::{
    ConnectorWriteAbortProduction, ConnectorWriteControl, ConnectorWriteSessionAbortRequest,
    ConnectorWriterPhysicalContext,
};
use novarocks_spi::connector::{ConnectorWriteAbortOutcome, ExternalMutationFinalization};

fn populated_compact_append(
    fixture: &Fixture,
    base: &TableMetadata,
    count: usize,
) -> (OperationIntent, Arc<AtomicUsize>, Vec<std::path::PathBuf>) {
    let receipts = install_appends(fixture, base, count);
    let data = fixture.directory.path().join("data");
    std::fs::create_dir_all(&data).unwrap();
    let paths = (0..count)
        .map(|index| data.join(format!("compact-{index:04}.parquet")))
        .collect::<Vec<_>>();
    for path in &paths {
        std::fs::write(path, b"owned original file").unwrap();
    }
    (
        fixture
            .intent(base, "main", Vec::new())
            .with_compact_stage(0)
            .unwrap(),
        receipts,
        paths,
    )
}

fn files_under(directory: &std::path::Path) -> Vec<std::path::PathBuf> {
    if !directory.exists() {
        return Vec::new();
    }
    let mut files = std::fs::read_dir(directory)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.is_file())
        .collect::<Vec<_>>();
    files.sort();
    files
}

async fn run_compact(
    fixture: &Fixture,
    intent: &OperationIntent,
    publisher: &ControlledPublisher<'_>,
) -> Report {
    run(
        &fixture.operation,
        intent,
        &PreparedChange::default(),
        &[&CompactFastAppendPreparer(fixture.operation.clone())],
        publisher,
        policy(),
    )
    .await
}

#[tokio::test]
async fn second_manifest_store_refusal_cleans_the_written_prefix_and_received_files() {
    let fixture = Fixture::new();
    let base = TableUpdate::SetProperties {
        updates: HashMap::from([("commit.manifest.target-size-bytes".into(), "1".into())]),
    }
    .apply(fixture.metadata(FormatVersion::V3).into_builder(None))
    .unwrap()
    .build()
    .unwrap()
    .metadata;
    let (intent, receipts, paths) = populated_compact_append(&fixture, &base, 6);
    let publisher = ControlledPublisher::new(&fixture, base, Dispatch::Commit);
    let trace = Arc::new(Mutex::new(Vec::new()));
    let report = crate::fs_io::TEST_STORAGE_REQUESTS
        .scope(trace.clone(), async {
            crate::fs_io::TEST_AVRO_WRITES_BEFORE_FAILURE
                .scope(
                    std::cell::Cell::new(1),
                    run_compact(&fixture, &intent, &publisher),
                )
                .await
        })
        .await;
    let PublicationOutcome::KnownUncommitted(failure) = report.publication else {
        panic!("a manifest storage refusal cannot dispatch: {report:?}");
    };
    assert!(failure.message().contains("injected Avro store refusal"));
    assert!(publisher.requests.lock().unwrap().is_empty());
    assert!(
        matches!(report.cleanup, IcebergCleanupReport::Complete { .. }),
        "{:?}",
        report.cleanup
    );
    assert_eq!(receipts.load(Ordering::SeqCst), 0);
    assert!(files_under(&fixture.directory.path().join("metadata")).is_empty());
    assert!(files_under(&fixture.directory.path().join("data")).is_empty());
    let events = trace.lock().unwrap();
    assert_eq!(
        events
            .iter()
            .filter(|event| event.operation == "write")
            .count(),
        2
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| event.operation == "write-refused")
            .count(),
        1
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| event.operation == "delete")
            .count(),
        paths.len() + 2
    );
}

#[tokio::test]
async fn conflict_retry_preserves_compact_manifest_until_real_terminal_cleanup() {
    let parent = Fixture::new();
    let base = append_state(&parent, parent.metadata(FormatVersion::V3), 2).await;
    let contender = Fixture::new();
    let changed = append_state(&contender, base.clone(), 4).await;
    let fixture = Fixture::new();
    let (intent, receipts, data) = populated_compact_append(&fixture, &base, 3);
    let publisher =
        ControlledPublisher::new(&fixture, base, Dispatch::ConflictThen(Box::new(changed)));
    let trace = Arc::new(Mutex::new(Vec::new()));
    let report = crate::fs_io::TEST_STORAGE_REQUESTS
        .scope(trace.clone(), run_compact(&fixture, &intent, &publisher))
        .await;
    assert!(
        matches!(report.publication, PublicationOutcome::Committed(_)),
        "{report:?}"
    );
    assert_eq!(
        report.cleanup,
        IcebergCleanupReport::Complete { deleted: 1 }
    );
    assert_eq!(publisher.requests.lock().unwrap().len(), 2);
    assert_eq!(receipts.load(Ordering::SeqCst), 0);
    assert_eq!(files_under(&fixture.directory.path().join("data")), data);
    let remaining = files_under(&fixture.directory.path().join("metadata"));
    assert_eq!(remaining.len(), 2);
    let events = trace.lock().unwrap();
    let dispatches: Vec<_> = events
        .iter()
        .enumerate()
        .filter(|(_, event)| event.operation == "dispatch")
        .map(|(i, _)| i)
        .collect();
    let actual_deletes: Vec<_> = events
        .iter()
        .enumerate()
        .filter(|(_, event)| event.operation == "delete")
        .collect();
    assert_eq!(actual_deletes.len(), 1);
    assert!(actual_deletes[0].0 > dispatches[1]);
}

#[tokio::test]
async fn committed_merge_keeps_data_and_only_frozen_metadata_references() {
    let parent = Fixture::new();
    let base = append_state(&parent, parent.metadata(FormatVersion::V2), 2).await;
    let base = TableUpdate::SetProperties {
        updates: HashMap::from([("commit.manifest.min-count-to-merge".into(), "2".into())]),
    }
    .apply(base.into_builder(None))
    .unwrap()
    .build()
    .unwrap()
    .metadata;
    let fixture = Fixture::new();
    let (intent, receipts, data) = populated_compact_append(&fixture, &base, 3);
    let publisher = ControlledPublisher::new(&fixture, base, Dispatch::Commit);
    let report = run_compact(&fixture, &intent, &publisher).await;
    assert!(
        matches!(report.publication, PublicationOutcome::Committed(_)),
        "{report:?}"
    );
    assert_eq!(
        report.cleanup,
        IcebergCleanupReport::Complete { deleted: 1 }
    );
    assert_eq!(publisher.requests.lock().unwrap().len(), 1);
    assert_eq!(receipts.load(Ordering::SeqCst), 0);
    assert_eq!(files_under(&fixture.directory.path().join("data")), data);
    let actual = files_under(&fixture.directory.path().join("metadata"));
    let frozen = publisher.artifact_requests.lock().unwrap();
    let selected = frozen[0]
        .iter()
        .filter(|path| path.path().ends_with(".avro"))
        .map(|path| std::path::PathBuf::from(path.path().trim_start_matches("file://")))
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(actual.len(), 2);
    assert_eq!(
        actual
            .into_iter()
            .collect::<std::collections::BTreeSet<_>>(),
        selected
    );
}

#[tokio::test]
async fn unknown_incremental_publication_retains_written_objects_and_refunds_compact_input() {
    crate::commit::run::assert_compact_keyed_unknown_retains_artifacts().await;
}

#[test]
fn incomplete_v3_delete_cannot_grant_cleanup_or_prepare_a_manifest() {
    let lake = FrozenRewriteLake::new();
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("unconfirmed.puffin");
    std::fs::write(&path, vec![0; 1024]).unwrap();
    let path_uri = format!("file://{}", path.display());
    let (handle, _) = dv_session();
    let scope = IcebergCreationScope::try_new(handle.session_id(), [2; 16], [3; 32]).unwrap();
    let handle = handle.try_with_creation_scope(scope).unwrap();
    let ordinal = handle
        .targets()
        .iter()
        .find(|target| target.branch() == IcebergWriteBranch::DeletionVector)
        .unwrap()
        .ordinal();
    let physical = ConnectorWriterPhysicalContext::new([4; 16], 1, [5; 16], 0, ordinal.get());
    let identity = scope
        .identity(
            physical,
            &path_uri,
            crate::delete_file::IcebergFileFormat::Puffin,
        )
        .unwrap();
    let fragment = fixed_dv_fragment(&path_uri, "s3://b/data/target.parquet", Vec::new())
        .try_with_creation(
            IcebergCreationReceipt::try_new(physical, [6; 16], identity, 1024, [7; 32]).unwrap(),
        )
        .unwrap();
    let commit = lake.adapter.wrap_commit_session(handle);
    let query = write_query_identity();
    lake.control
        .begin_write_query(
            &commit,
            &query,
            &[ordinal],
            Some(received_write_proof(ordinal, physical)),
            None,
        )
        .unwrap();
    let (mut incoming, author, live) = paid_fragments_counted(
        &lake.adapter,
        vec![lake.adapter.wrap_commit_fragment(fragment)],
    );
    lake.control
        .arrive_write_fragments(
            &commit,
            &query,
            vec![(ordinal, incoming.pop().unwrap())],
            &author,
        )
        .unwrap();
    assert!(live.load(Ordering::SeqCst) > 0);
    let handle = lake.adapter.commit_session(&commit).unwrap();
    let refusal = lake
        .control
        .abort_write(ConnectorWriteSessionAbortRequest {
            commit: &commit,
            context: write_request_context(),
            production: ConnectorWriteAbortProduction::CompleteReceived,
        })
        .unwrap_err();
    assert_eq!(refusal.kind(), ConnectorErrorKind::Internal);
    assert!(handle.retained_publication().unwrap().is_none());
    assert!(live.load(Ordering::SeqCst) > 0);
    let outcome = lake
        .control
        .abort_write(ConnectorWriteSessionAbortRequest {
            commit: &commit,
            context: write_request_context(),
            production: ConnectorWriteAbortProduction::Unproven,
        })
        .unwrap();
    assert!(
        matches!(outcome, ConnectorWriteAbortOutcome::KnownUncommitted { cleanup } if !matches!(cleanup, ExternalMutationFinalization::Complete))
    );
    assert!(handle.retained_publication().unwrap().is_none());
    assert_eq!(live.load(Ordering::SeqCst), 0);
    assert!(path.is_file());
    assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 1);
}
