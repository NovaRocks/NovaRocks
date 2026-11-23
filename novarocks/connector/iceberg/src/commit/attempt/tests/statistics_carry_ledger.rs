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

//! Tests ownership and publication of opaque statistics copies.

use super::*;
use crate::commit::model::{ReusableStatisticsFile, StatisticsReuseBasis};
use crate::commit::staging::StagedView;
use crate::iceberg::spec::StatisticsFile;
use bytes::Bytes;

const SOURCE_PROPERTY: &str = "test.statistics-copy.source";
const ORIGINAL_SOURCE: &str = "file:///caller-owned/source-a.puffin";
const ADVANCED_SOURCE: &str = "file:///caller-owned/source-b.puffin";
const COPY_BYTES: &[u8] = b"PFA1PFA1{\"blobs\":[]}\x0c\0\0\0\0\0\0\0PFA1";
const COPY_FOOTER_BYTES: i64 = 28;

struct SyntheticStatisticsCopy;

#[async_trait]
impl Preparer for SyntheticStatisticsCopy {
    async fn prepare(
        &self,
        view: &StagedView<'_>,
        _intent: &OperationIntent,
    ) -> Result<PreparedChange> {
        let staged = view.metadata().snapshot_for_ref(view.target_ref()).unwrap();
        let source = StatisticsFile {
            snapshot_id: staged.parent_snapshot_id().unwrap_or(staged.snapshot_id()),
            statistics_path: view
                .metadata()
                .properties()
                .get(SOURCE_PROPERTY)
                .map_or(ORIGINAL_SOURCE, String::as_str)
                .to_owned(),
            file_size_in_bytes: COPY_BYTES.len() as i64,
            file_footer_size_in_bytes: COPY_FOOTER_BYTES,
            key_metadata: None,
            blob_metadata: Vec::new(),
        };
        let basis = StatisticsReuseBasis {
            stage: view.stage_key(),
            source: (&source).into(),
            stage_snapshot_id: staged.snapshot_id(),
        };
        let artifacts = view.artifacts();
        let copy = match artifacts.find_reusable_statistics(&basis)? {
            Some(output) => output,
            None => {
                let object = artifacts.allocate_reusable_statistics()?;
                artifacts
                    .file_io()
                    .new_output(object.path())?
                    .write(Bytes::from_static(COPY_BYTES))
                    .await?;
                let output = ReusableStatisticsFile {
                    object,
                    snapshot_id: basis.stage_snapshot_id,
                    file_size_in_bytes: COPY_BYTES.len() as i64,
                    file_footer_size_in_bytes: COPY_FOOTER_BYTES,
                };
                artifacts.remember_reusable_statistics(basis, output.clone())?;
                output
            }
        };
        let statistics = StatisticsFile {
            snapshot_id: copy.snapshot_id,
            statistics_path: copy.object.path().to_owned(),
            file_size_in_bytes: copy.file_size_in_bytes,
            file_footer_size_in_bytes: copy.file_footer_size_in_bytes,
            key_metadata: None,
            blob_metadata: Vec::new(),
        };
        Ok(PreparedChange {
            updates: vec![TableUpdate::SetStatistics { statistics }],
            requirements: Vec::new(),
        })
    }
}

async fn run_with_copy(
    fixture: &Fixture,
    intent: &OperationIntent,
    publisher: &ControlledPublisher<'_>,
) -> Report {
    run(
        &fixture.operation,
        intent,
        &PreparedChange::default(),
        &[&FastAppendPreparer, &SyntheticStatisticsCopy],
        publisher,
        policy(),
    )
    .await
}

fn requested_copy(request: &serde_json::Value) -> StatisticsFile {
    let updates = request["updates"].as_array().unwrap();
    let mut statistics = updates
        .iter()
        .filter(|update| update["action"] == "set-statistics");
    let update = statistics.next().unwrap();
    assert!(
        statistics.next().is_none(),
        "one exact statistics pointer per request"
    );
    serde_json::from_value(update["statistics"].clone()).unwrap()
}

fn copy_exists(path: &str) -> bool {
    std::path::Path::new(path.strip_prefix("file://").unwrap()).is_file()
}

#[tokio::test]
async fn same_source_retries_reuse_written_copy_as_the_successful_pointer() {
    let fixture = Fixture::new();
    let base = fixture.metadata(FormatVersion::V2);
    let competitor = Fixture::new();
    let advanced = append_state(&competitor, base.clone(), 4).await;
    let intent = append_intent(&fixture, &base, vec![Dependency::NoRead]);
    receive_append_data(&fixture, &intent);
    let publisher =
        ControlledPublisher::new(&fixture, base, Dispatch::ConflictThen(Box::new(advanced)));
    let trace = Arc::new(Mutex::new(Vec::new()));
    let report = crate::fs_io::TEST_STORAGE_REQUESTS
        .scope(trace.clone(), run_with_copy(&fixture, &intent, &publisher))
        .await;
    assert!(
        matches!(report.publication, PublicationOutcome::Committed(_)),
        "{report:?}"
    );
    let requests = publisher.requests.lock().unwrap();
    let [first, second] = requests.as_slice() else {
        panic!("expected two dispatched rounds")
    };
    let output = requested_copy(first);
    assert_eq!(requested_copy(second), output);
    let object = ObjectIdentity::new(output.statistics_path.clone()).unwrap();
    let artifact_requests = publisher.artifact_requests.lock().unwrap();
    assert!(
        artifact_requests
            .iter()
            .all(|references| references.contains(&object))
    );
    assert_eq!(
        publisher
            .metadata
            .lock()
            .unwrap()
            .statistics_for_snapshot(output.snapshot_id),
        Some(&output)
    );
    assert!(copy_exists(object.path()));
    assert_eq!(
        report.cleanup,
        IcebergCleanupReport::Complete { deleted: 1 }
    );
    let trace = trace.lock().unwrap();
    assert_eq!(
        trace
            .iter()
            .filter(|event| event.operation == "write" && event.path == object.path())
            .count(),
        1
    );
    assert!(
        !trace
            .iter()
            .any(|event| event.operation == "delete" && event.path == object.path())
    );
}

#[tokio::test]
async fn changed_source_retains_the_second_request_copy_and_removes_the_loser() {
    let fixture = Fixture::new();
    let base = fixture.metadata(FormatVersion::V2);
    let competitor = Fixture::new();
    let advanced = append_state(&competitor, base.clone(), 4)
        .await
        .into_builder(None)
        .set_properties(HashMap::from([(
            SOURCE_PROPERTY.into(),
            ADVANCED_SOURCE.into(),
        )]))
        .unwrap()
        .build()
        .unwrap()
        .metadata;
    let intent = append_intent(&fixture, &base, vec![Dependency::NoRead]);
    receive_append_data(&fixture, &intent);
    let publisher =
        ControlledPublisher::new(&fixture, base, Dispatch::ConflictThen(Box::new(advanced)));
    let trace = Arc::new(Mutex::new(Vec::new()));
    let report = crate::fs_io::TEST_STORAGE_REQUESTS
        .scope(trace.clone(), run_with_copy(&fixture, &intent, &publisher))
        .await;
    assert!(
        matches!(report.publication, PublicationOutcome::Committed(_)),
        "{report:?}"
    );
    let requests = publisher.requests.lock().unwrap();
    let [first, second] = requests.as_slice() else {
        panic!("expected two dispatched rounds")
    };
    let first = requested_copy(first);
    let second = requested_copy(second);
    assert_eq!(first.snapshot_id, second.snapshot_id);
    assert_ne!(first.statistics_path, second.statistics_path);
    let loser = ObjectIdentity::new(first.statistics_path).unwrap();
    let winner = ObjectIdentity::new(second.statistics_path.clone()).unwrap();
    let artifact_requests = publisher.artifact_requests.lock().unwrap();
    assert!(artifact_requests[0].contains(&loser));
    assert!(!artifact_requests[0].contains(&winner));
    assert!(artifact_requests[1].contains(&winner));
    assert!(!artifact_requests[1].contains(&loser));
    assert_eq!(
        publisher
            .metadata
            .lock()
            .unwrap()
            .statistics_for_snapshot(second.snapshot_id),
        Some(&second)
    );
    assert!(copy_exists(winner.path()));
    assert!(!copy_exists(loser.path()));
    assert_eq!(
        report.cleanup,
        IcebergCleanupReport::Complete { deleted: 2 }
    );
    let trace = trace.lock().unwrap();
    let second_dispatch = trace
        .iter()
        .rposition(|event| event.operation == "dispatch")
        .unwrap();
    let deletion = trace
        .iter()
        .position(|event| event.operation == "delete" && event.path == loser.path())
        .unwrap();
    assert!(deletion > second_dispatch);
    assert!(
        !trace
            .iter()
            .any(|event| event.operation == "delete" && event.path == winner.path())
    );
}

#[tokio::test]
async fn known_uncommitted_drops_the_copy_but_unknown_dispatch_keeps_it() {
    for unknown in [false, true] {
        let fixture = Fixture::new();
        let base = fixture.metadata(FormatVersion::V2);
        let intent = append_intent(&fixture, &base, vec![Dependency::NoRead]);
        receive_append_data(&fixture, &intent);
        let disposition = if unknown {
            Dispatch::Unknown
        } else {
            Dispatch::AlwaysConflict
        };
        let publisher = ControlledPublisher::new(&fixture, base, disposition);
        let report = run_with_copy(&fixture, &intent, &publisher).await;
        let requests = publisher.requests.lock().unwrap();
        let [request] = requests.as_slice() else {
            panic!("terminal disposition has one dispatch")
        };
        let output = requested_copy(request);
        let object = ObjectIdentity::new(output.statistics_path).unwrap();
        let references = publisher.artifact_requests.lock().unwrap();
        assert!(references[0].contains(&object));
        let records = fixture.operation.recovery_artifacts();
        if unknown {
            assert!(
                matches!(report.publication, PublicationOutcome::Unknown { .. }),
                "{report:?}"
            );
            assert_eq!(report.cleanup, IcebergCleanupReport::NotAttempted);
            let owned = records
                .iter()
                .find(|record| record.object == object)
                .unwrap();
            assert!(!owned.retired);
            assert!(copy_exists(object.path()));
        } else {
            assert!(
                matches!(report.publication, PublicationOutcome::KnownUncommitted(_)),
                "{report:?}"
            );
            assert_eq!(
                report.cleanup,
                IcebergCleanupReport::Complete {
                    deleted: references[0].len()
                }
            );
            assert!(records.is_empty());
            assert!(!copy_exists(object.path()));
        }
    }
}

#[path = "../../statistics_carry/tests.rs"]
mod real_copy;
