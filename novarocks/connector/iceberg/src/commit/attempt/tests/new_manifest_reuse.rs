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

//! Observes exact manifest paths, storage requests, and assigned sequences across rounds.

use super::*;
use crate::commit::fast_append::CompactFastAppendPreparer;
use crate::commit::new_manifests::tests::install_appends;

#[tokio::test]
async fn already_written_compact_manifests_reuse_physical_paths_after_conflict() {
    let baseline = Fixture::new();
    let base = append_state(&baseline, baseline.metadata(FormatVersion::V3), 2).await;
    let competitor = Fixture::new();
    let raced = append_state(&competitor, base.clone(), 4).await;
    let fixture = Fixture::new();
    let live = install_appends(&fixture, &base, 6);
    let intent = fixture
        .intent(&base, "main", Vec::new())
        .with_compact_stage(0)
        .unwrap();
    let publisher = ControlledPublisher::new(
        &fixture,
        base,
        Dispatch::ConflictThen(Box::new(raced.clone())),
    );
    let trace = Arc::new(Mutex::new(Vec::new()));
    let report = crate::fs_io::TEST_STORAGE_REQUESTS
        .scope(trace.clone(), async {
            run(
                &fixture.operation,
                &intent,
                &PreparedChange::default(),
                &[&CompactFastAppendPreparer(fixture.operation.clone())],
                &publisher,
                policy(),
            )
            .await
        })
        .await;
    assert!(
        matches!(report.publication, PublicationOutcome::Committed(_)),
        "{report:?}"
    );
    assert_eq!(live.load(Ordering::SeqCst), 0);
    let requests = publisher.requests.lock().unwrap();
    let snapshots: Vec<_> = requests
        .iter()
        .map(|request| {
            &request["updates"]
                .as_array()
                .unwrap()
                .iter()
                .find(|update| update["action"] == "add-snapshot")
                .unwrap()["snapshot"]
        })
        .collect();
    assert_eq!(snapshots.len(), 2);
    assert_eq!(snapshots[0]["snapshot-id"], snapshots[1]["snapshot-id"]);
    assert_eq!(
        snapshots[1]["sequence-number"],
        raced.next_sequence_number()
    );
    assert_eq!(snapshots[1]["first-row-id"], raced.next_row_id());
    assert_ne!(snapshots[0]["manifest-list"], snapshots[1]["manifest-list"]);
    let artifacts = publisher.artifact_requests.lock().unwrap();
    let manifest = artifacts[0]
        .iter()
        .find(|object| {
            object.path().ends_with(".avro")
                && object.path() != snapshots[0]["manifest-list"].as_str().unwrap()
        })
        .unwrap();
    assert!(artifacts[1].contains(manifest));
    let events = trace.lock().unwrap();
    assert_eq!(
        events
            .iter()
            .filter(|event| event.path == manifest.path())
            .count(),
        1,
        "{events:?}"
    );
    assert!(
        events
            .iter()
            .any(|event| event.path == manifest.path() && event.operation == "write")
    );
}

#[tokio::test]
async fn format_upgrade_reauthors_pending_v1_sequence_inheritance() {
    use crate::commit::model::ArtifactWriter as _;
    use crate::manifest::source::ManifestSource;

    let fixture = Fixture::new();
    let base = fixture.metadata(FormatVersion::V1);
    let upgraded = base
        .clone()
        .into_builder(None)
        .upgrade_format_version(FormatVersion::V2)
        .unwrap()
        .build()
        .unwrap()
        .metadata;
    let competitor = Fixture::new();
    let raced = append_state(&competitor, upgraded, 4).await;
    install_appends(&fixture, &base, 2);
    let intent = fixture
        .intent(&base, "main", Vec::new())
        .with_compact_stage(0)
        .unwrap();
    let publisher = ControlledPublisher::new(
        &fixture,
        base,
        Dispatch::ConflictThen(Box::new(raced.clone())),
    );
    let report = run(
        &fixture.operation,
        &intent,
        &PreparedChange::default(),
        &[&CompactFastAppendPreparer(fixture.operation.clone())],
        &publisher,
        policy(),
    )
    .await;
    assert!(
        matches!(report.publication, PublicationOutcome::Committed(_)),
        "{report:?}"
    );
    let after = publisher.metadata.lock().unwrap().clone();
    let snapshot = after.current_snapshot().unwrap();
    let reader = fixture.operation.begin_attempt().unwrap();
    let list = crate::manifest::load_manifest_list(snapshot, reader.file_io(), &after)
        .await
        .unwrap();
    let descriptor = list
        .entries()
        .iter()
        .find(|m| m.added_snapshot_id == snapshot.snapshot_id())
        .unwrap();
    assert_eq!(descriptor.sequence_number, raced.next_sequence_number());
    let source =
        ManifestSource::committed(snapshot.snapshot_id(), snapshot.manifest_list(), descriptor);
    let raw = source.load_sized(reader.file_io()).await.unwrap();
    assert_eq!(raw.metadata().format_version, FormatVersion::V2);
    for entry in source.bind(&raw).unwrap() {
        let entry = entry.unwrap();
        assert_eq!(entry.raw.sequence_number(), None);
        assert_eq!(
            entry.sequences.data_sequence,
            Some(raced.next_sequence_number())
        );
        assert_eq!(
            entry.sequences.file_sequence,
            Some(raced.next_sequence_number())
        );
    }
    let artifacts = publisher.artifact_requests.lock().unwrap();
    assert_eq!(
        artifacts[0]
            .iter()
            .filter(|object| object.path().ends_with(".avro"))
            .filter(|object| artifacts[1].contains(object))
            .count(),
        0
    );
}
