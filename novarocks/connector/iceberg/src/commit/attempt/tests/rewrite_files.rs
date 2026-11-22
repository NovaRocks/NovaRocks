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

//! Exercises the catalog loop for the compact RewriteFiles transaction.

use super::*;
use crate::catalog::SnapshotLoading;
use crate::commit::dependency::ValidationInputs;
use crate::commit::model::CommitProtocol;
use crate::commit::new_manifests::tests::install_rewrite;
use crate::commit::selected_rewrite::CompactRewritePreparer;
use novarocks_failure_contract::FailureProjection;
use novarocks_failure_contract::connector_reason::{
    COMMIT_TRANSACTION_CONFLICT, ICEBERG_COMMIT_STATE_UNKNOWN, ICEBERG_COMMIT_VALIDATION_CONFLICT,
};

async fn rewrite_intent(fixture: &Fixture, source: &TableMetadata) -> OperationIntent {
    let seed = fixture.intent(source, "main", Vec::new());
    let attempt = fixture.operation.begin_attempt().unwrap();
    let replaced = ValidationInputs::new(source, source.current_snapshot_id(), &attempt)
        .live_set()
        .await
        .unwrap()
        .values()
        .map(|file| file.frozen.clone())
        .collect::<Vec<_>>();
    assert_eq!(replaced.len(), 1);
    install_rewrite(fixture, source);
    OperationIntent::new(OperationIntentParts {
        target: seed.target().clone(),
        target_ref: seed.target_ref().to_owned(),
        start: seed.start(),
        changes: FileChanges::default(),
        dependencies: Vec::new(),
        protocol: CommitProtocol::RewriteFiles {
            output_spec_id: source.default_partition_spec_id(),
            replaced: replaced.into(),
        },
        shape: RequestShape::SnapshotProducing,
        summary: Default::default(),
        token: fixture.operation.token(),
    })
    .unwrap()
    .with_compact_stage(0)
    .unwrap()
}

fn assert_publication_bases(publisher: &ControlledPublisher<'_>, expected: &[i64]) {
    assert_eq!(publisher.loads.load(Ordering::SeqCst), expected.len());
    assert_eq!(
        *publisher.load_modes.lock().unwrap(),
        vec![SnapshotLoading::All; expected.len()]
    );
    let uuid = publisher.metadata.lock().unwrap().uuid().to_string();
    let requests = publisher.requests.lock().unwrap();
    assert_eq!(requests.len(), expected.len());
    for (request, id) in requests.iter().zip(expected.iter().copied()) {
        let requirements = request["requirements"].as_array().unwrap();
        let [table, requirement, output_spec] = requirements.as_slice() else {
            panic!(
                "OPTIMIZE must assert its table UUID, target ref and default spec: {requirements:?}"
            );
        };
        assert_eq!(table["type"], "assert-table-uuid");
        assert_eq!(table["uuid"].as_str(), Some(uuid.as_str()));
        assert_eq!(requirement["type"], "assert-ref-snapshot-id");
        assert_eq!(requirement["ref"], "main");
        assert_eq!(requirement["snapshot-id"].as_i64(), Some(id));
        assert_eq!(output_spec["type"], "assert-default-spec-id");
        assert_eq!(output_spec["default-spec-id"], 0);

        let added = request["updates"]
            .as_array()
            .unwrap()
            .iter()
            .find(|update| update["action"] == "add-snapshot")
            .expect("OPTIMIZE must dispatch a replacement snapshot");
        let snapshot = &added["snapshot"];
        assert_eq!(snapshot["parent-snapshot-id"].as_i64(), Some(id));
        assert_eq!(snapshot["summary"]["operation"], "replace");
    }
}

#[tokio::test]
async fn optimize_reloads_the_target_and_reports_contention_when_retries_are_exhausted() {
    let fixture = Fixture::new();
    let seed = Fixture::new();
    let source = append_state(&seed, seed.metadata(FormatVersion::V2), 5).await;
    let intent = rewrite_intent(&fixture, &source).await;
    let first_base = append_state(&seed, source, 7).await;
    let second_base = append_state(&seed, first_base.clone(), 8).await;
    let final_winner = append_state(&seed, second_base.clone(), 9).await;
    let bases = [
        first_base.current_snapshot_id().unwrap(),
        second_base.current_snapshot_id().unwrap(),
    ];
    assert_ne!(intent.start().unwrap().snapshot_id, bases[0]);
    assert_ne!(bases[0], bases[1]);
    let final_id = final_winner.current_snapshot_id();
    let publisher = ControlledPublisher::new(
        &fixture,
        first_base,
        Dispatch::ConflictSequence(vec![second_base, final_winner]),
    );
    let report = run(
        &fixture.operation,
        &intent,
        &PreparedChange::default(),
        &[&CompactRewritePreparer {
            operation: fixture.operation.clone(),
            stage: 0,
        }],
        &publisher,
        RetryPolicy {
            retries: 1,
            ..policy()
        },
    )
    .await;
    let PublicationOutcome::KnownUncommitted(failure) = report.publication else {
        panic!("expected catalog contention after the original dispatch and one retry: {report:?}");
    };
    assert_eq!(failure.failure_fact().reason(), COMMIT_TRANSACTION_CONFLICT);
    assert_eq!(
        failure.connector_source().unwrap().public_failure_detail(),
        Some(
            "Failed to commit the transaction during optimize: Injected concurrent catalog commit (after 2 commit attempts)"
        )
    );
    assert_publication_bases(&publisher, &bases);
    assert_eq!(
        publisher.metadata.lock().unwrap().current_snapshot_id(),
        final_id
    );
}

#[tokio::test]
async fn optimize_unknown_result_never_loads_or_dispatches_again() {
    let fixture = Fixture::new();
    let seed = Fixture::new();
    let source = append_state(&seed, seed.metadata(FormatVersion::V2), 5).await;
    let intent = rewrite_intent(&fixture, &source).await;
    let base = append_state(&seed, source, 7).await;
    let base_id = base.current_snapshot_id().unwrap();
    assert_ne!(intent.start().unwrap().snapshot_id, base_id);

    let publisher = ControlledPublisher::new(&fixture, base, Dispatch::Unknown);
    let report = run(
        &fixture.operation,
        &intent,
        &PreparedChange::default(),
        &[&CompactRewritePreparer {
            operation: fixture.operation.clone(),
            stage: 0,
        }],
        &publisher,
        RetryPolicy {
            retries: 3,
            ..policy()
        },
    )
    .await;
    let PublicationOutcome::Unknown {
        failure, operation, ..
    } = report.publication
    else {
        panic!("expected a lost catalog response to remain unknown: {report:?}");
    };
    assert_eq!(operation, fixture.operation.token());
    assert_eq!(
        failure.failure_fact().reason(),
        ICEBERG_COMMIT_STATE_UNKNOWN
    );
    assert_eq!(
        failure.connector_source().unwrap().public_failure_detail(),
        Some(
            "Failed to commit the transaction during optimize: [CommitUnknown] Lost dispatch response"
        )
    );
    assert_eq!(report.cleanup, IcebergCleanupReport::NotAttempted);
    assert_publication_bases(&publisher, &[base_id]);
}

#[tokio::test]
async fn optimize_cannot_publish_after_its_output_spec_stops_being_default() {
    use crate::iceberg::spec::{Transform, UnboundPartitionSpec};

    let fixture = Fixture::new();
    let seed = Fixture::new();
    let source = append_state(&seed, seed.metadata(FormatVersion::V2), 5).await;
    let intent = rewrite_intent(&fixture, &source).await;
    let original_spec_id = source.default_partition_spec_id();
    let original_head = source.current_snapshot_id();
    let spec = UnboundPartitionSpec::builder()
        .add_partition_field(1, "evolved_partition", Transform::Identity)
        .unwrap()
        .build();
    let evolved = source
        .clone()
        .into_builder(None)
        .add_default_partition_spec(spec)
        .unwrap()
        .build()
        .unwrap()
        .metadata;
    let evolved_spec_id = evolved.default_partition_spec_id();
    assert_ne!(evolved_spec_id, original_spec_id);
    assert!(evolved.partition_spec_by_id(original_spec_id).is_some());
    assert_eq!(evolved.current_snapshot_id(), original_head);

    let publisher = ControlledPublisher::new(
        &fixture,
        source,
        Dispatch::UpdateBeforeRequirements(Box::new(evolved)),
    );
    let report = run(
        &fixture.operation,
        &intent,
        &PreparedChange::default(),
        &[&CompactRewritePreparer {
            operation: fixture.operation.clone(),
            stage: 0,
        }],
        &publisher,
        RetryPolicy {
            retries: 1,
            ..policy()
        },
    )
    .await;
    let PublicationOutcome::KnownUncommitted(failure) = report.publication else {
        panic!("the default spec changed before dispatch: {report:?}");
    };
    assert_eq!(
        failure.failure_fact().reason(),
        ICEBERG_COMMIT_VALIDATION_CONFLICT
    );
    assert_eq!(publisher.loads.load(Ordering::SeqCst), 2);
    let requests = publisher.requests.lock().unwrap();
    assert_eq!(
        requests.len(),
        1,
        "the changed B must stop the retry before staging"
    );
    let old_requirements = requests[0]["requirements"].as_array().unwrap();
    assert!(old_requirements.contains(&serde_json::json!({
        "type": "assert-default-spec-id",
        "default-spec-id": original_spec_id,
    })));
    let actual = publisher.metadata.lock().unwrap();
    assert_eq!(actual.default_partition_spec_id(), evolved_spec_id);
    assert_eq!(actual.current_snapshot_id(), original_head);
}
