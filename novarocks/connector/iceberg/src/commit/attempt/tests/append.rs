// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Verifies the atomic INSERT requirements and pre-dispatch identity checks.

use super::*;
use crate::commit::model::CommitProtocol;
use crate::iceberg::spec::{
    NestedField, PrimitiveType, Schema, Transform, Type, UnboundPartitionSpec,
};
use novarocks_failure_contract::{
    FailureProjection,
    connector_reason::{COMMIT_TRANSACTION_CONFLICT, ICEBERG_COMMIT_VALIDATION_CONFLICT},
};

fn insert_intent(fixture: &Fixture, metadata: &TableMetadata) -> OperationIntent {
    let seed = append_intent(fixture, metadata, Vec::new());
    OperationIntent::new(OperationIntentParts {
        target: seed.target().clone(),
        target_ref: seed.target_ref().to_owned(),
        start: seed.start(),
        changes: seed.changes().clone(),
        dependencies: Vec::new(),
        protocol: CommitProtocol::Append {
            output_spec_id: metadata.default_partition_spec_id(),
        },
        shape: RequestShape::SnapshotProducing,
        summary: seed.summary().clone(),
        token: fixture.operation.token(),
    })
    .unwrap()
}

#[tokio::test]
async fn insert_does_not_assert_the_writer_schema() {
    let fixture = Fixture::new();
    let original = fixture.metadata(FormatVersion::V2);
    let intent = insert_intent(&fixture, &original);
    let schema = original.current_schema();
    let fields =
        schema
            .as_struct()
            .fields()
            .iter()
            .cloned()
            .chain([Arc::new(NestedField::optional(
                original.last_column_id() + 1,
                "added_column",
                Type::Primitive(PrimitiveType::Long),
            ))]);
    let altered = original
        .clone()
        .into_builder(None)
        .add_current_schema(Schema::builder().with_fields(fields).build().unwrap())
        .unwrap()
        .build()
        .unwrap()
        .metadata;
    let evolved_schema = altered.current_schema_id();
    assert_ne!(evolved_schema, original.current_schema_id());
    let publisher = ControlledPublisher::new(
        &fixture,
        original,
        Dispatch::UpdateBeforeRequirements(Box::new(altered)),
    );

    let report = run(
        &fixture.operation,
        &intent,
        &PreparedChange::default(),
        &[&FastAppendPreparer],
        &publisher,
        policy(),
    )
    .await;

    assert!(matches!(
        report.publication,
        PublicationOutcome::Committed(_)
    ));
    assert_eq!(
        publisher.metadata.lock().unwrap().current_schema_id(),
        evolved_schema
    );
    let requests = publisher.requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    let requirements = requests[0]["requirements"].as_array().unwrap();
    let requirement_types: Vec<_> = requirements
        .iter()
        .map(|requirement| requirement["type"].as_str().unwrap())
        .collect();
    assert_eq!(
        requirement_types,
        [
            "assert-table-uuid",
            "assert-ref-snapshot-id",
            "assert-default-spec-id"
        ]
    );
}

#[tokio::test]
async fn insert_rechecks_default_spec_after_an_atomic_conflict() {
    let fixture = Fixture::new();
    let original = fixture.metadata(FormatVersion::V2);
    let intent = insert_intent(&fixture, &original);
    let spec = UnboundPartitionSpec::builder()
        .add_partition_field(1, "evolved_partition", Transform::Identity)
        .unwrap()
        .build();
    let evolved = original
        .clone()
        .into_builder(None)
        .add_default_partition_spec(spec)
        .unwrap()
        .build()
        .unwrap()
        .metadata;
    assert_ne!(
        evolved.default_partition_spec_id(),
        original.default_partition_spec_id()
    );
    let spec_id = original.default_partition_spec_id();
    let publisher = ControlledPublisher::new(
        &fixture,
        original,
        Dispatch::UpdateBeforeRequirements(Box::new(evolved)),
    );

    let report = run(
        &fixture.operation,
        &intent,
        &PreparedChange::default(),
        &[&FastAppendPreparer],
        &publisher,
        policy(),
    )
    .await;

    let PublicationOutcome::KnownUncommitted(failure) = report.publication else {
        panic!("changed default spec must prevent INSERT publication");
    };
    assert_eq!(
        failure.failure_fact().reason(),
        ICEBERG_COMMIT_VALIDATION_CONFLICT
    );
    assert_eq!(publisher.loads.load(Ordering::SeqCst), 2);
    let requests = publisher.requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    assert!(
        requests[0]["requirements"]
            .as_array()
            .unwrap()
            .contains(&serde_json::json!({
                "type": "assert-default-spec-id",
                "default-spec-id": spec_id,
            }))
    );
}

#[tokio::test]
async fn insert_classifies_contention_and_replacement() {
    for (replace_target, expected_reason) in [
        (false, COMMIT_TRANSACTION_CONFLICT),
        (true, ICEBERG_COMMIT_VALIDATION_CONFLICT),
    ] {
        let fixture = Fixture::new();
        let original = fixture.metadata(FormatVersion::V2);
        let intent = insert_intent(&fixture, &original);
        let concurrent = if replace_target {
            let replacement = fixture.metadata(FormatVersion::V2);
            assert_ne!(replacement.uuid(), original.uuid());
            replacement
        } else {
            original.clone()
        };
        let publisher = ControlledPublisher::new(
            &fixture,
            original,
            Dispatch::ConflictThen(Box::new(concurrent)),
        );

        let report = run(
            &fixture.operation,
            &intent,
            &PreparedChange::default(),
            &[&FastAppendPreparer],
            &publisher,
            RetryPolicy {
                retries: u32::from(replace_target),
                ..policy()
            },
        )
        .await;

        let PublicationOutcome::KnownUncommitted(failure) = report.publication else {
            panic!("competing INSERT must be known uncommitted");
        };
        assert_eq!(failure.failure_fact().reason(), expected_reason);
        assert_eq!(publisher.requests.lock().unwrap().len(), 1);
    }
}

#[tokio::test]
async fn insert_rejects_a_missing_target_without_dispatch() {
    let fixture = Fixture::new();
    let original = fixture.metadata(FormatVersion::V2);
    let intent = insert_intent(&fixture, &original);
    let mut publisher = ControlledPublisher::new(&fixture, original, Dispatch::Commit);
    publisher.missing_target = true;

    let report = run(
        &fixture.operation,
        &intent,
        &PreparedChange::default(),
        &[&FastAppendPreparer],
        &publisher,
        policy(),
    )
    .await;

    let PublicationOutcome::KnownUncommitted(failure) = report.publication else {
        panic!("missing INSERT target must be known uncommitted");
    };
    assert_eq!(
        failure.failure_fact().reason(),
        ICEBERG_COMMIT_VALIDATION_CONFLICT
    );
    assert!(publisher.requests.lock().unwrap().is_empty());
}

#[test]
fn insert_retry_window_expiry_preserves_recorded_deadline_ownership() {
    let original = ConnectorMutationFailure::new(
        ConnectorMutationFailureKind::DeadlineExceeded,
        "Iceberg commit preparation elapsed",
    );
    let expired_retry_deadline = Instant::now() - Duration::from_secs(1);
    let retry = classify_retry_deadline(
        CommitOperation::Insert,
        original.clone(),
        Some(expired_retry_deadline),
        2,
    );
    assert_eq!(retry.failure_fact().reason(), COMMIT_TRANSACTION_CONFLICT);
    assert_eq!(
        classify_retry_deadline(CommitOperation::Insert, original.clone(), None, 2),
        original
    );
}
