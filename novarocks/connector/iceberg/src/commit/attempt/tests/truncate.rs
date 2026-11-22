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

use super::*;
use crate::commit::filter_delete::FilterDeletePreparer;
use crate::commit::model::CommitProtocol;
use novarocks_failure_contract::FailureProjection;
use novarocks_failure_contract::connector_reason::{
    ICEBERG_COMMIT_ERROR, ICEBERG_COMMIT_STATE_UNKNOWN, ICEBERG_COMMIT_VALIDATION_CONFLICT,
};

fn truncate_intent(fixture: &Fixture, metadata: &TableMetadata) -> OperationIntent {
    OperationIntent::new(OperationIntentParts {
        target: TableTarget {
            ident: crate::iceberg::TableIdent::from_strs(["db", "t"]).unwrap(),
            uuid: Some(metadata.uuid()),
        },
        target_ref: "main".into(),
        start: None,
        changes: FileChanges::default(),
        dependencies: vec![],
        protocol: CommitProtocol::Truncate,
        shape: RequestShape::SnapshotProducing,
        summary: Default::default(),
        token: fixture.operation.token(),
    })
    .unwrap()
}

#[tokio::test]
async fn main_truncate_reprepares_for_the_concurrent_append() {
    let baseline = Fixture::new();
    let metadata = append_state(&baseline, baseline.metadata(FormatVersion::V3), 2).await;
    let competitor = Fixture::new();
    let raced = append_state(&competitor, metadata.clone(), 4).await;
    let new_parent = raced.current_snapshot_id();
    let fixture = Fixture::new();
    let intent = truncate_intent(&fixture, &metadata);
    let publisher =
        ControlledPublisher::new(&fixture, metadata, Dispatch::ConflictThen(Box::new(raced)));
    let report = run(
        &fixture.operation,
        &intent,
        &PreparedChange::default(),
        &[&FilterDeletePreparer::always_true()],
        &publisher,
        policy(),
    )
    .await;
    assert!(matches!(
        report.publication,
        PublicationOutcome::Committed(_)
    ));
    assert_eq!(publisher.requests.lock().unwrap().len(), 2);
    let committed = publisher.metadata.lock().unwrap();
    let snapshot = committed.current_snapshot().unwrap();
    assert_eq!(snapshot.parent_snapshot_id(), new_parent);
    let summary = &snapshot.summary().additional_properties;
    assert_eq!(
        summary.get("deleted-records").map(String::as_str),
        Some("6")
    );
    assert_eq!(summary.get("total-records").map(String::as_str), Some("0"));
}

#[tokio::test]
async fn main_truncate_open_retry_reports_typed_contention_when_exhausted() {
    let fixture = Fixture::new();
    let metadata = fixture.metadata(FormatVersion::V3);
    let intent = truncate_intent(&fixture, &metadata);
    let publisher = ControlledPublisher::new(
        &fixture,
        metadata.clone(),
        Dispatch::ConflictSequence(vec![metadata]),
    );
    let report = run(
        &fixture.operation,
        &intent,
        &PreparedChange::default(),
        &[&FilterDeletePreparer::always_true()],
        &publisher,
        RetryPolicy {
            retries: 0,
            ..policy()
        },
    )
    .await;
    let PublicationOutcome::KnownUncommitted(failure) = report.publication else {
        panic!("explicit catalog contention must be known uncommitted");
    };
    assert_eq!(failure.failure_fact().reason(), ICEBERG_COMMIT_ERROR);
    assert_eq!(
        failure.connector_source().unwrap().public_failure_detail(),
        Some(
            "Failed to commit during truncate: Injected concurrent catalog commit (after 1 commit attempts)"
        )
    );
    assert_eq!(publisher.requests.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn main_truncate_rejects_a_different_table_before_a_second_dispatch() {
    let fixture = Fixture::new();
    let metadata = fixture.metadata(FormatVersion::V3);
    let replacement = fixture.metadata(FormatVersion::V3);
    assert_ne!(metadata.uuid(), replacement.uuid());
    let intent = truncate_intent(&fixture, &metadata);
    let publisher = ControlledPublisher::new(
        &fixture,
        metadata,
        Dispatch::ConflictThen(Box::new(replacement)),
    );
    let report = run(
        &fixture.operation,
        &intent,
        &PreparedChange::default(),
        &[&FilterDeletePreparer::always_true()],
        &publisher,
        policy(),
    )
    .await;
    let PublicationOutcome::KnownUncommitted(failure) = report.publication else {
        panic!("a different table UUID must be known uncommitted");
    };
    assert_eq!(
        failure.failure_fact().reason(),
        ICEBERG_COMMIT_VALIDATION_CONFLICT
    );
    assert_eq!(publisher.requests.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn main_truncate_keeps_unknown_without_a_second_dispatch() {
    let fixture = Fixture::new();
    let metadata = fixture.metadata(FormatVersion::V3);
    let intent = truncate_intent(&fixture, &metadata);
    let publisher = ControlledPublisher::new(&fixture, metadata, Dispatch::Unknown);
    let report = run(
        &fixture.operation,
        &intent,
        &PreparedChange::default(),
        &[&FilterDeletePreparer::always_true()],
        &publisher,
        policy(),
    )
    .await;
    let PublicationOutcome::Unknown { failure, .. } = report.publication else {
        panic!("a lost catalog response leaves an unknown commit");
    };
    assert_eq!(
        failure.failure_fact().reason(),
        ICEBERG_COMMIT_STATE_UNKNOWN
    );
    assert_eq!(
        failure.connector_source().unwrap().public_failure_detail(),
        Some("Failed to commit during truncate: [CommitUnknown] Lost dispatch response")
    );
    assert_eq!(publisher.requests.lock().unwrap().len(), 1);
}
