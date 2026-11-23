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

fn original(kind: ConnectorMutationFailureKind) -> ConnectorMutationFailure {
    ConnectorMutationFailure::new(kind, "private catalog detail")
}

#[test]
fn known_conflict_uses_the_operation_style_and_public_detail() {
    for (operation, reason, prefix) in [
        (
            CommitOperation::Write,
            COMMIT_TRANSACTION_CONFLICT,
            "Failed to commit the transaction during write: ",
        ),
        (
            CommitOperation::Optimize,
            COMMIT_TRANSACTION_CONFLICT,
            "Failed to commit the transaction during optimize: ",
        ),
        (
            CommitOperation::Truncate,
            ICEBERG_COMMIT_ERROR,
            "Failed to commit during truncate: ",
        ),
    ] {
        let failure = commit_failure(
            operation,
            CommitFailureKind::Contention { attempts: 2 },
            original(ConnectorMutationFailureKind::Conflict),
            "Catalog compare-and-swap rejected the commit",
        );
        assert_eq!(failure.failure_fact().reason(), reason);
        assert_eq!(failure.kind(), ConnectorMutationFailureKind::Conflict);
        let expected = format!(
            "{prefix}Catalog compare-and-swap rejected the commit (after 2 commit attempts)"
        );
        assert_eq!(
            failure.connector_source().unwrap().public_failure_detail(),
            Some(expected.as_str())
        );
    }
}

#[test]
fn unknown_marker_is_written_exactly_once() {
    let failure = commit_failure(
        CommitOperation::Optimize,
        CommitFailureKind::Unknown,
        original(ConnectorMutationFailureKind::Unavailable),
        "[CommitUnknown] Catalog did not confirm the request",
    );
    assert_eq!(
        failure.failure_fact().reason(),
        ICEBERG_COMMIT_STATE_UNKNOWN
    );
    assert_eq!(
        failure.connector_source().unwrap().public_failure_detail(),
        Some(
            "Failed to commit the transaction during optimize: [CommitUnknown] Catalog did not confirm the request"
        )
    );
}

#[test]
fn validation_conflict_preserves_its_typed_reason() {
    let failure = commit_failure(
        CommitOperation::Optimize,
        CommitFailureKind::Validation,
        original(ConnectorMutationFailureKind::Conflict),
        "Missing required files to delete: data.parquet",
    );
    assert_eq!(
        failure.failure_fact().reason(),
        ICEBERG_COMMIT_VALIDATION_CONFLICT
    );
    assert_eq!(
        failure.connector_source().unwrap().public_failure_detail(),
        Some(
            "Failed to commit the transaction during optimize: Missing required files to delete: data.parquet"
        )
    );
}

#[test]
fn row_delta_commit_retains_public_affixes_outside_the_original_body_bound() {
    let detail = format!("{}é discarded tail", "x".repeat(510));
    let failure = commit_failure(
        CommitOperation::Write,
        CommitFailureKind::Unknown,
        original(ConnectorMutationFailureKind::Unavailable),
        &detail,
    );
    let source = failure.connector_source().unwrap();
    let cause = std::error::Error::source(source)
        .unwrap()
        .downcast_ref::<SharedFailureCause>()
        .unwrap();
    let expected = format!(
        "Failed to commit the transaction during write: [CommitUnknown] {}é",
        "x".repeat(510)
    );

    assert_eq!(failure.message(), detail);
    assert_eq!(cause.prefixed_public_detail(), Some(expected.as_str()));
    assert_eq!(expected.len(), 575);
    assert!(cause.detail().len() <= 512);
}
