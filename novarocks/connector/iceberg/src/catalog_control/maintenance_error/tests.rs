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

use super::*;

#[test]
fn metadata_and_filesystem_errors_keep_their_authored_public_detail() {
    for (error, class, reason, message) in [
        (
            invalid_metadata("Manifest file does not exist: s3://warehouse/snap.avro"),
            FailureClass::Data,
            ICEBERG_INVALID_METADATA,
            "Manifest file does not exist: s3://warehouse/snap.avro",
        ),
        (
            filesystem_failure("Failed removing orphan files for table: ns.t"),
            FailureClass::Dependency,
            ICEBERG_FILESYSTEM_ERROR,
            "Failed removing orphan files for table: ns.t",
        ),
    ] {
        assert_eq!(error.failure_fact(), FailureFact::new(class, reason));
        assert_eq!(error.public_failure_detail(), Some(message));
    }
}

#[test]
fn known_uncommitted_maintenance_keeps_independent_refusals() {
    for error in [
        ConnectorError::new(ConnectorErrorKind::Cancelled, "query canceled"),
        ConnectorError::new(ConnectorErrorKind::DeadlineExceeded, "original deadline"),
        ConnectorError::new(ConnectorErrorKind::ResourceExhausted, "original budget"),
        ConnectorError::new(ConnectorErrorKind::Unsupported, "original capability"),
        ConnectorError::new(ConnectorErrorKind::PermissionDenied, "original grant"),
        invalid_metadata("Manifest file does not exist: missing.avro"),
        filesystem_failure("Unable to list manifest file content from manifest.avro"),
    ] {
        let original = ConnectorMutationFailure::from_connector(error);
        assert_eq!(
            commit_failure(
                MaintenanceCommitProcedure::ExpireSnapshots,
                CommitFailureDisposition::KnownUncommitted,
                original.clone(),
            ),
            original
        );
    }
}

#[test]
fn commit_diagnostic_distinguishes_known_rejection_from_actual_unknown() {
    let rejected = commit_failure(
        MaintenanceCommitProcedure::ExpireSnapshots,
        CommitFailureDisposition::KnownUncommitted,
        ConnectorMutationFailure::new(
            ConnectorMutationFailureKind::Conflict,
            "unpublished internal catalog detail",
        ),
    );
    let original_unknown = ConnectorMutationFailure::from_connector(
        ConnectorError::new(
            ConnectorErrorKind::DeadlineExceeded,
            "catalog reply was lost",
        )
        .with_public_failure_detail(),
    );
    let unknown = commit_failure(
        MaintenanceCommitProcedure::OptimizeManifests,
        CommitFailureDisposition::Unknown,
        original_unknown.clone(),
    );
    let original_cause = unknown
        .connector_source()
        .unwrap()
        .source()
        .unwrap()
        .downcast_ref::<novarocks_failure_contract::SharedFailureCause>()
        .unwrap()
        .downcast_ref::<MaintenanceCommitFailure>()
        .unwrap();
    assert_eq!(original_cause.original, original_unknown);
    for (failure, message) in [
        (
            rejected,
            "Failed to commit during expire_snapshots: catalog commit conflict",
        ),
        (
            unknown,
            "Failed to commit during optimize_manifests: [CommitUnknown] catalog reply was lost",
        ),
    ] {
        assert_eq!(
            failure.failure_fact(),
            FailureFact::new(FailureClass::Dependency, ICEBERG_COMMIT_ERROR)
        );
        assert_eq!(
            failure.connector_source().unwrap().public_failure_detail(),
            Some(message)
        );
    }
}
