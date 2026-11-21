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
use novarocks_failure_contract::{
    FailureClass, FailureFact, FailureProjection,
    connector_reason::{
        COMMIT_TRANSACTION_CONFLICT, ICEBERG_COMMIT_STATE_UNKNOWN,
        ICEBERG_COMMIT_VALIDATION_CONFLICT,
    },
};

#[test]
fn commit_failure_projects_the_original_typed_category_and_message() {
    use novarocks_spi::connector::ConnectorMutationFailureKind::{Conflict, Unavailable};

    let validation = validation_conflict("Cannot commit, missing data files: path");
    let rejected = original_commit_failure(
        CommitOperation::Write,
        CommitFailureKind::Contention { attempts: 3 },
        ConnectorMutationFailure::new(Conflict, "Catalog returned HTTP 409"),
    );
    let unknown = commit_state_unknown(ConnectorMutationFailure::new(
        Unavailable,
        "Catalog response was lost",
    ));

    for (failure, kind, reason, message) in [
        (
            validation,
            Conflict,
            ICEBERG_COMMIT_VALIDATION_CONFLICT,
            "Cannot commit, missing data files: path",
        ),
        (
            rejected,
            Conflict,
            COMMIT_TRANSACTION_CONFLICT,
            "Catalog returned HTTP 409 (after 3 commit attempts)",
        ),
        (
            unknown,
            Unavailable,
            ICEBERG_COMMIT_STATE_UNKNOWN,
            "Catalog response was lost",
        ),
    ] {
        assert_eq!(failure.kind(), kind);
        assert_eq!(failure.message(), message);
        assert_eq!(
            failure.failure_fact(),
            FailureFact::new(FailureClass::Dependency, reason)
        );
        let source = failure.connector_source().unwrap();
        assert_eq!(source.failure_fact().reason(), reason);
        let marker = if reason == ICEBERG_COMMIT_STATE_UNKNOWN {
            "[CommitUnknown] "
        } else {
            ""
        };
        let expected = format!("Failed to commit the transaction during write: {marker}{message}");
        assert_eq!(source.public_failure_detail(), Some(expected.as_str()));
    }
}
