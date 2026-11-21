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
    FailureReason,
    connector_reason::{
        COMMIT_TRANSACTION_CONFLICT, ICEBERG_COMMIT_STATE_UNKNOWN,
        ICEBERG_COMMIT_VALIDATION_CONFLICT,
    },
};

fn commit_input(reason: FailureReason, detail: &str) -> ExecutionFailureInput {
    ExecutionFailureInput::new(
        FailureFact::new(FailureClass::Dependency, reason),
        SafeDetail::truncating(detail),
        ExecutionFailureSite::Coordinator,
    )
}

#[test]
fn typed_commit_failures_project_the_trino_code_and_original_detail() {
    for (reason, detail, expected, public_detail) in [
        (
            ICEBERG_COMMIT_VALIDATION_CONFLICT,
            "Found conflicting files that can contain records matching true",
            (0x0504_000C, "ICEBERG_COMMIT_ERROR", "EXTERNAL"),
            "Found conflicting files that can contain records matching true",
        ),
        (
            ICEBERG_COMMIT_STATE_UNKNOWN,
            "[CommitUnknown] catalog response was lost",
            (0x0504_000C, "ICEBERG_COMMIT_ERROR", "EXTERNAL"),
            "[CommitUnknown] catalog response was lost",
        ),
        (
            ICEBERG_COMMIT_STATE_UNKNOWN,
            "catalog response was lost",
            (0x0504_000C, "ICEBERG_COMMIT_ERROR", "EXTERNAL"),
            "[CommitUnknown] catalog response was lost",
        ),
        (
            COMMIT_TRANSACTION_CONFLICT,
            "Catalog rejected the commit after four attempts",
            (17, "TRANSACTION_CONFLICT", "USER_ERROR"),
            "Catalog rejected the commit after four attempts",
        ),
    ] {
        let input = commit_input(reason, detail);
        let execution = QueryExecutionError::from_execution(input.clone());
        let service = QueryServiceError::from_execution_failure(input);
        let from_execution = query_error(TrinoErrorSource::Execution {
            error: &execution,
            sql: "DELETE FROM t",
        });
        assert_eq!(
            (
                from_execution.error_code,
                from_execution.error_name,
                from_execution.error_type
            ),
            expected
        );
        assert_eq!(
            from_execution.message,
            format!("Failed to commit the transaction during write: {public_detail}")
        );
        assert_eq!(
            query_error(TrinoErrorSource::Service {
                error: &service,
                sql: "DELETE FROM t"
            }),
            from_execution
        );
    }
}

#[test]
fn typed_unknown_preserves_the_accumulated_effect() {
    let service = QueryServiceError::from_execution_failure(commit_input(
        ICEBERG_COMMIT_STATE_UNKNOWN,
        "original catalog error",
    ))
    .attach_external_effect_disposition(StatementExternalEffectDisposition::CommitUnknown);
    let wire = query_error_for_query_with_effect(
        "query-id",
        TrinoErrorSource::Service {
            error: &service,
            sql: "DELETE FROM t",
        },
        Some(StatementExternalEffectDisposition::KnownUncommitted),
    );
    assert_eq!(wire.error_name, "ICEBERG_COMMIT_ERROR");
    assert!(wire.message.starts_with(
        "Failed to commit the transaction during write: [CommitUnknown] original catalog error"
    ));
    assert_eq!(
        wire.message.matches("external effect disposition=").count(),
        1
    );
    assert!(wire.message.contains("CommitUnknown do_not_retry=true"));
}
