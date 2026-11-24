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
        COMMIT_TRANSACTION_CONFLICT, ICEBERG_COMMIT_ERROR, ICEBERG_COMMIT_STATE_UNKNOWN,
        ICEBERG_COMMIT_VALIDATION_CONFLICT, ICEBERG_TABLE_ALREADY_EXISTS,
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
fn duplicate_iceberg_row_field_projects_duplicate_column_name() {
    let error = QueryServiceError::from_execution_failure(ExecutionFailureInput::new(
        FailureFact::new(FailureClass::Data, ICEBERG_DUPLICATE_ROW_FIELD_NAME),
        SafeDetail::truncating("Iceberg schema contains duplicate ROW field names"),
        ExecutionFailureSite::Coordinator,
    ));
    let mapped = query_error(TrinoErrorSource::Service {
        error: &error,
        sql: "CREATE TABLE lake.inner.t AS SELECT ROW(1 AS a, 2 AS a) AS payload",
    });
    assert_eq!(
        (mapped.error_code, mapped.error_name),
        (54, "DUPLICATE_COLUMN_NAME")
    );
}

#[test]
fn staged_table_exists_projects_the_standard_code_and_exact_message() {
    let detail = "Table lake.inner.orders already exists";
    let input = ExecutionFailureInput::new(
        FailureFact::new(FailureClass::Data, ICEBERG_TABLE_ALREADY_EXISTS),
        SafeDetail::truncating(detail),
        ExecutionFailureSite::Coordinator,
    );
    let execution = QueryExecutionError::from_execution(input.clone());
    let service = QueryServiceError::from_execution_failure(input)
        .attach_external_effect_disposition(StatementExternalEffectDisposition::KnownUncommitted);
    let sql = "CREATE TABLE IF NOT EXISTS orders AS SELECT 1";
    let from_execution = query_error(TrinoErrorSource::Execution {
        error: &execution,
        sql,
    });
    assert_eq!(
        (
            from_execution.error_code,
            from_execution.error_name,
            from_execution.error_type,
            from_execution.message.as_str(),
        ),
        (50, "TABLE_ALREADY_EXISTS", "USER_ERROR", detail)
    );
    assert_eq!(
        query_error(TrinoErrorSource::Service {
            error: &service,
            sql,
        }),
        from_execution
    );
    let with_unknown = query_error_for_query_with_effect(
        "query-id",
        TrinoErrorSource::Service {
            error: &service,
            sql,
        },
        Some(StatementExternalEffectDisposition::CommitUnknown),
    );
    assert_eq!(with_unknown.error_code, 50);
    assert!(with_unknown.message.starts_with(detail));
    assert!(
        with_unknown
            .message
            .contains("CommitUnknown do_not_retry=true")
    );
}

#[test]
fn typed_commit_failures_project_the_trino_code_and_original_detail() {
    for (reason, body, expected) in [
        (
            ICEBERG_COMMIT_VALIDATION_CONFLICT,
            "Found conflicting files that can contain records matching true",
            (0x0504_000C, "ICEBERG_COMMIT_ERROR", "EXTERNAL"),
        ),
        (
            ICEBERG_COMMIT_STATE_UNKNOWN,
            "[CommitUnknown] catalog response was lost",
            (0x0504_000C, "ICEBERG_COMMIT_ERROR", "EXTERNAL"),
        ),
        (
            COMMIT_TRANSACTION_CONFLICT,
            "Catalog rejected the commit after four attempts",
            (17, "TRANSACTION_CONFLICT", "USER_ERROR"),
        ),
    ] {
        let detail = format!("Failed to commit the transaction during write: {body}");
        let input = commit_input(reason, &detail);
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
        assert_eq!(from_execution.message, detail);
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
        "Failed to commit the transaction during write: [CommitUnknown] original catalog error",
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

#[test]
fn maintenance_commit_messages_are_forwarded_as_authored() {
    for (reason, code, message) in [
        (
            COMMIT_TRANSACTION_CONFLICT,
            (17, "TRANSACTION_CONFLICT", "USER_ERROR"),
            "Failed to commit the transaction during optimize: Catalog conflict (after 1 commit attempts)",
        ),
        (
            ICEBERG_COMMIT_VALIDATION_CONFLICT,
            (0x0504_000C, "ICEBERG_COMMIT_ERROR", "EXTERNAL"),
            "Failed to commit the transaction during optimize: Missing required files to delete: old.parquet",
        ),
        (
            ICEBERG_COMMIT_ERROR,
            (0x0504_000C, "ICEBERG_COMMIT_ERROR", "EXTERNAL"),
            "Failed to commit during truncate: Catalog conflict (after 1 commit attempts)",
        ),
        (
            ICEBERG_COMMIT_STATE_UNKNOWN,
            (0x0504_000C, "ICEBERG_COMMIT_ERROR", "EXTERNAL"),
            "Failed to commit during truncate: [CommitUnknown] Catalog did not confirm the commit",
        ),
    ] {
        let input = commit_input(reason, message);
        let service = QueryServiceError::from_execution_failure(input);
        let wire = query_error(TrinoErrorSource::Service {
            error: &service,
            sql: "",
        });
        assert_eq!((wire.error_code, wire.error_name, wire.error_type), code);
        assert_eq!(wire.message, message);
    }
}

#[test]
fn commit_reasons_keep_their_publication_and_effect_diagnostic_paths() {
    use novarocks_spi::connector::{
        LakePublicationDisposition, LakePublicationFamily, LakePublicationId,
        LakePublicationMarkerHeader, LakePublicationNextAction, LakePublicationTarget,
        LakePublicationTerminal,
    };

    for reason in [
        COMMIT_TRANSACTION_CONFLICT,
        ICEBERG_COMMIT_VALIDATION_CONFLICT,
        ICEBERG_COMMIT_STATE_UNKNOWN,
        ICEBERG_COMMIT_ERROR,
    ] {
        let detail = "Provider-authored commit diagnostic";
        let (disposition, next_action, effect) = if reason == ICEBERG_COMMIT_STATE_UNKNOWN {
            (
                LakePublicationDisposition::CommitUnknown,
                LakePublicationNextAction::InspectPublishedState,
                StatementExternalEffectDisposition::CommitUnknown,
            )
        } else {
            (
                LakePublicationDisposition::KnownUncommitted,
                LakePublicationNextAction::RetryStatement,
                StatementExternalEffectDisposition::KnownCommitted,
            )
        };
        let publication_id = LakePublicationId::new_v7();
        let service = QueryServiceError::from_execution_failure(commit_input(reason, detail))
            .attach_external_effect_disposition(effect)
            .attach_publication_terminal(LakePublicationTerminal::new(
                LakePublicationMarkerHeader::new(publication_id, LakePublicationFamily::Write),
                LakePublicationTarget::try_new(
                    "ice".to_owned(),
                    "db".to_owned(),
                    Some("t".to_owned()),
                    Some("main".to_owned()),
                )
                .unwrap(),
                disposition,
                next_action,
                None,
            ));
        let wire = query_error(TrinoErrorSource::Service {
            error: &service,
            sql: "",
        });
        assert!(wire.message.starts_with(detail));
        assert_eq!(
            wire.message.matches("external effect disposition=").count(),
            1
        );
        let publication_suffix = format!("lake publication id={publication_id} family=write");
        assert_eq!(
            wire.message.contains(&publication_suffix),
            reason != ICEBERG_COMMIT_ERROR
        );
    }
}

#[test]
fn retained_provider_detail_preserves_the_full_original_utf8_body_and_suffixes() {
    use novarocks_failure_contract::{BoundedDetail, SharedFailureCause};
    use novarocks_spi::connector::{
        ConnectorError, ConnectorErrorKind, LakePublicationDisposition, LakePublicationFamily,
        LakePublicationId, LakePublicationMarkerHeader, LakePublicationNextAction,
        LakePublicationTarget, LakePublicationTerminal,
    };

    #[derive(Debug)]
    struct ProviderOrigin;

    impl std::fmt::Display for ProviderOrigin {
        fn fmt(&self, _: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            panic!("an explicit public diagnostic must not call its origin formatter")
        }
    }

    impl std::error::Error for ProviderOrigin {}

    impl FailureProjection for ProviderOrigin {
        fn failure_fact(&self) -> FailureFact {
            FailureFact::new(FailureClass::Dependency, ICEBERG_COMMIT_STATE_UNKNOWN)
        }
    }

    let original_body = format!("{}é discarded tail", "x".repeat(510));
    let bounded_body = BoundedDetail::truncating(&original_body);
    assert_eq!(bounded_body.as_str().len(), 512);
    let prefix = "Failed to commit the transaction during write: ";
    let marker = "[CommitUnknown] ";
    let expected = format!("{prefix}{marker}{bounded_body}");
    let cause = SharedFailureCause::from_prefixed_public_detail(
        ProviderOrigin,
        prefix,
        marker,
        bounded_body,
    );
    assert_eq!(cause.prefixed_public_detail(), Some(expected.as_str()));
    assert!(cause.detail().len() <= 512);
    assert!(cause.downcast_ref::<ProviderOrigin>().is_some());
    let connector = ConnectorError::from_domain(ConnectorErrorKind::Unavailable, cause.clone())
        .with_public_failure_detail();
    let original = std::error::Error::source(&connector)
        .unwrap()
        .downcast_ref::<SharedFailureCause>()
        .unwrap();
    assert_eq!(original, &cause);
    let connector_wrapper = SharedFailureCause::from_bounded_detail(
        connector,
        BoundedDetail::truncating(cause.detail()),
    );
    for origin in [cause, connector_wrapper] {
        let publication_id = LakePublicationId::new_v7();
        let service = QueryServiceError::from_failure_cause(origin)
            .attach_external_effect_disposition(StatementExternalEffectDisposition::CommitUnknown)
            .attach_publication_terminal(LakePublicationTerminal::new(
                LakePublicationMarkerHeader::new(publication_id, LakePublicationFamily::Write),
                LakePublicationTarget::try_new(
                    "ice".to_owned(),
                    "db".to_owned(),
                    Some("t".to_owned()),
                    Some("main".to_owned()),
                )
                .unwrap(),
                LakePublicationDisposition::CommitUnknown,
                LakePublicationNextAction::InspectPublishedState,
                None,
            ));
        let wire = query_error(TrinoErrorSource::Service {
            error: &service,
            sql: "DELETE FROM t WHERE id = 1",
        });
        assert_eq!(wire.error_name, "ICEBERG_COMMIT_ERROR");
        let suffix = wire
            .message
            .strip_prefix(&expected)
            .expect("the author's complete UTF-8 body reaches Trino");
        assert!(suffix.starts_with(&format!(" (lake publication id={publication_id}")));
        assert!(suffix.ends_with(
            " (external effect disposition=CommitUnknown do_not_retry=true; do not retry statement)"
        ));
    }
}
