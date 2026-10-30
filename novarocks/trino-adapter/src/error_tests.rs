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
use novarocks_execution_contract::{
    SafeDetail,
    failure::{FailureClass, FailureFact},
};
use novarocks_query_application::execution_error::{
    EXECUTION_ERROR_CODE_DESCRIPTORS, ExecutionFailureSite,
};
use novarocks_user_error::{
    ErrorCodeDescriptor, ErrorCodeId, ErrorCodeStatus, ErrorPhase, RetryClass, UserErrorLocation,
};

// Independent oracle transcribed from Trino tag 483 StandardErrorCode.java.
// Each emitted name must have exactly this number and error type.
const TRINO_483: &[(&str, i32, &str)] = &[
    ("GENERIC_USER_ERROR", 0, "USER_ERROR"),
    ("SYNTAX_ERROR", 1, "USER_ERROR"),
    ("ABANDONED_QUERY", 2, "USER_ERROR"),
    ("USER_CANCELED", 3, "USER_ERROR"),
    ("PERMISSION_DENIED", 4, "USER_ERROR"),
    ("NOT_FOUND", 5, "USER_ERROR"),
    ("FUNCTION_NOT_FOUND", 6, "USER_ERROR"),
    ("NOT_SUPPORTED", 13, "USER_ERROR"),
    ("INVALID_SESSION_PROPERTY", 14, "USER_ERROR"),
    ("INVALID_WINDOW_FRAME", 15, "USER_ERROR"),
    ("NUMERIC_VALUE_OUT_OF_RANGE", 19, "USER_ERROR"),
    ("ADMINISTRATIVELY_KILLED", 38, "USER_ERROR"),
    ("SCHEMA_NOT_FOUND", 45, "USER_ERROR"),
    ("TABLE_NOT_FOUND", 46, "USER_ERROR"),
    ("COLUMN_NOT_FOUND", 47, "USER_ERROR"),
    ("DUPLICATE_NAMED_QUERY", 53, "USER_ERROR"),
    ("TYPE_MISMATCH", 58, "USER_ERROR"),
    ("INVALID_LITERAL", 59, "USER_ERROR"),
    ("INVALID_ARGUMENTS", 76, "USER_ERROR"),
    ("DUPLICATE_WINDOW_NAME", 92, "USER_ERROR"),
    ("GENERIC_INTERNAL_ERROR", 65536, "INTERNAL_ERROR"),
    ("PAGE_TRANSPORT_ERROR", 65539, "INTERNAL_ERROR"),
    ("PAGE_TRANSPORT_TIMEOUT", 65540, "INTERNAL_ERROR"),
    ("REMOTE_TASK_ERROR", 65542, "INTERNAL_ERROR"),
    ("SERVER_SHUTTING_DOWN", 65545, "INTERNAL_ERROR"),
    ("SERVER_STARTING_UP", 65548, "INTERNAL_ERROR"),
    ("REMOTE_HOST_GONE", 65558, "INTERNAL_ERROR"),
    (
        "GENERIC_INSUFFICIENT_RESOURCES",
        131072,
        "INSUFFICIENT_RESOURCES",
    ),
    ("EXCEEDED_TIME_LIMIT", 131075, "INSUFFICIENT_RESOURCES"),
];
fn assert_wire(error: &TrinoQueryError, name: &str) {
    let (_, number, kind) = TRINO_483
        .iter()
        .find(|(candidate, _, _)| *candidate == name)
        .expect("name in Trino 483 oracle");
    assert_eq!(
        (error.error_name, error.error_code, error.error_type),
        (name, *number, *kind)
    );
}

fn expected_descriptor_name(code: &str) -> &'static str {
    match code {
        "sql.lex.unexpected_character"
        | "sql.lex.unterminated_string"
        | "sql.lex.unterminated_quoted_identifier"
        | "sql.lex.unterminated_comment"
        | "sql.parse.unexpected_token"
        | "sql.validate.invalid_structure" => "SYNTAX_ERROR",
        "sql.validate.duplicate_cte_name" => "DUPLICATE_NAMED_QUERY",
        "sql.validate.duplicate_window_name" => "DUPLICATE_WINDOW_NAME",
        "sql.validate.invalid_window_frame_bounds" => "INVALID_WINDOW_FRAME",
        "sql.analyze.unknown_table" => "TABLE_NOT_FOUND",
        "sql.analyze.unknown_column" => "COLUMN_NOT_FOUND",
        "sql.analyze.unknown_function" => "FUNCTION_NOT_FOUND",
        "sql.analyze.type_mismatch" => "TYPE_MISMATCH",
        "sql.analyze.invalid_literal" => "INVALID_LITERAL",
        "sql.analyze.invalid_argument" => "INVALID_ARGUMENTS",
        "sql.analyze.invalid_query_shape" | "query.execution.invalid_data" => "GENERIC_USER_ERROR",
        "sql.parse.unsupported_statement"
        | "sql.analyze.unsupported_expression"
        | "sql.analyze.unsupported_query_shape"
        | "sql.analyze.unavailable_implementation"
        | "sql.admit.session_global_scope_unsupported"
        | "sql.admit.session_transaction_unsupported"
        | "sql.admit.kill_connection_unsupported"
        | "sql.admit.persisted_definition_semantics_unsupported"
        | "sql.admit.client_type_unsupported"
        | "sql.admit.delete_requires_where"
        | "sql.admit.delete_unsupported_form"
        | "sql.admit.update_unsupported_form"
        | "sql.admit.merge_unsupported_form"
        | "sql.admit.insert_unsupported_form"
        | "sql.admit.create_table_unsupported_form"
        | "UnsupportedCompiledShape"
        | "query.execution.unsupported" => "NOT_SUPPORTED",
        "sql.admit.kill_denied" => "PERMISSION_DENIED",
        "sql.analyze.internal"
        | "query.execution.invariant"
        | "query.execution.unexpected"
        | "query.execution.control"
        | "query.execution.commit_unknown" => "GENERIC_INTERNAL_ERROR",
        "query.execution.numeric_out_of_range" => "NUMERIC_VALUE_OUT_OF_RANGE",
        "query.execution.cancelled" => "USER_CANCELED",
        "query.execution.deadline_exceeded" => "EXCEEDED_TIME_LIMIT",
        "query.execution.resource_exhausted" | "query.execution.representation" => {
            "GENERIC_INSUFFICIENT_RESOURCES"
        }
        "query.execution.dependency" => "REMOTE_TASK_ERROR",
        _ => {
            panic!("new descriptor {code} requires an explicit Trino mapping and expected contract")
        }
    }
}

#[test]
fn every_registered_user_descriptor_has_an_explicit_trino_mapping() {
    let descriptors = [
        novarocks_parser::ERROR_CODE_DESCRIPTORS,
        novarocks_sql::analyze_error::ERROR_CODE_DESCRIPTORS,
        novarocks_query_application::sql::session_admit::SESSION_ERROR_CODE_DESCRIPTORS,
        novarocks_query_application::sql::dml_admission::DML_ADMISSION_ERROR_CODE_DESCRIPTORS,
        EXECUTION_ERROR_CODE_DESCRIPTORS,
    ];
    let mut seen = std::collections::BTreeSet::new();
    for descriptor in descriptors.into_iter().flatten() {
        assert!(
            seen.insert(descriptor.code.as_str()),
            "duplicate descriptor {}",
            descriptor.code
        );
        let user = UserError::from_descriptor(
            *descriptor,
            "original domain detail",
            None,
            RetryClass::Never,
        );
        let service = QueryServiceError::from_user_error(user);
        let wire = query_error(TrinoErrorSource::Service {
            error: &service,
            sql: "SELECT 1",
        });
        assert!(
            user_code(descriptor.code.as_str()).is_some(),
            "unmapped {}",
            descriptor.code
        );
        assert_wire(&wire, expected_descriptor_name(descriptor.code.as_str()));
        assert_eq!(
            wire.message,
            format!("[{}] original domain detail", descriptor.code)
        );
    }
}

#[test]
fn client_type_unsupported_has_a_frozen_admission_mapping() {
    // Construct through P05's real author; the same descriptor is also covered
    // by the session registry iteration above after wave integration.
    let error = QueryServiceError::from_user_error(
        novarocks_query_application::sql::session_admit::client_type_unsupported_error(
            "column c is unsupported",
        ),
    );
    assert_wire(
        &query_error(TrinoErrorSource::Service {
            error: &error,
            sql: "SELECT c",
        }),
        "NOT_SUPPORTED",
    );
}

#[test]
fn service_kinds_are_typed_and_do_not_inspect_catalog_words() {
    use QueryServiceErrorKind::*;
    for (kind, expected) in [
        (Parse, "SYNTAX_ERROR"),
        (BadDatabase, "SCHEMA_NOT_FOUND"),
        (Unsupported, "NOT_SUPPORTED"),
        (PermissionDenied, "PERMISSION_DENIED"),
        (NoSuchSession, "NOT_FOUND"),
        (Interrupted, "USER_CANCELED"),
        (Timeout, "EXCEEDED_TIME_LIMIT"),
        (InvalidValue, "GENERIC_USER_ERROR"),
        (Unavailable, "SERVER_STARTING_UP"),
        (FrontendDraining, "SERVER_SHUTTING_DOWN"),
        (Internal, "GENERIC_INTERNAL_ERROR"),
    ] {
        let error = QueryServiceError::new(kind, "Catalog missing; syntax unsupported timeout");
        let wire = query_error(TrinoErrorSource::Service {
            error: &error,
            sql: "USE missing.s",
        });
        assert_wire(&wire, expected);
        assert_eq!(wire.message, error.message());
    }
    // No current QueryServiceErrorKind separately identifies a missing catalog.
    // The adapter must preserve the originating generic kind in that case.
}

#[test]
fn unknown_user_identity_is_internal_even_when_message_looks_supported() {
    let error = QueryServiceError::from_user_error(UserError::from_descriptor(
        ErrorCodeDescriptor {
            code: ErrorCodeId::new("sql.analyze.future_code"),
            phase: ErrorPhase::Analyze,
            status: ErrorCodeStatus::Active,
        },
        "unknown table: permission denied",
        None,
        RetryClass::Never,
    ));
    assert_wire(
        &query_error(TrinoErrorSource::Service {
            error: &error,
            sql: "SELECT 1",
        }),
        "GENERIC_INTERNAL_ERROR",
    );
}

#[test]
fn cancellations_preserve_the_original_requester_category() {
    use QueryCancellationReason::*;
    let requester_session =
        novarocks_query_application::session_control::ClientSessionKey::new(7).unwrap();
    for (reason, expected) in [
        (ExecutionCancellationRequested, "USER_CANCELED"),
        (ClientCancelRequested, "USER_CANCELED"),
        (ClientDisconnected, "ABANDONED_QUERY"),
        (ExecutionOwnerDropped, "GENERIC_INTERNAL_ERROR"),
        (
            ExplicitKill { requester_session },
            "ADMINISTRATIVELY_KILLED",
        ),
        (
            ExplicitKillConnection { requester_session },
            "ADMINISTRATIVELY_KILLED",
        ),
        (DeadlineExceeded { timeout_ms: 10 }, "EXCEEDED_TIME_LIMIT"),
        (
            FrontendDrainDeadlineExceeded { timeout_ms: 10 },
            "SERVER_SHUTTING_DOWN",
        ),
        (ServerShutdown, "SERVER_SHUTTING_DOWN"),
    ] {
        assert_wire(
            &query_error(TrinoErrorSource::Cancellation {
                reason: &reason,
                message: "original cancellation detail",
            }),
            expected,
        );
    }
}

fn execution(reason: QueryFailureReason) -> QueryExecutionError {
    QueryExecutionError::from_execution(ExecutionFailureInput::new(
        reason.failure_fact(),
        SafeDetail::truncating("original execution detail"),
        ExecutionFailureSite::Coordinator,
    ))
}
#[test]
fn execution_facts_retain_their_class_before_dependency_refinement() {
    for (reason, expected) in [
        (QueryFailureReason::BackendLost, "REMOTE_HOST_GONE"),
        (QueryFailureReason::ObservationLost, "REMOTE_HOST_GONE"),
        (
            QueryFailureReason::TransportDeadline,
            "PAGE_TRANSPORT_TIMEOUT",
        ),
        (QueryFailureReason::TransportRefused, "PAGE_TRANSPORT_ERROR"),
        (
            QueryFailureReason::RecoveryPreparationBudgetExceeded,
            "REMOTE_TASK_ERROR",
        ),
        (
            QueryFailureReason::TransportCapacity,
            "GENERIC_INSUFFICIENT_RESOURCES",
        ),
        (
            QueryFailureReason::TaskLeaseExpired,
            "GENERIC_INTERNAL_ERROR",
        ),
        (QueryFailureReason::Cancelled, "USER_CANCELED"),
        (QueryFailureReason::DeadlineExceeded, "EXCEEDED_TIME_LIMIT"),
        (QueryFailureReason::CommitUnknown, "GENERIC_INTERNAL_ERROR"),
    ] {
        let error = execution(reason);
        let from_execution = query_error(TrinoErrorSource::Execution {
            error: &error,
            sql: "INSERT INTO t SELECT 1",
        });
        assert_wire(&from_execution, expected);
        assert!(from_execution.message.starts_with('['));
        let service =
            QueryServiceError::from_execution_failure(error.execution_failure().unwrap().clone());
        assert_eq!(
            query_error(TrinoErrorSource::Service {
                error: &service,
                sql: "INSERT INTO t SELECT 1"
            }),
            from_execution
        );
    }
    let error = execution(QueryFailureReason::CommitUnknown);
    let wire = query_error(TrinoErrorSource::Execution {
        error: &error,
        sql: "INSERT INTO t SELECT 1",
    });
    assert_eq!(
        wire.message,
        "[query.execution.commit_unknown] original execution detail"
    );
}

#[test]
fn dependency_refinements_do_not_change_foreign_classes() {
    let reason = QueryFailureReason::BackendLost.failure_fact().reason();
    for (class, expected) in [
        (FailureClass::Unsupported, "NOT_SUPPORTED"),
        (FailureClass::Data, "GENERIC_USER_ERROR"),
        (FailureClass::Resource, "GENERIC_INSUFFICIENT_RESOURCES"),
        (
            FailureClass::Representation,
            "GENERIC_INSUFFICIENT_RESOURCES",
        ),
        (FailureClass::Invariant, "GENERIC_INTERNAL_ERROR"),
        (FailureClass::Unexpected, "GENERIC_INTERNAL_ERROR"),
        (FailureClass::Control, "GENERIC_INTERNAL_ERROR"),
        (FailureClass::Dependency, "REMOTE_HOST_GONE"),
    ] {
        let error = QueryExecutionError::from_execution(ExecutionFailureInput::new(
            FailureFact::new(class, reason),
            SafeDetail::truncating("detail"),
            ExecutionFailureSite::Coordinator,
        ));
        assert_wire(
            &query_error(TrinoErrorSource::Execution {
                error: &error,
                sql: "SELECT 1",
            }),
            expected,
        );
    }
}

#[test]
fn application_execution_errors_are_mapped_without_text_classification() {
    use QueryExecutionErrorKind::*;
    for (kind, expected) in [
        (InvalidRequest, "GENERIC_USER_ERROR"),
        (Rejected, "GENERIC_INSUFFICIENT_RESOURCES"),
        (Cancelled, "USER_CANCELED"),
        (DeadlineExceeded, "EXCEEDED_TIME_LIMIT"),
        (Failed, "GENERIC_INTERNAL_ERROR"),
    ] {
        let error = QueryExecutionError::new(kind, "permission denied timeout");
        assert_wire(
            &query_error(TrinoErrorSource::Execution {
                error: &error,
                sql: "SELECT 1",
            }),
            expected,
        );
    }
}

fn located(line: u32, column: u32) -> QueryServiceError {
    QueryServiceError::from_user_error(UserError::from_descriptor(
        novarocks_parser::ERROR_CODE_DESCRIPTORS[4],
        "unexpected token",
        Some(UserErrorLocation::new(line, column, None, None)),
        RetryClass::Never,
    ))
}
#[test]
fn location_requires_a_positive_real_utf8_boundary_in_the_original_sql() {
    let sql = "SELECT 1;\nSELECT 名";
    let error = located(2, 8);
    let wire = query_error(TrinoErrorSource::Service { error: &error, sql });
    assert_eq!(
        wire.error_location,
        Some(TrinoErrorLocation {
            line_number: 2,
            column_number: 8
        })
    );
    let json = serde_json::to_value(&wire).unwrap();
    assert_eq!(
        json["errorLocation"],
        serde_json::json!({"lineNumber": 2, "columnNumber": 8})
    );
    assert!(json["sqlState"].is_null());
    assert!(json["failureInfo"].is_null());
    for (line, column) in [(0, 1), (1, 0), (3, 1), (1, 50), (2, 9), (2, 11)] {
        let error = located(line, column);
        let wire = query_error(TrinoErrorSource::Service { error: &error, sql });
        assert_eq!(wire.error_location, None, "{line}:{column}");
        assert!(
            serde_json::to_value(wire)
                .unwrap()
                .get("errorLocation")
                .is_none()
        );
    }
    let error = QueryServiceError::new(QueryServiceErrorKind::Parse, "line 2 column 8");
    assert_eq!(
        query_error(TrinoErrorSource::Service { error: &error, sql }).error_location,
        None
    );
}

#[test]
fn location_columns_are_java_utf16_indices_at_the_same_original_character() {
    // The byte-column source contract is different from Java substring's
    // indexing. Multiple Chinese/non-BMP characters also expose CLI OOB risk.
    for (sql, line, byte_column, utf16_column) in [
        ("SELECT '中中中中中', missing", 1, 27, 17),
        ("SELECT '😀😀😀', missing", 1, 24, 18),
        ("-- 中文😀 comment\n  SELECT '中😀中', );;", 2, 24, 18),
    ] {
        let error = located(line, byte_column);
        let wire = query_error(TrinoErrorSource::Service { error: &error, sql });
        assert_eq!(
            wire.error_location,
            Some(TrinoErrorLocation {
                line_number: line,
                column_number: utf16_column,
            }),
            "{sql}"
        );
        let text = sql.split('\n').nth((line - 1) as usize).unwrap();
        assert!(utf16_column - 1 < text.encode_utf16().count() as u32);
        let byte_prefix = &text[..(byte_column - 1) as usize];
        assert_eq!(byte_prefix.encode_utf16().count() as u32, utf16_column - 1);
    }
    for (sql, byte_column) in [("SELECT 名", 9), ("SELECT 😀", 10), ("SELECT 名", 11)] {
        assert_eq!(
            query_error(TrinoErrorSource::Service {
                error: &located(1, byte_column),
                sql
            })
            .error_location,
            None,
            "invalid UTF-8 boundary and EOF remain absent"
        );
    }
}

#[test]
fn session_and_adapter_sources_have_their_own_typed_projection() {
    let error = StatementSessionError::InvalidSessionProperty {
        name: "unknown".into(),
        detail: "not registered".into(),
    };
    assert_wire(
        &query_error(TrinoErrorSource::Session {
            error: &error,
            sql: "SELECT 1",
        }),
        "INVALID_SESSION_PROPERTY",
    );
    let error = StatementSessionError::TimeZoneNotSupported {
        zone: "wrong".into(),
    };
    assert_wire(
        &query_error(TrinoErrorSource::Session {
            error: &error,
            sql: "SELECT 1",
        }),
        "NOT_SUPPORTED",
    );
    let error = StatementSessionError::Service(QueryServiceError::new(
        QueryServiceErrorKind::BadDatabase,
        "missing schema",
    ));
    assert_wire(
        &query_error(TrinoErrorSource::Session {
            error: &error,
            sql: "USE s",
        }),
        "SCHEMA_NOT_FOUND",
    );
    assert_wire(
        &query_error(TrinoErrorSource::Unsupported {
            message: "transaction unsupported",
        }),
        "NOT_SUPPORTED",
    );
    assert_wire(
        &query_error(TrinoErrorSource::RowTooLarge {
            message: "row exceeds page budget",
        }),
        "GENERIC_INSUFFICIENT_RESOURCES",
    );
    assert_wire(
        &query_error(TrinoErrorSource::FrontendDraining),
        "SERVER_SHUTTING_DOWN",
    );
}

#[test]
fn bounded_messages_keep_typed_codes_locations_and_stable_prefixes() {
    use crate::diagnostics::MAX_MESSAGE_JSON_BYTES;
    let descriptor = ErrorCodeDescriptor {
        code: ErrorCodeId::new("query.execution.commit_unknown"),
        phase: ErrorPhase::Execution,
        status: ErrorCodeStatus::Active,
    };
    let original = QueryServiceError::from_user_error(UserError::from_descriptor(
        descriptor,
        format!("{}終", "\u{0000}\"\\😀".repeat(MAX_MESSAGE_JSON_BYTES)),
        Some(UserErrorLocation::new(1, 1, None, None)),
        RetryClass::Never,
    ));
    let projected = query_error(TrinoErrorSource::Service {
        error: &original,
        sql: "INSERT INTO t SELECT 1",
    });
    assert_wire(&projected, "GENERIC_INTERNAL_ERROR");
    assert!(
        projected
            .message
            .starts_with("[query.execution.commit_unknown] ")
    );
    assert!(original.message().starts_with(&projected.message));
    assert!(projected.message.len() < original.message().len());
    assert!(serde_json::to_string(&projected.message).unwrap().len() <= MAX_MESSAGE_JSON_BYTES);
    assert_eq!(
        projected.error_location,
        Some(TrinoErrorLocation {
            line_number: 1,
            column_number: 1
        })
    );
    assert!(
        original.message().ends_with(" at line 1 column 1"),
        "the domain diagnostic remains complete"
    );
    let logged = query_error_for_query(
        "20261028_120000_00001_abcde",
        TrinoErrorSource::Service {
            error: &original,
            sql: "INSERT INTO t SELECT 1",
        },
    );
    assert_eq!(logged, projected);
}

#[test]
fn structured_session_errors_are_bounded_during_formatting() {
    use crate::diagnostics::MAX_MESSAGE_JSON_BYTES;
    for original in [
        StatementSessionError::InvalidSessionProperty {
            name: "query_timeout".into(),
            detail: "\"\n中".repeat(MAX_MESSAGE_JSON_BYTES),
        },
        StatementSessionError::TimeZoneNotSupported {
            zone: "😀\u{001f}".repeat(MAX_MESSAGE_JSON_BYTES),
        },
    ] {
        let projected = query_error(TrinoErrorSource::Session {
            error: &original,
            sql: "SELECT 1",
        });
        assert!(original.to_string().starts_with(&projected.message));
        assert!(serde_json::to_string(&projected.message).unwrap().len() <= MAX_MESSAGE_JSON_BYTES);
        assert!(projected.message.len() < original.to_string().len());
    }
}

#[test]
fn execution_rendering_matches_original_public_author_without_rewriting_effect_suffixes() {
    for reason in [
        QueryFailureReason::CommitUnknown,
        QueryFailureReason::Cancelled,
        QueryFailureReason::BackendLost,
    ] {
        let error = execution(reason);
        let expected = novarocks_query_application::session_error::execution_public_message(&error);
        let projected = query_error(TrinoErrorSource::Execution {
            error: &error,
            sql: "SELECT 1",
        });
        assert_eq!(projected.message, expected.as_ref());
    }
    let input = execution(QueryFailureReason::CommitUnknown)
        .execution_failure()
        .unwrap()
        .clone();
    let error = QueryServiceError::from_execution_failure(input)
        .attach_external_effect_disposition(
            novarocks_query_application::api::StatementExternalEffectDisposition::CommitUnknown,
        );
    let original = error.message();
    assert!(original.contains("external effect disposition=CommitUnknown"));
    assert!(original.contains("do_not_retry=true; do not retry statement"));
    let projected = query_error(TrinoErrorSource::Service {
        error: &error,
        sql: "INSERT INTO t SELECT 1",
    });
    assert_eq!(projected.message, original);
    assert_wire(&projected, "GENERIC_INTERNAL_ERROR");
}

#[test]
fn full_original_diagnostic_and_client_query_id_are_logged_only_when_truncated() {
    use std::{
        collections::BTreeMap,
        sync::{Arc, Mutex},
    };
    #[derive(Clone, Default)]
    struct Capture(Arc<Mutex<Vec<BTreeMap<String, String>>>>);
    impl tracing::Subscriber for Capture {
        fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
            true
        }
        fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
            tracing::span::Id::from_u64(1)
        }
        fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
        fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
        fn event(&self, event: &tracing::Event<'_>) {
            struct Fields(BTreeMap<String, String>);
            impl tracing::field::Visit for Fields {
                fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
                    self.0.insert(field.name().into(), value.into());
                }
                fn record_debug(
                    &mut self,
                    field: &tracing::field::Field,
                    value: &dyn std::fmt::Debug,
                ) {
                    self.0.insert(field.name().into(), format!("{value:?}"));
                }
            }
            let mut fields = Fields(BTreeMap::new());
            event.record(&mut fields);
            self.0.lock().unwrap().push(fields.0);
        }
        fn enter(&self, _: &tracing::span::Id) {}
        fn exit(&self, _: &tracing::span::Id) {}
    }
    let capture = Capture::default();
    let original = QueryServiceError::new(
        QueryServiceErrorKind::Internal,
        format!(
            "{}FULL_ORIGINAL_DIAGNOSTIC_END",
            "\"中\u{0000}".repeat(crate::diagnostics::MAX_MESSAGE_JSON_BYTES)
        ),
    );
    let small = QueryServiceError::new(QueryServiceErrorKind::Internal, "short diagnostic");
    tracing::subscriber::with_default(capture.clone(), || {
        query_error_for_query(
            "20261028_120000_00001_abcde",
            TrinoErrorSource::Service {
                error: &small,
                sql: "SELECT 1",
            },
        );
        query_error_for_query(
            "20261028_120000_00001_abcde",
            TrinoErrorSource::Service {
                error: &original,
                sql: "SELECT 1",
            },
        );
        query_error(TrinoErrorSource::Service {
            error: &original,
            sql: "SELECT 1",
        });
    });
    let events = capture.0.lock().unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0]["client_query_id"], "20261028_120000_00001_abcde");
    assert_eq!(events[0]["full_error"], original.message());
    assert!(events[0]["full_error"].ends_with("FULL_ORIGINAL_DIAGNOSTIC_END"));
}

#[test]
fn execution_control_heavy_safe_detail_stays_complete_when_it_fits_wire_bound() {
    let error = QueryExecutionError::from_execution(ExecutionFailureInput::new(
        QueryFailureReason::CommitUnknown.failure_fact(),
        SafeDetail::truncating(&"\u{0000}".repeat(crate::diagnostics::MAX_MESSAGE_JSON_BYTES)),
        ExecutionFailureSite::Coordinator,
    ));
    let original = novarocks_query_application::session_error::execution_public_message(&error);
    let projected = query_error(TrinoErrorSource::Execution {
        error: &error,
        sql: "INSERT INTO t SELECT 1",
    });
    assert_wire(&projected, "GENERIC_INTERNAL_ERROR");
    assert!(
        projected
            .message
            .starts_with("[query.execution.commit_unknown] ")
    );
    assert!(original.starts_with(&projected.message));
    assert_eq!(original.as_ref(), projected.message);
    assert!(
        serde_json::to_string(&projected.message).unwrap().len()
            <= crate::diagnostics::MAX_MESSAGE_JSON_BYTES
    );
}
