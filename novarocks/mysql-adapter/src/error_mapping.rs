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

//! Stable domain-code to MySQL wire-kind adaptation.

use opensrv_mysql::ErrorKind;

pub fn error_kind_for_domain_code(code: &str) -> Option<ErrorKind> {
    if let Some(descriptor) =
        novarocks_query_application::execution_error::EXECUTION_ERROR_CODE_DESCRIPTORS
            .iter()
            .find(|descriptor| descriptor.code.as_str() == code)
    {
        let execution =
            novarocks_query_application::execution_error::execution_descriptor_for_code(
                descriptor.code,
            )
            .expect("the execution registry resolves each registered descriptor");
        return Some(ErrorKind::from(execution.errno));
    }
    Some(match code {
        "sql.lex.unexpected_character"
        | "sql.lex.unterminated_string"
        | "sql.lex.unterminated_quoted_identifier"
        | "sql.lex.unterminated_comment"
        | "sql.parse.unexpected_token"
        | "sql.validate.invalid_structure"
        | "sql.validate.duplicate_cte_name"
        | "sql.validate.duplicate_window_name"
        | "sql.validate.invalid_window_frame_bounds" => ErrorKind::ER_PARSE_ERROR,
        "sql.parse.unsupported_statement"
        | "sql.admit.delete_requires_where"
        | "sql.admit.delete_unsupported_form"
        | "sql.admit.update_unsupported_form"
        | "sql.admit.merge_unsupported_form"
        | "sql.admit.insert_unsupported_form"
        | "sql.admit.create_table_unsupported_form"
        | "sql.admit.persisted_definition_semantics_unsupported"
        | "sql.admit.session_global_scope_unsupported"
        | "sql.admit.session_transaction_unsupported"
        | "sql.admit.client_type_unsupported"
        | "sql.analyze.unsupported_expression"
        | "sql.analyze.unavailable_implementation"
        | "sql.analyze.unsupported_query_shape" => ErrorKind::ER_NOT_SUPPORTED_YET,
        "sql.admit.kill_denied" => ErrorKind::ER_KILL_DENIED_ERROR,
        "sql.analyze.unknown_table" => ErrorKind::ER_NO_SUCH_TABLE,
        "sql.analyze.unknown_column" => ErrorKind::ER_BAD_FIELD_ERROR,
        "sql.analyze.unknown_function" => ErrorKind::ER_FUNCTION_NOT_DEFINED,
        "sql.analyze.type_mismatch" | "sql.analyze.invalid_argument" => {
            ErrorKind::ER_WRONG_ARGUMENTS
        }
        "sql.analyze.invalid_literal" => ErrorKind::ER_WRONG_VALUE,
        "sql.analyze.invalid_query_shape" => ErrorKind::ER_WRONG_USAGE,
        "sql.analyze.internal" => ErrorKind::ER_UNKNOWN_ERROR,
        _ => return None,
    })
}

/// Encode selected execution input with the same registry used before rows.
/// Untyped application failures retain their existing stream-only fallback.
pub(crate) fn execution_error_kind(
    error: &novarocks_query_application::api::QueryExecutionError,
) -> ErrorKind {
    if let Some(input) = error.execution_failure() {
        return error_kind_for_domain_code(input.descriptor().descriptor.code.as_str())
            .expect("a canonical execution descriptor has a MySQL mapping");
    }
    match error.kind() {
        novarocks_query_application::api::QueryExecutionErrorKind::Cancelled
        | novarocks_query_application::api::QueryExecutionErrorKind::DeadlineExceeded => {
            ErrorKind::ER_QUERY_INTERRUPTED
        }
        _ => ErrorKind::ER_UNKNOWN_ERROR,
    }
}

pub(crate) fn execution_error_message(
    error: &novarocks_query_application::api::QueryExecutionError,
) -> std::borrow::Cow<'_, str> {
    novarocks_query_application::session_error::execution_public_message(error)
}

#[cfg(test)]
#[path = "error_mapping/tests_manifest.rs"]
mod manifest_tests;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_execution_mapping_and_packet_share_wire_values() {
        use novarocks_execution_contract::{
            SafeDetail,
            failure::{FailureClass, FailureFact, FailureProjection, FailureReason, domain},
        };
        use novarocks_query_application::{
            api::{ExecutionFailureInput, ExecutionFailureSite, QueryExecutionError},
            execution_error::QueryFailureReason,
            session_outcome::governed_query_execution_error,
        };
        use std::num::NonZeroU16;
        let authored = |class| {
            FailureFact::new(
                class,
                FailureReason::new(
                    NonZeroU16::new(u16::MAX).unwrap(),
                    NonZeroU16::new(1).unwrap(),
                ),
            )
        };
        let cases = [
            (
                authored(FailureClass::Resource),
                1041,
                "HY000",
                "query.execution.resource_exhausted",
            ),
            (
                FailureFact::new(
                    FailureClass::Data,
                    FailureReason::new(domain::KERNEL, NonZeroU16::new(41).unwrap()),
                ),
                1690,
                "22003",
                "query.execution.numeric_out_of_range",
            ),
            (
                authored(FailureClass::Representation),
                1105,
                "HY000",
                "query.execution.representation",
            ),
            (
                authored(FailureClass::Dependency),
                1105,
                "HY000",
                "query.execution.dependency",
            ),
            (
                authored(FailureClass::Control),
                1105,
                "HY000",
                "query.execution.control",
            ),
            (
                FailureFact::stop_observation(
                    QueryFailureReason::Cancelled.failure_fact().reason(),
                ),
                1105,
                "HY000",
                "query.execution.control",
            ),
            (
                QueryFailureReason::Cancelled.failure_fact(),
                1317,
                "70100",
                "query.execution.cancelled",
            ),
            (
                QueryFailureReason::DeadlineExceeded.failure_fact(),
                1317,
                "70100",
                "query.execution.deadline_exceeded",
            ),
        ]
        .into_iter()
        .chain((1..=4).map(|code| {
            (
                FailureFact::new(
                    FailureClass::Dependency,
                    FailureReason::new(domain::STORAGE_AUTHORITY, NonZeroU16::new(code).unwrap()),
                ),
                1105,
                "HY000",
                "query.execution.dependency",
            )
        }));
        for (fact, errno, sqlstate, code) in cases {
            let input = ExecutionFailureInput::new(
                fact,
                SafeDetail::new("producer diagnostic").unwrap(),
                ExecutionFailureSite::Coordinator,
            );
            let error = QueryExecutionError::from_execution(input.clone());
            let service = governed_query_execution_error(error.clone());
            assert_eq!(service.execution_failure(), Some(&input));
            let startup_and_schema = crate::mysql_error_kind(&service);
            let closing = execution_error_kind(&error);
            assert_eq!(startup_and_schema, closing);
            assert_eq!(closing as u16, errno);
            assert_eq!(closing.sqlstate().as_slice(), sqlstate.as_bytes());
            let projected =
                novarocks_query_application::session_error::execution_public_message(&error);
            assert_eq!(execution_error_message(&error), projected);
            assert_eq!(service.message(), projected.as_ref());
            assert_eq!(error.execution_failure(), Some(&input));
            let payload = crate::relay_result_writer::error_payload(&error, 4096);
            assert_eq!(u16::from_le_bytes([payload[1], payload[2]]), errno);
            assert_eq!(&payload[4..9], sqlstate.as_bytes());
            assert_eq!(
                std::str::from_utf8(&payload[9..]).unwrap(),
                format!("[{code}] producer diagnostic")
            );
        }
    }

    #[test]
    fn authorization_login_and_external_dependency_remain_distinct() {
        use novarocks_query_application::session_error::{
            QueryServiceError, QueryServiceErrorKind,
        };
        assert_eq!(
            crate::mysql_error_kind(&QueryServiceError::new(
                QueryServiceErrorKind::PermissionDenied,
                "SQL authorization denied"
            )) as u16,
            1227
        );
        assert_eq!(
            ErrorKind::ER_SPECIFIC_ACCESS_DENIED_ERROR.sqlstate(),
            b"42000"
        );
        assert_eq!(ErrorKind::ER_TABLEACCESS_DENIED_ERROR as u16, 1142);
        assert_eq!(ErrorKind::ER_TABLEACCESS_DENIED_ERROR.sqlstate(), b"42000");
        assert_eq!(ErrorKind::ER_ACCESS_DENIED_ERROR as u16, 1045);
        assert_eq!(ErrorKind::ER_ACCESS_DENIED_ERROR.sqlstate(), b"28000");
        assert_eq!(
            error_kind_for_domain_code("query.execution.dependency"),
            Some(ErrorKind::ER_UNKNOWN_ERROR)
        );
    }

    #[test]
    fn bounded_execution_payload_does_not_split_utf8() {
        use novarocks_execution_contract::{
            SafeDetail,
            failure::{FailureClass, FailureFact, FailureReason},
        };
        use novarocks_query_application::api::{
            ExecutionFailureInput, ExecutionFailureSite, QueryExecutionError,
        };
        use std::num::NonZeroU16;
        let input = ExecutionFailureInput::new(
            FailureFact::new(
                FailureClass::Representation,
                FailureReason::new(
                    NonZeroU16::new(u16::MAX).unwrap(),
                    NonZeroU16::new(3).unwrap(),
                ),
            ),
            SafeDetail::new("范围").unwrap(),
            ExecutionFailureSite::Coordinator,
        );
        let error = QueryExecutionError::from_execution(input);
        let full = execution_error_message(&error);
        let first_char = full.find('范').unwrap();
        let payload = crate::relay_result_writer::error_payload(&error, first_char + 1);
        assert_eq!(
            std::str::from_utf8(&payload[9..]).unwrap(),
            &full[..first_char]
        );
        assert_eq!(&payload[4..9], b"HY000");
    }

    #[test]
    fn persisted_definition_semantics_has_explicit_unsupported_wire_mapping() {
        assert_eq!(
            error_kind_for_domain_code("sql.admit.persisted_definition_semantics_unsupported"),
            Some(ErrorKind::ER_NOT_SUPPORTED_YET),
        );
    }

    #[test]
    fn unavailable_compiled_implementation_is_an_explicit_unsupported_error() {
        assert_eq!(
            error_kind_for_domain_code("sql.analyze.unavailable_implementation"),
            Some(ErrorKind::ER_NOT_SUPPORTED_YET)
        );
    }

    #[test]
    fn maps_known_codes_and_rejects_unknown_codes() {
        assert_eq!(
            error_kind_for_domain_code("sql.analyze.unknown_table"),
            Some(ErrorKind::ER_NO_SUCH_TABLE)
        );
        assert_eq!(
            error_kind_for_domain_code("sql.admit.kill_denied"),
            Some(ErrorKind::ER_KILL_DENIED_ERROR)
        );
        assert_eq!(error_kind_for_domain_code("sql.analyze.unregistered"), None);
    }
}
