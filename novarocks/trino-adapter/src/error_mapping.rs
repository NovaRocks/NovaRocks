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

//! Typed projection of application failures into the Trino 483 client contract.
use crate::diagnostics::{self, BoundedMessage};
use novarocks_execution_contract::failure::FailureProjection;
use novarocks_query_application::{
    api::{QueryExecutionError, QueryExecutionErrorKind},
    cancellation::QueryCancellationReason,
    execution_error::{ExecutionFailureInput, QueryFailureReason},
    session_error::{QueryServiceError, QueryServiceErrorKind},
    session_value::StatementSessionError,
};
use novarocks_user_error::UserError;
use serde::Serialize;
use std::fmt;

/// Each variant records an observed failure; messages never select its class.
pub(crate) enum TrinoErrorSource<'a> {
    Service {
        error: &'a QueryServiceError,
        sql: &'a str,
    },
    Execution {
        error: &'a QueryExecutionError,
        sql: &'a str,
    },
    Session {
        error: &'a StatementSessionError,
        sql: &'a str,
    },
    Cancellation {
        reason: &'a QueryCancellationReason,
        message: &'a str,
    },
    Unsupported {
        message: &'a str,
    },
    RowTooLarge {
        message: &'a str,
    },
    FrontendDraining,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TrinoQueryError {
    pub(crate) message: String,
    pub(crate) error_code: i32,
    pub(crate) error_name: &'static str,
    pub(crate) error_type: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) error_location: Option<TrinoErrorLocation>,
    pub(crate) sql_state: Option<&'static str>,
    pub(crate) failure_info: Option<serde_json::Value>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TrinoErrorLocation {
    pub(crate) line_number: u32,
    pub(crate) column_number: u32,
}

// Numeric identities and error types are frozen by Trino 483 StandardErrorCode.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Code {
    GenericUser,
    Syntax,
    Abandoned,
    UserCanceled,
    PermissionDenied,
    NotFound,
    FunctionNotFound,
    NotSupported,
    InvalidSessionProperty,
    InvalidWindowFrame,
    NumericOutOfRange,
    AdministrativelyKilled,
    SchemaNotFound,
    TableNotFound,
    ColumnNotFound,
    DuplicateNamedQuery,
    TypeMismatch,
    InvalidLiteral,
    InvalidArguments,
    DuplicateWindowName,
    GenericInternal,
    PageTransportError,
    PageTransportTimeout,
    RemoteTaskError,
    ServerShuttingDown,
    ServerStartingUp,
    RemoteHostGone,
    InsufficientResources,
    ExceededTimeLimit,
}
impl Code {
    fn fields(self) -> (i32, &'static str, &'static str) {
        use Code::*;
        match self {
            GenericUser => (0, "GENERIC_USER_ERROR", "USER_ERROR"),
            Syntax => (1, "SYNTAX_ERROR", "USER_ERROR"),
            Abandoned => (2, "ABANDONED_QUERY", "USER_ERROR"),
            UserCanceled => (3, "USER_CANCELED", "USER_ERROR"),
            PermissionDenied => (4, "PERMISSION_DENIED", "USER_ERROR"),
            NotFound => (5, "NOT_FOUND", "USER_ERROR"),
            FunctionNotFound => (6, "FUNCTION_NOT_FOUND", "USER_ERROR"),
            NotSupported => (13, "NOT_SUPPORTED", "USER_ERROR"),
            InvalidSessionProperty => (14, "INVALID_SESSION_PROPERTY", "USER_ERROR"),
            InvalidWindowFrame => (15, "INVALID_WINDOW_FRAME", "USER_ERROR"),
            NumericOutOfRange => (19, "NUMERIC_VALUE_OUT_OF_RANGE", "USER_ERROR"),
            AdministrativelyKilled => (38, "ADMINISTRATIVELY_KILLED", "USER_ERROR"),
            SchemaNotFound => (45, "SCHEMA_NOT_FOUND", "USER_ERROR"),
            TableNotFound => (46, "TABLE_NOT_FOUND", "USER_ERROR"),
            ColumnNotFound => (47, "COLUMN_NOT_FOUND", "USER_ERROR"),
            DuplicateNamedQuery => (53, "DUPLICATE_NAMED_QUERY", "USER_ERROR"),
            TypeMismatch => (58, "TYPE_MISMATCH", "USER_ERROR"),
            InvalidLiteral => (59, "INVALID_LITERAL", "USER_ERROR"),
            InvalidArguments => (76, "INVALID_ARGUMENTS", "USER_ERROR"),
            DuplicateWindowName => (92, "DUPLICATE_WINDOW_NAME", "USER_ERROR"),
            GenericInternal => (65536, "GENERIC_INTERNAL_ERROR", "INTERNAL_ERROR"),
            PageTransportError => (65539, "PAGE_TRANSPORT_ERROR", "INTERNAL_ERROR"),
            PageTransportTimeout => (65540, "PAGE_TRANSPORT_TIMEOUT", "INTERNAL_ERROR"),
            RemoteTaskError => (65542, "REMOTE_TASK_ERROR", "INTERNAL_ERROR"),
            ServerShuttingDown => (65545, "SERVER_SHUTTING_DOWN", "INTERNAL_ERROR"),
            ServerStartingUp => (65548, "SERVER_STARTING_UP", "INTERNAL_ERROR"),
            RemoteHostGone => (65558, "REMOTE_HOST_GONE", "INTERNAL_ERROR"),
            InsufficientResources => (
                131072,
                "GENERIC_INSUFFICIENT_RESOURCES",
                "INSUFFICIENT_RESOURCES",
            ),
            ExceededTimeLimit => (131075, "EXCEEDED_TIME_LIMIT", "INSUFFICIENT_RESOURCES"),
        }
    }
}

/// Pure projection used by mapping tests. Production callers retain their query ID.
#[cfg(test)]
pub(crate) fn query_error(source: TrinoErrorSource<'_>) -> TrinoQueryError {
    project_source(None, source)
}

pub(crate) fn query_error_for_query(
    query_id: &str,
    source: TrinoErrorSource<'_>,
) -> TrinoQueryError {
    project_source(Some(query_id), source)
}

enum PublicMessage<'a> {
    Borrowed(&'a str),
    Session(&'a StatementSessionError),
    Execution(&'a ExecutionFailureInput),
}
impl fmt::Display for PublicMessage<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Borrowed(message) => formatter.write_str(message),
            Self::Session(error) => fmt::Display::fmt(error, formatter),
            // ExecutionFailureInput::user_error has no location and renders
            // exactly this descriptor prefix plus the original safe detail.
            // Borrow both facts instead of cloning the detail into a UserError.
            Self::Execution(input) => write!(
                formatter,
                "[{}] {}",
                input.descriptor().descriptor.code,
                input.detail()
            ),
        }
    }
}
fn project_source(query_id: Option<&str>, source: TrinoErrorSource<'_>) -> TrinoQueryError {
    let (code, message, location) = match source {
        TrinoErrorSource::Service { error, sql } => return service_error(query_id, error, sql),
        TrinoErrorSource::Execution { error, sql: _sql } => {
            let (code, message) = match error.execution_failure() {
                Some(input) => (execution_code(input), PublicMessage::Execution(input)),
                None => (
                    match error.kind() {
                        QueryExecutionErrorKind::InvalidRequest => Code::GenericUser,
                        QueryExecutionErrorKind::Rejected => Code::InsufficientResources,
                        QueryExecutionErrorKind::Cancelled => Code::UserCanceled,
                        QueryExecutionErrorKind::DeadlineExceeded => Code::ExceededTimeLimit,
                        QueryExecutionErrorKind::Failed => Code::GenericInternal,
                    },
                    PublicMessage::Borrowed(error.message()),
                ),
            };
            // ExecutionFailureInput owns no SQL location. Service UserError
            // locations retain their exact original source boundary below.
            (code, message, None)
        }
        TrinoErrorSource::Session { error, sql } => match error {
            StatementSessionError::Service(error) => return service_error(query_id, error, sql),
            StatementSessionError::InvalidSessionProperty { .. } => (
                Code::InvalidSessionProperty,
                PublicMessage::Session(error),
                None,
            ),
            StatementSessionError::TimeZoneNotSupported { .. } => {
                (Code::NotSupported, PublicMessage::Session(error), None)
            }
        },
        TrinoErrorSource::Cancellation { reason, message } => (
            cancellation_code(reason),
            PublicMessage::Borrowed(message),
            None,
        ),
        TrinoErrorSource::Unsupported { message } => {
            (Code::NotSupported, PublicMessage::Borrowed(message), None)
        }
        TrinoErrorSource::RowTooLarge { message } => (
            Code::InsufficientResources,
            PublicMessage::Borrowed(message),
            None,
        ),
        TrinoErrorSource::FrontendDraining => (
            Code::ServerShuttingDown,
            PublicMessage::Borrowed(QueryServiceError::FRONTEND_DRAINING_MESSAGE),
            None,
        ),
    };
    project_message(query_id, code, message, location)
}
fn project_message(
    query_id: Option<&str>,
    code: Code,
    message: PublicMessage<'_>,
    location: Option<TrinoErrorLocation>,
) -> TrinoQueryError {
    let bounded = match &message {
        PublicMessage::Borrowed(message) => diagnostics::bound_message(message),
        _ => diagnostics::bound_display(&message),
    };
    if bounded.truncated
        && let Some(query_id) = query_id
    {
        tracing::warn!(client_query_id = query_id, full_error = %message,
            client_message_json_bytes = bounded.json_bytes,
            max_message_json_bytes = diagnostics::MAX_MESSAGE_JSON_BYTES,
            "Trino client error message truncated; full public diagnostic retained here");
    }
    project(code, bounded, location)
}
fn project(
    code: Code,
    bounded: BoundedMessage,
    error_location: Option<TrinoErrorLocation>,
) -> TrinoQueryError {
    let (error_code, error_name, error_type) = code.fields();
    TrinoQueryError {
        message: bounded.message,
        error_code,
        error_name,
        error_type,
        error_location,
        sql_state: None,
        failure_info: None,
    }
}
fn service_error(query_id: Option<&str>, error: &QueryServiceError, sql: &str) -> TrinoQueryError {
    let code = if let Some(input) = error.execution_failure() {
        execution_code(input)
    } else if let Some(user) = error.user_error() {
        // Unknown stable identities are explicit internal failures. No message
        // or phase heuristic may turn a newly added code into a user error.
        user_code(user.code().as_str()).unwrap_or(Code::GenericInternal)
    } else {
        match error.kind() {
            QueryServiceErrorKind::Parse => Code::Syntax,
            QueryServiceErrorKind::BadDatabase => Code::SchemaNotFound,
            QueryServiceErrorKind::Unsupported => Code::NotSupported,
            QueryServiceErrorKind::PermissionDenied => Code::PermissionDenied,
            QueryServiceErrorKind::NoSuchSession => Code::NotFound,
            QueryServiceErrorKind::Interrupted => Code::UserCanceled,
            QueryServiceErrorKind::Timeout => Code::ExceededTimeLimit,
            QueryServiceErrorKind::InvalidValue => Code::GenericUser,
            QueryServiceErrorKind::Unavailable => Code::ServerStartingUp,
            QueryServiceErrorKind::FrontendDraining => Code::ServerShuttingDown,
            QueryServiceErrorKind::Internal => Code::GenericInternal,
        }
    };
    project_message(
        query_id,
        code,
        PublicMessage::Borrowed(error.message()),
        error.user_error().and_then(|user| user_location(user, sql)),
    )
}

fn cancellation_code(reason: &QueryCancellationReason) -> Code {
    match reason {
        QueryCancellationReason::ExecutionCancellationRequested
        | QueryCancellationReason::ClientCancelRequested => Code::UserCanceled,
        QueryCancellationReason::ExecutionOwnerDropped => Code::GenericInternal,
        QueryCancellationReason::ClientDisconnected => Code::Abandoned,
        QueryCancellationReason::ExplicitKill { .. }
        | QueryCancellationReason::ExplicitKillConnection { .. } => Code::AdministrativelyKilled,
        QueryCancellationReason::DeadlineExceeded { .. } => Code::ExceededTimeLimit,
        QueryCancellationReason::FrontendDrainDeadlineExceeded { .. }
        | QueryCancellationReason::ServerShutdown => Code::ServerShuttingDown,
    }
}

fn execution_code(input: &ExecutionFailureInput) -> Code {
    let descriptor = input.descriptor().descriptor;
    let code = user_code(descriptor.code.as_str()).unwrap_or(Code::GenericInternal);
    if code != Code::RemoteTaskError {
        return code;
    }
    let reason = input.fact().reason();
    // These refinements require an actual Dependency descriptor. Resource and
    // Control facts retain their authored classes even if their reason matches.
    if [
        QueryFailureReason::BackendLost,
        QueryFailureReason::TaskLeaseExpired,
        QueryFailureReason::ObservationLost,
    ]
    .iter()
    .any(|candidate| candidate.failure_fact().reason() == reason)
    {
        Code::RemoteHostGone
    } else if reason
        == QueryFailureReason::TransportDeadline
            .failure_fact()
            .reason()
    {
        Code::PageTransportTimeout
    } else if [
        QueryFailureReason::TransportRefused,
        QueryFailureReason::TransportCapacity,
    ]
    .iter()
    .any(|candidate| candidate.failure_fact().reason() == reason)
    {
        Code::PageTransportError
    } else {
        Code::RemoteTaskError
    }
}

fn user_code(code: &str) -> Option<Code> {
    use Code::*;
    Some(match code {
        "sql.lex.unexpected_character"
        | "sql.lex.unterminated_string"
        | "sql.lex.unterminated_quoted_identifier"
        | "sql.lex.unterminated_comment"
        | "sql.parse.unexpected_token"
        | "sql.validate.invalid_structure" => Syntax,
        "sql.validate.duplicate_cte_name" => DuplicateNamedQuery,
        "sql.validate.duplicate_window_name" => DuplicateWindowName,
        "sql.validate.invalid_window_frame_bounds" => InvalidWindowFrame,
        "sql.analyze.unknown_table" => TableNotFound,
        "sql.analyze.unknown_column" => ColumnNotFound,
        "sql.analyze.unknown_function" => FunctionNotFound,
        "sql.analyze.type_mismatch" => TypeMismatch,
        "sql.analyze.invalid_literal" => InvalidLiteral,
        "sql.analyze.invalid_argument" => InvalidArguments,
        "sql.analyze.invalid_query_shape" | "query.execution.invalid_data" => GenericUser,
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
        | "query.execution.unsupported" => NotSupported,
        "sql.admit.kill_denied" => PermissionDenied,
        "sql.analyze.internal"
        | "query.execution.invariant"
        | "query.execution.unexpected"
        | "query.execution.control"
        | "query.execution.commit_unknown" => GenericInternal,
        "query.execution.numeric_out_of_range" => NumericOutOfRange,
        "query.execution.cancelled" => UserCanceled,
        "query.execution.deadline_exceeded" => ExceededTimeLimit,
        "query.execution.resource_exhausted" | "query.execution.representation" => {
            InsufficientResources
        }
        "query.execution.dependency" => RemoteTaskError,
        _ => return None,
    })
}

fn user_location(error: &UserError, sql: &str) -> Option<TrinoErrorLocation> {
    let location = error.location()?;
    let line = usize::try_from(location.line().checked_sub(1)?).ok()?;
    let column = usize::try_from(location.column().checked_sub(1)?).ok()?;
    let text = sql.split('\n').nth(line)?;
    // UserError owns UTF-8 byte columns. A location must identify a real
    // character boundary inside the original SQL, never a guessed EOF point.
    if column >= text.len() || !text.is_char_boundary(column) {
        return None;
    }
    // Trino CLI indexes Java String.substring with this column. Preserve the
    // exact source byte position while projecting it into UTF-16 code units.
    let utf16_column = u32::try_from(text[..column].encode_utf16().count())
        .ok()?
        .checked_add(1)?;
    Some(TrinoErrorLocation {
        line_number: location.line(),
        column_number: utf16_column,
    })
}

#[cfg(test)]
#[path = "error_tests.rs"]
mod tests;
