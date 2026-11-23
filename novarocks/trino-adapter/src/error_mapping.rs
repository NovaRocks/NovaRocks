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

//! Typed projection of application failures into the Trino client contract.
use novarocks_query_application::api::ExecutionFailurePresentation;

use crate::diagnostics::{self, BoundedMessage};
use novarocks_execution_contract::failure::connector_reason::{
    ICEBERG_COMMIT_ERROR, ICEBERG_FILESYSTEM_ERROR, ICEBERG_INVALID_METADATA,
};
use novarocks_execution_contract::failure::{FailureClass, FailureProjection, domain};
use novarocks_query_application::{
    api::{QueryExecutionError, QueryExecutionErrorKind, StatementExternalEffectDisposition},
    cancellation::QueryCancellationReason,
    execution_error::{ExecutionFailureInput, QueryFailureReason},
    session_error::{QueryServiceError, QueryServiceErrorKind},
    session_value::StatementSessionError,
};
use novarocks_user_error::{ErrorPhase, UserError};
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
    QueryTextTooLarge {
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

// Numeric identities and error types are frozen by Trino StandardErrorCode and IcebergErrorCode.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Code {
    GenericUser,
    Syntax,
    Abandoned,
    UserCanceled,
    PermissionDenied,
    NotFound,
    FunctionNotFound,
    AmbiguousFunctionCall,
    InvalidFunctionArgument,
    DivisionByZero,
    InvalidCastArgument,
    InvalidView,
    AlreadyExists,
    NotSupported,
    InvalidSessionProperty,
    ConstraintViolation,
    NotInTransaction,
    InvalidAnalyzeProperty,
    InvalidCatalogProperty,
    InvalidDefaultColumnValue,
    InvalidSchemaProperty,
    InvalidTableProperty,
    InvalidMaterializedViewProperty,
    InvalidBranchProperty,

    InvalidWindowFrame,
    NumericOutOfRange,
    QueryTextTooLarge,
    AdministrativelyKilled,
    CatalogNotFound,
    SchemaNotFound,
    TableNotFound,
    SchemaAlreadyExists,
    TableAlreadyExists,
    ColumnNotFound,
    DuplicateNamedQuery,
    DuplicateColumnName,
    MissingColumnName,
    TypeMismatch,
    FunctionNotAggregate,
    ExpressionNotScalar,
    InvalidParameterUsage,
    TypeNotFound,
    InvalidLiteral,
    MismatchedColumnAliases,
    InvalidArguments,
    ExpressionNotConstant,
    MissingArgument,
    ProcedureNotFound,
    InvalidProcedureArgument,
    DuplicateProperty,
    TooManyArguments,
    ViewIsStale,
    ViewIsRecursive,
    DuplicateWindowName,
    MergeTargetRowMultipleMatches,
    IcebergInvalidMetadata,
    IcebergTooManyOpenPartitions,
    IcebergFilesystemError,
    IcebergCommitError,
    TransactionConflict,
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
    fn uses_iceberg_maintenance_detail(self) -> bool {
        matches!(
            self,
            Self::IcebergInvalidMetadata | Self::IcebergFilesystemError | Self::IcebergCommitError
        )
    }

    fn fields(self) -> (i32, &'static str, &'static str) {
        const EXTERNAL: &str = "EXTERNAL";
        use Code::*;
        match self {
            GenericUser => (0, "GENERIC_USER_ERROR", "USER_ERROR"),
            Syntax => (1, "SYNTAX_ERROR", "USER_ERROR"),
            Abandoned => (2, "ABANDONED_QUERY", "USER_ERROR"),
            UserCanceled => (3, "USER_CANCELED", "USER_ERROR"),
            PermissionDenied => (4, "PERMISSION_DENIED", "USER_ERROR"),
            NotFound => (5, "NOT_FOUND", "USER_ERROR"),
            FunctionNotFound => (6, "FUNCTION_NOT_FOUND", "USER_ERROR"),
            AmbiguousFunctionCall => (32, "AMBIGUOUS_FUNCTION_CALL", "USER_ERROR"),
            InvalidFunctionArgument => (7, "INVALID_FUNCTION_ARGUMENT", "USER_ERROR"),
            DivisionByZero => (8, "DIVISION_BY_ZERO", "USER_ERROR"),
            InvalidCastArgument => (9, "INVALID_CAST_ARGUMENT", "USER_ERROR"),
            InvalidView => (11, "INVALID_VIEW", "USER_ERROR"),
            AlreadyExists => (12, "ALREADY_EXISTS", "USER_ERROR"),
            NotSupported => (13, "NOT_SUPPORTED", "USER_ERROR"),
            InvalidSessionProperty => (14, "INVALID_SESSION_PROPERTY", "USER_ERROR"),
            NotInTransaction => (21, "NOT_IN_TRANSACTION", "USER_ERROR"),
            InvalidWindowFrame => (15, "INVALID_WINDOW_FRAME", "USER_ERROR"),
            ConstraintViolation => (16, "CONSTRAINT_VIOLATION", "USER_ERROR"),
            NumericOutOfRange => (19, "NUMERIC_VALUE_OUT_OF_RANGE", "USER_ERROR"),
            QueryTextTooLarge => (35, "QUERY_TEXT_TOO_LARGE", "USER_ERROR"),
            AdministrativelyKilled => (38, "ADMINISTRATIVELY_KILLED", "USER_ERROR"),
            InvalidAnalyzeProperty => (42, "INVALID_ANALYZE_PROPERTY", "USER_ERROR"),
            InvalidCatalogProperty => (124, "INVALID_CATALOG_PROPERTY", "USER_ERROR"),
            InvalidDefaultColumnValue => (137, "INVALID_DEFAULT_COLUMN_VALUE", "USER_ERROR"),
            InvalidSchemaProperty => (33, "INVALID_SCHEMA_PROPERTY", "USER_ERROR"),
            InvalidTableProperty => (18, "INVALID_TABLE_PROPERTY", "USER_ERROR"),
            InvalidMaterializedViewProperty => {
                (95, "INVALID_MATERIALIZED_VIEW_PROPERTY", "USER_ERROR")
            }
            InvalidBranchProperty => (136, "INVALID_BRANCH_PROPERTY", "USER_ERROR"),
            CatalogNotFound => (44, "CATALOG_NOT_FOUND", "USER_ERROR"),
            SchemaNotFound => (45, "SCHEMA_NOT_FOUND", "USER_ERROR"),
            TableNotFound => (46, "TABLE_NOT_FOUND", "USER_ERROR"),
            SchemaAlreadyExists => (49, "SCHEMA_ALREADY_EXISTS", "USER_ERROR"),
            TableAlreadyExists => (50, "TABLE_ALREADY_EXISTS", "USER_ERROR"),
            ColumnNotFound => (47, "COLUMN_NOT_FOUND", "USER_ERROR"),
            DuplicateNamedQuery => (53, "DUPLICATE_NAMED_QUERY", "USER_ERROR"),
            DuplicateColumnName => (54, "DUPLICATE_COLUMN_NAME", "USER_ERROR"),
            MissingColumnName => (55, "MISSING_COLUMN_NAME", "USER_ERROR"),
            TypeMismatch => (58, "TYPE_MISMATCH", "USER_ERROR"),
            FunctionNotAggregate => (72, "FUNCTION_NOT_AGGREGATE", "USER_ERROR"),
            ExpressionNotScalar => (74, "EXPRESSION_NOT_SCALAR", "USER_ERROR"),
            TypeNotFound => (43, "TYPE_NOT_FOUND", "USER_ERROR"),
            InvalidLiteral => (59, "INVALID_LITERAL", "USER_ERROR"),
            MismatchedColumnAliases => (61, "MISMATCHED_COLUMN_ALIASES", "USER_ERROR"),
            ExpressionNotConstant => (75, "EXPRESSION_NOT_CONSTANT", "USER_ERROR"),
            MissingArgument => (111, "MISSING_ARGUMENT", "USER_ERROR"),
            InvalidArguments => (76, "INVALID_ARGUMENTS", "USER_ERROR"),
            ProcedureNotFound => (29, "PROCEDURE_NOT_FOUND", "USER_ERROR"),
            InvalidProcedureArgument => (30, "INVALID_PROCEDURE_ARGUMENT", "USER_ERROR"),
            DuplicateProperty => (79, "DUPLICATE_PROPERTY", "USER_ERROR"),
            InvalidParameterUsage => (80, "INVALID_PARAMETER_USAGE", "USER_ERROR"),
            TooManyArguments => (77, "TOO_MANY_ARGUMENTS", "USER_ERROR"),
            ViewIsStale => (81, "VIEW_IS_STALE", "USER_ERROR"),
            ViewIsRecursive => (82, "VIEW_IS_RECURSIVE", "USER_ERROR"),
            DuplicateWindowName => (92, "DUPLICATE_WINDOW_NAME", "USER_ERROR"),
            MergeTargetRowMultipleMatches => {
                (119, "MERGE_TARGET_ROW_MULTIPLE_MATCHES", "USER_ERROR")
            }
            IcebergInvalidMetadata => (0x0504_0001, "ICEBERG_INVALID_METADATA", EXTERNAL),
            IcebergFilesystemError => (0x0504_0008, "ICEBERG_FILESYSTEM_ERROR", EXTERNAL),
            IcebergCommitError => (0x0504_000C, "ICEBERG_COMMIT_ERROR", EXTERNAL),
            IcebergTooManyOpenPartitions => (
                0x0504_0002,
                "ICEBERG_TOO_MANY_OPEN_PARTITIONS",
                "USER_ERROR",
            ),
            TransactionConflict => (17, "TRANSACTION_CONFLICT", "USER_ERROR"),
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
    project_source(None, source, None)
}

pub(crate) fn query_error_for_query(
    query_id: &str,
    source: TrinoErrorSource<'_>,
) -> TrinoQueryError {
    project_source(Some(query_id), source, None)
}

/// Preserve the originating typed source and the original owner's independent
/// cumulative effect fact before applying the protocol's display bound.
pub(crate) fn query_error_for_query_with_effect(
    query_id: &str,
    source: TrinoErrorSource<'_>,
    effect: Option<StatementExternalEffectDisposition>,
) -> TrinoQueryError {
    project_source(Some(query_id), source, effect)
}

enum PublicMessage<'a> {
    Borrowed(&'a str),
    AddFilesUnknown(&'a str),
    Session(&'a StatementSessionError),
    Execution(&'a ExecutionFailureInput),
    OpenPartitionLimit { limit: u64, opening: u64 },
}
impl fmt::Display for PublicMessage<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Borrowed(message) => formatter.write_str(message),
            Self::AddFilesUnknown(message) => write!(formatter, "Failed to add files: {message}"),
            Self::Session(error) => fmt::Display::fmt(error, formatter),
            Self::OpenPartitionLimit { limit, opening } => write!(
                formatter,
                "Exceeded limit of {limit} open writers for partitions: {opening}"
            ),
            // ExecutionFailureInput::user_error has no location and renders
            // exactly this descriptor prefix plus the original safe detail.
            // Borrow both facts instead of cloning the detail into a UserError.
            Self::Execution(input) if input.row_match_violation_message().is_some() => formatter
                .write_str(
                    input
                        .row_match_violation_message()
                        .expect("declared SQL message"),
                ),
            Self::Execution(input) => write!(
                formatter,
                "[{}] {}",
                input.descriptor().descriptor.code,
                input.detail()
            ),
        }
    }
}
fn project_source(
    query_id: Option<&str>,
    source: TrinoErrorSource<'_>,
    effect: Option<StatementExternalEffectDisposition>,
) -> TrinoQueryError {
    let (code, message, location) = match source {
        TrinoErrorSource::Service { error, sql } => {
            return service_error(query_id, error, sql, effect);
        }
        TrinoErrorSource::Execution { error, sql: _sql } => {
            let (code, message) = match error.execution_failure() {
                Some(input) => {
                    let code = execution_code(input);
                    (code, execution_message(input, code))
                }
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
            StatementSessionError::Service(error) => {
                return service_error(query_id, error, sql, effect);
            }
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
        TrinoErrorSource::QueryTextTooLarge { message } => (
            Code::QueryTextTooLarge,
            PublicMessage::Borrowed(message),
            None,
        ),
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
    project_message(query_id, code, message, location, effect)
}
fn project_message(
    query_id: Option<&str>,
    code: Code,
    message: PublicMessage<'_>,
    location: Option<TrinoErrorLocation>,
    effect: Option<StatementExternalEffectDisposition>,
) -> TrinoQueryError {
    if code == Code::MergeTargetRowMultipleMatches {
        return project(code, diagnostics::bound_display(&message), location);
    }
    let public = StatementExternalEffectDisposition::diagnostic(&message, effect);
    project_public_diagnostic(query_id, code, &public, location)
}

fn project_public_diagnostic(
    query_id: Option<&str>,
    code: Code,
    public: &impl fmt::Display,
    location: Option<TrinoErrorLocation>,
) -> TrinoQueryError {
    let bounded = diagnostics::bound_display(public);
    if bounded.truncated
        && let Some(query_id) = query_id
    {
        tracing::warn!(client_query_id = query_id, full_error = %public,
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
fn service_error(
    query_id: Option<&str>,
    error: &QueryServiceError,
    sql: &str,
    effect: Option<StatementExternalEffectDisposition>,
) -> TrinoQueryError {
    if let Some(input) = error.execution_failure() {
        let publication =
            novarocks_query_application::execution_error::data_mutation_publication_effect(
                error,
                "add_files",
            );
        let public_effect = StatementExternalEffectDisposition::combine(
            StatementExternalEffectDisposition::combine(
                error.external_effect_disposition(),
                effect,
            ),
            publication,
        )
        .filter(|effect| effect.do_not_retry());
        if publication == Some(StatementExternalEffectDisposition::CommitUnknown) {
            let message = if add_files_execution_code(input, publication.is_some())
                == Some(Code::IcebergCommitError)
            {
                PublicMessage::Borrowed(input.detail().as_str())
            } else {
                PublicMessage::AddFilesUnknown(input.detail().as_str())
            };
            return project_message(
                query_id,
                Code::IcebergCommitError,
                message,
                None,
                public_effect,
            );
        }
        if let Some(code) = add_files_execution_code(input, publication.is_some()) {
            return project_message(
                query_id,
                code,
                PublicMessage::Borrowed(input.detail().as_str()),
                None,
                public_effect,
            );
        }
        if input.fact().class() == FailureClass::Unsupported
            && novarocks_query_application::execution_error::has_explicit_connector_detail(error)
        {
            return project_message(
                query_id,
                Code::NotSupported,
                PublicMessage::Borrowed(input.detail().as_str()),
                None,
                public_effect,
            );
        }
    }
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
    if code == Code::MergeTargetRowMultipleMatches {
        let message = error
            .execution_failure()
            .and_then(|input| input.row_match_violation_message())
            .or_else(|| {
                error
                    .user_error()
                    .and_then(|user| row_match_message_for_code(user.code().as_str()))
            })
            .expect("declared row match SQL identity has a standard message");
        return project(code, diagnostics::bound_display(message), None);
    }
    if let Some(user) = error.user_error()
        && user.code().as_str() == "sql.admit.procedure_not_found"
    {
        return project_message(
            query_id,
            code,
            PublicMessage::Borrowed(user.message()),
            service_location(error, sql),
            StatementExternalEffectDisposition::combine(
                error.external_effect_disposition(),
                effect,
            ),
        );
    }
    if let Some(input) = error.execution_failure() {
        let has_publication_suffix = commit_failure_code(input).is_some();
        let message = if has_publication_suffix || code == Code::IcebergCommitError {
            novarocks_query_application::execution_error::prefixed_connector_failure_detail(error)
                .map(PublicMessage::Borrowed)
                .unwrap_or_else(|| execution_message(input, code))
        } else {
            execution_message(input, code)
        };
        if has_publication_suffix {
            let public = error.diagnostic_with_execution_detail(&message, effect);
            return project_public_diagnostic(query_id, code, &public, None);
        }
        if code == Code::IcebergTooManyOpenPartitions
            || code.uses_iceberg_maintenance_detail()
            || is_legacy_interval_key_collision(input)
        {
            let effect = StatementExternalEffectDisposition::combine(
                error.external_effect_disposition(),
                effect,
            );
            return project_message(query_id, code, message, None, effect);
        }
    }
    let public = error.diagnostic_with_external_effect(effect);
    project_public_diagnostic(query_id, code, &public, service_location(error, sql))
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

fn row_match_message_for_code(code: &str) -> Option<&'static str> {
    use novarocks_query_application::execution_error::{
        MERGE_TARGET_ROW_MULTIPLE_MATCHES_MESSAGE, UPDATE_TARGET_ROW_MULTIPLE_MATCHES_MESSAGE,
    };
    match code {
        "query.execution.merge_target_row_multiple_matches" => {
            Some(MERGE_TARGET_ROW_MULTIPLE_MATCHES_MESSAGE)
        }
        "query.execution.update_target_row_multiple_matches" => {
            Some(UPDATE_TARGET_ROW_MULTIPLE_MATCHES_MESSAGE)
        }
        _ => None,
    }
}

fn open_partition_limit(input: &ExecutionFailureInput) -> Option<(u64, u64)> {
    use novarocks_execution_contract::failure::{
        ResourceDimension, connector_reason::ICEBERG_TOO_MANY_OPEN_PARTITIONS,
    };
    let fact = input.fact();
    if fact.class() != FailureClass::Resource || fact.reason() != ICEBERG_TOO_MANY_OPEN_PARTITIONS {
        return None;
    }
    let resource = fact.resource()?;
    let (limit, opening) = (resource.limit?, resource.requested?);
    (resource.dimension == ResourceDimension::Items && opening > limit).then_some((limit, opening))
}

fn add_files_execution_code(input: &ExecutionFailureInput, registration: bool) -> Option<Code> {
    use novarocks_execution_contract::failure::connector_reason::*;
    let fact = input.fact();
    if matches!(fact.class(), FailureClass::Resource | FailureClass::Control) {
        return None;
    }
    Some(match fact.reason() {
        ADD_FILES_DISABLED => Code::PermissionDenied,
        PROCEDURE_ARGUMENT_INVALID => Code::InvalidProcedureArgument,
        LOCATION_NOT_FOUND => Code::NotFound,
        SOURCE_TYPE_MISMATCH => Code::TypeMismatch,
        NOT_NULL_VIOLATION => Code::ConstraintViolation,
        ICEBERG_COMMIT_ERROR | ICEBERG_COMMIT_STATE_UNKNOWN if registration => {
            Code::IcebergCommitError
        }
        COMMIT_TRANSACTION_CONFLICT if registration => Code::TransactionConflict,
        _ => return None,
    })
}

fn commit_failure_code(input: &ExecutionFailureInput) -> Option<Code> {
    use novarocks_execution_contract::failure::{
        FailureEvidence,
        connector_reason::{
            COMMIT_TRANSACTION_CONFLICT, ICEBERG_COMMIT_STATE_UNKNOWN,
            ICEBERG_COMMIT_VALIDATION_CONFLICT,
        },
    };
    use novarocks_query_application::execution_error::ExecutionFailureSite;

    let fact = input.fact();
    if fact.class() != FailureClass::Dependency
        || fact.evidence() != FailureEvidence::Originating
        || !matches!(input.site(), ExecutionFailureSite::Coordinator)
    {
        return None;
    }
    match fact.reason() {
        COMMIT_TRANSACTION_CONFLICT => Some(Code::TransactionConflict),
        ICEBERG_COMMIT_VALIDATION_CONFLICT | ICEBERG_COMMIT_STATE_UNKNOWN => {
            Some(Code::IcebergCommitError)
        }
        _ => None,
    }
}

fn is_legacy_interval_key_collision(input: &ExecutionFailureInput) -> bool {
    let fact = input.fact();
    fact.class() == FailureClass::Unsupported
        && fact.reason() == novarocks_result_render::LEGACY_INTERVAL_MAP_KEY_COLLISION
}

fn execution_message(input: &ExecutionFailureInput, code: Code) -> PublicMessage<'_> {
    if is_legacy_interval_key_collision(input) {
        return PublicMessage::Borrowed(
            "Interval map keys collide at millisecond precision; use a client supporting PARAMETRIC_INTERVAL",
        );
    }
    if commit_failure_code(input).is_some() || code.uses_iceberg_maintenance_detail() {
        return PublicMessage::Borrowed(input.detail().as_str());
    }
    match open_partition_limit(input) {
        Some((limit, opening)) => PublicMessage::OpenPartitionLimit { limit, opening },
        None => PublicMessage::Execution(input),
    }
}

fn execution_code(input: &ExecutionFailureInput) -> Code {
    if is_legacy_interval_key_collision(input) {
        return Code::NotSupported;
    }
    if let Some(code) = commit_failure_code(input) {
        return code;
    }
    if open_partition_limit(input).is_some() {
        return Code::IcebergTooManyOpenPartitions;
    }
    if input.row_match_violation_message().is_some() {
        return Code::MergeTargetRowMultipleMatches;
    }
    let fact = input.fact();
    match (fact.class(), fact.reason()) {
        (FailureClass::Data, ICEBERG_INVALID_METADATA) => return Code::IcebergInvalidMetadata,
        (FailureClass::Dependency, ICEBERG_FILESYSTEM_ERROR) => {
            return Code::IcebergFilesystemError;
        }
        (FailureClass::Dependency, ICEBERG_COMMIT_ERROR) => return Code::IcebergCommitError,
        _ => {}
    }
    let descriptor = input.descriptor().descriptor;
    let code = user_code(descriptor.code.as_str()).unwrap_or(Code::GenericInternal);
    // ConnectorMutationFailureKind::AlreadyExists owns the Data/CONNECTOR/6002
    // fact (spi/connector/mutation.rs). Its entity is unspecified, so the wire
    // uses Trino's general ALREADY_EXISTS rather than guessing a table kind.
    // CatalogApplicationErrorKind::AlreadyExists owns Data/METADATA/902.
    if code == Code::GenericUser
        && fact.class() == FailureClass::Data
        && ((fact.reason().domain() == domain::CONNECTOR.get() && fact.reason().code() == 6002)
            || (fact.reason().domain() == domain::METADATA.get() && fact.reason().code() == 902))
    {
        return Code::AlreadyExists;
    }
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
        "query.execution.merge_target_row_multiple_matches"
        | "query.execution.update_target_row_multiple_matches" => MergeTargetRowMultipleMatches,
        "sql.lex.unexpected_character"
        | "sql.lex.unterminated_string"
        | "sql.lex.unterminated_quoted_identifier"
        | "sql.lex.unterminated_comment"
        | "sql.parse.unexpected_token"
        | "sql.validate.invalid_structure" => Syntax,
        "sql.validate.duplicate_cte_name" => DuplicateNamedQuery,
        "sql.validate.duplicate_window_name" => DuplicateWindowName,
        "sql.validate.invalid_window_frame_bounds" => InvalidWindowFrame,
        "sql.analyze.unknown_catalog" => CatalogNotFound,
        "sql.analyze.unknown_schema" => SchemaNotFound,
        "sql.analyze.unknown_table" => TableNotFound,
        "sql.analyze.unknown_column" => ColumnNotFound,
        "sql.analyze.unknown_function" => FunctionNotFound,
        "sql.analyze.ambiguous_function_call" => AmbiguousFunctionCall,
        "sql.analyze.missing_column_name" => MissingColumnName,
        "sql.analyze.duplicate_column_name" => DuplicateColumnName,
        "sql.analyze.mismatched_column_aliases" => MismatchedColumnAliases,
        "sql.analyze.invalid_view" => InvalidView,
        "sql.analyze.view_is_stale" => ViewIsStale,
        "sql.analyze.view_is_recursive" => ViewIsRecursive,
        "sql.analyze.type_mismatch" => TypeMismatch,
        "sql.admit.prepared_statement_not_found" => NotFound,
        "sql.admit.expression_not_constant" => ExpressionNotConstant,
        "sql.admit.invalid_parameter_usage" => InvalidParameterUsage,
        "sql.analyze.function_not_aggregate" => FunctionNotAggregate,
        "sql.analyze.expression_not_scalar" => ExpressionNotScalar,
        "sql.admit.unknown_type" | "sql.analyze.unknown_type" => TypeNotFound,
        "sql.analyze.invalid_literal" => InvalidLiteral,
        "sql.analyze.numeric_out_of_range" => NumericOutOfRange,
        "sql.analyze.invalid_function_argument" => InvalidFunctionArgument,
        "sql.analyze.missing_argument" => MissingArgument,
        "sql.analyze.expression_not_constant" => ExpressionNotConstant,
        "sql.analyze.invalid_argument" => InvalidArguments,
        "sql.analyze.invalid_type_argument" => InvalidFunctionArgument,
        "sql.admit.invalid_table_procedure_arguments" => InvalidArguments,
        "sql.admit.table_procedure_not_found" | "sql.admit.procedure_not_found" => {
            ProcedureNotFound
        }
        "sql.admit.invalid_table_procedure_argument" => InvalidProcedureArgument,
        "sql.admit.duplicate_table_procedure_argument" => DuplicateProperty,
        "sql.analyze.invalid_analyze_property" => InvalidAnalyzeProperty,
        "sql.admit.invalid_catalog_property" => InvalidCatalogProperty,
        "sql.admit.invalid_default_column_value" => InvalidDefaultColumnValue,
        "sql.admit.invalid_schema_property" => InvalidSchemaProperty,
        "sql.admit.invalid_table_property" => InvalidTableProperty,
        "sql.admit.invalid_materialized_view_property" => InvalidMaterializedViewProperty,
        "sql.admit.invalid_analyze_property" => InvalidAnalyzeProperty,
        "sql.admit.invalid_branch_property" => InvalidBranchProperty,
        "sql.admit.invalid_session_property" => InvalidSessionProperty,
        "sql.admit.session_catalog_not_found" => CatalogNotFound,
        "sql.admit.not_in_transaction" => NotInTransaction,
        "sql.admit.schema_already_exists" => SchemaAlreadyExists,
        "sql.admit.table_already_exists" => TableAlreadyExists,
        "sql.analyze.invalid_query_shape" | "query.execution.invalid_data" => GenericUser,
        "sql.parse.unsupported_statement"
        | "sql.admit.unsupported_statement"
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
        | "query.execution.unsupported"
        | "query.execution.unknown_epoch_time_zone" => NotSupported,
        "sql.admit.kill_denied" => PermissionDenied,
        "sql.analyze.internal"
        | "query.execution.invariant"
        | "query.execution.unexpected"
        | "query.execution.control"
        | "query.execution.commit_unknown"
        | "query.execution.date_result_out_of_range" => GenericInternal,
        "query.execution.numeric_out_of_range" => NumericOutOfRange,
        "query.execution.division_by_zero" => DivisionByZero,
        "query.execution.invalid_cast_argument" => InvalidCastArgument,
        "query.execution.invalid_function_argument" => InvalidFunctionArgument,
        "sql.analyze.too_many_arguments" => TooManyArguments,
        "query.execution.cancelled" => UserCanceled,
        "query.execution.deadline_exceeded" => ExceededTimeLimit,
        "query.execution.resource_exhausted" | "query.execution.representation" => {
            InsufficientResources
        }
        "query.execution.dependency" => RemoteTaskError,
        _ => return None,
    })
}

fn service_location(error: &QueryServiceError, sql: &str) -> Option<TrinoErrorLocation> {
    let user = error.user_error()?;
    let Some(source) = error.trino_statement_source() else {
        return user_location(user, sql);
    };
    let mut location = user_location(user, source.sql())?;
    if let Some(span) = source.immediate_literal_span() {
        let anchor = utf16_location(
            &span.to_user_error_location(source.request_sql()),
            source.request_sql(),
        )?;
        let first_line = if matches!(user.phase(), ErrorPhase::Lex | ErrorPhase::Parse) {
            // Trino's parser exceptions use the outer line of the literal.
            anchor.line_number == 1
        } else {
            // Analyzed AST locations use the inner line of the reported node.
            location.line_number == 1
        };
        if first_line {
            location.column_number = location.column_number.checked_add(anchor.column_number)?;
        }
        location.line_number = anchor
            .line_number
            .checked_sub(1)?
            .checked_add(location.line_number)?;
    }
    Some(location)
}

fn user_location(error: &UserError, sql: &str) -> Option<TrinoErrorLocation> {
    utf16_location(error.location()?, sql)
}

fn utf16_location(
    location: &novarocks_user_error::UserErrorLocation,
    sql: &str,
) -> Option<TrinoErrorLocation> {
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
mod tests;

#[cfg(test)]
mod effect_projection_tests {
    use super::*;
    #[test]
    fn original_cause_code_location_and_cumulative_effect_share_one_projection() {
        use StatementExternalEffectDisposition::{
            CommitUnknown as X, KnownCommitted as C, KnownUncommitted as U,
        };
        use novarocks_user_error::{RetryClass, UserErrorLocation};
        let source = QueryServiceError::from_user_error(UserError::from_descriptor(
            novarocks_parser::ERROR_CODE_DESCRIPTORS[4],
            "unexpected token",
            Some(UserErrorLocation::new(1, 8, None, None)),
            RetryClass::Never,
        ));
        let sql = "SELECT ?";
        let original = query_error_for_query(
            "original-query",
            TrinoErrorSource::Service {
                error: &source,
                sql,
            },
        );
        assert!(original.error_location.is_some());
        let attached = source.clone().attach_external_effect_disposition(U);
        let error = query_error_for_query_with_effect(
            "original-query",
            TrinoErrorSource::Service {
                error: &attached,
                sql,
            },
            Some(C),
        );
        assert_eq!(
            (
                error.error_code,
                error.error_name,
                error.error_type,
                error.error_location
            ),
            (
                original.error_code,
                original.error_name,
                original.error_type,
                original.error_location
            )
        );
        assert_eq!(
            error
                .message
                .matches(" (external effect disposition=")
                .count(),
            1
        );
        assert!(error.message.contains("KnownCommitted do_not_retry=true"));
        assert_eq!(
            attached.external_effect_disposition(),
            Some(U),
            "projection does not mutate its original source"
        );
        let canceled = query_error_for_query_with_effect(
            "original-query",
            TrinoErrorSource::Cancellation {
                reason: &QueryCancellationReason::ClientCancelRequested,
                message: "original accepted cancellation",
            },
            Some(X),
        );
        assert_eq!(
            (canceled.error_code, canceled.error_name),
            (3, "USER_CANCELED")
        );
        assert!(canceled.message.contains("CommitUnknown do_not_retry=true"));
        let row = query_error_for_query_with_effect(
            "original-query",
            TrinoErrorSource::RowTooLarge {
                message: "original row refusal",
            },
            Some(C),
        );
        assert_eq!(
            (row.error_code, row.error_name),
            (131072, "GENERIC_INSUFFICIENT_RESOURCES")
        );
        assert!(row.message.starts_with("original row refusal"));
        let no_effect = query_error_for_query_with_effect(
            "original-query",
            TrinoErrorSource::Service {
                error: &source,
                sql,
            },
            None,
        );
        assert_eq!(no_effect, original);
    }
}

#[cfg(test)]
mod legacy_interval_map_projection_tests {
    use super::*;
    use novarocks_execution_contract::{SafeDetail, failure::FailureFact};
    use novarocks_query_application::execution_error::ExecutionFailureSite;

    #[test]
    fn legacy_interval_collision_projects_a_typed_trino_refusal() {
        let fact = FailureFact::new(
            FailureClass::Unsupported,
            novarocks_result_render::LEGACY_INTERVAL_MAP_KEY_COLLISION,
        );
        let input = ExecutionFailureInput::new(
            fact,
            SafeDetail::truncating("private render diagnostic"),
            ExecutionFailureSite::Coordinator,
        );
        let execution = QueryExecutionError::from_execution(input.clone());
        let service = QueryServiceError::from_execution_failure(input);
        let sql =
            "SELECT MAP(ARRAY[INTERVAL '0.0001' SECOND, INTERVAL '0.0002' SECOND], ARRAY[1, 2])";
        for source in [
            TrinoErrorSource::Execution {
                error: &execution,
                sql,
            },
            TrinoErrorSource::Service {
                error: &service,
                sql,
            },
        ] {
            let error = query_error(source);
            assert_eq!(
                (error.error_code, error.error_name, error.error_type),
                (13, "NOT_SUPPORTED", "USER_ERROR")
            );
            assert_eq!(
                error.message,
                "Interval map keys collide at millisecond precision; use a client supporting PARAMETRIC_INTERVAL"
            );
        }
    }
}

#[cfg(test)]
mod session_property_projection_tests {
    use super::*;
    use novarocks_query_application::sql::session_admit::SessionAdmitError;

    #[test]
    fn nominal_session_descriptors_select_14_and_44_for_service_and_session_sources() {
        for (kind, descriptor, number, name) in [
            (
                SessionAdmitError::InvalidSessionProperty,
                "sql.admit.invalid_session_property",
                14,
                "INVALID_SESSION_PROPERTY",
            ),
            (
                SessionAdmitError::CatalogNotFound,
                "sql.admit.session_catalog_not_found",
                44,
                "CATALOG_NOT_FOUND",
            ),
        ] {
            // Identical public detail cannot select these different identities.
            let user = kind.without_location("same original safe detail");
            assert_eq!(user.code().as_str(), descriptor);
            let service = QueryServiceError::from_user_error(user);
            let session = StatementSessionError::Service(service.clone());
            for source in [
                TrinoErrorSource::Service {
                    error: &service,
                    sql: "SET SESSION x = 1",
                },
                TrinoErrorSource::Session {
                    error: &session,
                    sql: "SELECT 1",
                },
            ] {
                let error = query_error(source);
                assert_eq!(
                    (error.error_code, error.error_name, error.error_type),
                    (number, name, "USER_ERROR")
                );
                assert_eq!(error.error_location, None);
            }
            assert_eq!(service.user_error().unwrap().code().as_str(), descriptor);
        }
    }
}

#[cfg(test)]
mod add_files_projection_tests {
    use super::*;
    use novarocks_execution_contract::{
        SafeDetail,
        failure::{FailureFact, connector_reason::*},
    };
    use novarocks_query_application::execution_error::ExecutionFailureSite;

    #[test]
    fn add_files_reason_projects_code_type_and_original_detail() {
        let cases = [
            (
                ADD_FILES_DISABLED,
                4,
                "PERMISSION_DENIED",
                "USER_ERROR",
                "add_files procedure is disabled",
            ),
            (
                PROCEDURE_ARGUMENT_INVALID,
                30,
                "INVALID_PROCEDURE_ARGUMENT",
                "USER_ERROR",
                "Required procedure argument 'location' is missing",
            ),
            (
                LOCATION_NOT_FOUND,
                5,
                "NOT_FOUND",
                "USER_ERROR",
                "Location not found: s3://bucket/source",
            ),
            (
                SOURCE_TYPE_MISMATCH,
                58,
                "TYPE_MISMATCH",
                "USER_ERROR",
                "Target 'id' column is 'int' type, but got source 'string' type",
            ),
            (
                NOT_NULL_VIOLATION,
                16,
                "CONSTRAINT_VIOLATION",
                "USER_ERROR",
                "NULL value not allowed for NOT NULL column: id",
            ),
            (
                ICEBERG_COMMIT_ERROR,
                0x0504_000c,
                "ICEBERG_COMMIT_ERROR",
                "EXTERNAL",
                "Failed to add files: File already exists: s3://bucket/source",
            ),
            (
                ICEBERG_COMMIT_STATE_UNKNOWN,
                0x0504_000c,
                "ICEBERG_COMMIT_ERROR",
                "EXTERNAL",
                "Failed to add files: publication response lost",
            ),
            (
                COMMIT_TRANSACTION_CONFLICT,
                17,
                "TRANSACTION_CONFLICT",
                "USER_ERROR",
                "Failed to add files: catalog rejected the current base",
            ),
        ];
        use novarocks_spi::connector::{
            LakePublicationDisposition, LakePublicationFamily, LakePublicationId,
            LakePublicationMarkerHeader, LakePublicationNextAction, LakePublicationStatementTag,
            LakePublicationTarget, LakePublicationTerminal,
        };
        for (reason, code, name, ty, expected_message) in cases {
            let publication = match reason {
                ICEBERG_COMMIT_STATE_UNKNOWN => Some((
                    LakePublicationDisposition::CommitUnknown,
                    LakePublicationNextAction::InspectPublishedState,
                )),
                ICEBERG_COMMIT_ERROR | COMMIT_TRANSACTION_CONFLICT => Some((
                    LakePublicationDisposition::KnownUncommitted,
                    LakePublicationNextAction::RetryStatement,
                )),
                _ => None,
            };
            let class = if publication.is_some() {
                FailureClass::Dependency
            } else {
                FailureClass::Data
            };
            let input = ExecutionFailureInput::new(
                FailureFact::new(class, reason),
                SafeDetail::truncating(expected_message),
                ExecutionFailureSite::Coordinator,
            );
            let mut error = QueryServiceError::from_execution_failure(input);
            if let Some((disposition, next_action)) = publication {
                error = error.attach_publication_terminal(LakePublicationTerminal::new(
                    LakePublicationMarkerHeader::new(
                        LakePublicationId::new_v7(),
                        LakePublicationFamily::DataMutation,
                    ),
                    LakePublicationTarget::try_new(
                        "ice".to_string(),
                        "db".to_string(),
                        Some("t".to_string()),
                        Some("main".to_string()),
                    )
                    .unwrap(),
                    disposition,
                    next_action,
                    Some(LakePublicationStatementTag::try_new("add_files".to_string()).unwrap()),
                ));
            }
            let projected = query_error(TrinoErrorSource::Service {
                error: &error,
                sql: "",
            });
            assert_eq!(
                (
                    projected.error_code,
                    projected.error_name,
                    projected.error_type
                ),
                (code, name, ty)
            );
            if reason == ICEBERG_COMMIT_STATE_UNKNOWN {
                assert!(
                    projected.message.contains(expected_message),
                    "{}",
                    projected.message
                );
            } else {
                assert_eq!(projected.message, expected_message);
            }
        }
    }

    #[test]
    fn add_files_reason_preserves_the_original_resource_class() {
        let input = ExecutionFailureInput::new(
            FailureFact::new(FailureClass::Resource, ICEBERG_COMMIT_ERROR),
            SafeDetail::truncating("original resource refusal"),
            ExecutionFailureSite::Coordinator,
        );
        let error = QueryServiceError::from_execution_failure(input);
        let projected = query_error(TrinoErrorSource::Service {
            error: &error,
            sql: "",
        });
        assert_eq!(projected.error_name, "GENERIC_INSUFFICIENT_RESOURCES");
        assert_eq!(projected.error_type, "INSUFFICIENT_RESOURCES");
    }
}
