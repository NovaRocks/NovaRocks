// SPDX-License-Identifier: Apache-2.0

//! The SQL contract of the original FE query-management procedure.

use crate::{
    query_control::KillClientQueryError,
    session_error::{QueryServiceError, QueryServiceErrorKind},
};
use novarocks_constant_contract::ConstantValue;
use novarocks_parser::{
    Span,
    ast::{Expr, ObjectName, TrinoCall},
};
use novarocks_sql::compiler::RequiredConstantTarget;
use novarocks_user_error::{
    ErrorCodeDescriptor, ErrorCodeId, ErrorCodeStatus, ErrorPhase, RetryClass, UserError,
};

use super::property_evaluation::{DdlPropertyConstantError, DdlPropertyConstantPort};

#[derive(Clone, Copy)]
pub(super) enum KillQuerySqlError {
    Arguments,
    ArgumentType,
    InvalidArgument,
    Missing,
    NotRunning,
    PermissionDenied,
}

impl KillQuerySqlError {
    pub(super) const fn descriptor(self) -> ErrorCodeDescriptor {
        let ident = match self {
            Self::Arguments => "sql.admit.kill_query_arguments",
            Self::ArgumentType => "sql.admit.kill_query_argument_type",
            Self::InvalidArgument => "sql.admit.kill_query_invalid_argument",
            Self::Missing => "sql.admit.kill_query_target_not_found",
            Self::NotRunning => "sql.admit.kill_query_target_not_running",
            Self::PermissionDenied => "sql.admit.kill_query_permission_denied",
        };
        ErrorCodeDescriptor {
            code: ErrorCodeId::new(ident),
            phase: match self {
                Self::Missing | Self::NotRunning | Self::PermissionDenied => ErrorPhase::Execution,
                _ => ErrorPhase::Admit,
            },
            status: ErrorCodeStatus::Active,
        }
    }

    fn service_error(
        self,
        source_span: Option<(&str, Span)>,
        message: impl Into<String>,
    ) -> QueryServiceError {
        QueryServiceError::from_user_error(UserError::from_descriptor(
            self.descriptor(),
            message,
            source_span.map(|(source, span)| span.to_user_error_location(source)),
            RetryClass::Never,
        ))
    }
}

/// Procedure namespace resolution preserves other CALL routes, including a
/// procedure with the same name in a different catalog.
pub(super) fn resolves_kill_query(
    name: &ObjectName,
    catalog: Option<&str>,
    schema: Option<&str>,
) -> bool {
    let eq = |value: &str, expected: &str| value.eq_ignore_ascii_case(expected);
    let (catalog, schema, procedure) = match name.parts.as_slice() {
        [catalog, schema, procedure] => (
            Some(catalog.value.as_str()),
            Some(schema.value.as_str()),
            procedure,
        ),
        [schema, procedure] => (catalog, Some(schema.value.as_str()), procedure),
        [procedure] => (catalog, schema, procedure),
        _ => return false,
    };
    catalog.is_some_and(|name| eq(name, "system"))
        && schema.is_some_and(|name| eq(name, "runtime"))
        && eq(&procedure.value, "kill_query")
}

/// Ordered inputs remain original expressions until the original statement's
/// scoped constant evaluator applies the procedure's VARCHAR contract.
#[derive(Debug)]
pub struct KillQueryCall {
    query_id: Expr,
    message: Option<Expr>,
    message_first: bool,
}

impl KillQueryCall {
    pub(super) fn bind(source: &str, call: TrinoCall) -> Result<Self, QueryServiceError> {
        let error = |span, message| {
            KillQuerySqlError::Arguments.service_error(Some((source, span)), message)
        };
        let named = call
            .arguments
            .iter()
            .any(|argument| argument.name.is_some());
        if named
            && call
                .arguments
                .iter()
                .any(|argument| argument.name.is_none())
        {
            return Err(error(
                call.span,
                "Named and positional arguments cannot be mixed".into(),
            ));
        }
        let mut arguments = [None, None];
        let mut message_first = false;
        for (position, argument) in call.arguments.into_iter().enumerate() {
            let index = match argument.name {
                Some(name) => {
                    let matches = |value: &str| {
                        if name.quoted {
                            name.value == value
                        } else {
                            name.value.eq_ignore_ascii_case(value)
                        }
                    };
                    if matches("QUERY_ID") {
                        0
                    } else if matches("MESSAGE") {
                        1
                    } else {
                        return Err(error(
                            argument.span,
                            format!("Unknown argument name: {}", name.value),
                        ));
                    }
                }
                None if position < arguments.len() => position,
                None => return Err(error(call.span, "Too many arguments for procedure".into())),
            };
            message_first |= position == 0 && index == 1;
            if arguments[index].replace(argument.value).is_some() {
                let name = ["QUERY_ID", "MESSAGE"][index];
                return Err(error(
                    argument.span,
                    format!("Duplicate procedure argument: {name}"),
                ));
            }
        }
        let [query_id, message] = arguments;
        let query_id = query_id.ok_or_else(|| {
            error(
                call.span,
                "Required procedure argument 'QUERY_ID' is missing".into(),
            )
        })?;
        Ok(Self {
            query_id,
            message,
            message_first,
        })
    }

    pub fn evaluate(
        &self,
        source: &str,
        constants: &mut impl DdlPropertyConstantPort,
    ) -> Result<KillQueryArguments, QueryServiceError> {
        let mut evaluate = |expression: &Expr| {
            constants
                .evaluate(expression, RequiredConstantTarget::Varchar)
                .map_err(|error| match error {
                    DdlPropertyConstantError::InvalidValue {
                        message,
                        cause: None,
                    } => KillQuerySqlError::ArgumentType
                        .service_error(Some((source, expression.span())), message),
                    DdlPropertyConstantError::InvalidValue {
                        cause: Some(cause), ..
                    } => QueryServiceError::from_failure_cause(cause),
                    DdlPropertyConstantError::Failure(error) => error,
                })
        };
        let (query_id, message) = if self.message_first {
            let message = self.message.as_ref().map(&mut evaluate).transpose()?;
            (evaluate(&self.query_id)?, message)
        } else {
            let query_id = evaluate(&self.query_id)?;
            (query_id, self.message.as_ref().map(evaluate).transpose()?)
        };
        if utf8(&query_id)?.is_none() {
            return Err(
                KillQuerySqlError::InvalidArgument.service_error(None, "query_id cannot be null")
            );
        }
        Ok(KillQueryArguments { query_id, message })
    }
}

fn utf8(value: &ConstantValue) -> Result<Option<&str>, QueryServiceError> {
    value
        .try_utf8()
        .map_err(|error| QueryServiceError::new(QueryServiceErrorKind::Internal, error.to_string()))
}

pub struct KillQueryArguments {
    query_id: ConstantValue,
    message: Option<ConstantValue>,
}

impl KillQueryArguments {
    pub fn borrow(&self) -> Result<(&str, Option<&str>), QueryServiceError> {
        let query_id = utf8(&self.query_id)?.expect("non-null validated procedure argument");
        let message = self.message.as_ref().map(utf8).transpose()?.flatten();
        Ok((query_id, message))
    }
}

pub fn control_error(error: KillClientQueryError, query_id: &str) -> QueryServiceError {
    match error {
        KillClientQueryError::InvalidId => KillQuerySqlError::InvalidArgument
            .service_error(None, format!("Invalid query ID: {query_id}")),
        KillClientQueryError::NotFound => KillQuerySqlError::Missing
            .service_error(None, format!("Target query not found: {query_id}")),
        KillClientQueryError::NotRunning => KillQuerySqlError::NotRunning
            .service_error(None, format!("Target query is not running: {query_id}")),
        KillClientQueryError::PermissionDenied => KillQuerySqlError::PermissionDenied
            .service_error(None, format!("Access Denied: Cannot kill query {query_id}")),
        KillClientQueryError::UnknownRequester => QueryServiceError::new(
            QueryServiceErrorKind::Internal,
            "Original management statement is no longer registered",
        ),
        KillClientQueryError::Work(error) => QueryServiceError::new(
            QueryServiceErrorKind::Internal,
            format!("Request target Work cancellation: {error}"),
        ),
    }
}
