// SPDX-License-Identifier: Apache-2.0

use std::{fmt, sync::Arc};

use novarocks_parser::{Span, ast};
use novarocks_sql::compiler::{PreparedParameterError, SqlPreparedParameters};
use novarocks_user_error::{
    ErrorCodeDescriptor, ErrorCodeId, ErrorCodeStatus, ErrorPhase, RetryClass, UserError,
};

use super::{
    TrinoSessionAction, TrinoStatementAction, select_trino_statement_owner,
    unsupported_statement_detail,
};
use crate::{
    session_error::{QueryServiceError, QueryServiceErrorKind},
    session_value::{PreparedStatementDefinition, PreparedStatementSnapshot},
    sql::parse_single_statement,
};

pub(super) const PREPARED_STATEMENT_NOT_FOUND: ErrorCodeDescriptor = ErrorCodeDescriptor {
    code: ErrorCodeId::new("sql.admit.prepared_statement_not_found"),
    phase: ErrorPhase::Admit,
    status: ErrorCodeStatus::Active,
};

pub(super) const EXPRESSION_NOT_CONSTANT: ErrorCodeDescriptor = ErrorCodeDescriptor {
    code: ErrorCodeId::new("sql.admit.expression_not_constant"),
    phase: ErrorPhase::Admit,
    status: ErrorCodeStatus::Active,
};

pub(super) const INVALID_PARAMETER_USAGE: ErrorCodeDescriptor = ErrorCodeDescriptor {
    code: ErrorCodeId::new("sql.admit.invalid_parameter_usage"),
    phase: ErrorPhase::Admit,
    status: ErrorCodeStatus::Active,
};

/// The source of an effective AST and its location relative to the request,
/// when it was embedded as an immediate SQL string literal.
#[derive(Clone, Eq, PartialEq)]
pub struct TrinoStatementSource {
    request: Arc<str>,
    effective: EffectiveSource,
}

#[derive(Clone, Eq, PartialEq)]
enum EffectiveSource {
    Request,
    Named(PreparedStatementDefinition),
    Immediate { sql: Arc<str>, literal_span: Span },
}

impl fmt::Debug for TrinoStatementSource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let origin = match self.effective {
            EffectiveSource::Request => "request",
            EffectiveSource::Named(_) => "named definition",
            EffectiveSource::Immediate { .. } => "immediate SQL",
        };
        formatter
            .debug_struct("TrinoStatementSource")
            .field("origin", &origin)
            .field("literal_span", &self.immediate_literal_span())
            .finish_non_exhaustive()
    }
}

impl TrinoStatementSource {
    pub fn request(request: Arc<str>) -> Self {
        Self {
            request,
            effective: EffectiveSource::Request,
        }
    }

    fn named(request: Arc<str>, definition: PreparedStatementDefinition) -> Self {
        Self {
            request,
            effective: EffectiveSource::Named(definition),
        }
    }

    fn immediate(request: Arc<str>, sql: Arc<str>, literal_span: Span) -> Self {
        Self {
            request,
            effective: EffectiveSource::Immediate { sql, literal_span },
        }
    }

    pub fn sql(&self) -> &Arc<str> {
        match &self.effective {
            EffectiveSource::Request => &self.request,
            EffectiveSource::Named(definition) => definition.sql(),
            EffectiveSource::Immediate { sql, .. } => sql,
        }
    }

    pub fn request_sql(&self) -> &Arc<str> {
        &self.request
    }

    pub fn immediate_literal_span(&self) -> Option<Span> {
        match &self.effective {
            EffectiveSource::Immediate { literal_span, .. } => Some(*literal_span),
            EffectiveSource::Request | EffectiveSource::Named(_) => None,
        }
    }

    fn parse_effective(&self) -> Result<ast::TrinoStatement, QueryServiceError> {
        parse_single_statement(self.sql()).map_err(|error| {
            QueryServiceError::from_user_error(error.to_user_error(self.sql()))
                .with_trino_statement_source(self.clone())
        })
    }
}

#[derive(Debug)]
pub struct AdmittedTrinoStatement {
    pub source: TrinoStatementSource,
    pub parameters: Option<Arc<SqlPreparedParameters>>,
    pub action: TrinoStatementAction,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TrinoDescriptionKind {
    Input,
    Output,
}

/// The admitted target retains its original preparation owner. Describing it
/// requests declarations and does not authorize executing that action.
#[derive(Debug)]
pub struct TrinoDescriptionAction {
    pub kind: TrinoDescriptionKind,
    pub target: Box<TrinoStatementAction>,
}

// Design: ADR-0258 (docs/adr/ADR-0258-prepared-sql-request-snapshots-and-semantic-authors.md)
/// Open the request wrapper once, preserving the source, effective AST,
/// parameter occurrences and original owner selected for the effective action.
pub fn admit_trino_statement_request(
    request: Arc<str>,
    statement: ast::TrinoStatement,
    snapshot: &PreparedStatementSnapshot,
) -> Result<AdmittedTrinoStatement, QueryServiceError> {
    use ast::{TrinoDescribeOutputInput as DescribeTarget, TrinoStatement as Statement};

    match statement {
        Statement::Prepare(node) => admit_prepare(request, node),
        Statement::Deallocate(node) => {
            let name = node.name.value;
            find_prepared(snapshot, &name)?;
            Ok(admitted_session(
                request,
                TrinoSessionAction::DeallocatePrepare { name },
            ))
        }
        Statement::Execute(node) => {
            let source = TrinoStatementSource::named(
                request,
                find_prepared(snapshot, &node.name.value)?.clone(),
            );
            admit_execute(source, node.parameters)
        }
        Statement::ExecuteImmediate(node) => {
            let source = TrinoStatementSource::immediate(
                request,
                node.statement_text.into(),
                node.statement_text_span,
            );
            admit_execute(source, node.parameters)
        }
        Statement::DescribeInput(node) => {
            let source = TrinoStatementSource::named(
                request,
                find_prepared(snapshot, &node.name.value)?.clone(),
            );
            let target = source.parse_effective()?;
            admit_description(source, TrinoDescriptionKind::Input, target)
        }
        Statement::DescribeOutput(node) => {
            let (source, target) = match node.input {
                DescribeTarget::Prepared(name) => {
                    let source = TrinoStatementSource::named(
                        request,
                        find_prepared(snapshot, &name.value)?.clone(),
                    );
                    let target = source.parse_effective()?;
                    (source, target)
                }
                DescribeTarget::Query(query) => (
                    TrinoStatementSource::request(request),
                    Statement::Query(query.into_inner()),
                ),
            };
            admit_description(source, TrinoDescriptionKind::Output, target)
        }
        statement => {
            let action = admit_ordinary_statement(&request, statement)?;
            Ok(AdmittedTrinoStatement {
                source: TrinoStatementSource::request(request),
                parameters: None,
                action,
            })
        }
    }
}

fn admitted_session(request: Arc<str>, action: TrinoSessionAction) -> AdmittedTrinoStatement {
    AdmittedTrinoStatement {
        source: TrinoStatementSource::request(request),
        parameters: None,
        action: TrinoStatementAction::Session(action),
    }
}

fn find_prepared<'a>(
    snapshot: &'a PreparedStatementSnapshot,
    name: &str,
) -> Result<&'a PreparedStatementDefinition, QueryServiceError> {
    snapshot.get(name).ok_or_else(|| {
        prepared_error(
            PREPARED_STATEMENT_NOT_FOUND,
            format!("Prepared statement not found: {name}"),
            None,
        )
    })
}

fn admit_prepare(
    request: Arc<str>,
    node: ast::TrinoPrepare,
) -> Result<AdmittedTrinoStatement, QueryServiceError> {
    use ast::TrinoStatement as Statement;
    let statement = node.statement.as_ref();
    let span = statement.span();
    let nested = match statement {
        Statement::Prepare(_) => Some("PREPARE"),
        Statement::Execute(_) => Some("EXECUTE"),
        Statement::Deallocate(_) => Some("DEALLOCATE PREPARE"),
        _ => None,
    };
    if let Some(nested) = nested {
        return Err(unsupported_statement_detail(
            &request,
            span,
            format!("Invalid statement type for prepared statement: {nested}"),
        ));
    }
    let sql = request
        .get(span.start()..span.end())
        .filter(|sql| !sql.is_empty())
        .ok_or_else(|| {
            QueryServiceError::new(
                QueryServiceErrorKind::Internal,
                "Prepared statement has no source text at its span",
            )
        })?;
    let sql = Arc::<str>::from(sql);
    Ok(admitted_session(
        request,
        TrinoSessionAction::Prepare {
            name: node.name.value,
            sql,
        },
    ))
}

fn admit_execute(
    source: TrinoStatementSource,
    values: ast::AstVec<ast::Expr>,
) -> Result<AdmittedTrinoStatement, QueryServiceError> {
    let target = source.parse_effective()?;
    refuse_persistent_parameters(source.sql(), &target)
        .map_err(|error| error.with_trino_statement_source(source.clone()))?;
    let parameters = SqlPreparedParameters::bind(
        &target,
        values.into_iter().collect(),
        source.request_sql().clone(),
    )
    .map_err(|error| binding_error(&source, target.span(), error))?;
    let action = select_trino_statement_owner(source.sql(), target)
        .map_err(|error| error.with_trino_statement_source(source.clone()))?;
    Ok(AdmittedTrinoStatement {
        source,
        parameters: Some(Arc::new(parameters)),
        action,
    })
}

fn admit_description(
    source: TrinoStatementSource,
    kind: TrinoDescriptionKind,
    target: ast::TrinoStatement,
) -> Result<AdmittedTrinoStatement, QueryServiceError> {
    refuse_persistent_parameters(source.sql(), &target)
        .map_err(|error| error.with_trino_statement_source(source.clone()))?;
    let parameters = Arc::new(SqlPreparedParameters::describe(&target));
    let action = select_trino_statement_owner(source.sql(), target)
        .map_err(|error| error.with_trino_statement_source(source.clone()))?;
    Ok(AdmittedTrinoStatement {
        source,
        parameters: Some(parameters),
        action: TrinoStatementAction::Describe(TrinoDescriptionAction {
            kind,
            target: Box::new(action),
        }),
    })
}

pub(super) fn admit_ordinary_statement(
    source: &str,
    statement: ast::TrinoStatement,
) -> Result<TrinoStatementAction, QueryServiceError> {
    refuse_persistent_parameters(source, &statement)?;
    let positions = SqlPreparedParameters::positions_in(&statement);
    let span = statement.span();
    let action = select_trino_statement_owner(source, statement)?;
    if !positions.is_empty() {
        return Err(prepared_error(
            INVALID_PARAMETER_USAGE,
            format!(
                "Incorrect number of parameters: expected {} but found 0",
                positions.len()
            ),
            Some((source, span)),
        ));
    }
    Ok(action)
}

fn refuse_persistent_parameters(
    source: &str,
    statement: &ast::TrinoStatement,
) -> Result<(), QueryServiceError> {
    let query = match statement {
        ast::TrinoStatement::CreateView(node) => &node.query,
        ast::TrinoStatement::CreateMaterializedView(node) => &node.query,
        _ => return Ok(()),
    };
    let parameters = SqlPreparedParameters::describe_query(query);
    if let Some(&span) = parameters.positions().first() {
        return Err(unsupported_statement_detail(
            source,
            span,
            "Parameters in persistent VIEW or MATERIALIZED VIEW definitions are not supported"
                .into(),
        ));
    }
    Ok(())
}

fn binding_error(
    source: &TrinoStatementSource,
    target_span: Span,
    error: PreparedParameterError,
) -> QueryServiceError {
    match error {
        PreparedParameterError::Arity { expected, actual } => prepared_error(
            INVALID_PARAMETER_USAGE,
            format!("Incorrect number of parameters: expected {expected} but found {actual}"),
            Some((source.sql(), target_span)),
        )
        .with_trino_statement_source(source.clone()),
        PreparedParameterError::NonConstant(span) => prepared_error(
            EXPRESSION_NOT_CONSTANT,
            "Constant expression cannot contain column references",
            Some((source.request_sql(), span)),
        )
        .with_trino_statement_source(TrinoStatementSource::request(source.request_sql().clone())),
        PreparedParameterError::UnboundArgument(span) => prepared_error(
            INVALID_PARAMETER_USAGE,
            "USING expression contains an unbound parameter",
            Some((source.request_sql(), span)),
        )
        .with_trino_statement_source(TrinoStatementSource::request(source.request_sql().clone())),
    }
}

fn prepared_error(
    descriptor: ErrorCodeDescriptor,
    message: impl Into<String>,
    location: Option<(&str, Span)>,
) -> QueryServiceError {
    QueryServiceError::from_user_error(UserError::from_descriptor(
        descriptor,
        message,
        location
            .filter(|(_, span)| !span.is_empty())
            .map(|(source, span)| span.to_user_error_location(source)),
        RetryClass::Never,
    ))
}
