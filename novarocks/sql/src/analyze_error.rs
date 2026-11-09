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

//! SQL analyze-domain user errors and their stable descriptors.
// Design: ADR-0095 (docs/adr/ADR-0095-typed-sql-analyze-errors-and-wire-mapping.md)

use std::fmt;

use novarocks_parser::Span;
use novarocks_user_error::{
    ErrorCodeDescriptor, ErrorCodeId, ErrorCodeStatus, ErrorPhase, RetryClass, UserError,
};

const UNKNOWN_TABLE: ErrorCodeDescriptor = ErrorCodeDescriptor {
    code: ErrorCodeId::new("sql.analyze.unknown_table"),
    phase: ErrorPhase::Analyze,
    status: ErrorCodeStatus::Active,
};
const UNKNOWN_COLUMN: ErrorCodeDescriptor = ErrorCodeDescriptor {
    code: ErrorCodeId::new("sql.analyze.unknown_column"),
    phase: ErrorPhase::Analyze,
    status: ErrorCodeStatus::Active,
};
const UNKNOWN_FUNCTION: ErrorCodeDescriptor = ErrorCodeDescriptor {
    code: ErrorCodeId::new("sql.analyze.unknown_function"),
    phase: ErrorPhase::Analyze,
    status: ErrorCodeStatus::Active,
};
const TYPE_MISMATCH: ErrorCodeDescriptor = ErrorCodeDescriptor {
    code: ErrorCodeId::new("sql.analyze.type_mismatch"),
    phase: ErrorPhase::Analyze,
    status: ErrorCodeStatus::Active,
};
const INVALID_LITERAL: ErrorCodeDescriptor = ErrorCodeDescriptor {
    code: ErrorCodeId::new("sql.analyze.invalid_literal"),
    phase: ErrorPhase::Analyze,
    status: ErrorCodeStatus::Active,
};
const INVALID_ARGUMENT: ErrorCodeDescriptor = ErrorCodeDescriptor {
    code: ErrorCodeId::new("sql.analyze.invalid_argument"),
    phase: ErrorPhase::Analyze,
    status: ErrorCodeStatus::Active,
};
const TOO_MANY_ARGUMENTS: ErrorCodeDescriptor = ErrorCodeDescriptor {
    code: ErrorCodeId::new("sql.analyze.too_many_arguments"),
    phase: ErrorPhase::Analyze,
    status: ErrorCodeStatus::Active,
};
const INVALID_QUERY_SHAPE: ErrorCodeDescriptor = ErrorCodeDescriptor {
    code: ErrorCodeId::new("sql.analyze.invalid_query_shape"),
    phase: ErrorPhase::Analyze,
    status: ErrorCodeStatus::Active,
};
const UNSUPPORTED_EXPRESSION: ErrorCodeDescriptor = ErrorCodeDescriptor {
    code: ErrorCodeId::new("sql.analyze.unsupported_expression"),
    phase: ErrorPhase::Analyze,
    status: ErrorCodeStatus::Active,
};
const UNAVAILABLE_IMPLEMENTATION: ErrorCodeDescriptor = ErrorCodeDescriptor {
    code: ErrorCodeId::new("sql.analyze.unavailable_implementation"),
    phase: ErrorPhase::Analyze,
    status: ErrorCodeStatus::Active,
};
const UNSUPPORTED_QUERY_SHAPE: ErrorCodeDescriptor = ErrorCodeDescriptor {
    code: ErrorCodeId::new("sql.analyze.unsupported_query_shape"),
    phase: ErrorPhase::Analyze,
    status: ErrorCodeStatus::Active,
};
const MISSING_COLUMN_NAME: ErrorCodeDescriptor = ErrorCodeDescriptor {
    code: ErrorCodeId::new("sql.analyze.missing_column_name"),
    phase: ErrorPhase::Analyze,
    status: ErrorCodeStatus::Active,
};
const DUPLICATE_COLUMN_NAME: ErrorCodeDescriptor = ErrorCodeDescriptor {
    code: ErrorCodeId::new("sql.analyze.duplicate_column_name"),
    phase: ErrorPhase::Analyze,
    status: ErrorCodeStatus::Active,
};
const MISMATCHED_COLUMN_ALIASES: ErrorCodeDescriptor = ErrorCodeDescriptor {
    code: ErrorCodeId::new("sql.analyze.mismatched_column_aliases"),
    phase: ErrorPhase::Analyze,
    status: ErrorCodeStatus::Active,
};
const INVALID_VIEW: ErrorCodeDescriptor = ErrorCodeDescriptor {
    code: ErrorCodeId::new("sql.analyze.invalid_view"),
    phase: ErrorPhase::Analyze,
    status: ErrorCodeStatus::Active,
};
const VIEW_IS_STALE: ErrorCodeDescriptor = ErrorCodeDescriptor {
    code: ErrorCodeId::new("sql.analyze.view_is_stale"),
    phase: ErrorPhase::Analyze,
    status: ErrorCodeStatus::Active,
};
const VIEW_IS_RECURSIVE: ErrorCodeDescriptor = ErrorCodeDescriptor {
    code: ErrorCodeId::new("sql.analyze.view_is_recursive"),
    phase: ErrorPhase::Analyze,
    status: ErrorCodeStatus::Active,
};
const INTERNAL: ErrorCodeDescriptor = ErrorCodeDescriptor {
    code: ErrorCodeId::new("sql.analyze.internal"),
    phase: ErrorPhase::Analyze,
    status: ErrorCodeStatus::Active,
};

/// All SQL analyze-domain descriptors. The independent manifest tool is their
/// only permitted aggregate owner.
pub const ERROR_CODE_DESCRIPTORS: &[ErrorCodeDescriptor] = &[
    UNKNOWN_TABLE,
    UNKNOWN_COLUMN,
    UNKNOWN_FUNCTION,
    TYPE_MISMATCH,
    INVALID_LITERAL,
    INVALID_ARGUMENT,
    TOO_MANY_ARGUMENTS,
    INVALID_QUERY_SHAPE,
    UNSUPPORTED_EXPRESSION,
    UNAVAILABLE_IMPLEMENTATION,
    UNSUPPORTED_QUERY_SHAPE,
    MISSING_COLUMN_NAME,
    DUPLICATE_COLUMN_NAME,
    MISMATCHED_COLUMN_ALIASES,
    INVALID_VIEW,
    VIEW_IS_STALE,
    VIEW_IS_RECURSIVE,
    INTERNAL,
];

/// The semantic category owned by SQL analysis.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AnalyzeErrorKind {
    UnknownTable,
    UnknownColumn,
    UnknownFunction,
    TypeMismatch,
    InvalidLiteral,
    InvalidArgument,
    TooManyArguments,
    InvalidQueryShape,
    UnsupportedExpression,
    /// A bound overload has no installed pure implementation.
    UnavailableImplementation,
    UnsupportedQueryShape,
    MissingColumnName,
    DuplicateColumnName,
    MismatchedColumnAliases,
    InvalidView,
    ViewIsStale,
    ViewIsRecursive,
    Internal,
}

impl AnalyzeErrorKind {
    /// Returns the sole descriptor for this semantic category.
    pub const fn descriptor(self) -> ErrorCodeDescriptor {
        match self {
            Self::UnknownTable => UNKNOWN_TABLE,
            Self::UnknownColumn => UNKNOWN_COLUMN,
            Self::UnknownFunction => UNKNOWN_FUNCTION,
            Self::TypeMismatch => TYPE_MISMATCH,
            Self::InvalidLiteral => INVALID_LITERAL,
            Self::InvalidArgument => INVALID_ARGUMENT,
            Self::TooManyArguments => TOO_MANY_ARGUMENTS,
            Self::InvalidQueryShape => INVALID_QUERY_SHAPE,
            Self::UnsupportedExpression => UNSUPPORTED_EXPRESSION,
            Self::UnavailableImplementation => UNAVAILABLE_IMPLEMENTATION,
            Self::UnsupportedQueryShape => UNSUPPORTED_QUERY_SHAPE,
            Self::MissingColumnName => MISSING_COLUMN_NAME,
            Self::DuplicateColumnName => DUPLICATE_COLUMN_NAME,
            Self::MismatchedColumnAliases => MISMATCHED_COLUMN_ALIASES,
            Self::InvalidView => INVALID_VIEW,
            Self::ViewIsStale => VIEW_IS_STALE,
            Self::ViewIsRecursive => VIEW_IS_RECURSIVE,
            Self::Internal => INTERNAL,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct StoredViewAnalysisCause {
    source_sql: std::sync::Arc<str>,
    error: AnalyzeError,
}

/// The persisted definition whose source is being read, independent of its caller.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoredDefinition {
    kind: StoredDefinitionKind,
    name: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StoredDefinitionKind {
    View,
    MaterializedView,
}

impl StoredDefinition {
    pub fn view(name: impl Into<String>) -> Self {
        Self {
            kind: StoredDefinitionKind::View,
            name: name.into(),
        }
    }
    pub fn materialized_view(name: impl Into<String>) -> Self {
        Self {
            kind: StoredDefinitionKind::MaterializedView,
            name: name.into(),
        }
    }
    pub fn kind(&self) -> StoredDefinitionKind {
        self.kind
    }
    pub fn name(&self) -> &str {
        &self.name
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct StoredDefinitionSourceCause {
    definition: StoredDefinition,
    source_sql: std::sync::Arc<str>,
    parser_error: Option<novarocks_parser::ParserError>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum StoredDefinitionCause {
    Analysis(StoredViewAnalysisCause),
    Source(StoredDefinitionSourceCause),
}

/// A user-visible SQL analysis failure.
///
/// Direct failures at user AST nodes carry their parser-owned span. Synthetic
/// post-analysis invariants use `None`; callers must not fabricate a span.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AnalyzeError {
    kind: AnalyzeErrorKind,
    message: String,
    span: Option<Span>,
    control: Option<novarocks_type_contract::CompileControlError>,
    source: Option<novarocks_failure_contract::SharedFailureCause>,
    view_cause: Option<Box<StoredDefinitionCause>>,
}

impl AnalyzeError {
    fn at(kind: AnalyzeErrorKind, message: impl Into<String>, span: Span) -> Self {
        Self::at_optional(kind, message, Some(span))
    }

    fn at_optional(kind: AnalyzeErrorKind, message: impl Into<String>, span: Option<Span>) -> Self {
        Self {
            kind,
            message: message.into(),
            span,
            control: None,
            source: None,
            view_cause: None,
        }
    }

    pub fn missing_column_name(message: impl Into<String>, span: impl Into<Option<Span>>) -> Self {
        Self::at_optional(
            AnalyzeErrorKind::MissingColumnName,
            message,
            span.into().filter(|span| !span.is_empty()),
        )
    }

    pub fn duplicate_column_name(
        message: impl Into<String>,
        span: impl Into<Option<Span>>,
    ) -> Self {
        Self::at_optional(
            AnalyzeErrorKind::DuplicateColumnName,
            message,
            span.into().filter(|span| !span.is_empty()),
        )
    }

    pub fn mismatched_column_aliases(
        message: impl Into<String>,
        span: impl Into<Option<Span>>,
    ) -> Self {
        Self::at_optional(
            AnalyzeErrorKind::MismatchedColumnAliases,
            message,
            span.into().filter(|span| !span.is_empty()),
        )
    }

    pub fn invalid_view(message: impl Into<String>, span: impl Into<Option<Span>>) -> Self {
        Self::at_optional(
            AnalyzeErrorKind::InvalidView,
            message,
            span.into().filter(|span| !span.is_empty()),
        )
    }

    pub(crate) fn view_cast_preparation(error: novarocks_functions::CastPrepareError) -> Self {
        if let Some(control) = error.control_error() {
            return Self::control(control);
        }
        Self::control(novarocks_type_contract::CompileControlError::Source(
            novarocks_failure_contract::SharedFailureCause::new(error),
        ))
    }

    pub(crate) fn invalid_view_from_analysis(
        view: &str,
        cause: AnalyzeError,
        source_sql: std::sync::Arc<str>,
        span: Span,
    ) -> Self {
        let mut error = Self::invalid_view(
            format!("Failed analyzing stored view '{view}': {cause}"),
            span,
        );
        error.view_cause = Some(Box::new(StoredDefinitionCause::Analysis(
            StoredViewAnalysisCause {
                source_sql,
                error: cause,
            },
        )));
        error
    }

    /// Retain the actual parser failure together with the saved source it describes.
    /// Only the reference span belongs to the caller's statement.
    pub fn stored_definition_parse_failure(
        definition: StoredDefinition,
        source_sql: std::sync::Arc<str>,
        parser_error: novarocks_parser::ParserError,
        reference_span: Option<Span>,
    ) -> Self {
        let public = parser_error.to_user_error(&source_sql);
        let detail = match public.location() {
            Some(location) => format!(
                "{} at {}:{}: {}",
                public.code(),
                location.line(),
                location.column(),
                public.message()
            ),
            None => format!("{}: {}", public.code(), public.message()),
        };
        let mut error = Self::invalid_view(
            format!(
                "Failed parsing stored view '{}': {detail}",
                definition.name()
            ),
            reference_span,
        );
        error.view_cause = Some(Box::new(StoredDefinitionCause::Source(
            StoredDefinitionSourceCause {
                definition,
                source_sql,
                parser_error: Some(parser_error),
            },
        )));
        error
    }

    pub fn stored_definition_not_query(
        definition: StoredDefinition,
        source_sql: std::sync::Arc<str>,
        reference_span: Option<Span>,
    ) -> Self {
        let mut error = Self::invalid_view(
            format!(
                "Stored view '{}' source must contain exactly one query",
                definition.name()
            ),
            reference_span,
        );
        error.view_cause = Some(Box::new(StoredDefinitionCause::Source(
            StoredDefinitionSourceCause {
                definition,
                source_sql,
                parser_error: None,
            },
        )));
        error
    }

    pub fn stored_definition(&self) -> Option<&StoredDefinition> {
        match self.view_cause.as_deref()? {
            StoredDefinitionCause::Analysis(cause) => cause.error.stored_definition(),
            StoredDefinitionCause::Source(cause) => Some(&cause.definition),
        }
    }

    /// Rebase only public context; the complete original error and its actual
    /// definition source remain together in the diagnostic cause chain.
    pub(crate) fn at_view_reference(self, source_sql: std::sync::Arc<str>, span: Span) -> Self {
        let mut outer = self.clone();
        outer.span = (!span.is_empty()).then_some(span);
        outer.view_cause = Some(Box::new(StoredDefinitionCause::Analysis(
            StoredViewAnalysisCause {
                source_sql,
                error: self,
            },
        )));
        outer
    }

    pub fn stored_view_definition_source(&self) -> Option<&str> {
        match self.view_cause.as_deref()? {
            StoredDefinitionCause::Analysis(cause) => Some(&cause.source_sql),
            StoredDefinitionCause::Source(cause) => Some(&cause.source_sql),
        }
    }

    /// A memoized pure catalog selection has one semantic verdict, while
    /// each syntax occurrence owns its own coordinates (possibly in another
    /// stored definition). Relocate only that occurrence, never its kind.
    pub(crate) fn at_catalog_selection_reference(mut self, span: Span) -> Self {
        self.span = (!span.is_empty()).then_some(span);
        self
    }

    pub fn view_is_stale(message: impl Into<String>, span: impl Into<Option<Span>>) -> Self {
        Self::at_optional(
            AnalyzeErrorKind::ViewIsStale,
            message,
            span.into().filter(|span| !span.is_empty()),
        )
    }

    pub fn view_is_recursive(message: impl Into<String>, span: impl Into<Option<Span>>) -> Self {
        Self::at_optional(
            AnalyzeErrorKind::ViewIsRecursive,
            message,
            span.into().filter(|span| !span.is_empty()),
        )
    }

    pub fn unknown_table(message: impl Into<String>, span: Span) -> Self {
        Self::at(AnalyzeErrorKind::UnknownTable, message, span)
    }

    pub fn unknown_column(message: impl Into<String>, span: Span) -> Self {
        Self::at(AnalyzeErrorKind::UnknownColumn, message, span)
    }

    pub fn unknown_function(message: impl Into<String>, span: Span) -> Self {
        Self::at(AnalyzeErrorKind::UnknownFunction, message, span)
    }

    pub fn type_mismatch(message: impl Into<String>, span: Span) -> Self {
        Self::at(AnalyzeErrorKind::TypeMismatch, message, span)
    }

    /// Keep semantic binding context independent of the originating facts.
    pub(crate) fn type_mismatch_source<E>(cause: E, span: Span) -> Self
    where
        E: std::error::Error
            + novarocks_failure_contract::OuterFailureProjection
            + Send
            + Sync
            + 'static,
    {
        let mut error = Self::domain_source(cause);
        error.kind = AnalyzeErrorKind::TypeMismatch;
        error.span = Some(span);
        error
    }
    pub(crate) fn domain_source<E>(cause: E) -> Self
    where
        E: std::error::Error
            + novarocks_failure_contract::OuterFailureProjection
            + Send
            + Sync
            + 'static,
    {
        Self::shared_domain_source(novarocks_failure_contract::SharedFailureCause::new(cause))
    }

    pub(crate) fn shared_domain_source(
        source: novarocks_failure_contract::SharedFailureCause,
    ) -> Self {
        Self {
            kind: AnalyzeErrorKind::Internal,
            message: source.detail().to_owned(),
            span: None,
            control: None,
            source: Some(source),
            view_cause: None,
        }
    }
    pub fn source_failure(&self) -> Option<&novarocks_failure_contract::SharedFailureCause> {
        self.source.as_ref()
    }

    pub(crate) fn invalid_literal_source<E>(cause: E, span: Span) -> Self
    where
        E: std::error::Error
            + novarocks_failure_contract::OuterFailureProjection
            + Send
            + Sync
            + 'static,
    {
        let mut error = Self::domain_source(cause);
        error.kind = AnalyzeErrorKind::InvalidLiteral;
        error.span = Some(span);
        error
    }
    pub fn invalid_literal(message: impl Into<String>, span: Span) -> Self {
        Self::at(AnalyzeErrorKind::InvalidLiteral, message, span)
    }

    pub fn too_many_arguments(message: impl Into<String>, span: Span) -> Self {
        Self::at(AnalyzeErrorKind::TooManyArguments, message, span)
    }

    pub fn invalid_argument(message: impl Into<String>, span: Span) -> Self {
        Self::at(AnalyzeErrorKind::InvalidArgument, message, span)
    }

    pub fn invalid_query_shape(message: impl Into<String>, span: Span) -> Self {
        Self::at(AnalyzeErrorKind::InvalidQueryShape, message, span)
    }

    pub fn unsupported_expression(message: impl Into<String>, span: Span) -> Self {
        Self::at(AnalyzeErrorKind::UnsupportedExpression, message, span)
    }

    /// Preserve an owner refusal until its real caller attaches AST context.
    pub(crate) fn unsupported_expression_without_span(message: impl Into<String>) -> Self {
        Self {
            kind: AnalyzeErrorKind::UnsupportedExpression,
            message: message.into(),
            span: None,
            control: None,
            source: None,
            view_cause: None,
        }
    }

    pub fn unsupported_query_shape(message: impl Into<String>, span: Span) -> Self {
        Self::at(AnalyzeErrorKind::UnsupportedQueryShape, message, span)
    }

    /// Constructs a source-less internal failure. User-AST errors must use a
    /// semantic constructor above instead.
    pub fn internal(message: impl Into<String>) -> Self {
        Self {
            kind: AnalyzeErrorKind::Internal,
            message: message.into(),
            span: None,
            control: None,
            source: None,
            view_cause: None,
        }
    }

    /// Preserve outer compilation control across the analyzer boundary.
    /// Compiler callers extract this category before user-error projection.
    pub fn control(error: novarocks_type_contract::CompileControlError) -> Self {
        Self {
            kind: AnalyzeErrorKind::Internal,
            message: error.to_string(),
            span: None,
            control: Some(error),
            source: None,
            view_cause: None,
        }
    }

    pub(crate) fn function_binding(error: novarocks_functions::FunctionBindingError) -> Self {
        match error {
            novarocks_functions::FunctionBindingError::Control(error) => Self::control(error),
            error @ novarocks_functions::FunctionBindingError::Constant(_) => {
                let source = novarocks_failure_contract::SharedFailureCause::new(error);
                Self {
                    kind: AnalyzeErrorKind::Internal,
                    message: source.detail().to_owned(),
                    span: None,
                    control: None,
                    source: Some(source),
                    view_cause: None,
                }
            }
            novarocks_functions::FunctionBindingError::VariantTargetType(source) => {
                let mut error = Self::domain_source(source);
                error.kind = AnalyzeErrorKind::InvalidLiteral;
                error
            }
            error @ novarocks_functions::FunctionBindingError::UnavailableImplementation(_) => {
                Self {
                    kind: AnalyzeErrorKind::UnavailableImplementation,
                    message: error.to_string(),
                    span: None,
                    control: None,
                    source: None,
                    view_cause: None,
                }
            }
            error => Self::internal(error.to_string()),
        }
    }

    pub(crate) fn function_resolution(error: novarocks_functions::FunctionResolutionError) -> Self {
        match error {
            novarocks_functions::FunctionResolutionError::Control(error) => Self::control(error),
            error => Self::internal(error.to_string()),
        }
    }

    /// Attach the existing user type-error context without reclassifying an
    /// outer control failure as a user expression error.
    pub(crate) fn at_type_mismatch(self, span: Span) -> Self {
        if self.control.is_some() {
            self
        } else if matches!(
            self.kind,
            AnalyzeErrorKind::UnavailableImplementation | AnalyzeErrorKind::UnsupportedExpression
        ) {
            Self {
                span: Some(span),
                ..self
            }
        } else if self.source.is_some() {
            Self {
                kind: AnalyzeErrorKind::TypeMismatch,
                span: Some(span),
                ..self
            }
        } else {
            Self::type_mismatch(self.message, span)
        }
    }

    pub(crate) fn at_invalid_argument(self, span: Span) -> Self {
        if self.control.is_some() {
            self
        } else if matches!(
            self.kind,
            AnalyzeErrorKind::UnavailableImplementation | AnalyzeErrorKind::UnsupportedExpression
        ) {
            Self {
                span: Some(span),
                ..self
            }
        } else if self.source.is_some() {
            Self {
                kind: AnalyzeErrorKind::InvalidArgument,
                span: Some(span),
                ..self
            }
        } else {
            Self::invalid_argument(self.message, span)
        }
    }

    pub fn control_error(&self) -> Option<novarocks_type_contract::CompileControlError> {
        self.control.clone()
    }

    pub const fn kind(&self) -> AnalyzeErrorKind {
        self.kind
    }

    pub const fn code(&self) -> ErrorCodeId {
        self.kind.descriptor().code
    }

    pub const fn span(&self) -> Option<Span> {
        self.span
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    /// Converts this domain error to its transport-neutral representation.
    /// A source-less caller preserves code and message but intentionally emits
    /// no user location.
    pub fn to_user_error(&self, source: Option<&str>) -> UserError {
        let location = self
            .span
            .filter(|span| !span.is_empty())
            .zip(source)
            .map(|(span, source)| span.to_user_error_location(source));
        UserError::from_descriptor(
            self.kind.descriptor(),
            self.message.clone(),
            location,
            RetryClass::Never,
        )
    }
}

impl fmt::Display for AnalyzeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for AnalyzeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source
            .as_ref()
            .map(|source| source as &(dyn std::error::Error + 'static))
            .or_else(|| match self.view_cause.as_deref()? {
                StoredDefinitionCause::Analysis(cause) => Some(&cause.error as _),
                StoredDefinitionCause::Source(cause) => {
                    cause.parser_error.as_ref().map(|error| error as _)
                }
            })
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn original_coercion_fact_survives_analyze_context_and_clone() {
        use novarocks_failure_contract::{FailureClass, FailureProjection};
        let cause = novarocks_type_contract::decimal_compare_type(
            &arrow::datatypes::DataType::Decimal256(76, 0),
            &arrow::datatypes::DataType::Decimal256(76, 38),
        )
        .unwrap_err();
        let fact = cause.failure_fact();
        let error = AnalyzeError::type_mismatch_source(cause, Span::new(7, 8));
        let cloned = error.clone().at_invalid_argument(Span::new(9, 10));
        assert_eq!(error.source_failure(), cloned.source_failure());
        assert_eq!(cloned.source_failure().unwrap().failure_fact(), fact);
        assert_eq!(fact.class(), FailureClass::Representation);
        assert_eq!(cloned.span(), Some(Span::new(9, 10)));
        assert_eq!(cloned.kind(), AnalyzeErrorKind::InvalidArgument);
        assert!(
            cloned
                .source_failure()
                .unwrap()
                .downcast_ref::<novarocks_type_contract::ComparisonCoercionFailure>()
                .is_some()
        );
    }

    use super::*;

    #[test]
    fn unsupported_expression_keeps_its_code_through_argument_context() {
        let error = AnalyzeError::unsupported_expression("unsupported conversion", Span::new(1, 2))
            .at_type_mismatch(Span::new(7, 8))
            .at_invalid_argument(Span::new(9, 10));
        assert_eq!(error.kind(), AnalyzeErrorKind::UnsupportedExpression);
        assert_eq!(error.code().as_str(), "sql.analyze.unsupported_expression");
        assert_eq!(error.span(), Some(Span::new(9, 10)));
        assert!(error.source_failure().is_none());
    }

    #[test]
    fn missing_pure_implementation_keeps_its_own_code_through_argument_context() {
        let overload =
            novarocks_functions::FunctionOverloadId::try_new("fixture/missing/pure-v1").unwrap();
        let error = AnalyzeError::function_binding(
            novarocks_functions::FunctionBindingError::UnavailableImplementation(overload),
        );
        let error = error
            .at_type_mismatch(Span::new(7, 8))
            .at_invalid_argument(Span::new(7, 8));
        assert_eq!(error.kind(), AnalyzeErrorKind::UnavailableImplementation);
        assert_eq!(
            error.code().as_str(),
            "sql.analyze.unavailable_implementation"
        );
        let user = error.to_user_error(None);
        assert_eq!(
            user.code().as_str(),
            "sql.analyze.unavailable_implementation"
        );
        assert_eq!(
            AnalyzeErrorKind::UnsupportedExpression
                .descriptor()
                .code
                .as_str(),
            "sql.analyze.unsupported_expression"
        );
    }

    #[test]
    fn descriptors_are_the_frozen_analyze_contract() {
        let codes = ERROR_CODE_DESCRIPTORS
            .iter()
            .map(|descriptor| descriptor.code.as_str())
            .collect::<Vec<_>>();
        assert_eq!(
            codes,
            vec![
                "sql.analyze.unknown_table",
                "sql.analyze.unknown_column",
                "sql.analyze.unknown_function",
                "sql.analyze.type_mismatch",
                "sql.analyze.invalid_literal",
                "sql.analyze.invalid_argument",
                "sql.analyze.too_many_arguments",
                "sql.analyze.invalid_query_shape",
                "sql.analyze.unsupported_expression",
                "sql.analyze.unavailable_implementation",
                "sql.analyze.unsupported_query_shape",
                "sql.analyze.missing_column_name",
                "sql.analyze.duplicate_column_name",
                "sql.analyze.mismatched_column_aliases",
                "sql.analyze.invalid_view",
                "sql.analyze.view_is_stale",
                "sql.analyze.view_is_recursive",
                "sql.analyze.internal",
            ]
        );
        assert!(
            ERROR_CODE_DESCRIPTORS
                .iter()
                .all(|descriptor| descriptor.phase == ErrorPhase::Analyze)
        );
    }

    #[test]
    fn user_ast_error_preserves_span_and_location() {
        let error = AnalyzeError::unknown_column("unknown column x", Span::new(7, 8));
        assert_eq!(error.kind(), AnalyzeErrorKind::UnknownColumn);
        assert_eq!(error.code().as_str(), "sql.analyze.unknown_column");
        assert_eq!(error.span(), Some(Span::new(7, 8)));

        let user_error = error.to_user_error(Some("SELECT x"));
        assert_eq!(user_error.phase(), ErrorPhase::Analyze);
        assert_eq!(user_error.location().expect("location").column(), 8);
    }

    #[test]
    fn empty_spans_have_no_user_location_without_changing_the_error() {
        let source = "SELECT 'é' AS label,\n  '雪', missing";
        let start = source.find("missing").unwrap();
        let span = Span::new(start, start + "missing".len());
        let user_node = AnalyzeError::unknown_column("unknown column missing", span);
        let synthetic = AnalyzeError::unknown_column("unknown column missing", Span::new(0, 0));
        let expected = user_node.to_user_error(Some(source));
        let location = expected.location().expect("user expression location");
        assert_eq!(location.line(), 2);
        assert_eq!(location.column(), 10);
        assert_eq!(location.end_line(), Some(2));
        assert_eq!(location.end_column(), Some(17));
        for projected in [
            synthetic.to_user_error(Some(source)),
            user_node.to_user_error(None),
            synthetic.to_user_error(None),
        ] {
            assert!(projected.location().is_none());
            assert_eq!(projected.code(), expected.code());
            assert_eq!(projected.message(), expected.message());
            assert_eq!(projected.phase(), expected.phase());
        }
    }

    #[test]
    fn source_less_internal_error_has_no_location() {
        let error = AnalyzeError::internal("unexpected resolved query invariant");
        assert_eq!(error.kind(), AnalyzeErrorKind::Internal);
        assert_eq!(error.span(), None);
        assert!(error.to_user_error(Some("SELECT 1")).location().is_none());
    }
}
