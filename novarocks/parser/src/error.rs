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

use std::fmt;

use novarocks_user_error::{
    ErrorCodeDescriptor, ErrorCodeId, ErrorCodeStatus, ErrorPhase, RetryClass, UserError,
};

use crate::{Span, messages};

const LEX_UNEXPECTED_CHARACTER: ErrorCodeDescriptor = ErrorCodeDescriptor {
    code: ErrorCodeId::new("sql.lex.unexpected_character"),
    phase: ErrorPhase::Lex,
    status: ErrorCodeStatus::Active,
};
const LEX_UNTERMINATED_STRING: ErrorCodeDescriptor = ErrorCodeDescriptor {
    code: ErrorCodeId::new("sql.lex.unterminated_string"),
    phase: ErrorPhase::Lex,
    status: ErrorCodeStatus::Active,
};
const LEX_UNTERMINATED_QUOTED_IDENTIFIER: ErrorCodeDescriptor = ErrorCodeDescriptor {
    code: ErrorCodeId::new("sql.lex.unterminated_quoted_identifier"),
    phase: ErrorPhase::Lex,
    status: ErrorCodeStatus::Active,
};
const LEX_UNTERMINATED_COMMENT: ErrorCodeDescriptor = ErrorCodeDescriptor {
    code: ErrorCodeId::new("sql.lex.unterminated_comment"),
    phase: ErrorPhase::Lex,
    status: ErrorCodeStatus::Active,
};
const PARSE_UNEXPECTED_TOKEN: ErrorCodeDescriptor = ErrorCodeDescriptor {
    code: ErrorCodeId::new("sql.parse.unexpected_token"),
    phase: ErrorPhase::Parse,
    status: ErrorCodeStatus::Active,
};
const PARSE_UNSUPPORTED_STATEMENT: ErrorCodeDescriptor = ErrorCodeDescriptor {
    code: ErrorCodeId::new("sql.parse.unsupported_statement"),
    phase: ErrorPhase::Parse,
    status: ErrorCodeStatus::Active,
};
const VALIDATE_INVALID_STRUCTURE: ErrorCodeDescriptor = ErrorCodeDescriptor {
    code: ErrorCodeId::new("sql.validate.invalid_structure"),
    phase: ErrorPhase::Validate,
    status: ErrorCodeStatus::Active,
};
const VALIDATE_DUPLICATE_CTE_NAME: ErrorCodeDescriptor = ErrorCodeDescriptor {
    code: ErrorCodeId::new("sql.validate.duplicate_cte_name"),
    phase: ErrorPhase::Validate,
    status: ErrorCodeStatus::Active,
};
const VALIDATE_DUPLICATE_WINDOW_NAME: ErrorCodeDescriptor = ErrorCodeDescriptor {
    code: ErrorCodeId::new("sql.validate.duplicate_window_name"),
    phase: ErrorPhase::Validate,
    status: ErrorCodeStatus::Active,
};
const VALIDATE_INVALID_WINDOW_FRAME_BOUNDS: ErrorCodeDescriptor = ErrorCodeDescriptor {
    code: ErrorCodeId::new("sql.validate.invalid_window_frame_bounds"),
    phase: ErrorPhase::Validate,
    status: ErrorCodeStatus::Active,
};

/// All parser-domain descriptors. The independent manifest tool is their only
/// permitted aggregate owner.
pub const ERROR_CODE_DESCRIPTORS: &[ErrorCodeDescriptor] = &[
    LEX_UNEXPECTED_CHARACTER,
    LEX_UNTERMINATED_STRING,
    LEX_UNTERMINATED_QUOTED_IDENTIFIER,
    LEX_UNTERMINATED_COMMENT,
    PARSE_UNEXPECTED_TOKEN,
    PARSE_UNSUPPORTED_STATEMENT,
    VALIDATE_INVALID_STRUCTURE,
    VALIDATE_DUPLICATE_CTE_NAME,
    VALIDATE_DUPLICATE_WINDOW_NAME,
    VALIDATE_INVALID_WINDOW_FRAME_BOUNDS,
];

/// A lexical failure with its source range.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LexError {
    UnexpectedCharacter { character: char, span: Span },
    UnterminatedString { span: Span },
    UnterminatedQuotedIdentifier { span: Span },
    UnterminatedComment { span: Span },
}

/// A syntax failure with expected and found token descriptions.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ParseError {
    UnexpectedToken {
        expected: &'static str,
        found: String,
        span: Span,
    },
    UnsupportedStatement {
        statement: String,
        span: Span,
    },
}

/// A construction-only invariant which has no independently addressable SQL
/// validation code. Keep this list typed so a public AST cannot reintroduce
/// arbitrary parser-domain error text.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StructuralViolation {
    EmptyWithCteList,
    EmptyValuesRowList,
    EmptyValuesRow,
    EmptySelectProjection,
    InvalidSetOperationOperandCount,
    InvalidLogicalTermCount,
    EmptyInList,
    EmptyOrderBy,
    InvalidQueryModifierOwner,
    JsonConstructorInvariant(crate::ast::JsonAstInvariantError),
    EmptyUnnestExpressionList,
    EmptyCaseBranches,
    DuplicateTableColumnName,
}

/// A source-independent structural AST validation failure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ValidateError {
    InvalidStructure {
        violation: StructuralViolation,
        span: Span,
    },
    DuplicateCteName {
        name: String,
        span: Span,
    },
    DuplicateWindowName {
        name: String,
        span: Span,
    },
    InvalidWindowFrameBounds {
        span: Span,
    },
}

impl ValidateError {
    pub const fn invalid_structure(violation: StructuralViolation, span: Span) -> Self {
        Self::InvalidStructure { violation, span }
    }

    pub fn duplicate_cte_name(name: String, span: Span) -> Self {
        Self::DuplicateCteName { name, span }
    }

    pub fn duplicate_window_name(name: String, span: Span) -> Self {
        Self::DuplicateWindowName { name, span }
    }

    pub const fn invalid_window_frame_bounds(span: Span) -> Self {
        Self::InvalidWindowFrameBounds { span }
    }

    pub const fn span(&self) -> Span {
        match self {
            Self::InvalidStructure { span, .. }
            | Self::DuplicateCteName { span, .. }
            | Self::DuplicateWindowName { span, .. }
            | Self::InvalidWindowFrameBounds { span } => *span,
        }
    }
}

/// The parser-domain error union returned by public parsing APIs.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ParserError {
    Lex(LexError),
    Parse(ParseError),
    Validate(ValidateError),
}

impl ParserError {
    /// Converts the parser-owned span into a transport-neutral user error.
    pub fn to_user_error(&self, source: &str) -> UserError {
        let (descriptor, message, span) = match self {
            Self::Lex(LexError::UnexpectedCharacter { character, span }) => (
                LEX_UNEXPECTED_CHARACTER,
                messages::unexpected_character(*character),
                *span,
            ),
            Self::Lex(LexError::UnterminatedString { span }) => (
                LEX_UNTERMINATED_STRING,
                messages::unterminated("string literal"),
                *span,
            ),
            Self::Lex(LexError::UnterminatedQuotedIdentifier { span }) => (
                LEX_UNTERMINATED_QUOTED_IDENTIFIER,
                messages::unterminated("quoted identifier"),
                *span,
            ),
            Self::Lex(LexError::UnterminatedComment { span }) => (
                LEX_UNTERMINATED_COMMENT,
                messages::unterminated("block comment"),
                *span,
            ),
            Self::Parse(ParseError::UnexpectedToken {
                expected,
                found,
                span,
            }) => (
                PARSE_UNEXPECTED_TOKEN,
                messages::unexpected_token(expected, found),
                *span,
            ),
            Self::Parse(ParseError::UnsupportedStatement { statement, span }) => (
                PARSE_UNSUPPORTED_STATEMENT,
                messages::unsupported_statement(statement),
                *span,
            ),
            Self::Validate(ValidateError::InvalidStructure { violation, span }) => (
                VALIDATE_INVALID_STRUCTURE,
                messages::invalid_structure(*violation),
                *span,
            ),
            Self::Validate(ValidateError::DuplicateCteName { name, span }) => (
                VALIDATE_DUPLICATE_CTE_NAME,
                messages::duplicate_cte_name(name),
                *span,
            ),
            Self::Validate(ValidateError::DuplicateWindowName { name, span }) => (
                VALIDATE_DUPLICATE_WINDOW_NAME,
                messages::duplicate_window_name(name),
                *span,
            ),
            Self::Validate(ValidateError::InvalidWindowFrameBounds { span }) => (
                VALIDATE_INVALID_WINDOW_FRAME_BOUNDS,
                messages::invalid_window_frame_bounds(),
                *span,
            ),
        };
        UserError::from_descriptor(
            descriptor,
            message,
            Some(span.to_user_error_location(source)),
            RetryClass::Never,
        )
    }
}

impl From<LexError> for ParserError {
    fn from(value: LexError) -> Self {
        Self::Lex(value)
    }
}

impl From<ParseError> for ParserError {
    fn from(value: ParseError) -> Self {
        Self::Parse(value)
    }
}

impl From<ValidateError> for ParserError {
    fn from(value: ValidateError) -> Self {
        Self::Validate(value)
    }
}

impl fmt::Display for ParserError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let message = match self {
            Self::Lex(LexError::UnexpectedCharacter { character, .. }) => {
                messages::unexpected_character(*character)
            }
            Self::Lex(LexError::UnterminatedString { .. }) => {
                messages::unterminated("string literal")
            }
            Self::Lex(LexError::UnterminatedQuotedIdentifier { .. }) => {
                messages::unterminated("quoted identifier")
            }
            Self::Lex(LexError::UnterminatedComment { .. }) => {
                messages::unterminated("block comment")
            }
            Self::Parse(ParseError::UnexpectedToken {
                expected, found, ..
            }) => messages::unexpected_token(expected, found),
            Self::Parse(ParseError::UnsupportedStatement { statement, .. }) => {
                messages::unsupported_statement(statement)
            }
            Self::Validate(ValidateError::InvalidStructure { violation, .. }) => {
                messages::invalid_structure(*violation)
            }
            Self::Validate(ValidateError::DuplicateCteName { name, .. }) => {
                messages::duplicate_cte_name(name)
            }
            Self::Validate(ValidateError::DuplicateWindowName { name, .. }) => {
                messages::duplicate_window_name(name)
            }
            Self::Validate(ValidateError::InvalidWindowFrameBounds { .. }) => {
                messages::invalid_window_frame_bounds()
            }
        };
        formatter.write_str(&message)
    }
}

impl std::error::Error for ParserError {}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_user_error::ErrorPhase;

    #[test]
    fn parser_error_uses_descriptor_phase_and_parser_location() {
        let error = ParserError::Parse(ParseError::UnexpectedToken {
            expected: "BACKENDS",
            found: "EOF".to_owned(),
            span: Span::new(5, 5),
        });

        let user_error = error.to_user_error("SHOW ");
        assert_eq!(user_error.code().as_str(), "sql.parse.unexpected_token");
        assert_eq!(user_error.phase(), ErrorPhase::Parse);
        assert_eq!(
            user_error.to_string(),
            "[sql.parse.unexpected_token] expected BACKENDS, found EOF at line 1 column 6"
        );
    }
}
