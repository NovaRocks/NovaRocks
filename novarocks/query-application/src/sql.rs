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

//! Query-application ownership of parser-admitted SQL statement shape.

use std::fmt;

use novarocks_parser::{ParserError, ast::Statement};

use crate::admitted_query_context::RequestContext;
use crate::api::CommandContext;
use crate::protocol_delivery::QuerySessionOutput;
use crate::session_error::{QueryServiceError, QueryServiceErrorKind};

/// SQL batch admission and test-only stable error injection.
pub mod admission;

/// Connection-local execution settings after SQL/session validation and before
/// a role adapter projects them into a particular wire contract.
pub mod session;

/// Typed session-admission errors and their stable user-error descriptors.
pub mod session_admit;

/// Typed DML statement-shape admission errors and their stable user-error descriptors.
pub mod dml_admission;

/// Read-only Catalog facts consumed by session SQL admission.
pub mod catalog;

/// Parser-admitted KILL statement execution over query and connection ports.
pub mod kill;

/// Query-result scalar conversion used by SQL session user variables.
pub mod user_variable;

/// Product-command port consumed after SQL admission has selected a typed
/// statement. Implementations remain role-local adapters: the port transfers
/// only immutable admitted context and the governed command context.
pub trait CoreCommandRoute: Send + Sync {
    fn execute_typed(
        &self,
        _statement: &Statement,
        _context: &RequestContext,
        _command_context: &CommandContext,
    ) -> Result<QuerySessionOutput, String> {
        Err("typed command route is unavailable".to_string())
    }
}

/// The application boundary accepts one framed SQL statement.
///
/// Query Application owns SQL batch framing and parser admission. Protocol
/// adapters negotiate multi-result capability and ask the application to
/// execute each admitted fragment in order; this function rejects a fragment
/// that contains more than one statement.
pub fn parse_single_statement(source: &str) -> Result<Statement, SqlStatementParseError> {
    parse_optional_single_statement(source)?
        .ok_or(SqlStatementParseError::ExpectedExactlyOne { actual: 0 })
}

/// Parses one protocol-framed SQL fragment, preserving a comment-only fragment
/// as the absence of a statement.
pub fn parse_optional_single_statement(
    source: &str,
) -> Result<Option<Statement>, SqlStatementParseError> {
    let statements = novarocks_parser::parse(source).map_err(SqlStatementParseError::Parser)?;
    match statements.as_slice() {
        [] => Ok(None),
        [statement] => Ok(Some(statement.clone())),
        _ => Err(SqlStatementParseError::ExpectedExactlyOne {
            actual: statements.len(),
        }),
    }
}

/// Query-application parser-admission failure.
#[derive(Debug)]
pub enum SqlStatementParseError {
    Parser(ParserError),
    ExpectedExactlyOne { actual: usize },
}

impl fmt::Display for SqlStatementParseError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Parser(error) => error.fmt(formatter),
            Self::ExpectedExactlyOne { actual } => {
                write!(formatter, "expected exactly one statement, found {actual}")
            }
        }
    }
}

impl std::error::Error for SqlStatementParseError {}

/// Projects parser admission failures into the protocol-neutral query-service
/// error vocabulary before any product route or protocol adapter is selected.
pub fn query_service_parse_error(error: SqlStatementParseError, source: &str) -> QueryServiceError {
    match error {
        SqlStatementParseError::Parser(error) => {
            QueryServiceError::from_user_error(error.to_user_error(source))
        }
        SqlStatementParseError::ExpectedExactlyOne { .. } => QueryServiceError::new(
            QueryServiceErrorKind::Parse,
            "command admission requires exactly one statement",
        ),
    }
}

/// Cursor over semicolon-delimited SQL protocol fragments.
///
/// This is protocol-neutral framing: it preserves quote and comment state but
/// does not decide whether a fragment is an executable product statement.
#[derive(Clone)]
pub struct SqlBatchCursor<'a> {
    sql: &'a str,
    offset: usize,
}

impl<'a> SqlBatchCursor<'a> {
    pub fn new(sql: &'a str) -> Self {
        Self { sql, offset: 0 }
    }

    pub fn has_remaining(&self) -> bool {
        self.offset < self.sql.len()
    }

    /// Returns one raw semicolon-delimited fragment.
    pub fn next_fragment(&mut self) -> Result<Option<&'a str>, QueryServiceError> {
        if !self.has_remaining() {
            return Ok(None);
        }
        #[derive(Clone, Copy)]
        enum State {
            Normal,
            SingleQuote,
            DoubleQuote,
            Backtick,
            LineComment,
            BlockComment,
        }

        let start = self.offset;
        let bytes = self.sql.as_bytes();
        let mut index = start;
        let mut state = State::Normal;
        while index < bytes.len() {
            match state {
                State::Normal => match bytes[index] {
                    b'\'' => state = State::SingleQuote,
                    b'"' => state = State::DoubleQuote,
                    b'`' => state = State::Backtick,
                    b'-' if bytes.get(index + 1) == Some(&b'-') => {
                        state = State::LineComment;
                        index += 1;
                    }
                    b'#' => state = State::LineComment,
                    b'/' if bytes.get(index + 1) == Some(&b'*') => {
                        state = State::BlockComment;
                        index += 1;
                    }
                    b';' => {
                        self.offset = index + 1;
                        return Ok(Some(&self.sql[start..index]));
                    }
                    _ => {}
                },
                State::SingleQuote if bytes[index] == b'\'' => state = State::Normal,
                State::DoubleQuote if bytes[index] == b'"' => state = State::Normal,
                State::Backtick if bytes[index] == b'`' => state = State::Normal,
                State::LineComment if bytes[index] == b'\n' => state = State::Normal,
                State::BlockComment
                    if bytes[index] == b'*' && bytes.get(index + 1) == Some(&b'/') =>
                {
                    state = State::Normal;
                    index += 1;
                }
                _ => {}
            }
            index += 1;
        }
        if matches!(
            state,
            State::SingleQuote | State::DoubleQuote | State::Backtick
        ) {
            self.offset = self.sql.len();
            return Err(QueryServiceError::new(
                QueryServiceErrorKind::Parse,
                "unterminated quoted string in SQL batch",
            ));
        }
        self.offset = self.sql.len();
        Ok(Some(&self.sql[start..]))
    }
}

/// Splits a protocol SQL batch without interpreting product-specific commands.
pub fn split_sql_statements(sql: &str) -> Result<Vec<String>, QueryServiceError> {
    let mut cursor = SqlBatchCursor::new(sql);
    let mut statements = Vec::new();
    while let Some(fragment) = cursor.next_fragment()? {
        let statement = fragment.trim();
        if !statement.is_empty() {
            statements.push(statement.to_string());
        }
    }
    Ok(statements)
}

/// Removes leading whole-line comments while retaining the first SQL token.
pub fn strip_leading_line_comments(sql: &str) -> &str {
    let mut remaining = sql.trim();
    loop {
        let Some(newline) = remaining.find('\n') else {
            return if remaining.starts_with("--") || remaining.starts_with('#') {
                ""
            } else {
                remaining
            };
        };
        let line = remaining[..newline].trim();
        if line.is_empty() || line.starts_with("--") || line.starts_with('#') {
            remaining = remaining[newline + 1..].trim_start();
            continue;
        }
        return remaining;
    }
}

#[cfg(test)]
mod tests {
    use super::{
        SqlBatchCursor, SqlStatementParseError, parse_optional_single_statement,
        parse_single_statement, query_service_parse_error, split_sql_statements,
        strip_leading_line_comments,
    };
    use crate::session_error::QueryServiceErrorKind;

    #[test]
    fn accepts_one_parser_statement() {
        let statement = parse_single_statement("SELECT 1").expect("one statement");
        assert!(matches!(
            statement,
            novarocks_parser::ast::Statement::Query(_)
        ));
    }

    #[test]
    fn rejects_multiple_parser_statements() {
        assert!(matches!(
            parse_single_statement("SELECT 1; SELECT 2"),
            Err(SqlStatementParseError::ExpectedExactlyOne { actual: 2 })
        ));
    }

    #[test]
    fn parser_admission_projection_keeps_the_parse_error_vocabulary() {
        let error = query_service_parse_error(
            SqlStatementParseError::ExpectedExactlyOne { actual: 2 },
            "SELECT 1; SELECT 2",
        );
        assert_eq!(error.kind(), QueryServiceErrorKind::Parse);
        assert_eq!(
            error.message(),
            "command admission requires exactly one statement"
        );
    }

    #[test]
    fn retains_comment_only_fragment_as_absent() {
        assert_eq!(
            parse_optional_single_statement("/* comment */").expect("comment parses"),
            None
        );
    }

    #[test]
    fn batch_framing_preserves_quoted_semicolons_and_statement_order() {
        assert_eq!(
            split_sql_statements("SET query_timeout = 1; SELECT ';'; SELECT 3")
                .expect("split SQL batch"),
            vec![
                "SET query_timeout = 1".to_string(),
                "SELECT ';'".to_string(),
                "SELECT 3".to_string(),
            ]
        );
    }

    #[test]
    fn batch_cursor_reports_an_unterminated_quote_before_a_later_fragment() {
        let mut cursor = SqlBatchCursor::new("SELECT 1; SELECT 'unterminated");
        assert_eq!(
            cursor.next_fragment().expect("first fragment"),
            Some("SELECT 1")
        );
        let error = cursor
            .next_fragment()
            .expect_err("must reject unterminated quote");
        assert_eq!(
            error.kind(),
            crate::session_error::QueryServiceErrorKind::Parse
        );
    }

    #[test]
    fn leading_line_comments_preserve_the_following_statement() {
        assert_eq!(
            strip_leading_line_comments("-- generated header\n# another line\nCREATE CATALOG c"),
            "CREATE CATALOG c"
        );
        assert_eq!(strip_leading_line_comments("-- comment only"), "");
    }
}
