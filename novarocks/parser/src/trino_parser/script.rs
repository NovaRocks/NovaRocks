// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

use super::Parser;
use crate::trino_token::{Keyword, Symbol, Token, TokenKind};
use crate::{ParserError, Span, trino_lexer};

/// The exact source slice of an executable script fragment.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StatementFrame {
    /// Includes the fragment's original trivia and excludes its delimiter.
    pub source: Span,
    pub delimiter: Option<Span>,
    /// The original strict-lexer refusal, when the scanner could still prove
    /// the complete fragment boundary. This never denotes successful parsing.
    pub lexical_failure: Option<ParserError>,
}

/// Frames a script using the original lexer and statement grammar.
///
/// A function specification owns the semicolons actually consumed by its
/// grammar. Bounded lexical failures keep their exact source bytes. Ambiguous
/// framing fails when an unterminated token could obscure a delimiter or a
/// damaged routine leaves its opening production unresolved.
pub fn frame_statements(source: &str) -> Result<Vec<StatementFrame>, ParserError> {
    let stream = trino_lexer::lex_for_framing(source)?;
    let mut parser = Parser::from_tokens(source, stream.tokens);
    let mut rejections = stream.rejections.into_iter().peekable();
    let mut frames = Vec::new();
    let mut start = 0;
    while parser.peek(0).kind != TokenKind::End || rejections.peek().is_some() {
        let prefix = parser.lexical_prefix();
        let contains_routine = match parser.try_own_routine_boundary(prefix) {
            Ok(owned) => owned,
            Err(error) => {
                // Keep the scanner's first rejection when it is already
                // within the definite lexical prefix of the damaged routine.
                return Err(rejections
                    .next_if(|rejection| {
                        rejection.span.start() < parser.tokens[prefix.delimiter_index].span.start()
                    })
                    .map_or(error, |rejection| rejection.error));
            }
        };
        let delimiter = parser.bump();
        let end = delimiter.span.start();
        let mut lexical_failure = None;
        while rejections
            .peek()
            .is_some_and(|rejection| rejection.span.start() < end)
        {
            let rejection = rejections.next().expect("rejection was just inspected");
            if contains_routine {
                return Err(rejection.error);
            }
            lexical_failure.get_or_insert(rejection.error);
        }
        if prefix.has_sql || lexical_failure.is_some() {
            frames.push(StatementFrame {
                source: Span::new(start, end),
                delimiter: (delimiter.kind != TokenKind::End).then_some(delimiter.span),
                lexical_failure,
            });
        }
        if delimiter.kind == TokenKind::End {
            break;
        }
        start = delimiter.span.end();
    }
    Ok(frames)
}

#[derive(Clone, Copy)]
struct LexicalPrefix {
    delimiter_index: usize,
    has_sql: bool,
    has_function_token: bool,
}

fn is_delimiter(token: Token) -> bool {
    matches!(
        token.kind,
        TokenKind::Symbol(Symbol::Semicolon) | TokenKind::End
    )
}

impl Parser<'_> {
    fn lexical_prefix(&self) -> LexicalPrefix {
        let mut has_sql = false;
        let mut has_function_token = false;
        for (index, token) in self.tokens.iter().copied().enumerate().skip(self.position) {
            if is_delimiter(token) {
                return LexicalPrefix {
                    delimiter_index: index,
                    has_sql,
                    has_function_token,
                };
            }
            has_sql |= !matches!(token.kind, TokenKind::Trivia(_));
            has_function_token |= token.kind == TokenKind::Keyword(Keyword::Function);
        }
        unreachable!("the scanner always supplies an End token")
    }

    fn try_own_routine_boundary(&mut self, prefix: LexicalPrefix) -> Result<bool, ParserError> {
        if prefix.has_function_token {
            self.next_parameter = 0;
            self.parameter_events.clear();
            self.routine_started = false;
            let parsed = self.parse_statement();
            if self.routine_started {
                // Only a complete grammar production can own inner
                // separators. Do not guess when a routine is damaged.
                parsed?;
                if !is_delimiter(self.peek(0)) {
                    return Err(self.error("statement delimiter or end of input").into());
                }
                return Ok(true);
            }
        }
        self.position = prefix.delimiter_index;
        Ok(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{LexError, ParseError};

    fn text(sql: &str, span: Span) -> &str {
        &sql[span.start()..span.end()]
    }

    #[test]
    fn actual_routine_productions_own_inner_delimiters_in_create_and_inline_queries() {
        let create = "-- café\r\nCREATE FUNCTION f() RETURNS BIGINT BEGIN RETURN 1; END";
        let inline = "\r\nWITH FUNCTION g() RETURNS BIGINT BEGIN RETURN 2; END SELECT g()";
        let ordinary = "\nSELECT function FROM t /* ; */";
        let sql = format!("{create};{inline};{ordinary}");
        let frames = frame_statements(&sql).unwrap();
        assert_eq!(frames.len(), 3);
        for (frame, expected) in frames.iter().zip([create, inline, ordinary]) {
            let actual = text(&sql, frame.source);
            assert_eq!(actual, expected);
            super::super::parse_statement(actual).unwrap();
            assert!(frame.lexical_failure.is_none());
        }
        assert_eq!(text(&sql, frames[0].delimiter.unwrap()), ";");
        assert_eq!(text(&sql, frames[1].delimiter.unwrap()), ";");
        assert!(frames[2].delimiter.is_none());
    }

    #[test]
    fn bounded_lexer_failures_preserve_raw_bytes_and_first_strict_rejection() {
        let first = " /* café ; */ SELECT #, `invalid;name`";
        let second = " SELECT `invalid;name`";
        let sql = format!("{first};{second}; SELECT 1;");
        let frames = frame_statements(&sql).unwrap();
        assert_eq!(frames.len(), 3);
        assert_eq!(text(&sql, frames[0].source), first);
        assert_eq!(text(&sql, frames[1].source), second);
        assert!(matches!(
            frames[0].lexical_failure,
            Some(ParserError::Lex(LexError::UnexpectedCharacter { character: '#', span }))
                if text(&sql, span) == "#"
        ));
        assert!(matches!(
            &frames[1].lexical_failure,
            Some(ParserError::Parse(ParseError::UnexpectedToken { span, .. }))
                if text(&sql, *span) == "`invalid;name`"
        ));
        assert_eq!(frames[0].lexical_failure, trino_lexer::lex(&sql).err());
        assert!(frames[2].lexical_failure.is_none());
    }

    #[test]
    fn an_unterminated_terminal_unit_without_a_possible_delimiter_keeps_its_error() {
        for sql in ["SELECT 'unclosed", "SELECT \"unclosed", "SELECT 1 /* open"] {
            let frames = frame_statements(sql).unwrap();
            assert_eq!(frames.len(), 1);
            assert_eq!(text(sql, frames[0].source), sql);
            assert!(frames[0].delimiter.is_none());
            assert_eq!(frames[0].lexical_failure, trino_lexer::lex(sql).err());
        }
    }

    #[test]
    fn rejects_ambiguous_bodies_without_splitting_them_into_smaller_submissions() {
        for sql in [
            "CREATE FUNCTION f() RETURNS BIGINT BEGIN RETURN 1; SELECT 2;",
            "WITH FUNCTION g() RETURNS BIGINT BEGIN RETURN #; END SELECT g();",
            "SELECT 'unclosed; SELECT 2;",
        ] {
            assert!(frame_statements(sql).is_err(), "{sql}");
        }
    }

    #[test]
    fn ordinary_function_identifier_in_syntax_negative_does_not_own_delimiters() {
        let sql = "SELECT function +; SELECT 1;";
        let frames = frame_statements(sql).unwrap();
        assert_eq!(frames.len(), 2);
        assert_eq!(text(sql, frames[0].source), "SELECT function +");
        assert!(frames[0].lexical_failure.is_none());
    }
}
