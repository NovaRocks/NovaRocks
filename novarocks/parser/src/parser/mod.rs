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

//! Recursive-descent parser components.
//!
//! The public statement `parse()` entry point is added by SQLP-1 T6 after the
//! expression and printer foundations have converged.

mod catalog;
mod command;
mod dml;
mod expr;
mod iceberg;
mod maintenance;
mod materialized_view;
mod pratt;
mod query;
mod session;
mod show_backends;
mod show_processlist;
mod statistics;
mod table;
mod view;

use crate::{
    ParseError, ParserError, Span, Token, TokenKind,
    ast::{Ident, Literal, LiteralKind, ObjectName, Statement, validate_statements},
    keyword_class, lex,
    token::{Keyword, Symbol},
};

pub use expr::parse_expression;
pub(crate) use pratt::balance_boolean_chain;
pub(crate) use query::balance_union_all;

/// `statements ::= { statement [ ";" ] } EOF`
///
/// Parses the currently-owned statement families. A statement head which is
/// syntactically recognizable but has not yet been adopted is a typed parser
/// outcome rather than a route miss or a text-classified fallback.
pub fn parse(source: &str) -> Result<Vec<Statement>, ParserError> {
    let tokens = lex(source)?;
    let statements = StatementParser::new(source, &tokens).parse_statements()?;
    validate_statements(&statements)?;
    Ok(statements)
}

struct StatementParser<'source, 'tokens> {
    source: &'source str,
    tokens: &'tokens [Token],
    position: usize,
    pending_type_gt: Option<Span>,
}

impl<'source, 'tokens> StatementParser<'source, 'tokens> {
    fn new(source: &'source str, tokens: &'tokens [Token]) -> Self {
        Self {
            source,
            tokens,
            position: 0,
            pending_type_gt: None,
        }
    }

    fn parse_statements(mut self) -> Result<Vec<Statement>, ParserError> {
        let mut statements = Vec::new();
        self.skip_trivia();
        while !self.is_end() {
            statements.push(self.parse_statement()?);
            self.skip_trivia();
            if self.current_is_symbol(Symbol::Semicolon) {
                self.advance();
                self.skip_trivia();
            } else if !self.is_end() {
                return Err(self.unexpected("';' or end of input").into());
            }
        }
        Ok(statements)
    }

    /// Dispatches one owned command family without legacy fallthrough.
    ///
    /// Family parsers return `None` only when the input is not their family.
    /// Once a parser has recognized a family, every malformed form is an
    /// immediate typed error from that parser.
    fn parse_statement(&mut self) -> Result<Statement, ParserError> {
        for parser in [
            view::parse as FamilyParser,
            // Ahead of `show_backends`, which claims any unrecognized `SHOW`
            // and then fails asking for BACKENDS.
            show_processlist::parse,
            show_backends::parse,
            statistics::parse,
            session::parse,
            catalog::parse,
            table::parse,
            dml::parse,
            iceberg::parse,
            maintenance::parse,
            materialized_view::parse,
            query::parse,
        ] {
            if let Some(statement) = parser(self)? {
                return Ok(statement);
            }
        }
        Err(ParseError::UnsupportedStatement {
            statement: self.current_description(),
            span: self.current_span(),
        }
        .into())
    }

    pub(super) fn current(&self) -> Option<&Token> {
        self.tokens.get(self.position)
    }

    pub(super) fn current_span(&self) -> Span {
        self.current()
            .map(|token| token.span)
            .unwrap_or_else(|| Span::new(self.source.len(), self.source.len()))
    }

    pub(super) fn current_is_keyword(&self, keyword: Keyword) -> bool {
        matches!(self.current().map(|token| &token.kind), Some(TokenKind::Keyword(found)) if *found == keyword)
    }

    pub(super) fn current_is_word(&self, word: &str) -> bool {
        self.current().is_some_and(|token| {
            matches!(token.kind, TokenKind::Ident | TokenKind::Keyword(_))
                && self.source[token.span.start()..token.span.end()].eq_ignore_ascii_case(word)
        })
    }

    pub(super) fn peek_word(&self, offset: usize, word: &str) -> bool {
        self.significant_token(offset).is_some_and(|token| {
            matches!(token.kind, TokenKind::Ident | TokenKind::Keyword(_))
                && self.source[token.span.start()..token.span.end()].eq_ignore_ascii_case(word)
        })
    }

    pub(super) fn consume_word(&mut self, word: &'static str) -> Result<Span, ParseError> {
        if !self.current_is_word(word) {
            return Err(self.unexpected(word));
        }
        let span = self.current_span();
        self.advance();
        self.skip_trivia();
        Ok(span)
    }

    pub(super) fn consume_symbol(&mut self, symbol: Symbol) -> Result<Span, ParseError> {
        if !self.current_is_symbol(symbol) {
            return Err(self.unexpected(symbol.sql()));
        }
        let span = self.current_span();
        self.advance();
        self.skip_trivia();
        Ok(span)
    }

    pub(super) fn consume_if_word(&mut self, word: &str) -> bool {
        if !self.current_is_word(word) {
            return false;
        }
        self.advance();
        self.skip_trivia();
        true
    }

    pub(super) fn consume_if_symbol(&mut self, symbol: Symbol) -> bool {
        if !self.current_is_symbol(symbol) {
            return false;
        }
        self.advance();
        self.skip_trivia();
        true
    }

    /// Consumes one generic-type closing delimiter. The lexer correctly owns
    /// `>>` as a shift operator, while nested generic types need to consume it
    /// as two adjacent `>` delimiters without changing lexical ownership.
    pub(super) fn consume_type_gt(&mut self) -> Result<Span, ParseError> {
        if let Some(span) = self.pending_type_gt.take() {
            return Ok(span);
        }
        if self.current_is_symbol(Symbol::Gt) {
            return self.consume_symbol(Symbol::Gt);
        }
        if self.current_is_symbol(Symbol::ShiftRight) {
            let span = self.current_span();
            let first = Span::new(span.start(), span.start() + 1);
            self.pending_type_gt = Some(Span::new(span.start() + 1, span.end()));
            self.advance();
            self.skip_trivia();
            return Ok(first);
        }
        Err(self.unexpected("'>'"))
    }

    pub(super) fn has_pending_type_gt(&self) -> bool {
        self.pending_type_gt.is_some()
    }

    pub(super) fn parse_ident(&mut self) -> Result<Ident, ParseError> {
        let token = self
            .current()
            .ok_or_else(|| self.unexpected("identifier"))?;
        match token.kind {
            TokenKind::Ident | TokenKind::QuotedIdent | TokenKind::String
                if matches!(token.kind, TokenKind::Ident | TokenKind::QuotedIdent)
                    || self.source[token.span.start()..token.span.end()].starts_with('"') =>
            {
                let source = &self.source[token.span.start()..token.span.end()];
                let (value, quoted, quote_style) = if matches!(token.kind, TokenKind::QuotedIdent) {
                    (
                        source[1..source.len() - 1].replace("``", "`"),
                        true,
                        Some('`'),
                    )
                } else if matches!(token.kind, TokenKind::String) {
                    (
                        source[1..source.len() - 1].replace("\"\"", "\""),
                        true,
                        Some('\"'),
                    )
                } else {
                    (source.to_owned(), false, None)
                };
                let ident = Ident {
                    value,
                    quoted,
                    quote_style,
                    span: token.span,
                };
                self.advance();
                self.skip_trivia();
                Ok(ident)
            }
            TokenKind::Keyword(keyword)
                if keyword_class(keyword) == crate::KeywordClass::NonReserved =>
            {
                let ident = Ident {
                    value: self.source[token.span.start()..token.span.end()].to_owned(),
                    quoted: false,
                    quote_style: None,
                    span: token.span,
                };
                self.advance();
                self.skip_trivia();
                Ok(ident)
            }
            _ => Err(self.unexpected("identifier")),
        }
    }

    /// Parses an identifier in a grammar-local contextual position. Named
    /// procedure arguments may use a clause word such as `where`; accepting it
    /// here does not make that word generally non-reserved.
    pub(super) fn parse_contextual_ident(&mut self) -> Result<Ident, ParseError> {
        let token = self
            .current()
            .cloned()
            .ok_or_else(|| self.unexpected("identifier"))?;
        if matches!(token.kind, TokenKind::Keyword(_)) {
            let ident = Ident {
                value: self.source[token.span.start()..token.span.end()].to_owned(),
                quoted: false,
                quote_style: None,
                span: token.span,
            };
            self.advance();
            self.skip_trivia();
            return Ok(ident);
        }
        self.parse_ident()
    }

    pub(super) fn parse_object_name(&mut self) -> Result<ObjectName, ParseError> {
        let first = self.parse_ident()?;
        let start = first.span.start();
        let mut end = first.span.end();
        let mut parts = vec![first];
        while self.consume_if_symbol(Symbol::Dot) {
            let part = self.parse_ident()?;
            end = part.span.end();
            parts.push(part);
        }
        Ok(ObjectName {
            parts,
            span: Span::new(start, end),
        })
    }

    pub(super) fn parse_literal(&mut self) -> Result<Literal, ParseError> {
        let token = self.current().ok_or_else(|| self.unexpected("literal"))?;
        let span = token.span;
        let source = &self.source[span.start()..span.end()];
        let kind = match token.kind {
            TokenKind::Keyword(Keyword::Null) => LiteralKind::Null,
            TokenKind::Keyword(Keyword::True) => LiteralKind::Boolean(true),
            TokenKind::Keyword(Keyword::False) => LiteralKind::Boolean(false),
            TokenKind::Number => number_literal_kind(source.to_owned(), span)?,
            TokenKind::HexNumber => LiteralKind::HexString(
                source
                    .strip_prefix("0x")
                    .or_else(|| source.strip_prefix("0X"))
                    .expect("hex token must have 0x prefix")
                    .to_owned(),
            ),
            TokenKind::String => hex_string_literal(source)
                .map(LiteralKind::HexString)
                .unwrap_or_else(|| LiteralKind::String(unquote_string(source))),
            _ => return Err(self.unexpected("literal")),
        };
        self.advance();
        self.skip_trivia();
        Ok(Literal { kind, span })
    }

    pub(super) fn current_offset(&self) -> usize {
        self.current_span().start()
    }

    pub(super) fn source_slice(&self, span: Span) -> &str {
        &self.source[span.start()..span.end()]
    }

    pub(super) fn current_is_symbol(&self, symbol: Symbol) -> bool {
        matches!(self.current().map(|token| &token.kind), Some(TokenKind::Symbol(found)) if *found == symbol)
    }

    pub(super) fn advance(&mut self) {
        if self.current().is_some() {
            self.position += 1;
        }
    }

    pub(super) fn unexpected(&self, expected: &'static str) -> ParseError {
        ParseError::UnexpectedToken {
            expected,
            found: self.current_description(),
            span: self.current_span(),
        }
    }

    pub(super) fn skip_trivia(&mut self) {
        while matches!(
            self.current().map(|token| &token.kind),
            Some(TokenKind::Trivia(_))
        ) {
            self.advance();
        }
    }

    fn significant_token(&self, offset: usize) -> Option<&Token> {
        self.tokens
            .iter()
            .skip(self.position)
            .filter(|token| !matches!(token.kind, TokenKind::Trivia(_)))
            .nth(offset)
    }

    pub(super) fn is_end(&self) -> bool {
        self.current()
            .is_none_or(|token| token.kind == TokenKind::End)
    }

    fn current_description(&self) -> String {
        match self.current() {
            None
            | Some(Token {
                kind: TokenKind::End,
                ..
            }) => "EOF".to_owned(),
            Some(token) => format!("`{}`", &self.source[token.span.start()..token.span.end()]),
        }
    }
}

type FamilyParser = for<'source, 'tokens> fn(
    &mut StatementParser<'source, 'tokens>,
) -> Result<Option<Statement>, ParseError>;

pub(crate) fn is_integer_number(value: &str) -> bool {
    !value.contains(['.', 'e', 'E'])
}

/// Check the complete signed spelling before rejecting its positive magnitude.
pub(crate) fn number_literal_kind(value: String, span: Span) -> Result<LiteralKind, ParseError> {
    if is_integer_number(&value) && value.parse::<i64>().is_err() {
        return Err(ParseError::UnexpectedToken {
            expected: "signed 64-bit integer literal",
            found: format!("`{value}`"),
            span,
        });
    }
    Ok(LiteralKind::Number(value))
}

fn unquote_string(source: &str) -> String {
    let body = &source[1..source.len() - 1];
    let quote = source.as_bytes()[0] as char;
    let mut output = String::with_capacity(body.len());
    let mut characters = body.chars().peekable();
    while let Some(character) = characters.next() {
        if character == quote && characters.peek() == Some(&quote) {
            output.push(character);
            characters.next();
        } else if character == '\\' {
            let escaped = characters.next().unwrap_or(character);
            output.push(match escaped {
                '0' => '\0',
                'b' => '\u{0008}',
                'n' => '\n',
                'r' => '\r',
                't' => '\t',
                'Z' => '\u{001a}',
                other => other,
            });
        } else {
            output.push(character);
        }
    }
    output
}

fn hex_string_literal(source: &str) -> Option<String> {
    let prefix = source.chars().next()?;
    if !matches!(prefix, 'x' | 'X') {
        return None;
    }
    let quoted = &source[prefix.len_utf8()..];
    let quote = quoted.chars().next()?;
    if !matches!(quote, '\'' | '"') {
        return None;
    }
    quoted
        .strip_prefix(quote)
        .and_then(|value| value.strip_suffix(quote))
        .map(str::to_owned)
}

#[cfg(test)]
mod tests {
    use crate::{
        ParserError, Span,
        ast::{DmlStatement, ShowBackends, Statement},
        lex,
        printer::print_statements,
    };

    use super::{parse, unquote_string};

    #[test]
    fn show_backends_is_a_complete_round_trip_vertical_slice() {
        let source = " /*+ SET_VAR(x=1) */ show backends; SHOW BACKENDS ";
        let ast = parse(source).expect("SHOW BACKENDS should parse");
        assert_eq!(ast.len(), 2);
        assert_eq!(print_statements(&ast), "SHOW BACKENDS; SHOW BACKENDS");
        let re_parsed = parse(&print_statements(&ast)).expect("printed statements should parse");
        assert_eq!(statement_shapes(&re_parsed), statement_shapes(&ast));
    }

    #[test]
    fn malformed_owned_statement_returns_code_span_and_location() {
        let source = "SHOW BACKENDZ";
        let parser_error = parse(source).expect_err("misspelled SHOW family must fail");
        let ParserError::Parse(_) = parser_error else {
            panic!("expected parser error");
        };
        assert_eq!(
            parser_error.to_user_error(source).code().as_str(),
            "sql.parse.unexpected_token"
        );
        assert_eq!(
            parser_error
                .to_user_error(source)
                .location()
                .unwrap()
                .column(),
            6
        );
        assert_eq!(
            parser_error
                .to_user_error(source)
                .location()
                .unwrap()
                .end_column(),
            Some(14)
        );
    }

    #[test]
    fn owned_dml_and_trailing_token_are_distinguished() {
        assert!(matches!(
            parse("INSERT INTO t VALUES (1)"),
            Ok(statements) if matches!(statements.as_slice(), [Statement::Dml(DmlStatement::Insert(_))])
        ));

        let trailing = parse("SHOW BACKENDS unexpected").expect_err("trailing token must fail");
        assert_eq!(
            trailing
                .to_user_error("SHOW BACKENDS unexpected")
                .code()
                .as_str(),
            "sql.parse.unexpected_token"
        );
        assert_eq!(
            parse("SHOW BACKENDS").unwrap(),
            vec![Statement::ShowBackends(ShowBackends {
                span: Span::new(0, 13),
            })]
        );
    }

    #[test]
    fn lex_errors_remain_parser_errors_at_the_public_boundary() {
        let error = parse("SHOW `unterminated").expect_err("lexing must fail");
        assert!(matches!(error, ParserError::Lex(_)));
        assert!(lex("SHOW `unterminated").is_err());
    }

    #[test]
    fn mysql_string_escapes_are_decoded_in_literal_values() {
        assert_eq!(unquote_string("'a,b\\nc'"), "a,b\nc");
        assert_eq!(unquote_string("'a\\tb\\r\\0\\Z'"), "a\tb\r\0\u{001a}");
        assert_eq!(unquote_string(r"'it''s \\ safe'"), "it's \\ safe");
    }

    #[test]
    fn query_expression_is_owned_as_typed_syntax_without_execution_admission() {
        let source = "SELECT a, count(*) AS n FROM db.t AS t WHERE a >= 1 ORDER BY a DESC LIMIT 5";
        let statements = parse(source).expect("query syntax must parse");
        assert!(matches!(statements.as_slice(), [Statement::Query(_)]));
        assert_eq!(
            crate::printer::Printer::new().statements(&statements),
            "SELECT a, count(*) AS n FROM db.t AS t WHERE a >= 1 ORDER BY a DESC LIMIT 5"
        );

        let statements =
            parse("EXPLAIN ANALYZE VALUES (1), (2)").expect("explain values must parse");
        assert!(matches!(
            statements.as_slice(),
            [Statement::ExplainQuery(_)]
        ));
        assert_eq!(
            crate::printer::Printer::new().statements(&statements),
            "EXPLAIN ANALYZE VALUES (1), (2)"
        );
    }

    fn statement_shapes(statements: &[Statement]) -> Vec<&'static str> {
        statements
            .iter()
            .map(|statement| match statement {
                Statement::ShowBackends(_) => "SHOW BACKENDS",
                Statement::ShowProcessList(_) => "SHOW PROCESSLIST",
                Statement::Statistics(_) => "STATISTICS",
                Statement::Catalog(_) => "CATALOG",
                Statement::Iceberg(_) => "ICEBERG",
                Statement::Maintenance(_) => "MAINTENANCE",
                Statement::MaterializedView(_) => "MATERIALIZED VIEW",
                Statement::View(_) => "VIEW",
                Statement::Table(_) => "TABLE",
                Statement::Dml(_) => "DML",
                Statement::Session(_) => "SESSION",
                Statement::Query(_) => "QUERY",
                Statement::ExplainQuery(_) => "EXPLAIN QUERY",
            })
            .collect()
    }
}
