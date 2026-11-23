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

//! Lossless scanning according to the SQL grammar's lexical rules.

use crate::keyword;
use crate::token::{Symbol, Token, TokenKind, TriviaKind};
use crate::{LexError, ParseError, ParserError, Span};

/// Produces contiguous source spans, including trivia and a final empty `End`.
///
/// Literal contents remain in the source. The grammar and AST builder own
/// Unicode escape clauses, binary contents, literal conversion, and identifier
/// use. This scanner only rejects identifier forms forbidden by the language.
pub fn lex(source: &str) -> Result<Vec<Token>, ParserError> {
    let mut lexer = Lexer::new(source);
    let mut tokens = Vec::new();
    while lexer.position < source.len() {
        tokens.push(lexer.token()?);
    }
    tokens.push(lexer.end_token());
    Ok(tokens)
}

/// A lexer-owned rejection within a provable source range.
pub(crate) struct FramingRejection {
    pub(crate) span: Span,
    pub(crate) error: ParserError,
}

pub(crate) struct FramingTokens {
    pub(crate) tokens: Vec<Token>,
    pub(crate) rejections: Vec<FramingRejection>,
}

/// Keeps the scanner's real token boundaries around a proved rejection.
/// This opt-in stream proves framing only. The strict lexer still stops at
/// the first rejection and is the sole authority for parsing a statement.
pub(crate) fn lex_for_framing(source: &str) -> Result<FramingTokens, ParserError> {
    let mut lexer = Lexer::new(source);
    let mut tokens = Vec::new();
    let mut rejections = Vec::new();
    while lexer.position < source.len() {
        let start = lexer.position;
        match lexer.token() {
            Ok(token) => tokens.push(token),
            Err(
                error @ (ParserError::Lex(LexError::UnexpectedCharacter { .. })
                | ParserError::Parse(ParseError::UnexpectedToken { .. })),
            ) => rejections.push(FramingRejection {
                span: Span::new(start, lexer.position),
                error,
            }),
            Err(
                error @ ParserError::Lex(
                    LexError::UnterminatedString { .. }
                    | LexError::UnterminatedQuotedIdentifier { .. }
                    | LexError::UnterminatedComment { .. },
                ),
            ) if !source.as_bytes()[start..].contains(&b';') => {
                // Nothing after this malformed opening token can be a
                // delimiter. Submit the exact terminal bytes only to a
                // caller that already owns an enrolled error expectation.
                lexer.position = source.len();
                rejections.push(FramingRejection {
                    span: Span::new(start, source.len()),
                    error,
                });
            }
            Err(error) => return Err(error),
        }
    }
    tokens.push(lexer.end_token());
    Ok(FramingTokens { tokens, rejections })
}

struct Lexer<'a> {
    source: &'a str,
    position: usize,
}

impl<'a> Lexer<'a> {
    fn new(source: &'a str) -> Self {
        Self {
            source,
            position: 0,
        }
    }

    fn end_token(&self) -> Token {
        Token::new(TokenKind::End, Span::new(self.position, self.position))
    }

    fn token(&mut self) -> Result<Token, ParserError> {
        let start = self.position;
        let rest = &self.source[start..];
        let first = rest.as_bytes()[0];
        let kind = if is_whitespace(first) {
            self.position += rest.bytes().take_while(|b| is_whitespace(*b)).count();
            TokenKind::Trivia(TriviaKind::Whitespace)
        } else if rest.starts_with("--") {
            self.position += 2;
            while self.position < self.source.len()
                && !matches!(self.source.as_bytes()[self.position], b'\r' | b'\n')
            {
                self.position += 1;
            }
            if self.byte() == Some(b'\r') {
                self.position += 1;
            }
            if self.byte() == Some(b'\n') {
                self.position += 1;
            }
            TokenKind::Trivia(TriviaKind::LineComment)
        } else if let Some(comment) = rest.strip_prefix("/*") {
            match comment.find("*/") {
                Some(end) => self.position += end + 4,
                None => {
                    return Err(LexError::UnterminatedComment {
                        span: Span::new(start, self.source.len()),
                    }
                    .into());
                }
            }
            TokenKind::Trivia(TriviaKind::BlockComment)
        } else if rest.starts_with('"') {
            self.consume_quote(start, start, b'"', true)?;
            if self.position == start + 2 {
                return Err(self.syntax_error(
                    "a non-empty quoted identifier",
                    "zero-length delimited identifier",
                    start,
                ));
            }
            TokenKind::QuotedIdentifier
        } else if first == b'`' {
            self.consume_quote(start, start, b'`', true)?;
            return Err(self.syntax_error(
                "double quotes around identifiers",
                "backquoted identifiers are not supported",
                start,
            ));
        } else if first == b'\'' {
            self.consume_quote(start, start, b'\'', true)?;
            TokenKind::String
        } else if rest.get(..3).is_some_and(|s| s.eq_ignore_ascii_case("U&'")) {
            self.consume_quote(start, start + 2, b'\'', true)?;
            TokenKind::UnicodeString
        } else if rest.get(..2).is_some_and(|s| s.eq_ignore_ascii_case("X'")) {
            self.consume_quote(start, start + 1, b'\'', false)?;
            TokenKind::BinaryLiteral
        } else if let Some(end) = rest.strip_prefix("$$").and_then(|rest| rest.find("$$")) {
            self.position += end + 4;
            TokenKind::DollarString
        } else if first.is_ascii_digit()
            || (first == b'.' && rest.as_bytes().get(1).is_some_and(u8::is_ascii_digit))
        {
            let (length, kind) = number(rest.as_bytes());
            self.position += length;
            match kind {
                Some(kind) => kind,
                None => {
                    return Err(self.syntax_error(
                        "an identifier beginning with a letter or underscore",
                        "identifiers must not start with a digit; surround the identifier with double quotes",
                        start,
                    ));
                }
            }
        } else if is_identifier_start(first) {
            self.position += rest.bytes().take_while(|b| is_identifier_part(*b)).count();
            let word = &self.source[start..self.position];
            keyword::lookup(word).map_or(TokenKind::Identifier, TokenKind::Keyword)
        } else if let Some((length, symbol)) = symbol(rest) {
            self.position += length;
            TokenKind::Symbol(symbol)
        } else {
            let character = rest.chars().next().expect("nonempty source remainder");
            self.position += character.len_utf8();
            return Err(LexError::UnexpectedCharacter {
                character,
                span: Span::new(start, self.position),
            }
            .into());
        };
        Ok(Token::new(kind, Span::new(start, self.position)))
    }

    fn byte(&self) -> Option<u8> {
        self.source.as_bytes().get(self.position).copied()
    }

    fn syntax_error(&self, expected: &'static str, found: &str, start: usize) -> ParserError {
        ParseError::UnexpectedToken {
            expected,
            found: found.to_owned(),
            span: Span::new(start, self.position),
        }
        .into()
    }

    fn consume_quote(
        &mut self,
        start: usize,
        opening: usize,
        quote: u8,
        doubled: bool,
    ) -> Result<(), ParserError> {
        self.position = opening + 1;
        while let Some(byte) = self.byte() {
            self.position += 1;
            if byte == quote {
                if doubled && self.byte() == Some(quote) {
                    self.position += 1;
                } else {
                    return Ok(());
                }
            }
        }
        let span = Span::new(start, self.position);
        Err(if quote == b'\'' {
            LexError::UnterminatedString { span }
        } else {
            LexError::UnterminatedQuotedIdentifier { span }
        }
        .into())
    }
}

fn is_whitespace(byte: u8) -> bool {
    matches!(byte, b' ' | b'\r' | b'\n' | b'\t')
}

fn is_identifier_start(byte: u8) -> bool {
    byte.is_ascii_alphabetic() || byte == b'_'
}

fn is_identifier_part(byte: u8) -> bool {
    is_identifier_start(byte) || byte.is_ascii_digit()
}

/// Maximal matching preserves the grammar's token priority for equal lengths.
fn number(bytes: &[u8]) -> (usize, Option<TokenKind>) {
    let integer = decimal_integer(bytes, 0);
    let mut best = (integer, Some(TokenKind::Integer));
    if bytes.starts_with(b"0x") || bytes.starts_with(b"0X") {
        choose(
            &mut best,
            based_integer(bytes, 2, u8::is_ascii_hexdigit),
            TokenKind::Integer,
        );
    } else if bytes.starts_with(b"0o") || bytes.starts_with(b"0O") {
        choose(
            &mut best,
            based_integer(bytes, 2, |b| matches!(b, b'0'..=b'7')),
            TokenKind::Integer,
        );
    } else if bytes.starts_with(b"0b") || bytes.starts_with(b"0B") {
        choose(
            &mut best,
            based_integer(bytes, 2, |b| matches!(b, b'0' | b'1')),
            TokenKind::Integer,
        );
    }
    let decimal = if bytes.first() == Some(&b'.') {
        let end = decimal_integer(bytes, 1);
        if end > 1 { end } else { 0 }
    } else if bytes.get(integer) == Some(&b'.') {
        decimal_integer(bytes, integer + 1)
    } else {
        0
    };
    choose(&mut best, decimal, TokenKind::Decimal);
    choose(&mut best, double(bytes), TokenKind::Double);
    if bytes.first().is_some_and(u8::is_ascii_digit) {
        let identifier = bytes.iter().take_while(|b| is_identifier_part(**b)).count();
        if identifier > 1 && identifier > best.0 {
            best = (identifier, None);
        }
    }
    best
}

fn choose(best: &mut (usize, Option<TokenKind>), length: usize, kind: TokenKind) {
    if length > best.0 {
        *best = (length, Some(kind));
    }
}

fn decimal_integer(bytes: &[u8], start: usize) -> usize {
    if !bytes.get(start).is_some_and(u8::is_ascii_digit) {
        return start;
    }
    let mut position = start + 1;
    loop {
        if bytes.get(position).is_some_and(u8::is_ascii_digit) {
            position += 1;
        } else if bytes.get(position) == Some(&b'_')
            && bytes.get(position + 1).is_some_and(u8::is_ascii_digit)
        {
            position += 2;
        } else {
            return position;
        }
    }
}

fn based_integer(bytes: &[u8], start: usize, digit: impl Fn(&u8) -> bool) -> usize {
    let mut position = start;
    loop {
        if bytes.get(position).is_some_and(&digit) {
            position += 1;
        } else if bytes.get(position) == Some(&b'_') && bytes.get(position + 1).is_some_and(&digit)
        {
            position += 2;
        } else {
            return if position > start { position } else { 0 };
        }
    }
}

fn double(bytes: &[u8]) -> usize {
    let mut position = 0;
    if bytes.first() == Some(&b'.') {
        position = 1;
        let start = position;
        while bytes.get(position).is_some_and(u8::is_ascii_digit) {
            position += 1;
        }
        if start == position {
            return 0;
        }
    } else {
        while bytes.get(position).is_some_and(u8::is_ascii_digit) {
            position += 1;
        }
        if position == 0 {
            return 0;
        }
        if bytes.get(position) == Some(&b'.') {
            position += 1;
            while bytes.get(position).is_some_and(u8::is_ascii_digit) {
                position += 1;
            }
        }
    }
    if !matches!(bytes.get(position), Some(b'e' | b'E')) {
        return 0;
    }
    position += 1;
    if matches!(bytes.get(position), Some(b'+' | b'-')) {
        position += 1;
    }
    let start = position;
    while bytes.get(position).is_some_and(u8::is_ascii_digit) {
        position += 1;
    }
    if start == position { 0 } else { position }
}

fn symbol(source: &str) -> Option<(usize, Symbol)> {
    const DOUBLE: &[(&str, Symbol)] = &[
        ("!=", Symbol::Neq),
        ("<>", Symbol::Neq),
        ("<=", Symbol::Lte),
        (">=", Symbol::Gte),
        ("||", Symbol::DoublePipe),
        ("->", Symbol::Arrow),
        ("::", Symbol::DoubleColon),
        ("=>", Symbol::FatArrow),
        ("{-", Symbol::ExcludeStart),
        ("-}", Symbol::ExcludeEnd),
    ];
    for &(spelling, symbol) in DOUBLE {
        if source.starts_with(spelling) {
            return Some((spelling.len(), symbol));
        }
    }
    let symbol = match source.as_bytes()[0] {
        b'=' => Symbol::Eq,
        b'<' => Symbol::Lt,
        b'>' => Symbol::Gt,
        b'+' => Symbol::Plus,
        b'-' => Symbol::Minus,
        b'*' => Symbol::Star,
        b'/' => Symbol::Slash,
        b'%' => Symbol::Percent,
        b'?' => Symbol::Question,
        b';' => Symbol::Semicolon,
        b'.' => Symbol::Dot,
        b'(' => Symbol::LParen,
        b')' => Symbol::RParen,
        b',' => Symbol::Comma,
        b':' => Symbol::Colon,
        b'@' => Symbol::At,
        b'[' => Symbol::LBracket,
        b']' => Symbol::RBracket,
        b'^' => Symbol::Caret,
        b'{' => Symbol::LBrace,
        b'}' => Symbol::RBrace,
        b'|' => Symbol::Pipe,
        b'$' => Symbol::Dollar,
        _ => return None,
    };
    Some((1, symbol))
}
