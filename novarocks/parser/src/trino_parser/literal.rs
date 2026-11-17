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

use super::{GrammarError as ParserError, Parser};
use crate::ast::{
    DoubleValue, GenericLiteral, IntervalLiteral, IntervalSign, Literal, LiteralKind,
};
use crate::trino_token::{Keyword, Symbol, TokenKind};
use crate::{ParseError, Span};

impl Parser<'_> {
    #[recursive::recursive]
    pub(super) fn parse_literal(&mut self) -> Result<Literal, ParserError> {
        let first = self.current();
        let kind = match first.kind {
            TokenKind::Keyword(Keyword::Null) => {
                self.bump();
                LiteralKind::Null
            }
            TokenKind::Keyword(Keyword::True | Keyword::False) => {
                self.bump();
                LiteralKind::Boolean(first.kind == TokenKind::Keyword(Keyword::True))
            }
            TokenKind::Integer
            | TokenKind::Decimal
            | TokenKind::Double
            | TokenKind::Symbol(Symbol::Minus) => {
                let negative = self.eat_symbol(Symbol::Minus).is_some();
                let number = self.current();
                if !matches!(
                    number.kind,
                    TokenKind::Integer | TokenKind::Decimal | TokenKind::Double
                ) {
                    return Err(self.error("a numeric literal after '-'"));
                }
                self.bump();
                // getText() concatenates grammar tokens, excluding intervening trivia.
                let raw = &self.source[number.span.start()..number.span.end()];
                let spelling = if negative {
                    format!("-{raw}")
                } else {
                    raw.to_owned()
                };
                match number.kind {
                    TokenKind::Integer => LiteralKind::Long {
                        parsed_value: integer_value(&spelling).ok_or_else(|| {
                            literal_error(
                                "a signed 64-bit integer literal",
                                &spelling,
                                self.span_since(first.span),
                            )
                        })?,
                        spelling,
                    },
                    TokenKind::Decimal => LiteralKind::Decimal(spelling),
                    TokenKind::Double => LiteralKind::Double(DoubleValue::from_f64(
                        spelling.parse::<f64>().map_err(|_| {
                            literal_error(
                                "a double literal",
                                &spelling,
                                self.span_since(first.span),
                            )
                        })?,
                    )),
                    _ => unreachable!("numeric token checked above"),
                }
            }
            TokenKind::String | TokenKind::UnicodeString => {
                LiteralKind::String(self.parse_string()?.0)
            }
            TokenKind::BinaryLiteral => {
                self.bump();
                let raw = &self.source[first.span.start()..first.span.end()];
                LiteralKind::Binary(binary_value(&raw[2..raw.len() - 1]).ok_or_else(|| {
                    literal_error(
                        "an even number of hexadecimal digits in a binary literal",
                        raw,
                        first.span,
                    )
                })?)
            }
            TokenKind::Keyword(Keyword::Double)
                if self.peek(1).kind == TokenKind::Keyword(Keyword::Precision) =>
            {
                self.bump();
                self.bump();
                let (value, _) = self.parse_string()?;
                LiteralKind::Generic(GenericLiteral {
                    type_name: "DOUBLE".to_owned(),
                    value,
                })
            }
            TokenKind::Keyword(Keyword::Interval) => {
                self.bump();
                let explicit_sign = if self.eat_symbol(Symbol::Minus).is_some() {
                    Some(IntervalSign::Negative)
                } else if self.eat_symbol(Symbol::Plus).is_some() {
                    Some(IntervalSign::Positive)
                } else {
                    None
                };
                let (value, _) = self.parse_string()?;
                if explicit_sign.is_some() || self.at_interval_field() {
                    LiteralKind::Interval(IntervalLiteral {
                        value,
                        sign: explicit_sign.unwrap_or(IntervalSign::Positive),
                        qualifier: self.parse_interval_qualifier()?,
                    })
                } else {
                    // INTERVAL is also a non-reserved identifier in a type constructor.
                    LiteralKind::Generic(GenericLiteral {
                        type_name: self.source[first.span.start()..first.span.end()].to_owned(),
                        value,
                    })
                }
            }
            _ if self.is_identifier(0) => {
                // GenericLiteral retains the raw tag, including quote delimiters.
                self.ident()?;
                let tag = self.source[first.span.start()..first.span.end()].to_owned();
                let (value, _) = self.parse_string()?;
                if tag.eq_ignore_ascii_case("DECIMAL") {
                    LiteralKind::Decimal(value)
                } else {
                    LiteralKind::Generic(GenericLiteral {
                        type_name: tag,
                        value,
                    })
                }
            }
            _ => return Err(self.error("a literal")),
        };
        Ok(Literal {
            kind,
            span: self.span_since(first.span),
        })
    }

    pub(super) fn parse_string(&mut self) -> Result<(String, Span), ParserError> {
        let token = self.current();
        if !matches!(token.kind, TokenKind::String | TokenKind::UnicodeString) {
            return Err(self.error("a string literal"));
        }
        self.bump();
        let raw = &self.source[token.span.start()..token.span.end()];
        if token.kind == TokenKind::String {
            return Ok((unquote(raw), token.span));
        }
        let raw_content = unquote(&raw[2..]);
        let mut escape = '\\';
        if self.eat_keyword(Keyword::Uescape).is_some() {
            let escape_token = self.current();
            if escape_token.kind != TokenKind::String {
                return Err(self.error("a single-character UESCAPE string"));
            }
            self.bump();
            let value = unquote(&self.source[escape_token.span.start()..escape_token.span.end()]);
            let mut characters = value.chars();
            escape = characters
                .next()
                .filter(|character| {
                    character.is_ascii()
                        && *character > ' '
                        && *character < '\u{7f}'
                        && !character.is_ascii_hexdigit()
                        && !matches!(*character, '\'' | '"' | '+')
                })
                .filter(|_| characters.next().is_none())
                .ok_or_else(|| {
                    literal_error(
                        "a valid single ASCII Unicode escape character",
                        &value,
                        self.span_since(token.span),
                    )
                })?;
        }
        let span = self.span_since(token.span);
        let value = unicode_value(&raw_content, escape).ok_or_else(|| {
            literal_error(
                "complete Unicode escapes without surrogate code points",
                raw,
                span,
            )
        })?;
        Ok((value, span))
    }

    /// Dollar strings belong to external routine definitions, not `string` or `literal`.
    pub(super) fn parse_function_definition(&mut self) -> Result<(String, Span), ParserError> {
        let token = self.current();
        if token.kind != TokenKind::DollarString {
            return Err(self.error("a dollar-quoted function definition"));
        }
        self.bump();
        let raw = &self.source[token.span.start()..token.span.end()];
        let value = function_definition_value(raw).ok_or_else(|| {
            literal_error(
                "a newline immediately after the opening dollar quotes",
                raw,
                token.span,
            )
        })?;
        Ok((value.to_owned(), token.span))
    }
}

pub(super) fn integer_value(spelling: &str) -> Option<i64> {
    let cleaned = spelling.replace('_', "");
    let (negative, unsigned) = cleaned
        .strip_prefix('-')
        .map_or((false, cleaned.as_str()), |value| (true, value));
    let (radix, digits) = if unsigned.starts_with("0x") || unsigned.starts_with("0X") {
        (16, &unsigned[2..])
    } else if unsigned.starts_with("0o") || unsigned.starts_with("0O") {
        (8, &unsigned[2..])
    } else if unsigned.starts_with("0b") || unsigned.starts_with("0B") {
        (2, &unsigned[2..])
    } else {
        (10, unsigned)
    };
    if negative {
        i64::from_str_radix(&format!("-{digits}"), radix).ok()
    } else {
        i64::from_str_radix(digits, radix).ok()
    }
}

pub(super) fn literal_error(expected: &'static str, found: &str, span: Span) -> ParserError {
    ParserError::Construction(
        ParseError::UnexpectedToken {
            expected,
            found: found.to_owned(),
            span,
        }
        .into(),
    )
}

fn unquote(raw: &str) -> String {
    raw[1..raw.len() - 1].replace("''", "'")
}

fn unicode_value(content: &str, escape: char) -> Option<String> {
    let mut output = String::new();
    let mut characters = content.chars();
    while let Some(character) = characters.next() {
        if character != escape {
            output.push(character);
            continue;
        }
        let escaped = characters.next()?;
        if escaped == escape {
            output.push(escape);
            continue;
        }
        let (mut value, rest) = if escaped == '+' {
            (0, 6)
        } else {
            (
                escaped
                    .to_digit(16)
                    .filter(|_| escaped.is_ascii_hexdigit())?,
                3,
            )
        };
        for _ in 0..rest {
            let digit = characters.next()?;
            if !digit.is_ascii_hexdigit() {
                return None;
            }
            value = value * 16 + digit.to_digit(16)?;
        }
        // Rust scalar values exclude both surrogate halves, as does the SQL decoder.
        output.push(char::from_u32(value)?);
    }
    Some(output)
}

fn binary_value(content: &str) -> Option<Vec<u8>> {
    // The reference strips Unicode White_Space before uppercasing the whole string.
    let upper = content
        .chars()
        .filter(|character| !character.is_whitespace())
        .flat_map(char::to_uppercase)
        .collect::<String>();
    if !upper.bytes().all(|byte| byte.is_ascii_hexdigit()) || upper.len() % 2 != 0 {
        return None;
    }
    upper
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            Some(((pair[0] as char).to_digit(16)? * 16 + (pair[1] as char).to_digit(16)?) as u8)
        })
        .collect()
}

fn function_definition_value(raw: &str) -> Option<&str> {
    let content = &raw[2..raw.len() - 2];
    if let Some(rest) = content.strip_prefix('\r') {
        Some(rest.strip_prefix('\n').unwrap_or(rest))
    } else {
        content.strip_prefix('\n')
    }
}

#[cfg(test)]
mod tests {
    use super::function_definition_value;

    #[test]
    fn external_definition_strips_exactly_the_required_first_line_break() {
        assert_eq!(
            function_definition_value("$$\nbody''\\\n$$"),
            Some("body''\\\n")
        );
        assert_eq!(function_definition_value("$$\r\nbody$$"), Some("body"));
        assert_eq!(function_definition_value("$$\rbody$$"), Some("body"));
        assert_eq!(function_definition_value("$$\n$$"), Some(""));
        assert_eq!(function_definition_value("$$$$"), None);
        assert_eq!(function_definition_value("$$body$$"), None);
        assert_eq!(function_definition_value("$$ \nbody$$"), None);
    }
}
