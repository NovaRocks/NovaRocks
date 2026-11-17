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
use crate::ast::*;
use crate::trino_token::{Keyword as K, Symbol as S, TokenKind as T};
use crate::{ParseError, SYNTHETIC, Span};

impl Parser<'_> {
    /// Non-reserved spellings also have ordinary identifier/call productions.
    /// Delimiters nested in any bracket container cannot select this production.
    pub(super) fn reserved_expression_ahead(&self) -> bool {
        let separator = match self.peek(0).kind {
            T::Keyword(K::Normalize | K::Extract | K::Trim | K::Grouping | K::Listagg) => {
                return true;
            }
            T::Keyword(K::Position) => K::In,
            T::Keyword(K::Substring) => K::From,
            T::Keyword(K::Overlay) => K::Placing,
            _ => return false,
        };
        self.special_marker_ahead(separator)
    }

    fn special_marker_ahead(&self, separator: K) -> bool {
        if self.peek(1).kind != T::Symbol(S::LParen) {
            return false;
        }
        let mut containers = vec![S::RParen];
        for token in self.tokens[self.position..]
            .iter()
            .filter(|token| !matches!(token.kind, T::Trivia(_)))
            .skip(2)
        {
            match token.kind {
                T::End => return false,
                T::Symbol(S::LParen) => containers.push(S::RParen),
                T::Symbol(S::LBracket) => containers.push(S::RBracket),
                T::Symbol(S::LBrace) => containers.push(S::RBrace),
                T::Symbol(close @ (S::RParen | S::RBracket | S::RBrace)) => {
                    if containers.pop() != Some(close) || containers.is_empty() {
                        return false;
                    }
                }
                T::Keyword(keyword) if containers.len() == 1 && keyword == separator => {
                    return true;
                }
                T::Symbol(S::Comma) if containers.len() == 1 => return false,
                _ => {}
            }
        }
        false
    }

    #[recursive::recursive]
    pub(super) fn parse_reserved_expression(&mut self) -> Result<Expr, ParserError> {
        match self.peek(0).kind {
            T::Keyword(K::Position) => self.parse_position(),
            T::Keyword(K::Substring) => self.parse_substring(),
            T::Keyword(K::Normalize) => self.parse_normalize(),
            T::Keyword(K::Extract) => self.parse_extract(),
            T::Keyword(K::Trim) => self.parse_trim(),
            T::Keyword(K::Overlay) => self.parse_overlay(),
            T::Keyword(K::Grouping) => self.parse_grouping(),
            T::Keyword(K::Listagg) => self.parse_listagg(),
            _ => Err(self.error("a reserved expression production")),
        }
    }

    #[recursive::recursive]
    fn parse_position(&mut self) -> Result<Expr, ParserError> {
        let start = self.expect_keyword(K::Position)?;
        self.expect_symbol(S::LParen)?;
        let needle = self.parse_value_expression()?;
        self.expect_keyword(K::In)?;
        let haystack = self.parse_value_expression()?;
        self.expect_symbol(S::RParen)?;
        // Visit the original needle first. Reversing the final AST argument
        // list must not reverse the already observed parameter events.
        Ok(special_call(
            "strpos",
            false,
            vec![haystack, needle],
            self.span_since(start),
        ))
    }

    #[recursive::recursive]
    fn parse_substring(&mut self) -> Result<Expr, ParserError> {
        let start = self.expect_keyword(K::Substring)?;
        self.expect_symbol(S::LParen)?;
        let mut arguments = vec![self.parse_value_expression()?];
        self.expect_keyword(K::From)?;
        arguments.push(self.parse_value_expression()?);
        if self.eat_keyword(K::For).is_some() {
            arguments.push(self.parse_value_expression()?);
        }
        self.expect_symbol(S::RParen)?;
        Ok(special_call(
            "substr",
            false,
            arguments,
            self.span_since(start),
        ))
    }

    #[recursive::recursive]
    fn parse_normalize(&mut self) -> Result<Expr, ParserError> {
        let start = self.expect_keyword(K::Normalize)?;
        self.expect_symbol(S::LParen)?;
        let value = self.parse_value_expression()?;
        let form = if self.eat_symbol(S::Comma).is_some() {
            let token = self.current();
            if !matches!(token.kind, T::Keyword(K::Nfc | K::Nfd | K::Nfkc | K::Nfkd)) {
                return Err(self.error("NFC, NFD, NFKC, or NFKD"));
            }
            self.bump();
            self.source[token.span.start()..token.span.end()].to_owned()
        } else {
            "NFC".to_owned()
        };
        self.expect_symbol(S::RParen)?;
        let span = self.span_since(start);
        Ok(special_call(
            "normalize",
            true,
            vec![value, string_literal(form, span)],
            span,
        ))
    }

    #[recursive::recursive]
    fn parse_extract(&mut self) -> Result<Expr, ParserError> {
        let start = self.expect_keyword(K::Extract)?;
        self.expect_symbol(S::LParen)?;
        let token = self.current();
        if !self.is_identifier(0) {
            return Err(self.error("an EXTRACT field identifier"));
        }
        self.bump();
        let field = match self.source[token.span.start()..token.span.end()]
            .to_ascii_uppercase()
            .as_str()
        {
            "YEAR" => ExtractField::Year,
            "QUARTER" => ExtractField::Quarter,
            "MONTH" => ExtractField::Month,
            "WEEK" => ExtractField::Week,
            "DAY" => ExtractField::Day,
            "DAY_OF_MONTH" => ExtractField::DayOfMonth,
            "DAY_OF_WEEK" => ExtractField::DayOfWeek,
            "DOW" => ExtractField::Dow,
            "DAY_OF_YEAR" => ExtractField::DayOfYear,
            "DOY" => ExtractField::Doy,
            "YEAR_OF_WEEK" => ExtractField::YearOfWeek,
            "YOW" => ExtractField::Yow,
            "HOUR" => ExtractField::Hour,
            "MINUTE" => ExtractField::Minute,
            "SECOND" => ExtractField::Second,
            "TIMEZONE_MINUTE" => ExtractField::TimezoneMinute,
            "TIMEZONE_HOUR" => ExtractField::TimezoneHour,
            _ => {
                return Err(special_error(
                    "a valid EXTRACT field",
                    self.source,
                    self.span_since(start),
                ));
            }
        };
        self.expect_keyword(K::From)?;
        let expression = self.parse_value_expression()?;
        self.expect_symbol(S::RParen)?;
        Ok(Expr::Extract(AstBox::new(ExtractExpr {
            expression,
            field,
            span: self.span_since(start),
        })))
    }

    #[recursive::recursive]
    fn parse_trim(&mut self) -> Result<Expr, ParserError> {
        let from_form = self.special_marker_ahead(K::From);
        let start = self.expect_keyword(K::Trim)?;
        self.expect_symbol(S::LParen)?;
        if !from_form {
            let trim_source = self.parse_value_expression()?;
            let trim_character = if self.eat_symbol(S::Comma).is_some() {
                Some(self.parse_value_expression()?)
            } else {
                None
            };
            self.expect_symbol(S::RParen)?;
            return Ok(Expr::Trim(AstBox::new(Trim {
                specification: TrimSpecification::Both,
                trim_source,
                trim_character,
                span: self.span_since(start),
            })));
        }
        let specification = match self.peek(0).kind {
            T::Keyword(K::Both) => Some(TrimSpecification::Both),
            T::Keyword(K::Leading) => Some(TrimSpecification::Leading),
            T::Keyword(K::Trailing) => Some(TrimSpecification::Trailing),
            _ => None,
        };
        if let Some(specification) = specification {
            let checkpoint = self.checkpoint();
            self.bump();
            match self.parse_trim_from(start, Some(specification)) {
                Ok(expression) => return Ok(expression),
                Err(ParserError::Construction(error)) => {
                    return Err(ParserError::Construction(error));
                }
                Err(ParserError::Syntax(_)) => self.restore(checkpoint),
            }
        }
        self.parse_trim_from(start, None)
    }

    #[recursive::recursive]
    fn parse_trim_from(
        &mut self,
        start: Span,
        specification: Option<TrimSpecification>,
    ) -> Result<Expr, ParserError> {
        let parameter_start = self.next_parameter;
        let character_start = self.next_parameter;
        let trim_character = if self.at_keyword(K::From) {
            None
        } else {
            Some(self.parse_value_expression()?)
        };
        let character_end = self.next_parameter;
        self.expect_keyword(K::From)?;
        let source_start = self.next_parameter;
        let trim_source = self.parse_value_expression()?;
        let source_end = self.next_parameter;
        self.expect_symbol(S::RParen)?;
        let span = self.span_since(start);
        if specification.is_none() && trim_character.is_none() {
            return Err(special_error(
                "a trim specification or trim character before FROM",
                self.source,
                span,
            ));
        }
        self.reorder_parameter_groups(
            parameter_start,
            &[source_start..source_end, character_start..character_end],
        );
        Ok(Expr::Trim(AstBox::new(Trim {
            specification: specification.unwrap_or(TrimSpecification::Both),
            trim_source,
            trim_character,
            span,
        })))
    }

    #[recursive::recursive]
    fn parse_overlay(&mut self) -> Result<Expr, ParserError> {
        let start = self.expect_keyword(K::Overlay)?;
        self.expect_symbol(S::LParen)?;
        let value = self.parse_value_expression()?;
        self.expect_keyword(K::Placing)?;
        let replacement = self.parse_value_expression()?;
        self.expect_keyword(K::From)?;
        let index = self.parse_value_expression()?;
        let length = if self.eat_keyword(K::For).is_some() {
            Some(self.parse_value_expression()?)
        } else {
            None
        };
        self.expect_symbol(S::RParen)?;
        Ok(Expr::Overlay(AstBox::new(Overlay {
            value,
            replacement,
            start: index,
            length,
            span: self.span_since(start),
        })))
    }

    fn parse_grouping(&mut self) -> Result<Expr, ParserError> {
        let start = self.expect_keyword(K::Grouping)?;
        self.expect_symbol(S::LParen)?;
        let mut names = Vec::new();
        if !self.at_symbol(S::RParen) {
            loop {
                names.push(self.qualified_name()?);
                if self.eat_symbol(S::Comma).is_none() {
                    break;
                }
            }
        }
        self.expect_symbol(S::RParen)?;
        let span = self.span_since(start);
        let node = GroupingOperation::new(names, span).map_err(|_| {
            special_error("at least one qualified name in GROUPING", self.source, span)
        })?;
        Ok(Expr::GroupingOperation(AstBox::new(node)))
    }

    #[recursive::recursive]
    fn parse_listagg(&mut self) -> Result<Expr, ParserError> {
        let start = self.expect_keyword(K::Listagg)?;
        let parameter_start = self.next_parameter;
        self.expect_symbol(S::LParen)?;
        self.with_optional_set_quantifier(|parser, quantifier| {
            parser.parse_listagg_body(start, parameter_start, quantifier.unwrap_or(false))
        })
    }

    #[recursive::recursive]
    fn parse_listagg_body(
        &mut self,
        start: Span,
        parameter_start: usize,
        distinct: bool,
    ) -> Result<Expr, ParserError> {
        let argument_start = self.next_parameter;
        let value = self.parse_expression()?;
        let argument_end = self.next_parameter;
        let separator = if self.eat_symbol(S::Comma).is_some() {
            Some(self.parse_string()?)
        } else {
            None
        };
        let mut overflow_error = true;
        let mut filler = None;
        let mut show_count = false;
        if self.eat_keyword(K::On).is_some() {
            self.expect_keyword(K::Overflow)?;
            if self.eat_keyword(K::Error).is_none() {
                self.expect_keyword(K::Truncate)?;
                overflow_error = false;
                if matches!(self.peek(0).kind, T::String | T::UnicodeString) {
                    filler = Some(self.parse_string()?);
                }
                if self.eat_keyword(K::With).is_some() {
                    show_count = true;
                } else {
                    self.expect_keyword(K::Without)?;
                }
                self.expect_keyword(K::Count)?;
            }
        }
        self.expect_symbol(S::RParen)?;
        self.expect_keyword(K::Within)?;
        self.expect_keyword(K::Group)?;
        self.expect_symbol(S::LParen)?;
        let order_start = self.next_parameter;
        let order_by = self.parse_order_by()?;
        let order_end = self.next_parameter;
        self.expect_symbol(S::RParen)?;
        let filter_start = self.next_parameter;
        let filter = if self.eat_keyword(K::Filter).is_some() {
            self.expect_symbol(S::LParen)?;
            self.expect_keyword(K::Where)?;
            let expression = self.parse_expression()?;
            self.expect_symbol(S::RParen)?;
            Some(expression)
        } else {
            None
        };
        let filter_end = self.next_parameter;
        let window_start = self.next_parameter;
        let window = if self.at_keyword(K::Over) {
            Some(self.parse_window()?)
        } else {
            None
        };
        let window_end = self.next_parameter;
        self.reorder_parameter_groups(
            parameter_start,
            &[
                window_start..window_end,
                order_start..order_end,
                argument_start..argument_end,
                filter_start..filter_end,
            ],
        );
        let span = self.span_since(start);
        let (separator_value, separator_span) = separator.unwrap_or_else(|| (String::new(), span));
        let (filler_value, filler_span) = filler.unwrap_or_else(|| ("...".to_owned(), span));
        let arguments = vec![
            value,
            string_literal(separator_value, separator_span),
            boolean_literal(overflow_error, span),
            string_literal(filler_value, filler_span),
            boolean_literal(show_count, span),
        ];
        Ok(Expr::FunctionCall(AstBox::new(FunctionCall {
            name: special_name("LISTAGG", false),
            arguments: special_arguments(arguments),
            distinct,
            order_by: Some(order_by),
            filter,
            window,
            null_treatment: None,
            processing_mode: None,
            span,
        })))
    }
}

fn special_call(name: &str, quoted: bool, arguments: Vec<Expr>, span: Span) -> Expr {
    Expr::FunctionCall(AstBox::new(FunctionCall {
        name: special_name(name, quoted),
        arguments: special_arguments(arguments),
        distinct: false,
        order_by: None,
        filter: None,
        null_treatment: None,
        window: None,
        processing_mode: None,
        span,
    }))
}
fn special_name(name: &str, quoted: bool) -> ObjectName {
    ObjectName {
        parts: vec![Ident {
            value: name.to_owned(),
            quoted,
            span: SYNTHETIC,
        }],
        span: SYNTHETIC,
    }
}
fn special_arguments(arguments: Vec<Expr>) -> AstVec<CallArgument> {
    arguments
        .into_iter()
        .map(|value| CallArgument {
            span: value.span(),
            value,
            name: None,
        })
        .collect()
}
fn string_literal(value: String, span: Span) -> Expr {
    Expr::Literal(Literal {
        kind: LiteralKind::String(value),
        span,
    })
}
fn boolean_literal(value: bool, span: Span) -> Expr {
    Expr::Literal(Literal {
        kind: LiteralKind::Boolean(value),
        span,
    })
}
fn special_error(expected: &'static str, source: &str, span: Span) -> ParserError {
    ParserError::Construction(
        ParseError::UnexpectedToken {
            expected,
            found: source[span.start()..span.end()].to_owned(),
            span,
        }
        .into(),
    )
}
