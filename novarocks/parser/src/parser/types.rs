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

use super::literal::{integer_value, literal_error};
use super::{GrammarError as ParserError, GrammarError as TypeFailure, Parser};
use crate::Span;
use crate::ast::{
    ArrayLength, AstBox, CompositeIntervalQualifier, DataTypeParameter, DateTimeDataType,
    DateTimeType, ExplicitArrayDataType, GenericDataType, Ident, IntervalDataType,
    IntervalQualifier, IntervalQualifierField, NumericParameter, RowDataType, RowDataTypeField,
    SimpleIntervalQualifier, TypeKind, TypeName, TypeParameter,
};
use crate::token::{Keyword, Symbol, TokenKind};

impl Parser<'_> {
    #[recursive::recursive]
    pub(super) fn parse_type(&mut self) -> Result<TypeName, ParserError> {
        self.parse_type_inner()
    }

    #[recursive::recursive]
    fn parse_type_inner(&mut self) -> Result<TypeName, TypeFailure> {
        let first = self.current();
        let checkpoint = self.position;
        let special = matches!(
            first.kind,
            TokenKind::Keyword(Keyword::Time | Keyword::Timestamp)
        ) || first.kind == TokenKind::Keyword(Keyword::Row)
            && self.peek(1).kind == TokenKind::Symbol(Symbol::LParen)
            || first.kind == TokenKind::Keyword(Keyword::Interval)
                && is_interval_field(self.peek(1).kind);
        let kind = if special {
            match self.parse_special_type() {
                Ok(kind) => kind,
                Err(TypeFailure::Construction(error)) => {
                    return Err(TypeFailure::Construction(error));
                }
                Err(TypeFailure::Syntax(original)) => {
                    self.position = checkpoint;
                    match self.parse_generic_type() {
                        Ok(kind) => kind,
                        Err(TypeFailure::Construction(error)) => {
                            return Err(TypeFailure::Construction(error));
                        }
                        Err(TypeFailure::Syntax(_)) => return Err(TypeFailure::Syntax(original)),
                    }
                }
            }
        } else if first.kind == TokenKind::Keyword(Keyword::Double)
            && self.peek(1).kind == TokenKind::Keyword(Keyword::Precision)
        {
            self.bump();
            self.bump();
            TypeKind::Generic(GenericDataType {
                name: self.type_token_ident(first.span),
                arguments: Vec::new().into(),
            })
        } else if matches!(
            first.kind,
            TokenKind::Keyword(Keyword::Array | Keyword::Map)
        ) && self.peek(1).kind == TokenKind::Symbol(Symbol::Lt)
        {
            self.bump();
            self.bump();
            let mut arguments = vec![self.type_as_parameter()?];
            if first.kind == TokenKind::Keyword(Keyword::Map) {
                self.expect_symbol(Symbol::Comma)?;
                arguments.push(self.type_as_parameter()?);
            }
            self.expect_symbol(Symbol::Gt)?;
            TypeKind::Generic(GenericDataType {
                name: self.type_token_ident(first.span),
                arguments: arguments.into(),
            })
        } else {
            self.parse_generic_type()?
        };
        let mut data_type = TypeName {
            kind,
            span: self.span_since(first.span),
        };
        while let Some(array_span) = self.eat_keyword(Keyword::Array) {
            let kind = if self.eat_symbol(Symbol::LBracket).is_some() {
                let length = self.current();
                if length.kind != TokenKind::Integer {
                    return Err(self.error("an integer token for an explicit array length"));
                }
                self.bump();
                // D31 retains this complete token even when it exceeds i64.
                let spelling = self.source[length.span.start()..length.span.end()].to_owned();
                self.expect_symbol(Symbol::RBracket)?;
                TypeKind::ExplicitArray(ExplicitArrayDataType {
                    element: AstBox::new(data_type),
                    length: ArrayLength {
                        spelling,
                        span: length.span,
                    },
                })
            } else {
                TypeKind::Generic(GenericDataType {
                    name: self.type_token_ident(array_span),
                    arguments: vec![DataTypeParameter::Type(TypeParameter {
                        data_type: AstBox::new(data_type),
                    })]
                    .into(),
                })
            };
            data_type = TypeName {
                kind,
                span: self.span_since(first.span),
            };
        }
        Ok(data_type)
    }

    #[recursive::recursive]
    fn parse_special_type(&mut self) -> Result<TypeKind, TypeFailure> {
        let first = self.bump();
        match first.kind {
            TokenKind::Keyword(Keyword::Row) => {
                self.expect_symbol(Symbol::LParen)?;
                let mut fields = Vec::new();
                loop {
                    fields.push(self.parse_row_type_field()?);
                    if self.eat_symbol(Symbol::Comma).is_none() {
                        break;
                    }
                }
                self.expect_symbol(Symbol::RParen)?;
                Ok(TypeKind::Row(RowDataType {
                    fields: fields.into(),
                }))
            }
            TokenKind::Keyword(Keyword::Time | Keyword::Timestamp) => {
                let precision = if self.eat_symbol(Symbol::LParen).is_some() {
                    let value = self.parse_type_parameter()?;
                    self.expect_symbol(Symbol::RParen)?;
                    Some(AstBox::new(value))
                } else {
                    None
                };
                // WITH may belong to a caller's property clause; consume only
                // the complete type suffix, not another rule's initial keyword.
                let zone_suffix = self.peek(1).kind == TokenKind::Keyword(Keyword::Time)
                    && self.peek(2).kind == TokenKind::Keyword(Keyword::Zone);
                let with_time_zone = if zone_suffix && self.at_keyword(Keyword::With) {
                    self.bump();
                    self.bump();
                    self.bump();
                    true
                } else {
                    if zone_suffix && self.at_keyword(Keyword::Without) {
                        self.bump();
                        self.bump();
                        self.bump();
                    }
                    false
                };
                Ok(TypeKind::DateTime(DateTimeDataType {
                    kind: if first.kind == TokenKind::Keyword(Keyword::Time) {
                        DateTimeType::Time
                    } else {
                        DateTimeType::Timestamp
                    },
                    precision,
                    with_time_zone,
                }))
            }
            TokenKind::Keyword(Keyword::Interval) => {
                let qualifier = self.parse_interval_qualifier_inner()?;
                Ok(TypeKind::Interval(IntervalDataType {
                    qualifier,
                    span: self.span_since(first.span),
                }))
            }
            _ => unreachable!("special type prefix selected by the type grammar"),
        }
    }

    #[recursive::recursive]
    fn parse_generic_type(&mut self) -> Result<TypeKind, TypeFailure> {
        let name = self.ident()?;
        let mut arguments = Vec::new();
        if self.eat_symbol(Symbol::LParen).is_some() {
            loop {
                arguments.push(self.parse_type_parameter()?);
                if self.eat_symbol(Symbol::Comma).is_none() {
                    break;
                }
            }
            self.expect_symbol(Symbol::RParen)?;
        }
        Ok(TypeKind::Generic(GenericDataType {
            name,
            arguments: arguments.into(),
        }))
    }

    #[recursive::recursive]
    fn parse_row_type_field(&mut self) -> Result<RowDataTypeField, TypeFailure> {
        let checkpoint = self.position;
        let start = self.current().span;
        let original = match self.parse_type_inner() {
            Ok(data_type) if self.at_symbol(Symbol::Comma) || self.at_symbol(Symbol::RParen) => {
                return Ok(RowDataTypeField {
                    name: None,
                    data_type,
                    span: self.span_since(start),
                });
            }
            Ok(_) => self.error("',' or ')' after the row field type").into(),
            Err(TypeFailure::Syntax(error)) => error,
            Err(TypeFailure::Construction(error)) => return Err(TypeFailure::Construction(error)),
        };
        self.position = checkpoint;
        let named = (|| -> Result<RowDataTypeField, TypeFailure> {
            let name = self.ident()?;
            let data_type = self.parse_type_inner()?;
            if !self.at_symbol(Symbol::Comma) && !self.at_symbol(Symbol::RParen) {
                return Err(self.error("',' or ')' after the named row field type"));
            }
            Ok(RowDataTypeField {
                name: Some(name),
                data_type,
                span: self.span_since(start),
            })
        })();
        match named {
            Err(TypeFailure::Syntax(_)) => Err(TypeFailure::Syntax(original)),
            result => result,
        }
    }

    fn type_token_ident(&self, span: Span) -> Ident {
        Ident {
            value: self.source[span.start()..span.end()].to_owned(),
            quoted: false,
            span,
        }
    }

    #[recursive::recursive]
    fn type_as_parameter(&mut self) -> Result<DataTypeParameter, TypeFailure> {
        Ok(DataTypeParameter::Type(TypeParameter {
            data_type: AstBox::new(self.parse_type_inner()?),
        }))
    }

    #[recursive::recursive]
    fn parse_type_parameter(&mut self) -> Result<DataTypeParameter, TypeFailure> {
        let token = self.current();
        if token.kind != TokenKind::Integer {
            return self.type_as_parameter();
        }
        self.bump();
        let spelling = self.source[token.span.start()..token.span.end()].to_owned();
        let parsed_value = integer_value(&spelling).ok_or_else(|| {
            literal_error(
                "a signed 64-bit numeric type parameter",
                &spelling,
                token.span,
            )
        })?;
        Ok(DataTypeParameter::Numeric(NumericParameter {
            spelling,
            parsed_value,
            span: token.span,
        }))
    }

    pub(super) fn at_interval_field(&self) -> bool {
        is_interval_field(self.peek(0).kind)
    }

    #[recursive::recursive]
    pub(super) fn parse_interval_qualifier(&mut self) -> Result<IntervalQualifier, ParserError> {
        self.parse_interval_qualifier_inner()
    }

    #[recursive::recursive]
    fn parse_interval_qualifier_inner(&mut self) -> Result<IntervalQualifier, TypeFailure> {
        let first = self.current();
        if !is_interval_field(first.kind) {
            return Err(self.error("an interval qualifier field"));
        }
        self.bump();
        let mut precision = None;
        let mut fractional_precision = None;
        if self.eat_symbol(Symbol::LParen).is_some() {
            precision = Some(AstBox::new(self.parse_type_parameter()?));
            if first.kind == TokenKind::Keyword(Keyword::Second)
                && self.eat_symbol(Symbol::Comma).is_some()
            {
                fractional_precision = Some(AstBox::new(self.parse_type_parameter()?));
            }
            self.expect_symbol(Symbol::RParen)?;
        }
        let from = qualifier_field(first.kind, fractional_precision);
        if self.eat_keyword(Keyword::To).is_none() {
            return Ok(IntervalQualifier::Simple(SimpleIntervalQualifier {
                precision,
                field: from,
                span: self.span_since(first.span),
            }));
        }
        let end = self.current();
        let grammar_end = match first.kind {
            TokenKind::Keyword(Keyword::Year) => end.kind == TokenKind::Keyword(Keyword::Month),
            TokenKind::Keyword(Keyword::Day | Keyword::Hour | Keyword::Minute) => matches!(
                end.kind,
                TokenKind::Keyword(Keyword::Hour | Keyword::Minute | Keyword::Second)
            ),
            _ => false,
        };
        if !grammar_end {
            return Err(self.error("a grammar ending field for this interval qualifier"));
        }
        let valid = match first.kind {
            TokenKind::Keyword(Keyword::Year) => end.kind == TokenKind::Keyword(Keyword::Month),
            TokenKind::Keyword(Keyword::Day) => matches!(
                end.kind,
                TokenKind::Keyword(Keyword::Hour | Keyword::Minute | Keyword::Second)
            ),
            TokenKind::Keyword(Keyword::Hour) => matches!(
                end.kind,
                TokenKind::Keyword(Keyword::Minute | Keyword::Second)
            ),
            TokenKind::Keyword(Keyword::Minute) => end.kind == TokenKind::Keyword(Keyword::Second),
            _ => false,
        };
        if !valid {
            let span = Span::new(first.span.start(), end.span.end());
            return Err(literal_error(
                "a valid ending field for this interval qualifier",
                &self.source[span.start()..span.end()],
                span,
            ));
        }
        self.bump();
        let fractional_precision = if end.kind == TokenKind::Keyword(Keyword::Second)
            && self.eat_symbol(Symbol::LParen).is_some()
        {
            let value = self.parse_type_parameter()?;
            self.expect_symbol(Symbol::RParen)?;
            Some(AstBox::new(value))
        } else {
            None
        };
        Ok(IntervalQualifier::Composite(CompositeIntervalQualifier {
            precision,
            from,
            to: qualifier_field(end.kind, fractional_precision),
            span: self.span_since(first.span),
        }))
    }
}

fn is_interval_field(kind: TokenKind) -> bool {
    matches!(
        kind,
        TokenKind::Keyword(
            Keyword::Year
                | Keyword::Month
                | Keyword::Day
                | Keyword::Hour
                | Keyword::Minute
                | Keyword::Second
        )
    )
}

fn qualifier_field(
    kind: TokenKind,
    fractional_precision: Option<AstBox<DataTypeParameter>>,
) -> IntervalQualifierField {
    match kind {
        TokenKind::Keyword(Keyword::Year) => IntervalQualifierField::Year,
        TokenKind::Keyword(Keyword::Month) => IntervalQualifierField::Month,
        TokenKind::Keyword(Keyword::Day) => IntervalQualifierField::Day,
        TokenKind::Keyword(Keyword::Hour) => IntervalQualifierField::Hour,
        TokenKind::Keyword(Keyword::Minute) => IntervalQualifierField::Minute,
        TokenKind::Keyword(Keyword::Second) => IntervalQualifierField::Second {
            fractional_precision,
        },
        _ => unreachable!("interval qualifier token checked by the parser"),
    }
}
