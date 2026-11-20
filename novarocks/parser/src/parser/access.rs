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

use super::{GrammarError, Parser};
use crate::ast::*;
use crate::token::{Symbol, TokenKind};
use crate::{ParseError, Span};

impl Parser<'_> {
    /// Parses only methodName, whose grammar additionally accepts keywords.
    pub(super) fn parse_method_name(&mut self) -> Result<Ident, GrammarError> {
        if self.is_identifier(0) {
            return self.ident();
        }
        let token = self.peek(0);
        if matches!(token.kind, TokenKind::Keyword(_)) {
            self.bump();
            return Ok(Ident {
                value: self.source[token.span.start()..token.span.end()].to_owned(),
                quoted: false,
                span: token.span,
            });
        }
        Err(self.error("a method name"))
    }

    /// The caller has consumed the real receiver, dot, and method name.
    #[recursive::recursive]
    pub(super) fn parse_method_call(
        &mut self,
        receiver: Expr,
        method: Ident,
        extent: Span,
    ) -> Result<Expr, GrammarError> {
        let arguments = self.parse_method_arguments()?;
        Ok(Expr::MethodCall(AstBox::new(MethodCall {
            receiver,
            method,
            arguments,
            span: self.span_since(extent),
        })))
    }

    /// The caller has consumed qualifiedName; this tail starts at `::`.
    #[recursive::recursive]
    pub(super) fn parse_static_method_call(
        &mut self,
        type_name: ObjectName,
        extent: Span,
    ) -> Result<Expr, GrammarError> {
        self.expect_symbol(Symbol::DoubleColon)?;
        let method = self.parse_method_name()?;
        let arguments = self.parse_method_arguments()?;
        Ok(Expr::StaticMethodCall(AstBox::new(StaticMethodCall {
            type_name,
            method,
            arguments,
            span: self.span_since(extent),
        })))
    }

    #[recursive::recursive]
    fn parse_method_arguments(&mut self) -> Result<AstVec<CallArgument>, GrammarError> {
        self.expect_symbol(Symbol::LParen)?;
        let mut arguments = Vec::new();
        if !self.at_symbol(Symbol::RParen) {
            loop {
                arguments.push(self.parse_call_argument()?);
                if self.eat_symbol(Symbol::Comma).is_none() {
                    break;
                }
            }
        }
        self.expect_symbol(Symbol::RParen)?;
        Ok(arguments.into())
    }

    /// The caller has consumed `(valueExpression)` and selected its qualifier.
    #[recursive::recursive]
    pub(super) fn parse_interval_value(
        &mut self,
        expression: Expr,
        extent: Span,
    ) -> Result<Expr, GrammarError> {
        let subtraction = match expression {
            Expr::Binary(binary) if binary.operator == BinaryOperator::Subtract => binary,
            _ => {
                // This builder check precedes visiting the qualifier. The
                // caller's actual parenthesized extent supplies its location.
                return Err(GrammarError::Construction(
                    ParseError::UnexpectedToken {
                        expected: "a subtraction for a qualified datetime difference",
                        found: self.source[extent.start()..extent.end()].to_owned(),
                        span: extent,
                    }
                    .into(),
                ));
            }
        };
        let qualifier = self.parse_interval_qualifier()?;
        let span = self.span_since(extent);
        Ok(Expr::IntervalValueExpression(AstBox::new(
            IntervalValueExpression {
                left: subtraction.left.into_inner(),
                right: subtraction.right.into_inner(),
                data_type: IntervalDataType { qualifier, span },
                span,
            },
        )))
    }
}
