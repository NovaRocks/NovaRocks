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

use super::{GrammarError as ParserError, Parser};
use crate::Span;
use crate::ast::*;
use crate::trino_token::{Keyword as K, Symbol as S, TokenKind as T};

/// Facts of the current grammar expression, independent of the stored node's
/// diagnostic range. A parenthesized primary starts a new logical context.
struct Parsed {
    expression: Expr,
    extent: Span,
    logical: Option<LogicalOperator>,
    primary: bool,
    predicate: bool,
    value_expression: bool,
}

impl Parsed {
    fn primary(expression: Expr, extent: Span) -> Self {
        Self {
            expression,
            extent,
            logical: None,
            primary: true,
            predicate: false,
            value_expression: true,
        }
    }

    fn value(expression: Expr, extent: Span) -> Self {
        Self {
            expression,
            extent,
            logical: None,
            primary: false,
            predicate: false,
            value_expression: true,
        }
    }
}

impl Parser<'_> {
    pub(super) fn parse_expression(&mut self) -> Result<Expr, ParserError> {
        self.with_alias_follow(false, |parser| Ok(parser.expression_bp(0)?.expression))
    }

    pub(super) fn parse_value_expression(&mut self) -> Result<Expr, ParserError> {
        self.with_alias_follow(false, |parser| Ok(parser.expression_bp(50)?.expression))
    }

    pub(super) fn parse_projection_expression(&mut self) -> Result<(Expr, bool), ParserError> {
        let parsed = self.with_alias_follow(true, |parser| parser.expression_bp(0))?;
        Ok((parsed.expression, parsed.primary))
    }

    #[recursive::recursive]
    fn expression_bp(&mut self, minimum: u8) -> Result<Parsed, ParserError> {
        if self.grammar_boundary_at_current() {
            return Err(self.error("an expression before the enclosing delimiter"));
        }
        let start = self.peek(0).span;
        let mut left = if minimum <= 30 && self.eat_keyword(K::Not).is_some() {
            let child = self.expression_bp(30)?;
            let extent = self.span_since(start);
            let mut parsed = Parsed::value(
                Expr::Unary(UnaryExpr {
                    operator: UnaryOperator::Not,
                    expression: AstBox::new(child.expression),
                    span: extent,
                }),
                extent,
            );
            parsed.value_expression = false;
            parsed
        } else if self.at_symbol(S::Plus)
            || (self.at_symbol(S::Minus)
                && !matches!(self.peek(1).kind, T::Integer | T::Decimal | T::Double))
        {
            let operator = if self.bump().kind == T::Symbol(S::Plus) {
                UnaryOperator::Plus
            } else {
                UnaryOperator::Minus
            };
            let child = self.expression_bp(80)?;
            let extent = self.span_since(start);
            Parsed::value(
                Expr::Unary(UnaryExpr {
                    operator,
                    expression: AstBox::new(child.expression),
                    span: extent,
                }),
                extent,
            )
        } else {
            self.primary_expression()?
        };
        loop {
            if self.grammar_boundary_at_current() {
                break;
            }
            let logical = if self.at_keyword(K::Or) {
                Some((10, LogicalOperator::Or))
            } else if self.at_keyword(K::And) {
                Some((20, LogicalOperator::And))
            } else {
                None
            };
            if let Some((power, operator)) = logical {
                if power < minimum {
                    break;
                }
                self.bump();
                let right = self.expression_bp(power + 1)?;
                let extent = self.span_since(start);
                if left.logical == Some(operator) {
                    let Expr::Logical(group) = &mut left.expression else {
                        unreachable!("logical grammar context owns a logical node")
                    };
                    group.terms.push(right.expression);
                    group.span = extent;
                } else {
                    left.expression = Expr::Logical(LogicalExpr {
                        operator,
                        terms: vec![left.expression, right.expression].into(),
                        span: extent,
                    });
                }
                left.extent = extent;
                left.logical = Some(operator);
                left.primary = false;
                left.predicate = false;
                left.value_expression = false;
                continue;
            }
            if minimum <= 40 && !left.predicate && self.predicate_start() {
                let predicate = self.parse_predicate()?;
                let extent = self.span_since(start);
                left = Parsed::value(
                    Expr::Predicated(AstBox::new(PredicatedExpr {
                        value: left.expression,
                        predicate,
                        span: extent,
                    })),
                    extent,
                );
                left.predicate = true;
                left.value_expression = false;
                continue;
            }
            let arithmetic = match self.peek(0).kind {
                T::Symbol(S::Plus) => Some((60, BinaryOperator::Add)),
                T::Symbol(S::Minus) => Some((60, BinaryOperator::Subtract)),
                T::Symbol(S::Star) => Some((70, BinaryOperator::Multiply)),
                T::Symbol(S::Slash) => Some((70, BinaryOperator::Divide)),
                T::Symbol(S::Percent) => Some((70, BinaryOperator::Modulo)),
                _ => None,
            };
            if let Some((power, operator)) = arithmetic {
                if power < minimum || left.predicate {
                    break;
                }
                self.bump();
                let right = self.expression_bp(power + 1)?;
                let extent = self.span_since(start);
                let expression = Expr::Binary(BinaryExpr {
                    left_operand_span: left.extent,
                    left: AstBox::new(left.expression),
                    operator,
                    right: AstBox::new(right.expression),
                    span: extent,
                });
                left = Parsed::value(expression, extent);
                continue;
            }
            if self.at_symbol(S::DoublePipe) && minimum <= 50 && !left.predicate {
                let operator = self.bump().span;
                let right = self.expression_bp(51)?;
                let extent = self.span_since(start);
                let name = ObjectName {
                    parts: vec![Ident {
                        value: "concat".into(),
                        quoted: false,
                        span: operator,
                    }],
                    span: operator,
                };
                let arguments = vec![
                    CallArgument {
                        name: None,
                        span: left.expression.span(),
                        value: left.expression,
                    },
                    CallArgument {
                        name: None,
                        span: right.expression.span(),
                        value: right.expression,
                    },
                ]
                .into();
                left = Parsed::value(
                    Expr::FunctionCall(AstBox::new(FunctionCall {
                        name,
                        arguments,
                        distinct: false,
                        order_by: None,
                        filter: None,
                        null_treatment: None,
                        window: None,
                        processing_mode: None,
                        span: extent,
                    })),
                    extent,
                );
                continue;
            }
            if self.at_keyword(K::At) && minimum <= 90 && !left.predicate {
                self.bump();
                let expression = if self.eat_keyword(K::Local).is_some() {
                    Expr::AtLocal(AstBox::new(AtLocal {
                        value: left.expression,
                        span: self.span_since(start),
                    }))
                } else {
                    self.expect_keyword(K::Time)?;
                    self.expect_keyword(K::Zone)?;
                    let time_zone = if self.at_keyword(K::Interval) {
                        Expr::Literal(self.parse_literal()?)
                    } else {
                        let (value, span) = self.parse_string()?;
                        Expr::Literal(Literal {
                            kind: LiteralKind::String(value),
                            span,
                        })
                    };
                    Expr::AtTimeZone(AstBox::new(AtTimeZone {
                        value: left.expression,
                        time_zone,
                        span: self.span_since(start),
                    }))
                };
                left = Parsed::value(expression, self.span_since(start));
                continue;
            }
            break;
        }
        Ok(left)
    }

    fn predicate_start(&self) -> bool {
        matches!(
            self.peek(0).kind,
            T::Symbol(S::Eq | S::Neq | S::Lt | S::Lte | S::Gt | S::Gte)
                | T::Keyword(K::Between | K::In | K::Like | K::Is | K::Overlaps | K::Match)
        ) || (self.at_keyword(K::Not)
            && matches!(self.peek(1).kind, T::Keyword(K::Between | K::In | K::Like)))
    }

    fn comparison_operator(&mut self) -> Result<ComparisonOperator, ParserError> {
        let operator = match self.peek(0).kind {
            T::Symbol(S::Eq) => ComparisonOperator::Equal,
            T::Symbol(S::Neq) => ComparisonOperator::NotEqual,
            T::Symbol(S::Lt) => ComparisonOperator::LessThan,
            T::Symbol(S::Lte) => ComparisonOperator::LessThanOrEqual,
            T::Symbol(S::Gt) => ComparisonOperator::GreaterThan,
            T::Symbol(S::Gte) => ComparisonOperator::GreaterThanOrEqual,
            _ => return Err(self.error("a comparison operator")),
        };
        self.bump();
        Ok(operator)
    }

    #[recursive::recursive]
    fn parse_predicate(&mut self) -> Result<Predicate, ParserError> {
        let start = self.peek(0).span;
        if matches!(
            self.peek(0).kind,
            T::Symbol(S::Eq | S::Neq | S::Lt | S::Lte | S::Gt | S::Gte)
        ) {
            let operator = self.comparison_operator()?;
            let quantifier = match self.peek(0).kind {
                T::Keyword(K::All) => Some(ComparisonQuantifier::All),
                T::Keyword(K::Any) => Some(ComparisonQuantifier::Any),
                T::Keyword(K::Some) => Some(ComparisonQuantifier::Some),
                _ => None,
            };
            if let Some(quantifier) = quantifier {
                self.bump();
                let subquery = self.expression_subquery()?;
                return Ok(Predicate::QuantifiedComparison(
                    QuantifiedComparisonPredicate {
                        operator,
                        quantifier,
                        subquery,
                        span: self.span_since(start),
                    },
                ));
            }
            let right = self.expression_bp(50)?.expression;
            return Ok(Predicate::Comparison(ComparisonPredicate {
                operator,
                right,
                span: self.span_since(start),
            }));
        }
        if self.eat_keyword(K::Is).is_some() {
            let negated = self.eat_keyword(K::Not).is_some();
            if self.eat_keyword(K::Null).is_some() {
                return Ok(Predicate::IsNull(IsNullPredicate {
                    negated,
                    span: self.span_since(start),
                }));
            }
            let truth_value = match self.peek(0).kind {
                T::Keyword(K::True) => Some(TruthValue::True),
                T::Keyword(K::False) => Some(TruthValue::False),
                T::Keyword(K::Unknown) => Some(TruthValue::Unknown),
                _ => None,
            };
            if let Some(truth_value) = truth_value {
                self.bump();
                return Ok(Predicate::BooleanTest(BooleanTestPredicate {
                    negated,
                    truth_value,
                    span: self.span_since(start),
                }));
            }
            self.expect_keyword(K::Distinct)?;
            self.expect_keyword(K::From)?;
            let right = self.expression_bp(50)?.expression;
            return Ok(Predicate::DistinctFrom(DistinctFromPredicate {
                negated,
                right,
                span: self.span_since(start),
            }));
        }
        if self.eat_keyword(K::Overlaps).is_some() {
            let right = self.expression_bp(50)?.expression;
            return Ok(Predicate::Overlaps(OverlapsPredicate {
                right,
                span: self.span_since(start),
            }));
        }
        if self.eat_keyword(K::Match).is_some() {
            let unique = self.eat_keyword(K::Unique).is_some();
            let match_type = if self.eat_keyword(K::Partial).is_some() {
                MatchType::Partial
            } else if self.eat_keyword(K::Full).is_some() {
                MatchType::Full
            } else {
                self.eat_keyword(K::Simple);
                MatchType::Simple
            };
            let subquery = self.expression_subquery()?;
            return Ok(Predicate::Match(MatchPredicate {
                unique,
                match_type,
                subquery,
                span: self.span_since(start),
            }));
        }
        let negated = self.eat_keyword(K::Not).is_some();
        if self.eat_keyword(K::Between).is_some() {
            let symmetry = if self.eat_keyword(K::Symmetric).is_some() {
                Some(BetweenSymmetry::Symmetric)
            } else if self.eat_keyword(K::Asymmetric).is_some() {
                Some(BetweenSymmetry::Asymmetric)
            } else {
                None
            };
            let min = self.expression_bp(50)?.expression;
            self.expect_keyword(K::And)?;
            let max = self.expression_bp(50)?.expression;
            return Ok(Predicate::Between(BetweenPredicate {
                negated,
                symmetry,
                min,
                max,
                span: self.span_since(start),
            }));
        }
        if self.eat_keyword(K::Like).is_some() {
            let pattern = self.expression_bp(50)?.expression;
            let escape = if self.eat_keyword(K::Escape).is_some() {
                Some(self.expression_bp(50)?.expression)
            } else {
                None
            };
            return Ok(Predicate::Like(LikePredicate {
                negated,
                pattern,
                escape,
                span: self.span_since(start),
            }));
        }
        self.expect_keyword(K::In)?;
        let open = self.expect_symbol(S::LParen)?;
        let value_list = if self.query_head() {
            let query = self.parse_query()?;
            self.expect_symbol(S::RParen)?;
            Expr::Subquery(SubqueryExpr {
                query: AstBox::new(query),
                span: self.span_since(open),
            })
        } else {
            let mut values = vec![self.parse_expression()?];
            while self.eat_symbol(S::Comma).is_some() {
                values.push(self.parse_expression()?);
            }
            self.expect_symbol(S::RParen)?;
            Expr::InList(AstBox::new(InListExpr {
                values: values.into(),
                span: self.span_since(open),
            }))
        };
        Ok(Predicate::In(InPredicate {
            negated,
            value_list,
            span: self.span_since(start),
        }))
    }

    fn query_head(&self) -> bool {
        matches!(
            self.peek(0).kind,
            T::Keyword(K::Select | K::With | K::Values | K::Table)
        )
    }

    fn expression_subquery(&mut self) -> Result<Expr, ParserError> {
        let start = self.expect_symbol(S::LParen)?;
        let query = self.parse_query()?;
        self.expect_symbol(S::RParen)?;
        Ok(Expr::Subquery(SubqueryExpr {
            query: AstBox::new(query),
            span: self.span_since(start),
        }))
    }

    #[recursive::recursive]
    fn primary_expression(&mut self) -> Result<Parsed, ParserError> {
        let start = self.peek(0).span;
        let token = self.peek(0);
        let expression = match token.kind {
            T::String
            | T::UnicodeString
            | T::BinaryLiteral
            | T::Integer
            | T::Decimal
            | T::Double
            | T::Keyword(K::Null | K::True | K::False)
            | T::Symbol(S::Minus) => Expr::Literal(self.parse_literal()?),
            T::Keyword(K::Interval)
                if matches!(self.peek(1).kind, T::String | T::UnicodeString)
                    || (matches!(self.peek(1).kind, T::Symbol(S::Plus | S::Minus))
                        && matches!(self.peek(2).kind, T::String | T::UnicodeString)) =>
            {
                Expr::Literal(self.parse_literal()?)
            }
            T::Keyword(K::Double)
                if self.peek(1).kind == T::Keyword(K::Precision)
                    && matches!(self.peek(2).kind, T::String | T::UnicodeString) =>
            {
                Expr::Literal(self.parse_literal()?)
            }
            T::Keyword(K::CurrentCatalog) => {
                self.bump();
                Expr::CurrentCatalog(CurrentCatalog { span: start })
            }
            T::Keyword(K::CurrentDate) => {
                self.bump();
                Expr::CurrentDate(CurrentDate { span: start })
            }
            T::Keyword(K::CurrentPath) => {
                self.bump();
                Expr::CurrentPath(CurrentPath { span: start })
            }
            T::Keyword(K::CurrentSchema) => {
                self.bump();
                Expr::CurrentSchema(CurrentSchema { span: start })
            }
            T::Keyword(K::CurrentUser) => {
                self.bump();
                Expr::CurrentUser(CurrentUser { span: start })
            }
            T::Keyword(K::CurrentTime | K::CurrentTimestamp | K::Localtime | K::Localtimestamp) => {
                self.bump();
                let precision = if self.eat_symbol(S::LParen).is_some() {
                    let token = self.peek(0);
                    if token.kind != T::Integer {
                        return Err(self.error("an integer precision"));
                    }
                    let value = self.source[token.span.start()..token.span.end()]
                        .parse::<i32>()
                        .map_err(|_| {
                            self.construction_error(
                                "an integer precision in the signed 32-bit range",
                            )
                        })?;
                    self.bump();
                    self.expect_symbol(S::RParen)?;
                    Some(value)
                } else {
                    None
                };
                let span = self.span_since(start);
                match token.kind {
                    T::Keyword(K::CurrentTime) => {
                        Expr::CurrentTime(CurrentTime { precision, span })
                    }
                    T::Keyword(K::CurrentTimestamp) => {
                        Expr::CurrentTimestamp(CurrentTimestamp { precision, span })
                    }
                    T::Keyword(K::Localtime) => Expr::LocalTime(LocalTime { precision, span }),
                    _ => Expr::LocalTimestamp(LocalTimestamp { precision, span }),
                }
            }
            T::Symbol(S::Question) => {
                self.bump();
                Expr::Parameter(self.new_parameter(start))
            }
            T::Keyword(K::Cast | K::TryCast) => {
                let safe = self.bump().kind == T::Keyword(K::TryCast);
                self.expect_symbol(S::LParen)?;
                let expression = self.parse_expression()?;
                self.expect_keyword(K::As)?;
                let data_type = self.parse_type()?;
                self.expect_symbol(S::RParen)?;
                Expr::Cast(AstBox::new(CastExpr {
                    expression,
                    data_type,
                    safe,
                    span: self.span_since(start),
                }))
            }
            T::Keyword(K::Exists | K::Unique) => {
                self.bump();
                let subquery = self.expression_subquery()?;
                let span = self.span_since(start);
                if token.kind == T::Keyword(K::Exists) {
                    Expr::Exists(AstBox::new(ExistsExpr { subquery, span }))
                } else {
                    Expr::Unique(AstBox::new(UniqueExpr { subquery, span }))
                }
            }
            T::Keyword(K::Case) => self.case_expression()?,
            T::Keyword(K::Array) if self.peek(1).kind == T::Symbol(S::LBracket) => {
                self.bump();
                self.array_expression(start)?
            }
            T::Symbol(S::LBracket) => self.array_expression(start)?,
            T::Keyword(K::Row) if self.peek(1).kind == T::Symbol(S::LParen) => {
                self.bump();
                self.expect_symbol(S::LParen)?;
                let mut fields = Vec::new();
                loop {
                    let field_start = self.peek(0).span;
                    let (expression, _) = self.parse_projection_expression()?;
                    let name = if self.eat_keyword(K::As).is_some() || self.is_identifier(0) {
                        Some(self.ident()?)
                    } else {
                        None
                    };
                    fields.push(RowField {
                        name,
                        expression,
                        span: self.span_since(field_start),
                    });
                    if self.eat_symbol(S::Comma).is_none() {
                        break;
                    }
                }
                self.expect_symbol(S::RParen)?;
                Expr::Row(RowExpr {
                    fields: fields.into(),
                    span: self.span_since(start),
                })
            }
            T::Symbol(S::LParen) => {
                self.bump();
                if self.query_head() {
                    let query = self.parse_query()?;
                    self.expect_symbol(S::RParen)?;
                    Expr::Subquery(SubqueryExpr {
                        query: AstBox::new(query),
                        span: self.span_since(start),
                    })
                } else if self.lambda_arguments_ahead() {
                    let mut arguments = Vec::new();
                    if !self.at_symbol(S::RParen) {
                        loop {
                            let name = self.ident()?;
                            arguments.push(LambdaArgumentDeclaration {
                                span: name.span,
                                name,
                            });
                            if self.eat_symbol(S::Comma).is_none() {
                                break;
                            }
                        }
                    }
                    self.expect_symbol(S::RParen)?;
                    self.expect_symbol(S::Arrow)?;
                    let body = self.expression_bp(0)?.expression;
                    Expr::Lambda(AstBox::new(LambdaExpr {
                        arguments: arguments.into(),
                        body,
                        span: self.span_since(start),
                    }))
                } else {
                    let first = self.with_alias_follow(false, |parser| parser.expression_bp(0))?;
                    if self.eat_symbol(S::Comma).is_some() {
                        let mut fields = vec![RowField {
                            span: first.expression.span(),
                            name: None,
                            expression: first.expression,
                        }];
                        loop {
                            let expression = self.parse_expression()?;
                            fields.push(RowField {
                                span: expression.span(),
                                name: None,
                                expression,
                            });
                            if self.eat_symbol(S::Comma).is_none() {
                                break;
                            }
                        }
                        self.expect_symbol(S::RParen)?;
                        Expr::Row(RowExpr {
                            fields: fields.into(),
                            span: self.span_since(start),
                        })
                    } else {
                        self.expect_symbol(S::RParen)?;
                        if first.value_expression && self.interval_value_tail_ahead() {
                            self.parse_interval_value(first.expression, self.span_since(start))?
                        } else {
                            first.expression
                        }
                    }
                }
            }
            T::Keyword(K::Running | K::Final) if self.processing_call_ahead() => {
                self.bump();
                let mode = if token.kind == T::Keyword(K::Running) {
                    ProcessingModeKind::Running
                } else {
                    ProcessingModeKind::Final
                };
                let name = self.qualified_name()?;
                self.function_expression(name, Some(ProcessingMode { mode, span: start }))?
            }
            T::Keyword(K::Json) if self.peek(1).kind == T::Symbol(S::LParen) => {
                self.json_constructor_or_call()?
            }
            T::Keyword(
                K::JsonExists
                | K::JsonValue
                | K::JsonQuery
                | K::JsonSerialize
                | K::JsonObject
                | K::JsonArray,
            ) => self.parse_json_expression()?,
            _ if self.reserved_expression_ahead() => self.parse_reserved_expression()?,
            _ if self.is_identifier(0)
                && matches!(self.peek(1).kind, T::String | T::UnicodeString) =>
            {
                Expr::Literal(self.parse_literal()?)
            }
            _ if self.is_identifier(0) => {
                if self.peek(1).kind == T::Symbol(S::Arrow) {
                    let name = self.ident()?;
                    let argument = LambdaArgumentDeclaration {
                        span: name.span,
                        name,
                    };
                    self.expect_symbol(S::Arrow)?;
                    let body = self.expression_bp(0)?.expression;
                    Expr::Lambda(AstBox::new(LambdaExpr {
                        arguments: vec![argument].into(),
                        body,
                        span: self.span_since(start),
                    }))
                } else {
                    let name = self.qualified_name()?;
                    if self.at_symbol(S::DoubleColon) {
                        self.parse_static_method_call(name, start)?
                    } else if self.at_symbol(S::LParen) {
                        self.function_expression(name, None)?
                    } else if name.parts.len() == 1 && self.at_keyword(K::Over) {
                        let window = self.parse_window()?;
                        Expr::WindowOperation(AstBox::new(WindowOperation {
                            name: name.parts.into_iter().next().expect("single measure name"),
                            window,
                            span: self.span_since(start),
                        }))
                    } else {
                        name_expression(name)
                    }
                }
            }
            _ => return Err(self.error("an expression")),
        };
        let mut parsed = Parsed::primary(expression, self.span_since(start));
        loop {
            if self.eat_symbol(S::LBracket).is_some() {
                let expression = if self.eat_symbol(S::Star).is_some() {
                    self.expect_symbol(S::RBracket)?;
                    Expr::ArrayWildcardSubscript(AstBox::new(ArrayWildcardSubscriptExpr {
                        base: parsed.expression,
                        span: self.span_since(start),
                    }))
                } else {
                    let index = self.expression_bp(50)?.expression;
                    self.expect_symbol(S::RBracket)?;
                    Expr::Subscript(AstBox::new(SubscriptExpr {
                        base: parsed.expression,
                        index,
                        span: self.span_since(start),
                    }))
                };
                parsed = Parsed::primary(expression, self.span_since(start));
            } else if self.at_symbol(S::Dot)
                && self.peek(2).kind == T::Symbol(S::LParen)
                && (self.is_identifier(1) || matches!(self.peek(1).kind, T::Keyword(_)))
            {
                self.bump();
                let method = self.parse_method_name()?;
                let expression = self.parse_method_call(parsed.expression, method, start)?;
                parsed = Parsed::primary(expression, self.span_since(start));
            } else if self.at_symbol(S::Dot)
                && (self.is_identifier(1)
                    || matches!(self.peek(1).kind, T::String | T::UnicodeString))
            {
                self.bump();
                let field = if self.is_identifier(0) {
                    self.ident()?
                } else {
                    let (value, span) = self.parse_string()?;
                    Ident {
                        value,
                        quoted: true,
                        span,
                    }
                };
                let expression = Expr::Dereference(AstBox::new(DereferenceExpr {
                    base: parsed.expression,
                    field: Some(field),
                    span: self.span_since(start),
                }));
                parsed = Parsed::primary(expression, self.span_since(start));
            } else {
                break;
            }
        }
        Ok(parsed)
    }

    fn interval_value_tail_ahead(&mut self) -> bool {
        if !matches!(
            self.peek(0).kind,
            T::Keyword(K::Year | K::Month | K::Day | K::Hour | K::Minute | K::Second)
        ) {
            return false;
        }
        if !self.alias_follow {
            return true;
        }
        // A bare field followed by the actual aliasedExpression boundary
        // belongs to its parent. Precision, TO, and expression continuations
        // instead require the intervalValueExpression alternative.
        let checkpoint = self.checkpoint();
        self.bump();
        let alias_ends = matches!(
            self.peek(0).kind,
            T::End
                | T::Symbol(S::Comma | S::RParen)
                | T::Keyword(
                    K::For
                        | K::From
                        | K::Where
                        | K::Group
                        | K::Having
                        | K::Order
                        | K::Union
                        | K::Intersect
                        | K::Except
                )
        ) || self.query_clause_follows_alias();
        self.restore(checkpoint);
        !alias_ends
    }

    fn json_constructor_or_call(&mut self) -> Result<Expr, ParserError> {
        let checkpoint = self.checkpoint();
        match self.parse_json_expression() {
            Ok(expression) if !self.function_modifier_ahead() => return Ok(expression),
            Ok(_) | Err(ParserError::Syntax(_)) => self.restore(checkpoint),
            Err(error @ ParserError::Construction(_)) => return Err(error),
        }
        let name = self.qualified_name()?;
        self.function_expression(name, None)
    }

    fn function_modifier_ahead(&self) -> bool {
        (self.at_keyword(K::Filter) && self.peek(1).kind == T::Symbol(S::LParen))
            || ((self.at_keyword(K::Ignore) || self.at_keyword(K::Respect))
                && self.peek(1).kind == T::Keyword(K::Nulls))
            || (self.at_keyword(K::Over)
                && (self.is_identifier(1) || self.peek(1).kind == T::Symbol(S::LParen)))
    }

    fn processing_call_ahead(&self) -> bool {
        if !self.is_identifier(1) {
            return false;
        }
        let mut offset = 2;
        while self.peek(offset).kind == T::Symbol(S::Dot) && self.is_identifier(offset + 1) {
            offset += 2;
        }
        self.peek(offset).kind == T::Symbol(S::LParen)
    }

    fn lambda_arguments_ahead(&self) -> bool {
        let mut offset = 0;
        if self.peek(offset).kind == T::Symbol(S::RParen) {
            return self.peek(offset + 1).kind == T::Symbol(S::Arrow);
        }
        loop {
            if !self.is_identifier(offset) {
                return false;
            }
            offset += 1;
            match self.peek(offset).kind {
                T::Symbol(S::Comma) => offset += 1,
                T::Symbol(S::RParen) => return self.peek(offset + 1).kind == T::Symbol(S::Arrow),
                _ => return false,
            }
        }
    }

    fn array_expression(&mut self, start: Span) -> Result<Expr, ParserError> {
        self.expect_symbol(S::LBracket)?;
        let mut values = Vec::new();
        if !self.at_symbol(S::RBracket) {
            loop {
                values.push(self.parse_expression()?);
                if self.eat_symbol(S::Comma).is_none() {
                    break;
                }
            }
        }
        self.expect_symbol(S::RBracket)?;
        Ok(Expr::Array(ArrayExpr {
            values: values.into(),
            span: self.span_since(start),
        }))
    }

    fn case_expression(&mut self) -> Result<Expr, ParserError> {
        let start = self.expect_keyword(K::Case)?;
        let operand = if self.at_keyword(K::When) {
            None
        } else {
            Some(self.parse_expression()?)
        };
        let mut when_clauses = Vec::new();
        loop {
            let when = self.expect_keyword(K::When)?;
            let match_value = if operand.is_some() && self.predicate_start() {
                let checkpoint = self.checkpoint();
                match self.parse_predicate() {
                    Ok(predicate) => WhenMatch::Partial(predicate),
                    Err(ParserError::Construction(error)) => {
                        return Err(ParserError::Construction(error));
                    }
                    Err(ParserError::Syntax(_)) => {
                        self.restore(checkpoint);
                        WhenMatch::Operand(self.parse_expression()?)
                    }
                }
            } else {
                WhenMatch::Operand(self.parse_expression()?)
            };
            self.expect_keyword(K::Then)?;
            let result = self.parse_expression()?;
            when_clauses.push(WhenClause {
                match_value,
                result,
                span: self.span_since(when),
            });
            if !self.at_keyword(K::When) {
                break;
            }
        }
        let default_value = if self.eat_keyword(K::Else).is_some() {
            Some(self.parse_expression()?)
        } else {
            None
        };
        self.expect_keyword(K::End)?;
        let span = self.span_since(start);
        Ok(if let Some(operand) = operand {
            Expr::SimpleCase(AstBox::new(SimpleCaseExpr {
                operand,
                when_clauses: when_clauses.into(),
                default_value,
                span,
            }))
        } else {
            Expr::SearchedCase(AstBox::new(SearchedCaseExpr {
                when_clauses: when_clauses.into(),
                default_value,
                span,
            }))
        })
    }

    pub(super) fn parse_call_argument(&mut self) -> Result<CallArgument, ParserError> {
        let start = self.peek(0).span;
        let name = if self.is_identifier(0) && self.peek(1).kind == T::Symbol(S::FatArrow) {
            let name = self.ident()?;
            self.bump();
            Some(name)
        } else {
            None
        };
        let value = self.parse_expression()?;
        Ok(CallArgument {
            name,
            value,
            span: self.span_since(start),
        })
    }

    fn function_expression(
        &mut self,
        name: ObjectName,
        processing_mode: Option<ProcessingMode>,
    ) -> Result<Expr, ParserError> {
        let start = processing_mode.as_ref().map_or(name.span, |mode| mode.span);
        self.expect_symbol(S::LParen)?;
        let parameter_start = self.next_parameter;
        let distinct = self.eat_keyword(K::Distinct).is_some();
        let all = !distinct && self.eat_keyword(K::All).is_some();
        let mut wildcard = false;
        let mut arguments = Vec::new();
        if self.eat_symbol(S::Star).is_some() {
            wildcard = true;
            if distinct || all {
                return Err(self.error("an argument after DISTINCT"));
            }
        } else if self.is_identifier(0)
            && self.peek(1).kind == T::Symbol(S::Dot)
            && self.peek(2).kind == T::Symbol(S::Star)
        {
            wildcard = true;
            if distinct || all {
                return Err(self.error("an argument after a set quantifier"));
            }
            let label = self.ident()?;
            self.expect_symbol(S::Dot)?;
            self.expect_symbol(S::Star)?;
            let span = self.span_since(label.span);
            arguments.push(CallArgument {
                name: None,
                span,
                value: Expr::Dereference(AstBox::new(DereferenceExpr {
                    base: Expr::Identifier(label),
                    field: None,
                    span,
                })),
            });
        } else if !self.at_symbol(S::RParen) && !self.at_keyword(K::Order) {
            loop {
                arguments.push(self.parse_call_argument()?);
                if self.eat_symbol(S::Comma).is_none() {
                    break;
                }
            }
        }
        if (distinct || all) && arguments.is_empty() {
            return Err(self.error("an argument after a set quantifier"));
        }
        if wildcard && self.at_keyword(K::Order) {
            return Err(self.error("closing parenthesis after wildcard"));
        }
        let arguments_end = self.next_parameter;
        let order_by = if self.at_keyword(K::Order) {
            Some(self.parse_order_by()?)
        } else {
            None
        };
        let order_end = self.next_parameter;
        self.expect_symbol(S::RParen)?;
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
        let null_treatment = if self.eat_keyword(K::Ignore).is_some() {
            self.expect_keyword(K::Nulls)?;
            Some(NullTreatment::IgnoreNulls)
        } else if self.eat_keyword(K::Respect).is_some() {
            self.expect_keyword(K::Nulls)?;
            Some(NullTreatment::RespectNulls)
        } else {
            None
        };
        let window = if self.at_keyword(K::Over) {
            Some(self.parse_window()?)
        } else {
            None
        };
        if wildcard && null_treatment.is_some() {
            return Err(self.error("no null treatment after a wildcard"));
        }
        if null_treatment.is_some() && window.is_none() {
            return Err(self.error("OVER after a null treatment"));
        }
        let span = self.span_since(start);
        let window_end = self.next_parameter;
        self.reorder_parameter_groups(
            parameter_start,
            &[
                order_end..filter_end,
                filter_end..window_end,
                arguments_end..order_end,
                parameter_start..arguments_end,
            ],
        );
        let call = FunctionCall {
            name,
            arguments: arguments.into(),
            distinct,
            order_by,
            filter,
            null_treatment,
            window,
            processing_mode,
            span,
        };
        self.function_node(call)
    }

    fn function_node(&self, call: FunctionCall) -> Result<Expr, ParserError> {
        let name =
            (call.name.parts.len() == 1).then(|| call.name.parts[0].value.to_ascii_lowercase());
        let control = matches!(
            name.as_deref(),
            Some("if" | "nullif" | "coalesce" | "try" | "format")
        );
        if !control {
            return Ok(Expr::FunctionCall(AstBox::new(call)));
        }
        let arity = call.arguments.len();
        let valid_arity = match name.as_deref() {
            Some("if") => (2..=3).contains(&arity),
            Some("nullif") => arity == 2,
            Some("try") => arity == 1,
            _ => arity >= 2,
        };
        if !valid_arity
            || call.distinct
            || call.filter.is_some()
            || call.window.is_some()
            || call.null_treatment.is_some()
            || call.processing_mode.is_some()
            || call
                .arguments
                .iter()
                .any(|argument| argument.name.is_some())
        {
            return Err(ParserError::Construction(
                crate::ParseError::UnexpectedToken {
                    expected: "valid control function arguments and modifiers",
                    found: call.name.parts[0].value.clone(),
                    span: call.span,
                }
                .into(),
            ));
        }
        let span = call.span;
        let mut values = call.arguments.into_iter().map(|argument| argument.value);
        Ok(match name.as_deref() {
            Some("if") => Expr::If(AstBox::new(IfExpr {
                condition: values.next().expect("checked arity"),
                true_value: values.next().expect("checked arity"),
                false_value: values.next(),
                span,
            })),
            Some("nullif") => Expr::NullIf(AstBox::new(NullIfExpr {
                first: values.next().expect("checked arity"),
                second: values.next().expect("checked arity"),
                span,
            })),
            Some("try") => Expr::Try(AstBox::new(TryExpr {
                inner: values.next().expect("checked arity"),
                span,
            })),
            Some("coalesce") => Expr::Coalesce(AstBox::new(CoalesceExpr {
                operands: values.collect(),
                span,
            })),
            Some("format") => Expr::Format(AstBox::new(FormatExpr {
                arguments: values.collect(),
                span,
            })),
            _ => unreachable!("control function classification"),
        })
    }
}

pub(super) fn name_expression(name: ObjectName) -> Expr {
    let mut parts = name.parts.into_iter();
    let mut expression = Expr::Identifier(parts.next().expect("qualified name is nonempty"));
    for field in parts {
        let span = Span::new(expression.span().start(), field.span.end());
        expression = Expr::Dereference(AstBox::new(DereferenceExpr {
            base: expression,
            field: Some(field),
            span,
        }));
    }
    expression
}
