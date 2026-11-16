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

//! Data-driven Pratt parsing machinery for the initial expression grammar.

use crate::ast::{
    AstBox, BetweenPredicate, BooleanTestPredicate, ComparisonOperator, ComparisonPredicate,
    DistinctFromPredicate, InPredicate, IsNullPredicate, LikePredicate, LogicalExpr,
    LogicalOperator, Predicate, PredicatedExpr, TruthValue, TypeKind, TypeParameter,
};

use crate::{
    Span, Symbol, Token, TokenKind,
    ast::{
        AccessExpr, AccessKind, ArrayExpr, BinaryExpr, BinaryOperator, CastExpr, DataTypeParameter,
        DereferenceExpr, ExistsExpr, Expr, FunctionCall, FunctionOrderBy, FunctionQuantifier,
        Ident, InListExpr, IntervalExpr, IntervalField, JsonOperator, LambdaArgumentDeclaration,
        LambdaExpr, Literal, LiteralKind, MapEntry, MapExpr, NestedExpr, NullTreatment, ObjectName,
        RowDataTypeField, RowExpr, RowField, SearchedCaseExpr, SimpleCaseExpr, SubqueryExpr,
        TypeName, UnaryExpr, UnaryOperator, WhenClause, WhenMatch, WindowFrame, WindowFrameBound,
        WindowFrameExclusion, WindowFrameUnits, WindowSpec,
    },
    error::ParseError,
    keyword_class,
    token::Keyword,
};

use super::{StatementParser, query};

const OR_PRECEDENCE: u8 = 10;
const AND_PRECEDENCE: u8 = 20;
const NOT_PRECEDENCE: u8 = 30;
const COMPARISON_PRECEDENCE: u8 = 40;
const ADDITIVE_PRECEDENCE: u8 = 50;
const MULTIPLICATIVE_PRECEDENCE: u8 = 60;
const UNARY_ARITHMETIC_PRECEDENCE: u8 = 70;

#[derive(Clone, Copy)]
enum TokenPattern {
    Keyword(Keyword),
    Symbol(Symbol),
}

impl TokenPattern {
    fn matches(self, kind: &TokenKind) -> bool {
        match (self, kind) {
            (Self::Keyword(expected), TokenKind::Keyword(found)) => expected == *found,
            (Self::Symbol(expected), TokenKind::Symbol(found)) => expected == *found,
            _ => false,
        }
    }
}

#[derive(Clone, Copy)]
enum InfixOperator {
    Comparison(ComparisonOperator),
    NullSafeEqual,
    Binary(BinaryOperator),
    Logical(LogicalOperator),
}

#[derive(Clone, Copy)]
struct InfixBinding {
    token: TokenPattern,
    operator: InfixOperator,
    precedence: u8,
}

#[derive(Clone, Copy)]
struct PrefixBinding {
    token: TokenPattern,
    operator: UnaryOperator,
    precedence: u8,
}

// SQLP-4 extends this table without changing the Pratt loop below.
const INFIX_BINDINGS: &[InfixBinding] = &[
    InfixBinding {
        token: TokenPattern::Keyword(Keyword::Or),
        operator: InfixOperator::Logical(LogicalOperator::Or),
        precedence: OR_PRECEDENCE,
    },
    InfixBinding {
        token: TokenPattern::Keyword(Keyword::And),
        operator: InfixOperator::Logical(LogicalOperator::And),
        precedence: AND_PRECEDENCE,
    },
    InfixBinding {
        token: TokenPattern::Symbol(Symbol::Eq),
        operator: InfixOperator::Comparison(ComparisonOperator::Equal),
        precedence: COMPARISON_PRECEDENCE,
    },
    InfixBinding {
        token: TokenPattern::Symbol(Symbol::NullSafeEq),
        operator: InfixOperator::NullSafeEqual,
        precedence: COMPARISON_PRECEDENCE,
    },
    InfixBinding {
        token: TokenPattern::Symbol(Symbol::Neq),
        operator: InfixOperator::Comparison(ComparisonOperator::NotEqual),
        precedence: COMPARISON_PRECEDENCE,
    },
    InfixBinding {
        token: TokenPattern::Symbol(Symbol::Lt),
        operator: InfixOperator::Comparison(ComparisonOperator::LessThan),
        precedence: COMPARISON_PRECEDENCE,
    },
    InfixBinding {
        token: TokenPattern::Symbol(Symbol::Lte),
        operator: InfixOperator::Comparison(ComparisonOperator::LessThanOrEqual),
        precedence: COMPARISON_PRECEDENCE,
    },
    InfixBinding {
        token: TokenPattern::Symbol(Symbol::Gt),
        operator: InfixOperator::Comparison(ComparisonOperator::GreaterThan),
        precedence: COMPARISON_PRECEDENCE,
    },
    InfixBinding {
        token: TokenPattern::Symbol(Symbol::Gte),
        operator: InfixOperator::Comparison(ComparisonOperator::GreaterThanOrEqual),
        precedence: COMPARISON_PRECEDENCE,
    },
    InfixBinding {
        token: TokenPattern::Symbol(Symbol::Plus),
        operator: InfixOperator::Binary(BinaryOperator::Add),
        precedence: ADDITIVE_PRECEDENCE,
    },
    InfixBinding {
        token: TokenPattern::Symbol(Symbol::Minus),
        operator: InfixOperator::Binary(BinaryOperator::Subtract),
        precedence: ADDITIVE_PRECEDENCE,
    },
    InfixBinding {
        token: TokenPattern::Symbol(Symbol::Star),
        operator: InfixOperator::Binary(BinaryOperator::Multiply),
        precedence: MULTIPLICATIVE_PRECEDENCE,
    },
    InfixBinding {
        token: TokenPattern::Symbol(Symbol::Slash),
        operator: InfixOperator::Binary(BinaryOperator::Divide),
        precedence: MULTIPLICATIVE_PRECEDENCE,
    },
    InfixBinding {
        token: TokenPattern::Symbol(Symbol::Percent),
        operator: InfixOperator::Binary(BinaryOperator::Modulo),
        precedence: MULTIPLICATIVE_PRECEDENCE,
    },
    InfixBinding {
        token: TokenPattern::Symbol(Symbol::DoublePipe),
        operator: InfixOperator::Binary(BinaryOperator::StringConcat),
        precedence: ADDITIVE_PRECEDENCE,
    },
    InfixBinding {
        token: TokenPattern::Symbol(Symbol::ShiftLeft),
        operator: InfixOperator::Binary(BinaryOperator::ShiftLeft),
        precedence: ADDITIVE_PRECEDENCE,
    },
    InfixBinding {
        token: TokenPattern::Symbol(Symbol::ShiftRight),
        operator: InfixOperator::Binary(BinaryOperator::ShiftRight),
        precedence: ADDITIVE_PRECEDENCE,
    },
    InfixBinding {
        token: TokenPattern::Symbol(Symbol::Ampersand),
        operator: InfixOperator::Binary(BinaryOperator::BitwiseAnd),
        precedence: ADDITIVE_PRECEDENCE,
    },
    InfixBinding {
        token: TokenPattern::Symbol(Symbol::Caret),
        operator: InfixOperator::Binary(BinaryOperator::BitwiseXor),
        precedence: ADDITIVE_PRECEDENCE,
    },
    InfixBinding {
        token: TokenPattern::Symbol(Symbol::Pipe),
        operator: InfixOperator::Binary(BinaryOperator::BitwiseOr),
        precedence: ADDITIVE_PRECEDENCE,
    },
];

const PREFIX_BINDINGS: &[PrefixBinding] = &[
    PrefixBinding {
        token: TokenPattern::Keyword(Keyword::Not),
        operator: UnaryOperator::Not,
        precedence: NOT_PRECEDENCE,
    },
    PrefixBinding {
        token: TokenPattern::Symbol(Symbol::Plus),
        operator: UnaryOperator::Plus,
        precedence: UNARY_ARITHMETIC_PRECEDENCE,
    },
    PrefixBinding {
        token: TokenPattern::Symbol(Symbol::Minus),
        operator: UnaryOperator::Minus,
        precedence: UNARY_ARITHMETIC_PRECEDENCE,
    },
    PrefixBinding {
        token: TokenPattern::Symbol(Symbol::Tilde),
        operator: UnaryOperator::BitwiseNot,
        precedence: UNARY_ARITHMETIC_PRECEDENCE,
    },
];

fn infix_binding(kind: &TokenKind) -> Option<InfixBinding> {
    INFIX_BINDINGS
        .iter()
        .copied()
        .find(|binding| binding.token.matches(kind))
}

fn prefix_binding(kind: &TokenKind) -> Option<PrefixBinding> {
    PREFIX_BINDINGS
        .iter()
        .copied()
        .find(|binding| binding.token.matches(kind))
}

pub(super) struct PrattParser<'source, 'tokens> {
    source: &'source str,
    tokens: &'tokens [Token],
    position: usize,
    pending_type_gt: Option<Span>,
    allow_table_function_named_arguments: bool,
    function_call_depth: usize,
}

impl<'source, 'tokens> PrattParser<'source, 'tokens> {
    pub(super) fn new(source: &'source str, tokens: &'tokens [Token]) -> Self {
        Self {
            source,
            tokens,
            position: 0,
            pending_type_gt: None,
            allow_table_function_named_arguments: false,
            function_call_depth: 0,
        }
    }

    fn with_table_function_named_arguments(mut self) -> Self {
        self.allow_table_function_named_arguments = true;
        self
    }

    /// `expression ::= prefix-expression { infix-operator prefix-expression }`
    pub(super) fn parse(mut self) -> Result<Expr, ParseError> {
        let expression = self.parse_binding_power(0)?;
        self.skip_trivia();
        if self.is_end() {
            Ok(expression)
        } else if self.current_is_symbol(Symbol::FatArrow) {
            Err(self.unexpected("syntax error"))
        } else {
            Err(self.unexpected("end of expression"))
        }
    }

    /// `expression ::= prefix-expression { infix-operator expression }`
    fn parse_binding_power(&mut self, minimum_precedence: u8) -> Result<Expr, ParseError> {
        let mut left = self.parse_prefix_expression()?;

        loop {
            self.skip_trivia();
            if self.current_is_symbol(Symbol::Arrow) && self.arrow_starts_lambda(&left) {
                let parameters = Self::lambda_parameters(&left)
                    .expect("arrow_starts_lambda only accepts identifier parameters");
                let start = left.span().start();
                self.advance();
                self.skip_trivia();
                let body = self.parse_binding_power(0)?;
                let span = Span::new(start, body.span().end());
                left = Expr::Lambda(AstBox::new(LambdaExpr {
                    arguments: parameters
                        .into_iter()
                        .map(|name| LambdaArgumentDeclaration {
                            span: name.span,
                            name,
                        })
                        .collect(),
                    body,
                    span,
                }));
                continue;
            }
            if self.current_is_symbol(Symbol::LBracket)
                || self.current_is_symbol(Symbol::Arrow)
                || self.current_is_symbol(Symbol::LongArrow)
                || self.current_is_symbol(Symbol::Dot)
            {
                left = self.parse_postfix_access(left)?;
                continue;
            }
            if minimum_precedence <= COMPARISON_PRECEDENCE && self.starts_comparison_special() {
                left = self.parse_comparison_special(left)?;
                continue;
            }
            let Some(binding) = self.current().and_then(|token| infix_binding(&token.kind)) else {
                break;
            };
            if binding.precedence < minimum_precedence {
                break;
            }

            let predicate_start = self.current_span().start();
            self.advance();
            // Incrementing the minimum implements left associativity for every
            // current table entry. A right-associative future entry can carry a
            // distinct right binding power without changing this parser shape.
            let right = self.parse_binding_power(binding.precedence + 1)?;
            let span = Span::new(left.span().start(), right.span().end());
            left = match binding.operator {
                InfixOperator::Comparison(operator) => {
                    let predicate_span = Span::new(predicate_start, right.span().end());
                    Expr::Predicated(AstBox::new(PredicatedExpr {
                        value: left,
                        predicate: Predicate::Comparison(ComparisonPredicate {
                            operator,
                            right,
                            span: predicate_span,
                        }),
                        span,
                    }))
                }
                InfixOperator::NullSafeEqual => {
                    let predicate_span = Span::new(predicate_start, right.span().end());
                    Expr::Predicated(AstBox::new(PredicatedExpr {
                        value: left,
                        predicate: Predicate::DistinctFrom(DistinctFromPredicate {
                            negated: true,
                            right,
                            span: predicate_span,
                        }),
                        span,
                    }))
                }
                InfixOperator::Binary(operator) => Expr::Binary(BinaryExpr {
                    left: Box::new(left),
                    operator,
                    right: Box::new(right),
                    span,
                }),
                InfixOperator::Logical(operator) => match left {
                    Expr::Logical(mut logical) if logical.operator == operator => {
                        logical.terms.push(right);
                        logical.span = span;
                        Expr::Logical(logical)
                    }
                    left => Expr::Logical(LogicalExpr {
                        operator,
                        terms: vec![left, right].into(),
                        span,
                    }),
                },
            };
        }

        Ok(left)
    }

    fn parse_postfix_access(&mut self, base: Expr) -> Result<Expr, ParseError> {
        let start = base.span().start();
        if self.current_is_symbol(Symbol::LBracket) {
            self.advance();
            self.skip_trivia();
            let index = self.parse_binding_power(0)?;
            self.skip_trivia();
            if !self.current_is_symbol(Symbol::RBracket) {
                return Err(self.unexpected("']' after subscript expression"));
            }
            let end = self.current_span().end();
            self.advance();
            self.skip_trivia();
            return Ok(Expr::Subscript(AstBox::new(crate::ast::SubscriptExpr {
                base,
                index,
                span: Span::new(start, end),
            })));
        }
        if self.current_is_symbol(Symbol::Dot) {
            self.advance();
            self.skip_trivia();
            let Some(token) = self.current().cloned() else {
                return Err(self.unexpected("field name after '.'"));
            };
            if !matches!(
                token.kind,
                TokenKind::Ident | TokenKind::QuotedIdent | TokenKind::Keyword(_)
            ) {
                return Err(self.unexpected("field name after '.'"));
            }
            let field = self.parse_identifier(token.span);
            return Ok(Expr::Dereference(AstBox::new(DereferenceExpr {
                span: Span::new(start, field.span.end()),
                base,
                field: Some(field),
            })));
        }
        let kind = if self.current_is_symbol(Symbol::Arrow) {
            JsonOperator::Arrow
        } else {
            JsonOperator::ArrowText
        };
        self.advance();
        self.skip_trivia();
        let path = self.parse_binding_power(UNARY_ARITHMETIC_PRECEDENCE + 1)?;
        let end = path.span().end();
        Ok(Expr::Access(AccessExpr {
            expr: Box::new(base),
            kind: AccessKind::Json {
                operator: kind,
                path: Box::new(path),
            },
            span: Span::new(start, end),
        }))
    }

    fn arrow_starts_lambda(&self, left: &Expr) -> bool {
        Self::lambda_parameters(left).is_some() && !self.peek_nontrivia_is_string(1)
    }

    fn lambda_parameters(expression: &Expr) -> Option<Vec<Ident>> {
        match expression {
            Expr::Identifier(ident) => Some(vec![ident.clone()]),
            Expr::Nested(nested) => Self::lambda_parameters(&nested.expression),
            Expr::Row(row) if row.fields.iter().all(|field| field.name.is_none()) => row
                .fields
                .iter()
                .map(|field| match &field.expression {
                    Expr::Identifier(ident) => Some(ident.clone()),
                    _ => None,
                })
                .collect(),
            _ => None,
        }
    }

    fn peek_nontrivia_is_string(&self, offset: usize) -> bool {
        self.tokens
            .iter()
            .skip(self.position)
            .filter(|token| !matches!(token.kind, TokenKind::Trivia(_)))
            .nth(offset)
            .is_some_and(|token| matches!(token.kind, TokenKind::String))
    }

    fn peek_nontrivia_is_symbol(&self, offset: usize, symbol: Symbol) -> bool {
        self.tokens
            .iter()
            .skip(self.position)
            .filter(|token| !matches!(token.kind, TokenKind::Trivia(_)))
            .nth(offset)
            .is_some_and(|token| matches!(token.kind, TokenKind::Symbol(found) if found == symbol))
    }

    fn substring_special_form_follows(&self) -> bool {
        let mut depth = 0;
        for token in self
            .tokens
            .iter()
            .skip(self.position)
            .filter(|token| !matches!(token.kind, TokenKind::Trivia(_)))
        {
            match token.kind {
                TokenKind::Symbol(Symbol::LParen) => depth += 1,
                TokenKind::Symbol(Symbol::RParen) if depth == 1 => return false,
                TokenKind::Symbol(Symbol::RParen) => depth -= 1,
                TokenKind::Keyword(Keyword::From) if depth == 1 => return true,
                _ => {}
            }
        }
        false
    }

    fn starts_comparison_special(&self) -> bool {
        self.current_is_keyword(Keyword::Between)
            || self.current_is_keyword(Keyword::In)
            || self.current_is_keyword(Keyword::Like)
            || self.current_is_keyword(Keyword::Ilike)
            || self.current_is_keyword(Keyword::Rlike)
            || self.current_is_keyword(Keyword::Similar)
            || self.current_is_keyword(Keyword::Is)
            || (self.current_is_keyword(Keyword::Not)
                && (self.peek_keyword(1, Keyword::Between)
                    || self.peek_keyword(1, Keyword::In)
                    || self.peek_keyword(1, Keyword::Like)
                    || self.peek_keyword(1, Keyword::Ilike)
                    || self.peek_keyword(1, Keyword::Rlike)
                    || self.peek_keyword(1, Keyword::Similar)))
    }

    fn parse_comparison_special(&mut self, left: Expr) -> Result<Expr, ParseError> {
        let start = self.current_span().start();
        let negated = if self.current_is_keyword(Keyword::Not) {
            self.advance();
            self.skip_trivia();
            true
        } else {
            false
        };
        if self.current_is_keyword(Keyword::Between) {
            self.advance();
            self.skip_trivia();
            let min = self.parse_binding_power(COMPARISON_PRECEDENCE + 1)?;
            if !self.current_is_keyword(Keyword::And) {
                return Err(self.unexpected("AND in BETWEEN expression"));
            }
            self.advance();
            self.skip_trivia();
            let max = self.parse_binding_power(COMPARISON_PRECEDENCE + 1)?;
            let span = Span::new(start, max.span().end());
            return Ok(predicate_node(
                left,
                Predicate::Between(BetweenPredicate {
                    negated,
                    symmetry: None,
                    min,
                    max,
                    span,
                }),
            ));
        }
        if self.current_is_keyword(Keyword::In) {
            self.advance();
            self.skip_trivia();
            if !self.current_is_symbol(Symbol::LParen) {
                return Err(self.unexpected("'(' after IN"));
            }
            let list_start = self.current_span().start();
            self.advance();
            self.skip_trivia();
            if self.query_follows() {
                let (query, span) = self.parse_subquery_body(list_start)?;
                let predicate_span = Span::new(start, span.end());
                return Ok(predicate_node(
                    left,
                    Predicate::In(InPredicate {
                        negated,
                        value_list: Expr::Subquery(SubqueryExpr { query, span }),
                        span: predicate_span,
                    }),
                ));
            }
            if self.current_is_symbol(Symbol::RParen) {
                return Err(self.unexpected("expression in IN list"));
            }
            let mut values = Vec::new();
            loop {
                values.push(self.parse_binding_power(0)?);
                self.skip_trivia();
                if !self.current_is_symbol(Symbol::Comma) {
                    break;
                }
                self.advance();
                self.skip_trivia();
            }
            if !self.current_is_symbol(Symbol::RParen) {
                return Err(self.unexpected("')' after IN list"));
            }
            let end = self.current_span().end();
            self.advance();
            let value_list = Expr::InList(AstBox::new(InListExpr {
                values: values.into(),
                span: Span::new(list_start, end),
            }));
            return Ok(predicate_node(
                left,
                Predicate::In(InPredicate {
                    negated,
                    value_list,
                    span: Span::new(start, end),
                }),
            ));
        }
        if self.current_is_keyword(Keyword::Like)
            || self.current_is_keyword(Keyword::Ilike)
            || self.current_is_keyword(Keyword::Rlike)
            || self.current_is_keyword(Keyword::Similar)
        {
            // The retiring operators all selected the original LIKE kernel.
            let similar = self.current_is_keyword(Keyword::Similar);
            self.advance();
            self.skip_trivia();
            if similar && self.current_is_keyword(Keyword::To) {
                self.advance();
                self.skip_trivia();
            }
            let pattern = self.parse_binding_power(COMPARISON_PRECEDENCE + 1)?;
            let escape = if self.current_is_keyword(Keyword::Escape) {
                self.advance();
                self.skip_trivia();
                Some(self.parse_binding_power(COMPARISON_PRECEDENCE + 1)?)
            } else {
                None
            };
            let end = escape
                .as_ref()
                .map_or_else(|| pattern.span().end(), |v| v.span().end());
            return Ok(predicate_node(
                left,
                Predicate::Like(LikePredicate {
                    negated,
                    pattern,
                    escape,
                    span: Span::new(start, end),
                }),
            ));
        }
        if self.current_is_keyword(Keyword::Is) {
            if negated {
                return Err(self.unexpected("BETWEEN, IN, or LIKE after NOT"));
            }
            self.advance();
            self.skip_trivia();
            let negated = if self.current_is_keyword(Keyword::Not) {
                self.advance();
                self.skip_trivia();
                true
            } else {
                false
            };
            if self.current_is_keyword(Keyword::Distinct) {
                self.advance();
                self.skip_trivia();
                if !self.current_is_keyword(Keyword::From) {
                    return Err(self.unexpected("FROM after IS DISTINCT"));
                }
                self.advance();
                self.skip_trivia();
                let right = self.parse_binding_power(COMPARISON_PRECEDENCE + 1)?;
                let span = Span::new(start, right.span().end());
                return Ok(predicate_node(
                    left,
                    Predicate::DistinctFrom(DistinctFromPredicate {
                        negated,
                        right,
                        span,
                    }),
                ));
            }
            let token = self.current().cloned().ok_or_else(|| {
                self.unexpected("NULL, TRUE, FALSE, UNKNOWN, or DISTINCT after IS")
            })?;
            let span = Span::new(start, token.span.end());
            let predicate = match token.kind {
                TokenKind::Keyword(Keyword::Null) => {
                    Predicate::IsNull(IsNullPredicate { negated, span })
                }
                TokenKind::Keyword(Keyword::True) => Predicate::BooleanTest(BooleanTestPredicate {
                    negated,
                    truth_value: TruthValue::True,
                    span,
                }),
                TokenKind::Keyword(Keyword::False) => {
                    Predicate::BooleanTest(BooleanTestPredicate {
                        negated,
                        truth_value: TruthValue::False,
                        span,
                    })
                }
                TokenKind::Keyword(Keyword::Unknown) => {
                    Predicate::BooleanTest(BooleanTestPredicate {
                        negated,
                        truth_value: TruthValue::Unknown,
                        span,
                    })
                }
                _ => {
                    return Err(self.unexpected("NULL, TRUE, FALSE, UNKNOWN, or DISTINCT after IS"));
                }
            };
            self.advance();
            return Ok(predicate_node(left, predicate));
        }
        Err(self.unexpected("comparison special form"))
    }

    /// `prefix-expression ::= unary-operator expression | primary-expression`
    fn parse_prefix_expression(&mut self) -> Result<Expr, ParseError> {
        self.skip_trivia();
        if self.current_is_keyword(Keyword::Not) && self.peek_keyword(1, Keyword::Exists) {
            let start = self.current_span().start();
            self.advance();
            self.skip_trivia();
            return self.parse_exists_expression(start, true);
        }
        if let Some(binding) = self.current().and_then(|token| prefix_binding(&token.kind)) {
            let start = self.current_span().start();
            self.advance();
            self.skip_trivia();
            if binding.operator == UnaryOperator::Minus
                && let Some(Token {
                    kind: TokenKind::Number,
                    span,
                }) = self.current().cloned()
                && super::is_integer_number(self.token_text(span))
            {
                let literal_span = Span::new(start, span.end());
                let kind = super::number_literal_kind(
                    format!("-{}", self.token_text(span)),
                    literal_span,
                )?;
                self.advance();
                return Ok(Expr::Literal(Literal {
                    kind,
                    span: literal_span,
                }));
            }
            let expression = self.parse_binding_power(binding.precedence)?;
            let span = Span::new(start, expression.span().end());
            return Ok(Expr::Unary(UnaryExpr {
                operator: binding.operator,
                expression: Box::new(expression),
                span,
            }));
        }

        self.parse_primary_expression()
    }

    /// `primary-expression ::= literal | identifier [ function-call ] | "(" expression ")"`
    fn parse_primary_expression(&mut self) -> Result<Expr, ParseError> {
        self.skip_trivia();
        if self.current_is_word("ARRAY") && self.peek_nontrivia_is_symbol(1, Symbol::Lt) {
            let start = self.current_span().start();
            let array_type = self.parse_type_name()?;
            let TypeKind::Generic(generic) = &array_type.kind else {
                return Err(self.unexpected("ARRAY element type"));
            };
            if generic.arguments.len() != 1
                || !matches!(generic.arguments.first(), Some(DataTypeParameter::Type(_)))
            {
                return Err(self.unexpected("ARRAY element type"));
            }
            if !self.current_is_symbol(Symbol::LBracket) {
                return Err(self.unexpected("'[' after ARRAY element type"));
            }
            let expression = self.parse_array_expression(self.current_span())?;
            let span = Span::new(start, expression.span().end());
            // The source type belongs to the outer CAST. ARRAY inference sees
            // the original children without a target context.
            return Ok(Expr::Cast(AstBox::new(CastExpr {
                expression,
                data_type: array_type,
                safe: false,
                span,
            })));
        }
        if self.current_is_word("MAP") && self.peek_nontrivia_is_symbol(1, Symbol::LBrace) {
            let start = self.current_span().start();
            self.advance();
            self.skip_trivia();
            return self.parse_map_expression(start);
        }
        if self.current_is_word("SUBSTRING") && self.substring_special_form_follows() {
            return self.parse_substring_expression();
        }
        if self.current_is_word("EXTRACT") && self.peek_nontrivia_is_symbol(1, Symbol::LParen) {
            return self.parse_extract_expression();
        }
        if (self.current_is_word("DATETIME")
            || self.current_is_word("JSON")
            || self.current_is_word("DECIMAL"))
            && self.peek_nontrivia_is_string(1)
        {
            return self.parse_typed_string_expression();
        }
        let token = self.current().cloned();
        match token {
            Some(Token {
                kind: TokenKind::Number,
                span,
            }) => {
                self.advance();
                Ok(Expr::Literal(Literal {
                    kind: super::number_literal_kind(self.token_text(span).to_owned(), span)?,
                    span,
                }))
            }
            Some(Token {
                kind: TokenKind::HexNumber,
                span,
            }) => {
                self.advance();
                Ok(Expr::Literal(Literal {
                    kind: super::binary_literal_kind(
                        self.hex_string_value(span).expect("hex token"),
                        span,
                    )?,
                    span,
                }))
            }
            Some(Token {
                kind: TokenKind::String,
                span,
            }) => {
                self.advance();
                Ok(Expr::Literal(Literal {
                    kind: match self.hex_string_value(span) {
                        Some(value) => super::binary_literal_kind(value, span)?,
                        None => LiteralKind::String(self.string_value(span)),
                    },
                    span,
                }))
            }
            Some(Token {
                kind: TokenKind::Keyword(Keyword::Null),
                span,
            }) => {
                self.advance();
                Ok(Expr::Literal(Literal {
                    kind: LiteralKind::Null,
                    span,
                }))
            }
            Some(Token {
                kind: TokenKind::Keyword(Keyword::True),
                span,
            }) => {
                self.advance();
                Ok(Expr::Literal(Literal {
                    kind: LiteralKind::Boolean(true),
                    span,
                }))
            }
            Some(Token {
                kind: TokenKind::Keyword(Keyword::False),
                span,
            }) => {
                self.advance();
                Ok(Expr::Literal(Literal {
                    kind: LiteralKind::Boolean(false),
                    span,
                }))
            }
            Some(Token {
                kind: TokenKind::Keyword(Keyword::Case),
                ..
            }) => self.parse_case_expression(),
            Some(Token {
                kind: TokenKind::Keyword(Keyword::Cast),
                ..
            }) => self.parse_cast_expression(false),
            Some(Token {
                kind: TokenKind::Keyword(Keyword::TryCast),
                ..
            }) => self.parse_cast_expression(true),
            Some(Token {
                kind: TokenKind::Keyword(Keyword::Interval),
                ..
            }) => self.parse_interval_expression(),
            Some(Token {
                kind: TokenKind::Keyword(Keyword::Date | Keyword::Time | Keyword::Timestamp),
                ..
            }) if self.peek_nontrivia_is_string(1) => self.parse_typed_string_expression(),
            Some(Token {
                kind: TokenKind::Keyword(Keyword::Exists),
                span,
            }) => self.parse_exists_expression(span.start(), false),
            Some(Token {
                kind: TokenKind::Ident | TokenKind::QuotedIdent,
                span,
            }) => self.parse_identifier_or_function_call(span),
            Some(Token {
                kind: TokenKind::Keyword(keyword),
                span,
            }) if keyword_class(keyword) == crate::KeywordClass::NonReserved => {
                self.parse_identifier_or_function_call(span)
            }
            Some(Token {
                kind: TokenKind::Symbol(Symbol::LParen),
                span,
            }) if self.parenthesized_query_follows() => self.parse_scalar_subquery(span),
            Some(Token {
                kind: TokenKind::Symbol(Symbol::LParen),
                span,
            }) => self.parse_nested_expression(span),
            Some(Token {
                kind: TokenKind::Symbol(Symbol::LBracket),
                span,
            }) => self.parse_array_expression(span),
            _ => Err(self.unexpected("expression")),
        }
    }

    fn parse_interval_expression(&mut self) -> Result<Expr, ParseError> {
        let start = self.current_span().start();
        self.advance();
        self.skip_trivia();
        let value = self.parse_binding_power(0)?;
        self.skip_trivia();
        let (leading_field, field_span) = self.parse_interval_field()?;
        Ok(Expr::Interval(IntervalExpr {
            value: Box::new(value),
            leading_field,
            leading_precision: None,
            last_field: None,
            fractional_seconds_precision: None,
            span: Span::new(start, field_span.end()),
        }))
    }

    fn parse_interval_field(&mut self) -> Result<(IntervalField, Span), ParseError> {
        let field = if self.current_is_word("YEAR") {
            IntervalField::Year
        } else if self.current_is_word("QUARTER") {
            IntervalField::Quarter
        } else if self.current_is_word("MONTH") {
            IntervalField::Month
        } else if self.current_is_word("WEEK") {
            IntervalField::Week
        } else if self.current_is_word("DAY") {
            IntervalField::Day
        } else if self.current_is_word("HOUR") {
            IntervalField::Hour
        } else if self.current_is_word("MINUTE") {
            IntervalField::Minute
        } else if self.current_is_word("SECOND") {
            IntervalField::Second
        } else if self.current_is_word("MILLISECOND") {
            IntervalField::Millisecond
        } else if self.current_is_word("MICROSECOND") {
            IntervalField::Microsecond
        } else {
            return Err(self.unexpected("interval field"));
        };
        let span = self.current_span();
        self.advance();
        Ok((field, span))
    }

    fn parse_exists_expression(&mut self, start: usize, negated: bool) -> Result<Expr, ParseError> {
        if !self.current_is_keyword(Keyword::Exists) {
            return Err(self.unexpected("EXISTS"));
        }
        let exists_start = self.current_span().start();
        self.advance();
        self.skip_trivia();
        if !self.current_is_symbol(Symbol::LParen) {
            return Err(self.unexpected("'(' after EXISTS"));
        }
        let opening = self.current_span().start();
        self.advance();
        self.skip_trivia();
        if !self.query_follows() {
            return Err(self.unexpected("query after EXISTS ("));
        }
        let (query, subquery_span) = self.parse_subquery_body(opening)?;
        let span = Span::new(exists_start, subquery_span.end());
        let exists = Expr::Exists(AstBox::new(ExistsExpr {
            subquery: Expr::Subquery(SubqueryExpr {
                query,
                span: subquery_span,
            }),
            span,
        }));
        Ok(if negated {
            Expr::Unary(UnaryExpr {
                operator: UnaryOperator::Not,
                expression: Box::new(exists),
                span: Span::new(start, span.end()),
            })
        } else {
            exists
        })
    }

    fn parse_scalar_subquery(&mut self, opening_span: Span) -> Result<Expr, ParseError> {
        self.advance();
        self.skip_trivia();
        let (query, span) = self.parse_subquery_body(opening_span.start())?;
        Ok(Expr::Subquery(SubqueryExpr { query, span }))
    }

    fn parse_subquery_body(
        &mut self,
        start: usize,
    ) -> Result<(Box<crate::ast::Query>, Span), ParseError> {
        let mut parser = StatementParser::new(self.source, &self.tokens[self.position..]);
        let mut query = query::parse_query(&mut parser)?;
        query.session_properties.clear();
        self.position += parser.position;
        self.skip_trivia();
        if !self.current_is_symbol(Symbol::RParen) {
            return Err(self.unexpected("')' after subquery"));
        }
        let end = self.current_span().end();
        self.advance();
        self.skip_trivia();
        Ok((Box::new(query), Span::new(start, end)))
    }

    fn parenthesized_query_follows(&self) -> bool {
        self.tokens
            .iter()
            .skip(self.position + 1)
            .find(|token| !matches!(token.kind, TokenKind::Trivia(_)))
            .is_some_and(|token| Self::token_starts_query(&token.kind))
    }

    fn query_follows(&self) -> bool {
        self.current()
            .is_some_and(|token| Self::token_starts_query(&token.kind))
    }

    fn token_starts_query(kind: &TokenKind) -> bool {
        matches!(
            kind,
            TokenKind::Keyword(Keyword::Select | Keyword::Values | Keyword::With)
        )
    }

    fn parse_case_expression(&mut self) -> Result<Expr, ParseError> {
        let start = self.current_span().start();
        self.advance();
        self.skip_trivia();
        let operand = if self.current_is_keyword(Keyword::When) {
            None
        } else {
            Some(self.parse_binding_power(0)?)
        };
        let mut when_clauses = Vec::new();
        while self.current_is_keyword(Keyword::When) {
            let when_start = self.current_span().start();
            self.advance();
            self.skip_trivia();
            let condition = self.parse_binding_power(0)?;
            if !self.current_is_keyword(Keyword::Then) {
                return Err(self.unexpected("THEN in CASE expression"));
            }
            self.advance();
            self.skip_trivia();
            let result = self.parse_binding_power(0)?;
            let span = Span::new(when_start, result.span().end());
            when_clauses.push(WhenClause {
                match_value: WhenMatch::Operand(condition),
                result,
                span,
            });
        }
        if when_clauses.is_empty() {
            return Err(self.unexpected("WHEN in CASE expression"));
        }
        let default_value = if self.current_is_keyword(Keyword::Else) {
            self.advance();
            self.skip_trivia();
            Some(self.parse_binding_power(0)?)
        } else {
            None
        };
        if !self.current_is_keyword(Keyword::End) {
            return Err(self.unexpected("END in CASE expression"));
        }
        let end = self.current_span().end();
        self.advance();
        let span = Span::new(start, end);
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

    fn parse_cast_expression(&mut self, safe: bool) -> Result<Expr, ParseError> {
        let start = self.current_span().start();
        self.advance();
        self.skip_trivia();
        if !self.current_is_symbol(Symbol::LParen) {
            return Err(self.unexpected("'(' after CAST"));
        }
        self.advance();
        self.skip_trivia();
        let expr = self.parse_binding_power(0)?;
        if !self.current_is_keyword(Keyword::As) {
            return Err(self.unexpected("AS in CAST expression"));
        }
        self.advance();
        self.skip_trivia();
        let data_type = self.parse_type_name()?;
        if !self.current_is_symbol(Symbol::RParen) {
            return Err(self.unexpected("')' after CAST type"));
        }
        let end = self.current_span().end();
        self.advance();
        Ok(Expr::Cast(AstBox::new(CastExpr {
            expression: expr,
            data_type,
            safe,
            span: Span::new(start, end),
        })))
    }

    fn parse_typed_string_expression(&mut self) -> Result<Expr, ParseError> {
        let start = self.current_span().start();
        let raw_tag = self.token_text(self.current_span()).to_owned();
        self.advance();
        self.skip_trivia();
        let Some(token) = self.current().cloned() else {
            return Err(self.unexpected("string literal after type name"));
        };
        if token.kind != TokenKind::String {
            return Err(self.unexpected("string literal after type name"));
        }
        self.advance();
        let value = self.string_value(token.span);
        let kind = if raw_tag.eq_ignore_ascii_case("DECIMAL") {
            LiteralKind::Decimal(value)
        } else {
            LiteralKind::Generic(crate::ast::GenericLiteral {
                type_name: if raw_tag.eq_ignore_ascii_case("DATETIME") {
                    "TIMESTAMP".into()
                } else {
                    raw_tag
                },
                value,
            })
        };
        Ok(Expr::Literal(Literal {
            kind,
            span: Span::new(start, token.span.end()),
        }))
    }

    fn parse_extract_expression(&mut self) -> Result<Expr, ParseError> {
        let start = self.current_span().start();
        self.advance();
        self.skip_trivia();
        if !self.current_is_symbol(Symbol::LParen) {
            return Err(self.unexpected("'(' after EXTRACT"));
        }
        self.advance();
        self.skip_trivia();
        let field_span = self.current_span();
        let Some(field_token) = self.current().cloned() else {
            return Err(self.unexpected("EXTRACT field"));
        };
        if !matches!(
            field_token.kind,
            TokenKind::Ident | TokenKind::QuotedIdent | TokenKind::Keyword(_)
        ) {
            return Err(self.unexpected("EXTRACT field"));
        }
        let field = Expr::Identifier(self.parse_identifier(field_span));
        self.skip_trivia();
        if !self.current_is_keyword(Keyword::From) {
            return Err(self.unexpected("FROM in EXTRACT"));
        }
        self.advance();
        self.skip_trivia();
        let expression = self.parse_binding_power(0)?;
        self.skip_trivia();
        if !self.current_is_symbol(Symbol::RParen) {
            return Err(self.unexpected("')' after EXTRACT"));
        }
        let end = self.current_span().end();
        self.advance();
        Ok(Expr::FunctionCall(FunctionCall {
            name: ObjectName {
                parts: vec![Ident {
                    value: "EXTRACT".to_owned(),
                    quoted: false,
                    span: Span::new(start, start + "EXTRACT".len()),
                }],
                span: Span::new(start, start + "EXTRACT".len()),
            },
            arguments: vec![field, expression],
            quantifier: FunctionQuantifier::None,
            order_by: Vec::new(),
            separator: None,
            filter: None,
            null_treatment: None,
            over: None,
            substring_from_syntax: false,
            span: Span::new(start, end),
        }))
    }

    fn parse_substring_expression(&mut self) -> Result<Expr, ParseError> {
        let start = self.current_span().start();
        let name_span = self.current_span();
        self.advance();
        self.skip_trivia();
        if !self.current_is_symbol(Symbol::LParen) {
            return Err(self.unexpected("'(' after SUBSTRING"));
        }
        self.advance();
        self.skip_trivia();
        let value = self.parse_binding_power(0)?;
        self.skip_trivia();
        if !self.current_is_keyword(Keyword::From) {
            return Err(self.unexpected("FROM in SUBSTRING"));
        }
        self.advance();
        self.skip_trivia();
        let start_position = self.parse_binding_power(0)?;
        self.skip_trivia();
        let length = if self.current_is_keyword(Keyword::For) {
            self.advance();
            self.skip_trivia();
            Some(self.parse_binding_power(0)?)
        } else {
            None
        };
        self.skip_trivia();
        if !self.current_is_symbol(Symbol::RParen) {
            return Err(self.unexpected("')' after SUBSTRING"));
        }
        let end = self.current_span().end();
        self.advance();

        let mut arguments = vec![value, start_position];
        if let Some(length) = length {
            arguments.push(length);
        }
        Ok(Expr::FunctionCall(FunctionCall {
            name: ObjectName {
                parts: vec![Ident {
                    value: self.token_text(name_span).to_owned(),
                    quoted: false,
                    span: name_span,
                }],
                span: name_span,
            },
            arguments,
            quantifier: FunctionQuantifier::None,
            order_by: Vec::new(),
            separator: None,
            filter: None,
            null_treatment: None,
            over: None,
            substring_from_syntax: true,
            span: Span::new(start, end),
        }))
    }

    fn parse_type_name(&mut self) -> Result<TypeName, ParseError> {
        let start = self.current_span().start();
        let Some(token) = self.current().cloned() else {
            return Err(self.unexpected("type name"));
        };
        if !matches!(
            token.kind,
            TokenKind::Ident | TokenKind::QuotedIdent | TokenKind::Keyword(_)
        ) {
            return Err(self.unexpected("type name"));
        }
        let mut parts = vec![self.parse_identifier(token.span)];
        while self.current_is_symbol(Symbol::Dot) {
            self.advance();
            self.skip_trivia();
            let Some(token) = self.current().cloned() else {
                return Err(self.unexpected("type name segment"));
            };
            if !matches!(
                token.kind,
                TokenKind::Ident | TokenKind::QuotedIdent | TokenKind::Keyword(_)
            ) {
                return Err(self.unexpected("type name segment"));
            }
            parts.push(self.parse_identifier(token.span));
        }
        if parts.len() != 1 {
            return Err(self.unexpected("unqualified type name"));
        }
        let name = parts.pop().expect("first type identifier");
        let name_end = name.span.end();
        let mut arguments = Vec::new();
        let mut fields = None;
        let mut end = name_end;
        if self.current_is_symbol(Symbol::LParen) {
            self.advance();
            self.skip_trivia();
            loop {
                let literal = match self.current().map(|token| &token.kind) {
                    Some(TokenKind::Number) => {
                        let span = self.current_span();
                        let value = self.token_text(span).to_owned();
                        self.advance();
                        self.skip_trivia();
                        Literal {
                            kind: super::number_literal_kind(value, span)?,
                            span,
                        }
                    }
                    _ => return Err(self.unexpected("type parameter")),
                };
                arguments.push(super::numeric_type_parameter(literal)?);
                if self.pending_type_gt.is_some() || !self.current_is_symbol(Symbol::Comma) {
                    break;
                }
                self.advance();
                self.skip_trivia();
            }
            if !self.current_is_symbol(Symbol::RParen) {
                return Err(self.unexpected("')' after type parameters"));
            }
            end = self.current_span().end();
            self.advance();
            self.skip_trivia();
        } else if self.current_is_symbol(Symbol::Lt) {
            self.advance();
            self.skip_trivia();
            let is_struct = name.value.eq_ignore_ascii_case("STRUCT");
            loop {
                if is_struct {
                    let field_start = self.current_span().start();
                    let Some(token) = self.current().cloned() else {
                        return Err(self.unexpected("STRUCT field name"));
                    };
                    if !matches!(
                        token.kind,
                        TokenKind::Ident | TokenKind::QuotedIdent | TokenKind::Keyword(_)
                    ) {
                        return Err(self.unexpected("STRUCT field name"));
                    }
                    let field_name = self.parse_identifier(token.span);
                    self.skip_trivia();
                    let data_type = self.parse_type_name()?;
                    let field_end = data_type.span.end();
                    fields.get_or_insert_with(Vec::new).push(RowDataTypeField {
                        name: Some(field_name),
                        data_type,
                        span: Span::new(field_start, field_end),
                    });
                } else {
                    arguments.push(DataTypeParameter::Type(TypeParameter {
                        data_type: AstBox::new(self.parse_type_name()?),
                    }));
                }
                self.skip_trivia();
                if self.pending_type_gt.is_some() || !self.current_is_symbol(Symbol::Comma) {
                    break;
                }
                self.advance();
                self.skip_trivia();
            }
            end = self.consume_type_gt()?;
        }
        super::finish_type_name(name, arguments, fields, Span::new(start, end))
    }

    /// `identifier ::= IDENT | QUOTED_IDENT`
    fn parse_identifier(&mut self, span: Span) -> Ident {
        let quoted = matches!(
            self.current().map(|token| &token.kind),
            Some(TokenKind::QuotedIdent)
        );
        self.advance();
        Ident {
            value: if quoted {
                self.quoted_identifier_value(span)
            } else {
                self.token_text(span).to_owned()
            },
            quoted,
            span,
        }
    }

    /// `function-call ::= object-name "(" [ expression { "," expression } ] ")"`
    fn parse_identifier_or_function_call(&mut self, span: Span) -> Result<Expr, ParseError> {
        let first = self.parse_identifier(span);
        let mut end = first.span.end();
        let mut parts = vec![first];
        while self.current_is_symbol(Symbol::Dot) {
            self.advance();
            self.skip_trivia();
            let Some(token) = self.current().cloned() else {
                return Err(self.unexpected("identifier after '.'"));
            };
            if !matches!(
                token.kind,
                TokenKind::Ident | TokenKind::QuotedIdent | TokenKind::Keyword(_)
            ) {
                return Err(self.unexpected("identifier after '.'"));
            }
            let ident = self.parse_identifier(token.span);
            end = ident.span.end();
            parts.push(ident);
        }
        let name = ObjectName {
            parts,
            span: Span::new(span.start(), end),
        };
        self.skip_trivia();
        if !self.current_is_symbol(Symbol::LParen) {
            let mut parts = name.parts.into_iter();
            let mut expression = Expr::Identifier(parts.next().expect("nonempty name"));
            for field in parts {
                let span = expression.span().cover(field.span);
                expression = Expr::Dereference(AstBox::new(DereferenceExpr {
                    base: expression,
                    field: Some(field),
                    span,
                }));
            }
            return Ok(expression);
        }

        self.advance();
        let mut arguments = Vec::new();
        self.skip_trivia();
        let quantifier = if self.current_is_keyword(Keyword::Distinct) {
            self.advance();
            self.skip_trivia();
            FunctionQuantifier::Distinct
        } else if self.current_is_word("ALL") {
            self.advance();
            self.skip_trivia();
            FunctionQuantifier::All
        } else {
            FunctionQuantifier::None
        };
        let allow_named_arguments =
            self.allow_table_function_named_arguments && self.function_call_depth == 0;
        self.function_call_depth += 1;
        let mut null_treatment = None;
        if !self.current_is_symbol(Symbol::RParen)
            && !self.current_is_keyword(Keyword::Order)
            && !self.current_is_word("SEPARATOR")
        {
            loop {
                if self.current_is_symbol(Symbol::Star) {
                    let star = self.current_span();
                    self.advance();
                    arguments.push(Expr::Identifier(Ident {
                        value: "*".to_owned(),
                        quoted: false,
                        span: star,
                    }));
                } else if allow_named_arguments && self.table_function_named_argument_ahead() {
                    arguments.push(self.parse_table_function_named_argument()?);
                } else {
                    arguments.push(self.parse_binding_power(0)?);
                }
                self.skip_trivia();
                if null_treatment.is_none() {
                    null_treatment = self.consume_null_treatment()?;
                }
                if !self.current_is_symbol(Symbol::Comma) {
                    break;
                }
                self.advance();
                self.skip_trivia();
                if self.current_is_symbol(Symbol::RParen) || self.is_end() {
                    return Err(self.unexpected("expression after ','"));
                }
            }
        }

        let mut order_by = Vec::new();
        if self.current_is_keyword(Keyword::Order) {
            self.advance();
            self.skip_trivia();
            if !self.current_is_keyword(Keyword::By) {
                return Err(self.unexpected("BY after function ORDER"));
            }
            self.advance();
            self.skip_trivia();
            loop {
                let expr = self.parse_binding_power(0)?;
                let asc = if self.current_is_keyword(Keyword::Asc) {
                    self.advance();
                    self.skip_trivia();
                    Some(true)
                } else if self.current_is_keyword(Keyword::Desc) {
                    self.advance();
                    self.skip_trivia();
                    Some(false)
                } else {
                    None
                };
                let nulls_first = if self.current_is_keyword(Keyword::Nulls) {
                    self.advance();
                    self.skip_trivia();
                    if self.current_is_keyword(Keyword::First) {
                        self.advance();
                        self.skip_trivia();
                        Some(true)
                    } else if self.current_is_keyword(Keyword::Last) {
                        self.advance();
                        self.skip_trivia();
                        Some(false)
                    } else {
                        return Err(self.unexpected("FIRST or LAST after NULLS"));
                    }
                } else {
                    None
                };
                order_by.push(FunctionOrderBy {
                    span: Span::new(expr.span().start(), self.current_span().start()),
                    expr,
                    asc,
                    nulls_first,
                });
                if !self.current_is_symbol(Symbol::Comma) {
                    break;
                }
                self.advance();
                self.skip_trivia();
            }
        }
        let separator = if self.current_is_word("SEPARATOR") {
            self.advance();
            self.skip_trivia();
            Some(Box::new(self.parse_binding_power(0)?))
        } else {
            None
        };

        self.skip_trivia();
        if !self.current_is_symbol(Symbol::RParen) {
            return Err(self.unexpected("')'"));
        }
        let end = self.current_span().end();
        self.advance();
        self.skip_trivia();
        if null_treatment.is_none() {
            null_treatment = self.consume_null_treatment()?;
        }
        let filter = if self.current_is_keyword(Keyword::Filter) {
            self.advance();
            self.skip_trivia();
            if !self.current_is_symbol(Symbol::LParen) {
                return Err(self.unexpected("'(' after FILTER"));
            }
            self.advance();
            self.skip_trivia();
            if !self.current_is_keyword(Keyword::Where) {
                return Err(self.unexpected("WHERE in FILTER"));
            }
            self.advance();
            self.skip_trivia();
            let filter = self.parse_binding_power(0)?;
            self.skip_trivia();
            if !self.current_is_symbol(Symbol::RParen) {
                return Err(self.unexpected("')' after FILTER"));
            }
            self.advance();
            self.skip_trivia();
            Some(Box::new(filter))
        } else {
            None
        };
        let over = if self.current_is_keyword(Keyword::Over) {
            self.advance();
            self.skip_trivia();
            if matches!(
                self.current().map(|token| &token.kind),
                Some(TokenKind::Ident | TokenKind::QuotedIdent)
            ) {
                let name = self.parse_identifier(self.current_span());
                Some(Box::new(WindowSpec {
                    span: name.span,
                    existing_window_name: Some(name),
                    partition_by: Vec::new(),
                    order_by: Vec::new(),
                    window_frame: None,
                }))
            } else {
                Some(Box::new(self.parse_window_spec()?))
            }
        } else {
            None
        };
        let span_end = over.as_ref().map_or_else(
            || {
                filter.as_ref().map_or_else(
                    || separator.as_ref().map_or(end, |value| value.span().end()),
                    |value| value.span().end(),
                )
            },
            |window| window.span.end(),
        );
        self.function_call_depth -= 1;
        Ok(Expr::FunctionCall(FunctionCall {
            name,
            arguments,
            quantifier,
            order_by,
            separator,
            filter,
            null_treatment,
            over,
            substring_from_syntax: false,
            span: Span::new(span.start(), span_end),
        }))
    }

    fn table_function_named_argument_ahead(&self) -> bool {
        matches!(
            self.current().map(|token| &token.kind),
            Some(TokenKind::Ident | TokenKind::QuotedIdent | TokenKind::Keyword(_))
        ) && self.peek_nontrivia_is_symbol(1, Symbol::FatArrow)
    }

    fn parse_table_function_named_argument(&mut self) -> Result<Expr, ParseError> {
        let start = self.current_span().start();
        let name = self.parse_identifier(self.current_span());
        self.skip_trivia();
        if !self.current_is_symbol(Symbol::FatArrow) {
            return Err(self.unexpected("'=>' after table function argument name"));
        }
        self.advance();
        self.skip_trivia();
        let value = self.parse_binding_power(0)?;
        let span = Span::new(start, value.span().end());
        Ok(Expr::Binary(BinaryExpr {
            left: Box::new(Expr::Identifier(name)),
            operator: BinaryOperator::NamedArgument,
            right: Box::new(value),
            span,
        }))
    }

    fn parse_window_spec(&mut self) -> Result<WindowSpec, ParseError> {
        let start = self.current_span().start();
        if !self.current_is_symbol(Symbol::LParen) {
            return Err(self.unexpected("'(' after OVER"));
        }
        self.advance();
        self.skip_trivia();
        let mut existing_window_name = None;
        let mut partition_by = Vec::new();
        let mut order_by = Vec::new();
        let mut window_frame = None;
        if matches!(
            self.current().map(|token| &token.kind),
            Some(TokenKind::Ident | TokenKind::QuotedIdent)
        ) && !self.peek_keyword(1, Keyword::By)
        {
            existing_window_name = Some(self.parse_identifier(self.current_span()));
        }
        if self.current_is_keyword(Keyword::Partition) {
            self.advance();
            self.skip_trivia();
            if !self.current_is_keyword(Keyword::By) {
                return Err(self.unexpected("BY after PARTITION"));
            }
            self.advance();
            self.skip_trivia();
            loop {
                partition_by.push(self.parse_binding_power(0)?);
                self.skip_trivia();
                if !self.current_is_symbol(Symbol::Comma) {
                    break;
                }
                self.advance();
                self.skip_trivia();
            }
        }
        if self.current_is_keyword(Keyword::Order) {
            self.advance();
            self.skip_trivia();
            if !self.current_is_keyword(Keyword::By) {
                return Err(self.unexpected("BY after ORDER"));
            }
            self.advance();
            self.skip_trivia();
            loop {
                let expr = self.parse_binding_power(0)?;
                let asc = if self.current_is_keyword(Keyword::Asc) {
                    self.advance();
                    self.skip_trivia();
                    Some(true)
                } else if self.current_is_keyword(Keyword::Desc) {
                    self.advance();
                    self.skip_trivia();
                    Some(false)
                } else {
                    None
                };
                let nulls_first = if self.current_is_keyword(Keyword::Nulls) {
                    self.advance();
                    self.skip_trivia();
                    if self.current_is_keyword(Keyword::First) {
                        self.advance();
                        self.skip_trivia();
                        Some(true)
                    } else if self.current_is_keyword(Keyword::Last) {
                        self.advance();
                        self.skip_trivia();
                        Some(false)
                    } else {
                        return Err(self.unexpected("FIRST or LAST after NULLS"));
                    }
                } else {
                    None
                };
                let span = Span::new(expr.span().start(), self.current_span().start());
                order_by.push(crate::ast::OrderByExpr {
                    expr,
                    asc,
                    nulls_first,
                    span,
                });
                if !self.current_is_symbol(Symbol::Comma) {
                    break;
                }
                self.advance();
                self.skip_trivia();
            }
        }
        if let Some(units) = self.parse_window_frame_units() {
            let frame_start = self.current_span().start();
            let (start_bound, end_bound) = if self.current_is_keyword(Keyword::Between) {
                self.advance();
                self.skip_trivia();
                let start_bound = self.parse_window_frame_bound()?;
                if !self.current_is_keyword(Keyword::And) {
                    return Err(self.unexpected("AND in window frame"));
                }
                self.advance();
                self.skip_trivia();
                (start_bound, Some(self.parse_window_frame_bound()?))
            } else {
                (self.parse_window_frame_bound()?, None)
            };
            let frame_end = end_bound
                .as_ref()
                .map_or_else(|| start_bound.span().end(), |bound| bound.span().end());
            window_frame = Some(WindowFrame {
                units,
                start_bound,
                end_bound,
                exclusion: WindowFrameExclusion::NoOthers,
                span: Span::new(frame_start, frame_end),
            });
        }
        if !self.current_is_symbol(Symbol::RParen) {
            return Err(self.unexpected("')' after window specification"));
        }
        let end = self.current_span().end();
        self.advance();
        self.skip_trivia();
        Ok(WindowSpec {
            existing_window_name,
            partition_by,
            order_by,
            window_frame,
            span: Span::new(start, end),
        })
    }

    fn parse_window_frame_units(&mut self) -> Option<WindowFrameUnits> {
        let units = if self.current_is_keyword(Keyword::Rows) {
            WindowFrameUnits::Rows
        } else if self.current_is_keyword(Keyword::Range) {
            WindowFrameUnits::Range
        } else if self.current_is_keyword(Keyword::Groups) {
            WindowFrameUnits::Groups
        } else {
            return None;
        };
        self.advance();
        self.skip_trivia();
        Some(units)
    }

    fn parse_window_frame_bound(&mut self) -> Result<WindowFrameBound, ParseError> {
        let start = self.current_span().start();
        if self.current_is_keyword(Keyword::Current) {
            self.advance();
            self.skip_trivia();
            if !self.current_is_keyword(Keyword::Row) {
                return Err(self.unexpected("ROW after CURRENT"));
            }
            let end = self.current_span().end();
            self.advance();
            self.skip_trivia();
            return Ok(WindowFrameBound::CurrentRow(Span::new(start, end)));
        }
        if self.current_is_keyword(Keyword::Unbounded) {
            self.advance();
            self.skip_trivia();
            if self.current_is_keyword(Keyword::Preceding) {
                let end = self.current_span().end();
                self.advance();
                self.skip_trivia();
                return Ok(WindowFrameBound::Preceding(None, Span::new(start, end)));
            }
            if self.current_is_keyword(Keyword::Following) {
                let end = self.current_span().end();
                self.advance();
                self.skip_trivia();
                return Ok(WindowFrameBound::Following(None, Span::new(start, end)));
            }
            return Err(self.unexpected("PRECEDING or FOLLOWING after UNBOUNDED"));
        }
        let value = self.parse_binding_power(0)?;
        if self.current_is_keyword(Keyword::Preceding) {
            let end = self.current_span().end();
            self.advance();
            self.skip_trivia();
            return Ok(WindowFrameBound::Preceding(
                Some(value),
                Span::new(start, end),
            ));
        }
        if self.current_is_keyword(Keyword::Following) {
            let end = self.current_span().end();
            self.advance();
            self.skip_trivia();
            return Ok(WindowFrameBound::Following(
                Some(value),
                Span::new(start, end),
            ));
        }
        Err(self.unexpected("PRECEDING or FOLLOWING in window frame"))
    }

    /// `nested-expression ::= "(" expression ")" | "(" expression { "," expression } ")"`
    fn parse_nested_expression(&mut self, opening_span: Span) -> Result<Expr, ParseError> {
        self.advance();
        self.skip_trivia();
        let first = self.parse_binding_power(0)?;
        self.skip_trivia();
        if self.current_is_symbol(Symbol::Comma) {
            let mut expressions = vec![first];
            while self.current_is_symbol(Symbol::Comma) {
                self.advance();
                self.skip_trivia();
                expressions.push(self.parse_binding_power(0)?);
                self.skip_trivia();
            }
            if !self.current_is_symbol(Symbol::RParen) {
                return Err(self.unexpected("')' after tuple expression"));
            }
            let end = self.current_span().end();
            self.advance();
            return Ok(Expr::Row(RowExpr {
                fields: expressions
                    .into_iter()
                    .map(|expression| RowField {
                        span: expression.span(),
                        name: None,
                        expression,
                    })
                    .collect(),
                span: Span::new(opening_span.start(), end),
            }));
        }
        if !self.current_is_symbol(Symbol::RParen) {
            return Err(self.unexpected("')'"));
        }
        let end = self.current_span().end();
        self.advance();
        Ok(Expr::Nested(NestedExpr {
            expression: Box::new(first),
            span: Span::new(opening_span.start(), end),
        }))
    }

    fn parse_array_expression(&mut self, opening_span: Span) -> Result<Expr, ParseError> {
        self.advance();
        self.skip_trivia();
        let mut elements = Vec::new();
        if !self.current_is_symbol(Symbol::RBracket) {
            loop {
                elements.push(self.parse_binding_power(0)?);
                self.skip_trivia();
                if !self.current_is_symbol(Symbol::Comma) {
                    break;
                }
                self.advance();
                self.skip_trivia();
            }
        }
        if !self.current_is_symbol(Symbol::RBracket) {
            return Err(self.unexpected("']' after array expression"));
        }
        let end = self.current_span().end();
        self.advance();
        Ok(Expr::Array(ArrayExpr {
            values: elements.into(),
            span: Span::new(opening_span.start(), end),
        }))
    }

    fn parse_map_expression(&mut self, start: usize) -> Result<Expr, ParseError> {
        self.advance();
        self.skip_trivia();
        let mut entries = Vec::new();
        while !self.current_is_symbol(Symbol::RBrace) {
            let entry_start = self.current_span().start();
            let key = self.parse_binding_power(0)?;
            self.skip_trivia();
            if !self.current_is_symbol(Symbol::Colon) {
                return Err(self.unexpected("':' after map key"));
            }
            self.advance();
            self.skip_trivia();
            let value = self.parse_binding_power(0)?;
            let entry_end = value.span().end();
            entries.push(MapEntry {
                key,
                value,
                span: Span::new(entry_start, entry_end),
            });
            self.skip_trivia();
            if !self.current_is_symbol(Symbol::Comma) {
                break;
            }
            self.advance();
            self.skip_trivia();
        }
        if !self.current_is_symbol(Symbol::RBrace) {
            return Err(self.unexpected("'}' after map expression"));
        }
        let end = self.current_span().end();
        self.advance();
        Ok(Expr::Map(MapExpr {
            entries,
            span: Span::new(start, end),
        }))
    }

    fn consume_type_gt(&mut self) -> Result<usize, ParseError> {
        if let Some(span) = self.pending_type_gt.take() {
            return Ok(span.end());
        }
        if self.current_is_symbol(Symbol::Gt) {
            let end = self.current_span().end();
            self.advance();
            self.skip_trivia();
            return Ok(end);
        }
        if self.current_is_symbol(Symbol::ShiftRight) {
            let span = self.current_span();
            self.advance();
            self.skip_trivia();
            self.pending_type_gt = Some(Span::new(span.start() + 1, span.end()));
            return Ok(span.start() + 1);
        }
        Err(self.unexpected("'>' after type parameters"))
    }

    fn current(&self) -> Option<&Token> {
        self.tokens.get(self.position)
    }

    fn current_span(&self) -> Span {
        self.current()
            .map(|token| token.span)
            .unwrap_or_else(|| Span::new(self.source.len(), self.source.len()))
    }

    fn current_is_symbol(&self, symbol: Symbol) -> bool {
        matches!(self.current().map(|token| &token.kind), Some(TokenKind::Symbol(found)) if *found == symbol)
    }

    fn current_is_keyword(&self, keyword: Keyword) -> bool {
        matches!(self.current().map(|token| &token.kind), Some(TokenKind::Keyword(found)) if *found == keyword)
    }

    fn current_is_word(&self, word: &str) -> bool {
        self.current().is_some_and(|token| {
            matches!(token.kind, TokenKind::Ident | TokenKind::Keyword(_))
                && self.token_text(token.span).eq_ignore_ascii_case(word)
        })
    }

    fn consume_null_treatment(&mut self) -> Result<Option<NullTreatment>, ParseError> {
        let treatment = if self.current_is_keyword(Keyword::Ignore) {
            NullTreatment::IgnoreNulls
        } else if self.current_is_keyword(Keyword::Respect) {
            NullTreatment::RespectNulls
        } else {
            return Ok(None);
        };
        self.advance();
        self.skip_trivia();
        if !self.current_is_keyword(Keyword::Nulls) {
            return Err(self.unexpected("NULLS after null treatment"));
        }
        self.advance();
        self.skip_trivia();
        Ok(Some(treatment))
    }

    fn peek_keyword(&self, offset: usize, keyword: Keyword) -> bool {
        self.tokens
            .iter()
            .skip(self.position)
            .filter(|token| !matches!(token.kind, TokenKind::Trivia(_)))
            .nth(offset)
            .is_some_and(
                |token| matches!(token.kind, TokenKind::Keyword(found) if found == keyword),
            )
    }

    fn is_end(&self) -> bool {
        self.current()
            .is_none_or(|token| token.kind == TokenKind::End)
    }

    fn advance(&mut self) {
        if self.current().is_some() {
            self.position += 1;
        }
    }

    fn skip_trivia(&mut self) {
        while matches!(
            self.current().map(|token| &token.kind),
            Some(TokenKind::Trivia(_))
        ) {
            self.advance();
        }
    }

    fn unexpected(&self, expected: &'static str) -> ParseError {
        ParseError::UnexpectedToken {
            expected,
            found: self.current_description(),
            span: self.current_span(),
        }
    }

    fn current_description(&self) -> String {
        match self.current() {
            None
            | Some(Token {
                kind: TokenKind::End,
                ..
            }) => "EOF".to_owned(),
            Some(token) => format!("`{}`", self.token_text(token.span)),
        }
    }

    fn token_text(&self, span: Span) -> &str {
        self.source
            .get(span.start()..span.end())
            .unwrap_or("<invalid token span>")
    }

    fn quoted_identifier_value(&self, span: Span) -> String {
        let text = self.token_text(span);
        text.strip_prefix('`')
            .and_then(|text| text.strip_suffix('`'))
            .unwrap_or(text)
            .replace("``", "`")
    }

    fn string_value(&self, span: Span) -> String {
        let text = self.token_text(span);
        let Some(quote) = text.chars().next() else {
            return String::new();
        };
        let body = text
            .strip_prefix(quote)
            .and_then(|text| text.strip_suffix(quote))
            .unwrap_or(text);
        unescape_string(body, quote)
    }

    fn hex_string_value(&self, span: Span) -> Option<String> {
        let text = self.token_text(span);
        if let Some(value) = text.strip_prefix("0x").or_else(|| text.strip_prefix("0X")) {
            return Some(value.to_owned());
        }
        let prefix = text.chars().next()?;
        if !matches!(prefix, 'x' | 'X') {
            return None;
        }
        let quoted = &text[prefix.len_utf8()..];
        let quote = quoted.chars().next()?;
        if !matches!(quote, '\'' | '"') {
            return None;
        }
        quoted
            .strip_prefix(quote)
            .and_then(|value| value.strip_suffix(quote))
            .map(str::to_owned)
    }
}

/// Parses exactly one parenthesized window specification from a query-clause
/// token slice. Named `WINDOW name AS (...)` definitions and function `OVER
/// (...)` deliberately share the same syntax production.
pub(super) fn parse_window_spec(source: &str, tokens: &[Token]) -> Result<WindowSpec, ParseError> {
    let mut parser = PrattParser::new(source, tokens);
    let specification = parser.parse_window_spec()?;
    parser.skip_trivia();
    if parser.is_end() {
        Ok(specification)
    } else {
        Err(parser.unexpected("end of window specification"))
    }
}

/// Parses a table-function expression, allowing named arguments only in the
/// outer table-function call. Nested expressions keep the ordinary grammar.
pub(super) fn parse_table_function_expression(
    source: &str,
    tokens: &[Token],
) -> Result<Expr, ParseError> {
    PrattParser::new(source, tokens)
        .with_table_function_named_arguments()
        .parse()
}

fn unescape_string(text: &str, quote: char) -> String {
    let mut value = String::with_capacity(text.len());
    let mut characters = text.chars();
    while let Some(character) = characters.next() {
        if character == '\\' {
            if let Some(escaped) = characters.next() {
                value.push(match escaped {
                    '0' => '\0',
                    'b' => '\u{0008}',
                    'n' => '\n',
                    'r' => '\r',
                    't' => '\t',
                    'v' => '\u{000b}',
                    'f' => '\u{000c}',
                    'Z' => '\u{001a}',
                    other => other,
                });
            } else {
                value.push(character);
            }
        } else if character == quote && characters.clone().next() == Some(quote) {
            value.push(quote);
            characters.next();
        } else {
            value.push(character);
        }
    }
    value
}

#[cfg(test)]
mod tests {
    use crate::{lex, parser::parse_expression, printer::Printer};

    #[test]
    fn user_and_system_variables_are_lexically_rejected() {
        for source in [
            "@user",
            "@@time_zone",
            "@@session.sql_mode",
            "@@global.sql_mode",
        ] {
            assert!(lex(source).is_err(), "{source}");
        }
    }

    #[test]
    fn map_brace_literal_is_a_primary_expression_and_round_trips_in_a_query() {
        let expression_source = "map{1:[2,3], NULL:map{4:5}}";
        let tokens = lex(expression_source).expect("map literal should lex");
        let expression =
            parse_expression(expression_source, &tokens).expect("map literal should parse");
        let canonical_expression = Printer::new().expression(&expression);
        assert_eq!(canonical_expression, "MAP{1: [2, 3], NULL: MAP{4: 5}}");

        let source = format!("SELECT {canonical_expression} AS value_map");
        let statements = crate::parse(&source).expect("map literal should parse in SELECT");
        let canonical_query = Printer::new().statements(&statements);
        assert_eq!(
            canonical_query,
            "SELECT MAP{1: [2, 3], NULL: MAP{4: 5}} AS value_map"
        );
        crate::parse(&canonical_query).expect("canonical map query should reparse");
    }

    #[test]
    fn substring_from_for_form_preserves_its_syntax_and_reparses() {
        for (source, canonical) in [
            (
                "SUBSTRING('STARROCKS' FROM 2 FOR 3)",
                "SUBSTRING('STARROCKS' FROM 2 FOR 3)",
            ),
            (
                "SUBSTRING('STARROCKS' FROM 5)",
                "SUBSTRING('STARROCKS' FROM 5)",
            ),
            (
                "SUBSTRING('STARROCKS', 2, 3)",
                "SUBSTRING('STARROCKS', 2, 3)",
            ),
        ] {
            let tokens = lex(source).expect("substring source should lex");
            let expression =
                parse_expression(source, &tokens).expect("substring source should parse");
            assert_eq!(
                Printer::new().expression(&expression),
                canonical,
                "{source}"
            );

            let query = format!("SELECT {canonical} AS fragment");
            let statements = crate::parse(&query).expect("substring query should parse");
            assert_eq!(Printer::new().statements(&statements), query, "{source}");
            crate::parse(&query).expect("canonical substring query should reparse");
        }
    }

    #[test]
    fn interval_primary_expression_round_trips_in_time_slice_and_array_generate() {
        let interval_units = [
            "year",
            "quarter",
            "month",
            "week",
            "day",
            "hour",
            "minute",
            "second",
            "millisecond",
            "microsecond",
        ];
        for unit in interval_units {
            let source =
                format!("SELECT time_slice('2023-10-31 23:59:59', interval 5 {unit}, ceil)");
            let statements = crate::parse(&source).expect("time_slice interval query should parse");
            let canonical = Printer::new().statements(&statements);
            assert_eq!(
                canonical,
                format!(
                    "SELECT time_slice('2023-10-31 23:59:59', INTERVAL 5 {}, ceil)",
                    unit.to_ascii_uppercase()
                )
            );
            let reparsed =
                crate::parse(&canonical).expect("canonical time_slice query should reparse");
            assert_eq!(Printer::new().statements(&reparsed), canonical);
        }

        let source = "SELECT time_slice('2023-10-31 23:59:59', INTERVAL idx * 370 DAY)";
        let statements =
            crate::parse(source).expect("computed time_slice interval query should parse");
        let canonical = Printer::new().statements(&statements);
        assert_eq!(canonical, source);
        let reparsed =
            crate::parse(&canonical).expect("canonical computed time_slice query should reparse");
        assert_eq!(Printer::new().statements(&reparsed), canonical);

        let source = "SELECT array_generate(DATE '2024-01-05', DATE '2024-01-01', INTERVAL -1 DAY)";
        let statements = crate::parse(source).expect("array_generate interval query should parse");
        let canonical = Printer::new().statements(&statements);
        assert_eq!(canonical, source);
        let reparsed =
            crate::parse(&canonical).expect("canonical array_generate query should reparse");
        assert_eq!(Printer::new().statements(&reparsed), canonical);
    }

    #[test]
    fn typed_datetime_strings_and_mysql_escapes_round_trip() {
        let source = "SELECT array_generate(DATETIME\"2025-10-01\", DATETIME\"2025-10-05\", INTERVAL 1 DAY), '\\n\\t\\v\\f\\r'";
        let statements = crate::parse(source).expect("typed DATETIME and escaped strings parse");
        let canonical = Printer::new().statements(&statements);
        assert_eq!(
            canonical,
            "SELECT array_generate(TIMESTAMP '2025-10-01', TIMESTAMP '2025-10-05', INTERVAL 1 DAY), '\\n\\t\\v\\f\\r'"
        );
        let reparsed = crate::parse(&canonical).expect("canonical SQL should reparse");
        assert_eq!(Printer::new().statements(&reparsed), canonical);
    }

    #[test]
    fn fat_arrow_is_rejected_in_ordinary_expressions() {
        for source in ["s.s_1 => t.i_1", "argument => 5"] {
            let tokens = lex(source).expect("fat-arrow source should lex");
            let error = parse_expression(source, &tokens)
                .expect_err("fat arrow must not parse as an expression operator");
            assert!(matches!(
                error,
                crate::ParseError::UnexpectedToken {
                    expected: "syntax error",
                    ..
                }
            ));
        }
    }
}

fn predicate_node(value: Expr, predicate: Predicate) -> Expr {
    let span = value.span().cover(predicate.span());
    Expr::Predicated(AstBox::new(PredicatedExpr {
        value,
        predicate,
        span,
    }))
}
