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
    DistinctFromPredicate, InPredicate, IsNullPredicate, JsonPathInvocation, JsonPathString,
    JsonQuery, LikePredicate, LogicalExpr, LogicalOperator, Predicate, PredicatedExpr, TruthValue,
    TypeKind, TypeParameter,
};

use crate::{
    Span, Symbol, Token, TokenKind,
    ast::{
        ArrayExpr, AstVec, BinaryExpr, BinaryOperator, CastExpr, DataTypeParameter,
        DereferenceExpr, ExistsExpr, Expr, FunctionCall, Ident, InListExpr, IntervalField,
        LambdaArgumentDeclaration, LambdaExpr, LegacyIntervalExpr, LegacyParenthesizedExpr,
        Literal, LiteralKind, NullOrdering, NullTreatment, ObjectName, OrderBy, RowDataTypeField,
        RowExpr, RowField, SearchedCaseExpr, SimpleCaseExpr, SortItem, SortOrdering, SubqueryExpr,
        TypeName, UnaryExpr, UnaryOperator, WhenClause, WhenMatch, Window, WindowFrame,
        WindowFrameBound, WindowFrameExclusion, WindowFrameUnits, WindowReference,
        WindowSpecification,
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
        operator: InfixOperator::Binary(BinaryOperator::Legacy(
            crate::ast::legacy::BinaryOperator::StringConcat,
        )),
        precedence: ADDITIVE_PRECEDENCE,
    },
    InfixBinding {
        token: TokenPattern::Symbol(Symbol::ShiftLeft),
        operator: InfixOperator::Binary(BinaryOperator::Legacy(
            crate::ast::legacy::BinaryOperator::ShiftLeft,
        )),
        precedence: ADDITIVE_PRECEDENCE,
    },
    InfixBinding {
        token: TokenPattern::Symbol(Symbol::ShiftRight),
        operator: InfixOperator::Binary(BinaryOperator::Legacy(
            crate::ast::legacy::BinaryOperator::ShiftRight,
        )),
        precedence: ADDITIVE_PRECEDENCE,
    },
    InfixBinding {
        token: TokenPattern::Symbol(Symbol::Ampersand),
        operator: InfixOperator::Binary(BinaryOperator::Legacy(
            crate::ast::legacy::BinaryOperator::BitwiseAnd,
        )),
        precedence: ADDITIVE_PRECEDENCE,
    },
    InfixBinding {
        token: TokenPattern::Symbol(Symbol::Caret),
        operator: InfixOperator::Binary(BinaryOperator::Legacy(
            crate::ast::legacy::BinaryOperator::BitwiseXor,
        )),
        precedence: ADDITIVE_PRECEDENCE,
    },
    InfixBinding {
        token: TokenPattern::Symbol(Symbol::Pipe),
        operator: InfixOperator::Binary(BinaryOperator::Legacy(
            crate::ast::legacy::BinaryOperator::BitwiseOr,
        )),
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
        operator: UnaryOperator::Legacy(crate::ast::legacy::UnaryOperator::BitwiseNot),
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

/// One direct parser result and two explicitly authored source ranges.
#[derive(Debug)]
pub(super) struct ParsedExpression {
    pub(super) expression: Expr,
    pub(super) syntax_span: Span,
    pub(super) legacy_metadata_span: Span,
    pending_parentheses: Vec<Span>,
}
impl ParsedExpression {
    fn authored(expression: Expr, syntax_span: Span, legacy_metadata_span: Span) -> Self {
        Self {
            expression,
            syntax_span,
            legacy_metadata_span,
            pending_parentheses: Vec::new(),
        }
    }

    /// Materialize only this uncommitted root's real delimiter pairs.
    fn keep_parentheses(mut self) -> Self {
        for span in std::mem::take(&mut self.pending_parentheses) {
            self.expression = Expr::LegacyParenthesized(LegacyParenthesizedExpr {
                expression: AstBox::new(self.expression),
                span,
            });
        }
        self
    }

    /// The actual arithmetic or logical parent consumes this computed root.
    fn commit_computed_operand(mut self) -> Self {
        let computed = match &self.expression {
            Expr::Logical(_) => true,
            Expr::Binary(binary) => matches!(
                binary.operator,
                BinaryOperator::Add
                    | BinaryOperator::Subtract
                    | BinaryOperator::Multiply
                    | BinaryOperator::Divide
                    | BinaryOperator::Modulo
            ),
            _ => false,
        };
        if computed {
            self.pending_parentheses.clear();
            self
        } else {
            self.keep_parentheses()
        }
    }

    fn into_call_argument(self) -> crate::ast::CallArgument {
        let retained = self.keep_parentheses();
        crate::ast::CallArgument {
            name: None,
            value: retained.expression,
            span: retained.legacy_metadata_span,
        }
    }
}

pub(super) struct PrattParser<'source, 'tokens> {
    source: &'source str,
    tokens: &'tokens [Token],
    boundary_span: Span,
    position: usize,
    last_syntax_end: usize,
    pending_type_gt: Option<Span>,
    allow_table_function_named_arguments: bool,
    function_call_depth: usize,
}

impl<'source, 'tokens> PrattParser<'source, 'tokens> {
    pub(super) fn new(source: &'source str, tokens: &'tokens [Token]) -> Self {
        let boundary_span = tokens
            .last()
            .filter(|token| token.kind == TokenKind::End)
            .map_or(Span::new(source.len(), source.len()), |token| token.span);
        Self::bounded(source, tokens, boundary_span)
    }

    /// Borrows only the owning caller's token range. The excluded original
    /// boundary supplies EOF diagnostics without inserting another token.
    pub(super) fn bounded(
        source: &'source str,
        tokens: &'tokens [Token],
        boundary_span: Span,
    ) -> Self {
        Self {
            source,
            tokens,
            boundary_span,
            position: 0,
            last_syntax_end: boundary_span.start(),
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
    pub(super) fn parse(self) -> Result<Expr, ParseError> {
        self.parse_spanned().map(|parsed| parsed.expression)
    }

    pub(super) fn parse_spanned(mut self) -> Result<ParsedExpression, ParseError> {
        let expression = self.parse_binding_power_frame(0)?;
        self.skip_trivia();
        if self.is_end() {
            Ok(expression)
        } else if self.current_is_symbol(Symbol::FatArrow) {
            Err(self.unexpected("syntax error"))
        } else {
            Err(self.unexpected("end of expression"))
        }
    }

    /// Consume a value when its caller does not author another source range.
    fn parse_binding_power(&mut self, minimum_precedence: u8) -> Result<Expr, ParseError> {
        self.parse_binding_power_frame(minimum_precedence)
            .map(|parsed| parsed.expression)
    }

    /// Unknown callers retain the original root shape before taking its value.
    fn parse_binding_power_frame(
        &mut self,
        minimum_precedence: u8,
    ) -> Result<ParsedExpression, ParseError> {
        self.parse_deferred_binding_power(minimum_precedence)
            .map(ParsedExpression::keep_parentheses)
    }

    /// Keep grammar ranges local to the invocation that authored them.
    #[recursive::recursive]
    fn parse_deferred_binding_power(
        &mut self,
        minimum_precedence: u8,
    ) -> Result<ParsedExpression, ParseError> {
        self.skip_trivia();
        let prefix_start = self.position;
        let mut left = self.parse_prefix_expression()?;
        let mut extendable_logical = None;
        loop {
            self.skip_trivia();
            if self.current_is_symbol(Symbol::Arrow) {
                left = left.keep_parentheses();
            }
            if self.current_is_symbol(Symbol::Arrow)
                && self.arrow_starts_lambda(&left.expression, prefix_start)
            {
                let parameters = Self::lambda_parameters(&left.expression)
                    .expect("arrow_starts_lambda only accepts identifier parameters");
                self.advance();
                self.skip_trivia();
                let body = self.parse_binding_power_frame(0)?;
                let syntax_span = left.syntax_span.cover(body.syntax_span);
                let span = left.legacy_metadata_span.cover(body.legacy_metadata_span);
                left = ParsedExpression::authored(
                    Expr::Lambda(AstBox::new(LambdaExpr {
                        arguments: parameters
                            .into_iter()
                            .map(|name| LambdaArgumentDeclaration {
                                span: name.span,
                                name,
                            })
                            .collect(),
                        body: body.expression,
                        span,
                    })),
                    syntax_span,
                    span,
                );
                extendable_logical = None;
                continue;
            }
            if self.current_is_symbol(Symbol::LBracket)
                || self.current_is_symbol(Symbol::Arrow)
                || self.current_is_symbol(Symbol::LongArrow)
                || self.current_is_symbol(Symbol::Dot)
            {
                left = self.parse_postfix_access(left)?;
                extendable_logical = None;
                continue;
            }
            if minimum_precedence <= COMPARISON_PRECEDENCE && self.starts_comparison_special() {
                left = self.parse_comparison_special(left)?;
                extendable_logical = None;
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
            let right = self.parse_deferred_binding_power(binding.precedence + 1)?;
            let syntax_span = left.syntax_span.cover(right.syntax_span);
            let span = left.legacy_metadata_span.cover(right.legacy_metadata_span);
            let next_logical = match binding.operator {
                InfixOperator::Logical(operator) => Some(operator),
                _ => None,
            };
            let left_operand_span = left.legacy_metadata_span;
            let computed_parent = matches!(binding.operator, InfixOperator::Logical(_))
                || matches!(
                    binding.operator,
                    InfixOperator::Binary(
                        BinaryOperator::Add
                            | BinaryOperator::Subtract
                            | BinaryOperator::Multiply
                            | BinaryOperator::Divide
                            | BinaryOperator::Modulo
                    )
                );
            left = if computed_parent {
                left.commit_computed_operand()
            } else {
                left.keep_parentheses()
            };
            let right = if computed_parent {
                right.commit_computed_operand()
            } else {
                right.keep_parentheses()
            };
            let expression = match binding.operator {
                InfixOperator::Comparison(operator) => {
                    Expr::Predicated(AstBox::new(PredicatedExpr {
                        value: left.expression,
                        predicate: Predicate::Comparison(ComparisonPredicate {
                            operator,
                            right: right.expression,
                            span: Span::new(predicate_start, right.legacy_metadata_span.end()),
                        }),
                        span,
                    }))
                }
                InfixOperator::NullSafeEqual => Expr::Predicated(AstBox::new(PredicatedExpr {
                    value: left.expression,
                    predicate: Predicate::DistinctFrom(DistinctFromPredicate {
                        negated: true,
                        right: right.expression,
                        span: Span::new(predicate_start, right.legacy_metadata_span.end()),
                    }),
                    span,
                })),
                InfixOperator::Binary(operator) => Expr::Binary(BinaryExpr {
                    left_operand_span,
                    left: AstBox::new(left.expression),
                    operator,
                    right: AstBox::new(right.expression),
                    span,
                }),
                InfixOperator::Logical(operator) => {
                    if extendable_logical == Some(operator) {
                        let Expr::Logical(mut logical) = left.expression else {
                            unreachable!("this frame authored its open logical chain")
                        };
                        logical.terms.push(right.expression);
                        logical.span = span;
                        Expr::Logical(logical)
                    } else {
                        Expr::Logical(LogicalExpr {
                            operator,
                            terms: vec![left.expression, right.expression].into(),
                            span,
                        })
                    }
                }
            };
            left = ParsedExpression::authored(expression, syntax_span, span);
            extendable_logical = next_logical;
        }
        Ok(left)
    }

    fn parse_postfix_access(
        &mut self,
        base: ParsedExpression,
    ) -> Result<ParsedExpression, ParseError> {
        let base = base.keep_parentheses();
        let start = base.legacy_metadata_span.start();
        let syntax_start = base.syntax_span.start();
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
            let span = Span::new(start, end);
            return Ok(ParsedExpression::authored(
                Expr::Subscript(AstBox::new(crate::ast::SubscriptExpr {
                    base: base.expression,
                    index,
                    span,
                })),
                Span::new(syntax_start, end),
                span,
            ));
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
            let span = Span::new(start, field.span.end());
            let syntax_span = Span::new(syntax_start, field.span.end());
            return Ok(ParsedExpression::authored(
                Expr::Dereference(AstBox::new(DereferenceExpr {
                    span,
                    base: base.expression,
                    field: Some(field),
                })),
                syntax_span,
                span,
            ));
        }
        self.advance();
        self.skip_trivia();
        let path = self.parse_binding_power_frame(UNARY_ARITHMETIC_PRECEDENCE + 1)?;
        let span = Span::new(start, path.legacy_metadata_span.end());
        let syntax_span = Span::new(syntax_start, path.syntax_span.end());
        let path = match path {
            ParsedExpression {
                expression:
                    Expr::Literal(Literal {
                        kind: LiteralKind::String(value),
                        span: path_span,
                    }),
                ..
            } => {
                let invocation = JsonPathInvocation::plain(
                    base.expression,
                    JsonPathString::new(value, path_span),
                    span,
                );
                return Ok(ParsedExpression::authored(
                    Expr::JsonQuery(AstBox::new(JsonQuery::plain(invocation, span))),
                    syntax_span,
                    span,
                ));
            }
            path => path,
        };
        // Keep all other legacy path expressions in the ordinary scalar call.
        Ok(ParsedExpression::authored(
            Expr::FunctionCall(AstBox::new(FunctionCall {
                name: ObjectName {
                    parts: vec![Ident {
                        value: "json_query".to_owned(),
                        quoted: false,
                        span: crate::SYNTHETIC,
                    }],
                    span: crate::SYNTHETIC,
                },
                arguments: [base, path]
                    .into_iter()
                    .map(ParsedExpression::into_call_argument)
                    .collect(),
                distinct: false,
                order_by: None,
                filter: None,
                null_treatment: None,
                window: None,
                processing_mode: None,
                span,
            })),
            syntax_span,
            span,
        ))
    }

    fn arrow_starts_lambda(&self, left: &Expr, prefix_start: usize) -> bool {
        if Self::lambda_parameters(left).is_none() || self.peek_nontrivia_is_string(1) {
            return false;
        }
        // Constructor ROW(...) and parenthesized parameters share Row nodes.
        // The original tokens still distinguish calls from lambda parameters,
        // including a constructor enclosed by additional parentheses.
        let mut previous_is_name = false;
        for token in &self.tokens[prefix_start..self.position] {
            if matches!(token.kind, TokenKind::Trivia(_)) {
                continue;
            }
            if previous_is_name && matches!(token.kind, TokenKind::Symbol(Symbol::LParen)) {
                return false;
            }
            previous_is_name = matches!(
                token.kind,
                TokenKind::Ident | TokenKind::QuotedIdent | TokenKind::Keyword(_)
            );
        }
        true
    }

    #[recursive::recursive]
    fn lambda_parameters(expression: &Expr) -> Option<Vec<Ident>> {
        match expression {
            Expr::Identifier(ident) => Some(vec![ident.clone()]),
            Expr::LegacyParenthesized(nested) => Self::lambda_parameters(&nested.expression),
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

    fn position_special_form_follows(&self) -> bool {
        let mut depth = 0_usize;
        let mut separator = false;
        for token in self
            .tokens
            .iter()
            .skip(self.position)
            .filter(|token| !matches!(token.kind, TokenKind::Trivia(_)))
        {
            match token.kind {
                TokenKind::Symbol(Symbol::LParen | Symbol::LBracket | Symbol::LBrace) => depth += 1,
                TokenKind::Symbol(Symbol::RParen) if depth == 1 => return separator,
                TokenKind::Symbol(Symbol::RParen | Symbol::RBracket | Symbol::RBrace) => {
                    depth = depth.saturating_sub(1)
                }
                // Any top-level comma selects the ordinary function grammar,
                // including POSITION(predicate IN (...), another_argument).
                TokenKind::Symbol(Symbol::Comma) if depth == 1 => return false,
                TokenKind::Keyword(Keyword::In) if depth == 1 => separator = true,
                _ => {}
            }
        }
        separator
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

    fn parse_comparison_special(
        &mut self,
        left: ParsedExpression,
    ) -> Result<ParsedExpression, ParseError> {
        let left = left.keep_parentheses();
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
            let max = self.parse_binding_power_frame(COMPARISON_PRECEDENCE + 1)?;
            let span = Span::new(start, max.legacy_metadata_span.end());
            return Ok(self.predicate_node(
                left,
                Predicate::Between(BetweenPredicate {
                    negated,
                    symmetry: None,
                    min,
                    max: max.expression,
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
                return Ok(self.predicate_node(
                    left,
                    Predicate::In(InPredicate {
                        negated,
                        value_list: Expr::Subquery(SubqueryExpr {
                            query: query,
                            span: span,
                        }),
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
            return Ok(self.predicate_node(
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
            let pattern = self.parse_binding_power_frame(COMPARISON_PRECEDENCE + 1)?;
            let escape = if self.current_is_keyword(Keyword::Escape) {
                self.advance();
                self.skip_trivia();
                Some(self.parse_binding_power_frame(COMPARISON_PRECEDENCE + 1)?)
            } else {
                None
            };
            let end = escape.as_ref().map_or_else(
                || pattern.legacy_metadata_span.end(),
                |v| v.legacy_metadata_span.end(),
            );
            return Ok(self.predicate_node(
                left,
                Predicate::Like(LikePredicate {
                    negated,
                    pattern: pattern.expression,
                    escape: escape.map(|value| value.expression),
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
                let right = self.parse_binding_power_frame(COMPARISON_PRECEDENCE + 1)?;
                let span = Span::new(start, right.legacy_metadata_span.end());
                return Ok(self.predicate_node(
                    left,
                    Predicate::DistinctFrom(DistinctFromPredicate {
                        negated,
                        right: right.expression,
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
            return Ok(self.predicate_node(left, predicate));
        }
        Err(self.unexpected("comparison special form"))
    }

    /// `prefix-expression ::= unary-operator expression | primary-expression`
    fn parse_prefix_expression(&mut self) -> Result<ParsedExpression, ParseError> {
        self.skip_trivia();
        let begin = self.position;
        if self.current_is_keyword(Keyword::Not) && self.peek_keyword(1, Keyword::Exists) {
            let start = self.current_span().start();
            self.advance();
            self.skip_trivia();
            let expression = self.parse_exists_expression(start, true)?;
            return Ok(self.full_expression(begin, expression));
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
                return Ok(self.full_expression(
                    begin,
                    Expr::Literal(Literal {
                        kind,
                        span: literal_span,
                    }),
                ));
            }
            let expression = self.parse_deferred_binding_power(binding.precedence)?;
            let expression = match binding.operator {
                UnaryOperator::Plus | UnaryOperator::Minus => expression.commit_computed_operand(),
                _ => expression.keep_parentheses(),
            };
            let span = Span::new(start, expression.legacy_metadata_span.end());
            return Ok(ParsedExpression::authored(
                Expr::Unary(UnaryExpr {
                    operator: binding.operator,
                    expression: crate::ast::AstBox::new(expression.expression),
                    span: span,
                }),
                self.syntax_extent(begin),
                span,
            ));
        }

        self.parse_primary_expression()
    }

    /// `primary-expression ::= literal | identifier [ function-call ] | "(" expression ")"`
    fn parse_primary_expression(&mut self) -> Result<ParsedExpression, ParseError> {
        self.skip_trivia();
        let begin = self.position;
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
            let span = Span::new(start, self.consumed_syntax_end());
            // The source type belongs to the outer CAST. ARRAY inference sees
            // the original children without a target context.
            return Ok(ParsedExpression::authored(
                Expr::Cast(AstBox::new(CastExpr {
                    expression,
                    data_type: array_type,
                    safe: false,
                    span,
                })),
                self.syntax_extent(begin),
                span,
            ));
        }
        if self.current_is_word("MAP") && self.peek_nontrivia_is_symbol(1, Symbol::LBrace) {
            let start = self.current_span().start();
            self.advance();
            self.skip_trivia();
            let expression = self.parse_map_expression(start)?;
            return Ok(self.full_expression(begin, expression));
        }
        if self.current_is_word("ROW") && self.peek_nontrivia_is_symbol(1, Symbol::LParen) {
            let expression = self.parse_row_expression()?;
            return Ok(self.full_expression(begin, expression));
        }
        if self.current_is_word("POSITION")
            && self.peek_nontrivia_is_symbol(1, Symbol::LParen)
            && self.position_special_form_follows()
        {
            let expression = self.parse_position_expression()?;
            return Ok(self.full_expression(begin, expression));
        }
        if self.current_is_word("NORMALIZE") && self.peek_nontrivia_is_symbol(1, Symbol::LParen) {
            let expression = self.parse_normalize_expression()?;
            return Ok(self.full_expression(begin, expression));
        }
        if self.current_is_word("SUBSTRING") && self.substring_special_form_follows() {
            let expression = self.parse_substring_expression()?;
            return Ok(self.full_expression(begin, expression));
        }
        if self.current_is_word("EXTRACT") && self.peek_nontrivia_is_symbol(1, Symbol::LParen) {
            let expression = self.parse_extract_expression()?;
            return Ok(self.full_expression(begin, expression));
        }
        if (self.current_is_word("DATETIME")
            || self.current_is_word("JSON")
            || self.current_is_word("DECIMAL"))
            && self.peek_nontrivia_is_string(1)
        {
            let expression = self.parse_typed_string_expression()?;
            return Ok(self.full_expression(begin, expression));
        }
        let token = self.current().cloned();
        let expression = match token {
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
            }) => return self.parse_identifier_or_function_call(span),
            Some(Token {
                kind: TokenKind::Keyword(keyword),
                span,
            }) if keyword_class(keyword) == crate::KeywordClass::NonReserved => {
                return self.parse_identifier_or_function_call(span);
            }
            Some(Token {
                kind: TokenKind::Symbol(Symbol::LParen),
                span,
            }) if self.parenthesized_query_follows() => self.parse_scalar_subquery(span),
            Some(Token {
                kind: TokenKind::Symbol(Symbol::LParen),
                span,
            }) => return self.parse_nested_expression(span),
            Some(Token {
                kind: TokenKind::Symbol(Symbol::LBracket),
                span,
            }) => self.parse_array_expression(span),
            _ => Err(self.unexpected("expression")),
        }?;
        Ok(self.full_expression(begin, expression))
    }

    fn parse_interval_expression(&mut self) -> Result<Expr, ParseError> {
        let start = self.current_span().start();
        self.advance();
        self.skip_trivia();
        let value = self.parse_binding_power(0)?;
        self.skip_trivia();
        let (leading_field, field_span) = self.parse_interval_field()?;
        Ok(Expr::LegacyInterval(LegacyIntervalExpr {
            value: crate::ast::AstBox::new(value),
            leading_field: leading_field,
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
            IntervalField::Legacy(crate::ast::legacy::IntervalField::Quarter)
        } else if self.current_is_word("MONTH") {
            IntervalField::Month
        } else if self.current_is_word("WEEK") {
            IntervalField::Legacy(crate::ast::legacy::IntervalField::Week)
        } else if self.current_is_word("DAY") {
            IntervalField::Day
        } else if self.current_is_word("HOUR") {
            IntervalField::Hour
        } else if self.current_is_word("MINUTE") {
            IntervalField::Minute
        } else if self.current_is_word("SECOND") {
            IntervalField::Second
        } else if self.current_is_word("MILLISECOND") {
            IntervalField::Legacy(crate::ast::legacy::IntervalField::Millisecond)
        } else if self.current_is_word("MICROSECOND") {
            IntervalField::Legacy(crate::ast::legacy::IntervalField::Microsecond)
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
                query: query,
                span: subquery_span,
            }),
            span,
        }));
        Ok(if negated {
            Expr::Unary(UnaryExpr {
                operator: UnaryOperator::Not,
                expression: crate::ast::AstBox::new(exists),
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
        Ok(Expr::Subquery(SubqueryExpr {
            query: query,
            span: span,
        }))
    }

    fn parse_subquery_body(
        &mut self,
        start: usize,
    ) -> Result<(AstBox<crate::ast::Query>, Span), ParseError> {
        let mut parser = StatementParser::bounded(
            self.source,
            &self.tokens[self.position..],
            self.boundary_span,
        );
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
        Ok((AstBox::new(query), Span::new(start, end)))
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
            let result = self.parse_binding_power_frame(0)?;
            let span = Span::new(when_start, result.legacy_metadata_span.end());
            when_clauses.push(WhenClause {
                match_value: WhenMatch::Operand(condition),
                result: result.expression,
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
        let expr = self
            .parse_deferred_binding_power(0)?
            .commit_computed_operand()
            .expression;
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
        let Some(field_token) = self.current() else {
            return Err(self.unexpected("EXTRACT field"));
        };
        if !matches!(
            field_token.kind,
            TokenKind::Ident | TokenKind::QuotedIdent | TokenKind::Keyword(_)
        ) {
            return Err(self.unexpected("EXTRACT field"));
        }
        // The pinned builder uppercases identifier.getText(), including raw
        // quoting. Do not decode a quoted field into an admitted enum value.
        let field = match self.token_text(field_span).to_uppercase().as_str() {
            "YEAR" => crate::ast::ExtractField::Year,
            "QUARTER" => crate::ast::ExtractField::Quarter,
            "MONTH" => crate::ast::ExtractField::Month,
            "WEEK" => crate::ast::ExtractField::Week,
            "DAY" => crate::ast::ExtractField::Day,
            "DAY_OF_MONTH" => crate::ast::ExtractField::DayOfMonth,
            "DAY_OF_WEEK" => crate::ast::ExtractField::DayOfWeek,
            "DOW" => crate::ast::ExtractField::Dow,
            "DAY_OF_YEAR" => crate::ast::ExtractField::DayOfYear,
            "DOY" => crate::ast::ExtractField::Doy,
            "YEAR_OF_WEEK" => crate::ast::ExtractField::YearOfWeek,
            "YOW" => crate::ast::ExtractField::Yow,
            "HOUR" => crate::ast::ExtractField::Hour,
            "MINUTE" => crate::ast::ExtractField::Minute,
            "SECOND" => crate::ast::ExtractField::Second,
            "TIMEZONE_MINUTE" => crate::ast::ExtractField::TimezoneMinute,
            "TIMEZONE_HOUR" => crate::ast::ExtractField::TimezoneHour,
            _ => return Err(self.unexpected("a supported EXTRACT field")),
        };
        self.advance();
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
        Ok(Expr::Extract(AstBox::new(crate::ast::ExtractExpr {
            expression,
            field,
            span: Span::new(start, end),
        })))
    }

    fn parse_position_expression(&mut self) -> Result<Expr, ParseError> {
        let start = self.current_span().start();
        self.advance();
        self.skip_trivia();
        if !self.current_is_symbol(Symbol::LParen) {
            return Err(self.unexpected("'(' after POSITION"));
        }
        self.advance();
        self.skip_trivia();
        // Preserve the original TABLE named-argument nesting boundary.
        self.function_call_depth += 1;
        let needle = self.parse_binding_power_frame(COMPARISON_PRECEDENCE + 1)?;
        self.skip_trivia();
        if !self.current_is_keyword(Keyword::In) {
            return Err(self.unexpected("IN in POSITION"));
        }
        self.advance();
        self.skip_trivia();
        let haystack = self.parse_binding_power_frame(COMPARISON_PRECEDENCE + 1)?;
        self.skip_trivia();
        if !self.current_is_symbol(Symbol::RParen) {
            return Err(self.unexpected("')' after POSITION"));
        }
        let end = self.current_span().end();
        self.advance();
        self.function_call_depth -= 1;
        self.skip_trivia();
        Ok(ordinary_special_call(
            "strpos",
            false,
            vec![haystack, needle],
            Span::new(start, end),
        ))
    }

    fn parse_normalize_expression(&mut self) -> Result<Expr, ParseError> {
        let start = self.current_span().start();
        self.advance();
        self.skip_trivia();
        if !self.current_is_symbol(Symbol::LParen) {
            return Err(self.unexpected("'(' after NORMALIZE"));
        }
        self.advance();
        self.skip_trivia();
        // Preserve the original TABLE named-argument nesting boundary.
        self.function_call_depth += 1;
        let value = self.parse_binding_power_frame(COMPARISON_PRECEDENCE + 1)?;
        self.skip_trivia();
        let form = if self.current_is_symbol(Symbol::Comma) {
            self.advance();
            self.skip_trivia();
            let raw = self.token_text(self.current_span());
            if !matches!(
                self.current().map(|token| &token.kind),
                Some(TokenKind::Ident | TokenKind::Keyword(_))
            ) || !["NFC", "NFD", "NFKC", "NFKD"]
                .iter()
                .any(|label| raw.eq_ignore_ascii_case(label))
            {
                return Err(self.unexpected("NFC, NFD, NFKC, or NFKD in NORMALIZE"));
            }
            // Pinned AstBuilder uses normalForm().getText(), not uppercase.
            let form = raw.to_owned();
            self.advance();
            self.skip_trivia();
            form
        } else {
            "NFC".to_owned()
        };
        if !self.current_is_symbol(Symbol::RParen) {
            return Err(self.unexpected("')' after NORMALIZE"));
        }
        let end = self.current_span().end();
        self.advance();
        self.function_call_depth -= 1;
        self.skip_trivia();
        let span = Span::new(start, end);
        // The generated literal's pinned location is the whole context's
        // start, for explicit and absent forms; there is no source string.
        let form = ParsedExpression::authored(
            Expr::Literal(Literal {
                kind: LiteralKind::String(form),
                span,
            }),
            span,
            span,
        );
        Ok(ordinary_special_call(
            "normalize",
            true,
            vec![value, form],
            span,
        ))
    }

    fn parse_substring_expression(&mut self) -> Result<Expr, ParseError> {
        let start = self.current_span().start();
        self.advance();
        self.skip_trivia();
        if !self.current_is_symbol(Symbol::LParen) {
            return Err(self.unexpected("'(' after SUBSTRING"));
        }
        self.advance();
        self.skip_trivia();
        let value = self.parse_binding_power_frame(0)?;
        self.skip_trivia();
        if !self.current_is_keyword(Keyword::From) {
            return Err(self.unexpected("FROM in SUBSTRING"));
        }
        self.advance();
        self.skip_trivia();
        let start_position = self.parse_binding_power_frame(0)?;
        self.skip_trivia();
        let length = if self.current_is_keyword(Keyword::For) {
            self.advance();
            self.skip_trivia();
            Some(self.parse_binding_power_frame(0)?)
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
        Ok(Expr::FunctionCall(AstBox::new(FunctionCall {
            name: ObjectName {
                parts: vec![Ident {
                    value: "substr".to_owned(),
                    quoted: false,
                    span: crate::SYNTHETIC,
                }],
                span: crate::SYNTHETIC,
            },
            arguments: arguments
                .into_iter()
                .map(ParsedExpression::into_call_argument)
                .collect(),
            processing_mode: None,
            distinct: false,
            order_by: None,
            filter: None,
            null_treatment: None,
            window: None,
            span: Span::new(start, end),
        })))
    }

    #[recursive::recursive]
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
    fn parse_identifier_or_function_call(
        &mut self,
        span: Span,
    ) -> Result<ParsedExpression, ParseError> {
        let begin = self.position;
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
                let span = Span::new(span.start(), field.span.end());
                expression = Expr::Dereference(AstBox::new(DereferenceExpr {
                    base: expression,
                    field: Some(field),
                    span,
                }));
            }
            return Ok(self.full_expression(begin, expression));
        }

        self.advance();
        let mut arguments = Vec::new();
        self.skip_trivia();
        let distinct = self.current_is_keyword(Keyword::Distinct);
        let has_quantifier = distinct || self.current_is_word("ALL");
        if has_quantifier {
            self.advance();
            self.skip_trivia();
        }
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
                    arguments.push(ParsedExpression::authored(
                        Expr::Identifier(Ident {
                            value: "*".to_owned(),
                            quoted: false,
                            span: star,
                        }),
                        star,
                        star,
                    ));
                } else if allow_named_arguments && self.table_function_named_argument_ahead() {
                    arguments.push(self.parse_table_function_named_argument()?);
                } else {
                    arguments.push(self.parse_binding_power_frame(0)?);
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

        let mut order_by = None;
        if self.current_is_keyword(Keyword::Order) {
            let order_start = self.current_span().start();
            let mut order_items = Vec::new();
            self.advance();
            self.skip_trivia();
            if !self.current_is_keyword(Keyword::By) {
                return Err(self.unexpected("BY after function ORDER"));
            }
            self.advance();
            self.skip_trivia();
            loop {
                let expr = self.parse_binding_power_frame(0)?;
                let mut item_end = expr.legacy_metadata_span.end();
                let asc = if self.current_is_keyword(Keyword::Asc) {
                    item_end = self.current_span().end();
                    self.advance();
                    self.skip_trivia();
                    Some(true)
                } else if self.current_is_keyword(Keyword::Desc) {
                    item_end = self.current_span().end();
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
                        item_end = self.current_span().end();
                        self.advance();
                        self.skip_trivia();
                        Some(true)
                    } else if self.current_is_keyword(Keyword::Last) {
                        item_end = self.current_span().end();
                        self.advance();
                        self.skip_trivia();
                        Some(false)
                    } else {
                        return Err(self.unexpected("FIRST or LAST after NULLS"));
                    }
                } else {
                    None
                };
                order_items.push(SortItem {
                    span: Span::new(expr.legacy_metadata_span.start(), item_end),
                    sort_key: expr.expression,
                    ordering: if asc == Some(false) {
                        SortOrdering::Descending
                    } else {
                        SortOrdering::Ascending
                    },
                    null_ordering: match nulls_first {
                        Some(true) => NullOrdering::First,
                        Some(false) => NullOrdering::Last,
                        None => NullOrdering::Undefined,
                    },
                });
                if !self.current_is_symbol(Symbol::Comma) {
                    break;
                }
                self.advance();
                self.skip_trivia();
            }
            let order_end = order_items
                .last()
                .expect("nonempty parsed ORDER BY")
                .span
                .end();
            order_by = Some(OrderBy {
                items: order_items.into(),
                span: Span::new(order_start, order_end),
            });
        }
        let separator = if self.current_is_word("SEPARATOR") {
            self.advance();
            self.skip_trivia();
            Some(self.parse_binding_power_frame(0)?)
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
            let filter = self.parse_binding_power_frame(0)?;
            self.skip_trivia();
            if !self.current_is_symbol(Symbol::RParen) {
                return Err(self.unexpected("')' after FILTER"));
            }
            self.advance();
            self.skip_trivia();
            Some(filter)
        } else {
            None
        };
        let window = if self.current_is_keyword(Keyword::Over) {
            let over_start = self.current_span().start();
            self.advance();
            self.skip_trivia();
            if matches!(
                self.current().map(|token| &token.kind),
                Some(TokenKind::Ident | TokenKind::QuotedIdent)
            ) {
                let name = self.parse_identifier(self.current_span());
                Some(Window::Reference(WindowReference {
                    span: Span::new(over_start, name.span.end()),
                    name,
                }))
            } else {
                Some(Window::Specification(AstBox::new(
                    self.parse_window_specification()?,
                )))
            }
        } else {
            None
        };
        let span_end = window.as_ref().map_or_else(
            || {
                filter
                    .as_ref()
                    .map_or_else(|| end, |value| value.legacy_metadata_span.end())
            },
            |window| window.span().end(),
        );
        self.function_call_depth -= 1;
        let call_span = Span::new(span.start(), span_end);
        // QualifiedName.toString() joins all decoded parts. Only a single
        // part can equal one of these undotted names; quoting does not exempt it.
        let special = match name.parts.as_slice() {
            [part] if part.value.eq_ignore_ascii_case("if") => Some("if"),
            [part] if part.value.eq_ignore_ascii_case("nullif") => Some("nullif"),
            [part] if part.value.eq_ignore_ascii_case("coalesce") => Some("coalesce"),
            [part] if part.value.eq_ignore_ascii_case("try") => Some("try"),
            [part] if part.value.eq_ignore_ascii_case("format") => Some("format"),
            _ => None,
        };
        if let Some(special) = special {
            let arity = arguments.len();
            let expected = match special {
                "if" if !matches!(arity, 2 | 3) => Some("two or three IF arguments"),
                "nullif" if arity != 2 => Some("two NULLIF arguments"),
                "coalesce" if arity < 2 => Some("at least two COALESCE arguments"),
                "try" if arity != 1 => Some("one TRY argument"),
                "format" if arity < 2 => Some("at least two FORMAT arguments"),
                _ => None,
            };
            if let Some(expected) = expected {
                return Err(ParseError::UnexpectedToken {
                    expected,
                    found: format!("{arity} arguments"),
                    span: call_span,
                });
            }
            let metadata_error = if window.is_some() { Some(("a control expression without OVER","OVER")) }
                else if distinct { Some(("a control expression without DISTINCT","DISTINCT")) }
                else if null_treatment.is_some() { Some(("a control expression without null treatment","NULL treatment")) }
                // The current grammar has no RUNNING/FINAL producer. P05
                // checks that mode at this same stage before constructing a node.
                else if filter.is_some() { Some(("a control expression without FILTER","FILTER")) }
                else if arguments.iter().any(|value| matches!(&value.expression,Expr::Binary(binary) if binary.operator==BinaryOperator::Legacy(crate::ast::legacy::BinaryOperator::NamedArgument))) { Some(("positional control-expression arguments","named argument")) }
                else if separator.is_some() { Some(("a control expression without SEPARATOR","SEPARATOR")) }
                else { None };
            if let Some((expected, found)) = metadata_error {
                return Err(ParseError::UnexpectedToken {
                    expected,
                    found: found.to_owned(),
                    span: call_span,
                });
            }
            // The pinned builder visits ORDER BY but does not retain it for
            // these five nodes; ALL likewise carries no distinct fact.
            let expression = match special {
                "if" => {
                    let mut values = arguments.into_iter().map(|value| value.expression);
                    Expr::If(AstBox::new(crate::ast::IfExpr {
                        condition: values.next().expect("checked IF arity"),
                        true_value: values.next().expect("checked IF arity"),
                        false_value: values.next(),
                        span: call_span,
                    }))
                }
                "nullif" => {
                    let mut values = arguments.into_iter().map(|value| value.expression);
                    Expr::NullIf(AstBox::new(crate::ast::NullIfExpr {
                        first: values.next().expect("checked NULLIF arity"),
                        second: values.next().expect("checked NULLIF arity"),
                        span: call_span,
                    }))
                }
                "coalesce" => Expr::Coalesce(AstBox::new(crate::ast::CoalesceExpr {
                    operands: arguments
                        .into_iter()
                        .map(|value| value.expression)
                        .collect(),
                    span: call_span,
                })),
                "try" => Expr::Try(AstBox::new(crate::ast::TryExpr {
                    inner: arguments
                        .into_iter()
                        .next()
                        .expect("checked TRY arity")
                        .expression,
                    span: call_span,
                })),
                "format" => Expr::Format(AstBox::new(crate::ast::FormatExpr {
                    arguments: arguments
                        .into_iter()
                        .map(|value| value.expression)
                        .collect(),
                    span: call_span,
                })),
                _ => unreachable!("recognized control expression"),
            };
            return Ok(ParsedExpression::authored(
                expression,
                self.syntax_extent(begin),
                call_span,
            ));
        }
        // The standard operation accepts names. Keep the old function form
        // for computed inputs and any syntax whose call facts would be lost.
        if !self.allow_table_function_named_arguments
            && matches!(name.parts.as_slice(), [part]
                if !part.quoted && part.value.eq_ignore_ascii_case("grouping"))
            && !has_quantifier
            && order_by.is_none()
            && separator.is_none()
            && filter.is_none()
            && null_treatment.is_none()
            && window.is_none()
            && !arguments.is_empty()
            && arguments.iter().all(|argument| {
                matches!(&argument.expression, Expr::Identifier(part)
                    if crate::ast::GroupingOperation::legacy_identifier_is_canonical(part))
            })
        {
            return Ok(ParsedExpression::authored(
                Expr::GroupingOperation(AstBox::new(crate::ast::GroupingOperation {
                    grouping_columns: arguments
                        .into_iter()
                        .map(|argument| argument.expression)
                        .collect(),
                    span: call_span,
                })),
                self.syntax_extent(begin),
                call_span,
            ));
        }
        // A plain positional JSON_OBJECT with complete pairs has the same
        // member facts as the standard constructor. Keep incomplete calls
        // and calls with modifiers in the legacy function binding path.
        if matches!(name.parts.as_slice(), [part] if !part.quoted && part.value == "json_object")
            && !has_quantifier
            && order_by.is_none()
            && separator.is_none()
            && filter.is_none()
            && null_treatment.is_none()
            && window.is_none()
            && !arguments.is_empty()
            && arguments.len().is_multiple_of(2)
            && !arguments.iter().any(|argument| {
                matches!(&argument.expression, Expr::Binary(binary)
                    if binary.operator == BinaryOperator::Legacy(crate::ast::legacy::BinaryOperator::NamedArgument))
            })
        {
            let mut members = Vec::with_capacity(arguments.len() / 2);
            let mut arguments = arguments.into_iter();
            while let Some(key) = arguments.next() {
                let value = arguments.next().expect("checked complete JSON_OBJECT pairs");
                members.push(crate::ast::JsonObjectMember {
                    span: Span::new(key.syntax_span.start(), value.syntax_span.end()),
                    key: key.expression,
                    value: value.expression,
                    format: None,
                });
            }
            return Ok(ParsedExpression::authored(
                Expr::JsonObject(AstBox::new(crate::ast::JsonObject {
                    members,
                    null_on_null: true,
                    unique_keys: false,
                    returned_type: None,
                    output_format: None,
                    span: call_span,
                })),
                self.syntax_extent(begin),
                call_span,
            ));
        }
        // Erase the legacy clause only after its complete source shape is
        // admitted. Ordinary calls remain raw for SQL policy preparation.
        let name = if let Some(separator) = separator {
            if arguments.is_empty() {
                return Err(ParseError::UnexpectedToken {
                    expected: "at least one value before SEPARATOR",
                    found: "zero value arguments".to_owned(),
                    span: call_span,
                });
            }
            let group_concat = match name.parts.as_slice() {
                [part] if !part.quoted && part.value.eq_ignore_ascii_case("group_concat") => true,
                [part] if !part.quoted && part.value.eq_ignore_ascii_case("string_agg") => false,
                _ => {
                    return Err(ParseError::UnexpectedToken {
                        expected: "an unquoted GROUP_CONCAT or STRING_AGG call with SEPARATOR",
                        found: "SEPARATOR on another function".to_owned(),
                        span: call_span,
                    });
                }
            };
            arguments.push(separator);
            if group_concat {
                ObjectName {
                    parts: vec![Ident {
                        value: "string_agg".to_owned(),
                        quoted: false,
                        span: crate::SYNTHETIC,
                    }],
                    span: crate::SYNTHETIC,
                }
            } else {
                name
            }
        } else {
            name
        };
        Ok(ParsedExpression::authored(
            Expr::FunctionCall(AstBox::new(FunctionCall {
                name,
                arguments: arguments
                    .into_iter()
                    .map(ParsedExpression::into_call_argument)
                    .collect(),
                processing_mode: None,
                distinct,
                order_by,
                filter: filter.map(|value| value.expression),
                null_treatment,
                window,
                span: Span::new(span.start(), span_end),
            })),
            self.syntax_extent(begin),
            call_span,
        ))
    }

    fn table_function_named_argument_ahead(&self) -> bool {
        matches!(
            self.current().map(|token| &token.kind),
            Some(TokenKind::Ident | TokenKind::QuotedIdent | TokenKind::Keyword(_))
        ) && self.peek_nontrivia_is_symbol(1, Symbol::FatArrow)
    }

    fn parse_table_function_named_argument(&mut self) -> Result<ParsedExpression, ParseError> {
        let start = self.current_span().start();
        let name = self.parse_identifier(self.current_span());
        self.skip_trivia();
        if !self.current_is_symbol(Symbol::FatArrow) {
            return Err(self.unexpected("'=>' after table function argument name"));
        }
        self.advance();
        self.skip_trivia();
        let value = self.parse_binding_power_frame(0)?;
        let span = Span::new(start, value.legacy_metadata_span.end());
        Ok(ParsedExpression::authored(
            Expr::Binary(BinaryExpr {
                left_operand_span: name.span,
                left: crate::ast::AstBox::new(Expr::Identifier(name)),
                operator: BinaryOperator::Legacy(crate::ast::legacy::BinaryOperator::NamedArgument),
                right: crate::ast::AstBox::new(value.expression),
                span: span,
            }),
            Span::new(start, value.syntax_span.end()),
            span,
        ))
    }

    fn parse_window_specification(&mut self) -> Result<WindowSpecification, ParseError> {
        let start = self.current_span().start();
        if !self.current_is_symbol(Symbol::LParen) {
            return Err(self.unexpected("'(' after OVER"));
        }
        self.advance();
        self.skip_trivia();
        let mut existing_window_name = None;
        let mut partition_by = AstVec::new();
        let mut order_by = None;
        let mut window_frame = None;
        if matches!(
            self.current().map(|token| &token.kind),
            Some(TokenKind::Ident | TokenKind::QuotedIdent)
        ) && !self.peek_keyword(1, Keyword::By)
        {
            existing_window_name = Some(self.parse_identifier(self.current_span()));
            self.skip_trivia();
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
            let order_start = self.current_span().start();
            let mut order_items = Vec::new();
            self.advance();
            self.skip_trivia();
            if !self.current_is_keyword(Keyword::By) {
                return Err(self.unexpected("BY after ORDER"));
            }
            self.advance();
            self.skip_trivia();
            loop {
                let expr = self.parse_binding_power_frame(0)?;
                let mut item_end = expr.legacy_metadata_span.end();
                let asc = if self.current_is_keyword(Keyword::Asc) {
                    item_end = self.current_span().end();
                    self.advance();
                    self.skip_trivia();
                    Some(true)
                } else if self.current_is_keyword(Keyword::Desc) {
                    item_end = self.current_span().end();
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
                        item_end = self.current_span().end();
                        self.advance();
                        self.skip_trivia();
                        Some(true)
                    } else if self.current_is_keyword(Keyword::Last) {
                        item_end = self.current_span().end();
                        self.advance();
                        self.skip_trivia();
                        Some(false)
                    } else {
                        return Err(self.unexpected("FIRST or LAST after NULLS"));
                    }
                } else {
                    None
                };
                let span = Span::new(expr.legacy_metadata_span.start(), item_end);
                order_items.push(SortItem {
                    sort_key: expr.expression,
                    ordering: if asc == Some(false) {
                        SortOrdering::Descending
                    } else {
                        SortOrdering::Ascending
                    },
                    null_ordering: match nulls_first {
                        Some(true) => NullOrdering::First,
                        Some(false) => NullOrdering::Last,
                        None => NullOrdering::Undefined,
                    },
                    span,
                });
                if !self.current_is_symbol(Symbol::Comma) {
                    break;
                }
                self.advance();
                self.skip_trivia();
            }
            let order_end = order_items
                .last()
                .expect("nonempty parsed ORDER BY")
                .span
                .end();
            order_by = Some(OrderBy {
                items: order_items.into(),
                span: Span::new(order_start, order_end),
            });
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
                measures: AstVec::new(),
                after_match_skip_to: None,
                pattern_search_mode: None,
                pattern: None,
                subsets: AstVec::new(),
                variable_definitions: AstVec::new(),
                span: Span::new(frame_start, frame_end),
            });
        }
        if !self.current_is_symbol(Symbol::RParen) {
            return Err(self.unexpected("')' after window specification"));
        }
        let end = self.current_span().end();
        self.advance();
        self.skip_trivia();
        Ok(WindowSpecification {
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
    fn parse_nested_expression(
        &mut self,
        opening_span: Span,
    ) -> Result<ParsedExpression, ParseError> {
        self.advance();
        self.skip_trivia();
        let mut first = self.parse_deferred_binding_power(0)?;
        self.skip_trivia();
        if self.current_is_symbol(Symbol::Comma) {
            let mut expressions = vec![first.keep_parentheses()];
            while self.current_is_symbol(Symbol::Comma) {
                self.advance();
                self.skip_trivia();
                expressions.push(self.parse_binding_power_frame(0)?);
                self.skip_trivia();
            }
            if !self.current_is_symbol(Symbol::RParen) {
                return Err(self.unexpected("')' after tuple expression"));
            }
            let end = self.current_span().end();
            self.advance();
            let span = Span::new(opening_span.start(), end);
            return Ok(ParsedExpression::authored(
                Expr::Row(RowExpr {
                    fields: expressions
                        .into_iter()
                        .map(|expression| RowField {
                            span: expression.legacy_metadata_span,
                            name: None,
                            expression: expression.expression,
                        })
                        .collect(),
                    span,
                }),
                span,
                span,
            ));
        }
        if !self.current_is_symbol(Symbol::RParen) {
            return Err(self.unexpected("')'"));
        }
        let end = self.current_span().end();
        self.advance();
        let span = Span::new(opening_span.start(), end);
        first.pending_parentheses.push(span);
        first.syntax_span = span;
        first.legacy_metadata_span = span;
        Ok(first)
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

    /// ROW is a constructor token, not an ordinary function-name specialization.
    fn parse_row_expression(&mut self) -> Result<Expr, ParseError> {
        let start = self.current_span().start();
        self.advance();
        self.skip_trivia();
        if !self.current_is_symbol(Symbol::LParen) {
            return Err(self.unexpected("'(' after ROW"));
        }
        self.advance();
        self.skip_trivia();
        if self.current_is_symbol(Symbol::RParen)
            || self.current_is_keyword(Keyword::Distinct)
            || self.current_is_word("ALL")
        {
            return Err(self.unexpected("expression in ROW constructor"));
        }
        // Preserve the original TABLE named-argument nesting boundary.
        self.function_call_depth += 1;
        let mut fields = AstVec::new();
        loop {
            let expression = self.parse_binding_power_frame(0)?;
            fields.push(RowField {
                name: None,
                span: expression.legacy_metadata_span,
                expression: expression.expression,
            });
            self.skip_trivia();
            if !self.current_is_symbol(Symbol::Comma) {
                break;
            }
            self.advance();
            self.skip_trivia();
        }
        if !self.current_is_symbol(Symbol::RParen) {
            return Err(self.unexpected("')' after ROW fields"));
        }
        let end = self.current_span().end();
        self.advance();
        self.function_call_depth -= 1;
        Ok(Expr::Row(RowExpr {
            fields,
            span: Span::new(start, end),
        }))
    }

    fn parse_map_expression(&mut self, start: usize) -> Result<Expr, ParseError> {
        self.advance();
        self.skip_trivia();
        let mut arguments = AstVec::new();
        while !self.current_is_symbol(Symbol::RBrace) {
            let key = self.parse_binding_power_frame(0)?;
            self.skip_trivia();
            if !self.current_is_symbol(Symbol::Colon) {
                return Err(self.unexpected("':' after map key"));
            }
            self.advance();
            self.skip_trivia();
            let value = self.parse_binding_power_frame(0)?;
            arguments.push(key.into_call_argument());
            arguments.push(value.into_call_argument());
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
        Ok(Expr::FunctionCall(AstBox::new(FunctionCall {
            name: ObjectName {
                parts: vec![Ident {
                    value: "map".to_owned(),
                    quoted: false,
                    span: crate::SYNTHETIC,
                }],
                span: crate::SYNTHETIC,
            },
            arguments,
            processing_mode: None,
            distinct: false,
            order_by: None,
            filter: None,
            null_treatment: None,
            window: None,
            span: Span::new(start, end),
        })))
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

    fn predicate_node(&self, value: ParsedExpression, predicate: Predicate) -> ParsedExpression {
        let syntax_span = Span::new(value.syntax_span.start(), self.consumed_syntax_end());
        let span = value.legacy_metadata_span.cover(predicate.span());
        ParsedExpression::authored(
            Expr::Predicated(AstBox::new(PredicatedExpr {
                value: value.expression,
                predicate,
                span,
            })),
            syntax_span,
            span,
        )
    }

    fn consumed_syntax_end(&self) -> usize {
        self.last_syntax_end
    }
    fn syntax_extent(&self, begin: usize) -> Span {
        let start = self.tokens[begin..self.position]
            .iter()
            .find(|token| !matches!(token.kind, TokenKind::Trivia(_) | TokenKind::End))
            .expect("a successful expression consumed its first syntax token")
            .span
            .start();
        Span::new(start, self.consumed_syntax_end())
    }
    fn full_expression(&self, begin: usize, expression: Expr) -> ParsedExpression {
        let extent = self.syntax_extent(begin);
        ParsedExpression::authored(expression, extent, extent)
    }

    fn current(&self) -> Option<&Token> {
        self.tokens.get(self.position)
    }

    fn current_span(&self) -> Span {
        self.current()
            .map(|token| token.span)
            .unwrap_or(self.boundary_span)
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
        if let Some(token) = self.current() {
            if !matches!(token.kind, TokenKind::Trivia(_) | TokenKind::End) {
                self.last_syntax_end = token.span.end();
            }
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
pub(super) fn parse_window_specification(
    source: &str,
    tokens: &[Token],
    boundary_span: Span,
) -> Result<WindowSpecification, ParseError> {
    let mut parser = PrattParser::bounded(source, tokens, boundary_span);
    let specification = parser.parse_window_specification()?;
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
    boundary_span: Span,
) -> Result<ParsedExpression, ParseError> {
    PrattParser::bounded(source, tokens, boundary_span)
        .with_table_function_named_arguments()
        .parse_spanned()
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
    fn bounded_expressions_keep_original_boundary_spans_and_do_not_consume_outside_operands() {
        use crate::{ParseError, Span};
        for source in ["1 + 9 trailing", "(VALUES (1 + 9)) + 100", "9 trailing"] {
            let tokens = lex(source).unwrap();
            let start = source.find('9').unwrap();
            let span = Span::new(start, start + 1);
            let end = tokens.iter().position(|token| token.span == span).unwrap();
            let error = super::PrattParser::bounded(source, &tokens[..end], span)
                .parse()
                .unwrap_err();
            assert_eq!(
                error,
                ParseError::UnexpectedToken {
                    expected: "expression",
                    found: "EOF".to_owned(),
                    span
                }
            );
        }
        let source = "(PARTITION BY 1 + 9) outside";
        let tokens = lex(source).unwrap();
        let start = source.find('9').unwrap();
        let span = Span::new(start, start + 1);
        let end = tokens.iter().position(|token| token.span == span).unwrap();
        assert_eq!(
            super::parse_window_specification(source, &tokens[..end], span).unwrap_err(),
            ParseError::UnexpectedToken {
                expected: "expression",
                found: "EOF".to_owned(),
                span
            }
        );
        let source = "1 +";
        let tokens = lex(source).unwrap();
        assert_eq!(
            super::PrattParser::new(source, &tokens)
                .parse()
                .unwrap_err(),
            ParseError::UnexpectedToken {
                expected: "expression",
                found: "EOF".to_owned(),
                span: Span::new(source.len(), source.len())
            }
        );
        let source = "(VALUES (7)) + 100";
        let tokens = lex(source).unwrap();
        let start = source.find('+').unwrap();
        let boundary = Span::new(start, start + 1);
        let end = tokens
            .iter()
            .position(|token| token.span == boundary)
            .unwrap();
        let expression = super::PrattParser::bounded(source, &tokens[..end], boundary)
            .parse()
            .unwrap();
        let crate::ast::Expr::Subquery(node) = expression else {
            panic!("actual bounded scalar subquery")
        };
        assert_eq!(node.span, Span::new(0, source.find(" +").unwrap()));
        let crate::ast::SetExpr::Values(values) = node.query.body.as_ref() else {
            panic!("original VALUES")
        };
        let crate::ast::Expr::Literal(value) = crate::ast::ValuesRowCells::new(&values.rows[0])
            .get(0)
            .unwrap()
        else {
            panic!("literal")
        };
        assert!(matches!(
            value.kind,
            crate::ast::LiteralKind::Long {
                parsed_value: 7,
                ..
            }
        ));
        let start = source.find('7').unwrap();
        assert_eq!(value.span, Span::new(start, start + 1));
    }

    #[test]
    fn bounded_table_function_port_preserves_named_argument_scope_and_excluded_boundary() {
        use crate::ast::{BinaryOperator, Expr, LiteralKind};
        use crate::{ParseError, Span};
        let source = "f(end => 9), outside";
        let tokens = lex(source).unwrap();
        let start = source.find('9').unwrap();
        let missing = Span::new(start, start + 1);
        let end = tokens
            .iter()
            .position(|token| token.span == missing)
            .unwrap();
        assert_eq!(
            super::parse_table_function_expression(source, &tokens[..end], missing).unwrap_err(),
            ParseError::UnexpectedToken {
                expected: "expression",
                found: "EOF".to_owned(),
                span: missing
            }
        );
        let start = source.find(',').unwrap();
        let boundary = Span::new(start, start + 1);
        let end = tokens
            .iter()
            .position(|token| token.span == boundary)
            .unwrap();
        let expression = super::parse_table_function_expression(source, &tokens[..end], boundary)
            .unwrap()
            .expression;
        let Expr::FunctionCall(call) = expression else {
            panic!("original table call")
        };
        assert_eq!(call.name.parts[0].value, "f");
        assert_eq!(call.arguments.len(), 1);
        let Expr::Binary(named) = &call.arguments[0].value else {
            panic!("original named argument producer")
        };
        assert_eq!(
            named.operator,
            BinaryOperator::Legacy(crate::ast::legacy::BinaryOperator::NamedArgument)
        );
        assert!(matches!(named.left.as_ref(),Expr::Identifier(name)if name.value=="end"));
        assert!(
            matches!(named.right.as_ref(),Expr::Literal(value)if matches!(value.kind,LiteralKind::Long{parsed_value:9,..}))
        );
        assert_eq!(
            &source[call.arguments[0].span.start()..call.arguments[0].span.end()],
            "end => 9"
        );
    }

    #[test]
    fn deep_function_and_cast_type_sources_use_their_real_recursive_entries() {
        use crate::Span;
        use crate::ast::{DataTypeParameter, Expr, LiteralKind, TypeKind};
        const DEPTH: usize = 4_096;
        let source = format!("{}7{}", "f(".repeat(DEPTH), ")".repeat(DEPTH));
        let expression = parse_expression(&source, &lex(&source).unwrap()).unwrap();
        let mut cursor = &expression;
        for _ in 0..DEPTH {
            let Expr::FunctionCall(call) = cursor else {
                panic!("one actual call per source level")
            };
            assert_eq!(call.name.parts[0].value, "f");
            assert_eq!(call.arguments.len(), 1);
            assert!(call.arguments[0].name.is_none());
            cursor = &call.arguments[0].value;
        }
        assert!(
            matches!(cursor,Expr::Literal(value)if matches!(value.kind,LiteralKind::Long{parsed_value:7,..}))
        );
        let start = source.find('7').unwrap();
        assert_eq!(cursor.span(), Span::new(start, start + 1));
        drop(expression);
        let source = format!(
            "CAST(7 AS {}INT{})",
            "ARRAY<".repeat(DEPTH),
            ">".repeat(DEPTH)
        );
        let expression = parse_expression(&source, &lex(&source).unwrap()).unwrap();
        let Expr::Cast(cast) = &expression else {
            panic!("real CAST")
        };
        assert!(!cast.safe);
        assert!(
            matches!(&cast.expression,Expr::Literal(value)if matches!(value.kind,LiteralKind::Long{parsed_value:7,..}))
        );
        assert_eq!(
            cast.data_type.span,
            Span::new(source.find("ARRAY<").unwrap(), source.len() - 1)
        );
        let mut data_type = &cast.data_type;
        for _ in 0..DEPTH {
            let TypeKind::Generic(array) = &data_type.kind else {
                panic!("generic ARRAY")
            };
            assert_eq!(array.name.value, "ARRAY");
            assert_eq!(array.arguments.len(), 1);
            let DataTypeParameter::Type(parameter) = &array.arguments[0] else {
                panic!("actual type child")
            };
            data_type = &parameter.data_type;
        }
        assert!(
            matches!(&data_type.kind,TypeKind::Generic(integer)if integer.name.value=="INT" && integer.arguments.is_empty())
        );
        drop(expression);
    }

    #[test]
    fn row_constructor_arrow_and_table_arguments_keep_their_original_grammar() {
        use crate::ast::Expr;
        for source in ["ROW(a, b) -> path", "(ROW(a, b)) -> path"] {
            let tokens = lex(source).unwrap();
            let expression = super::PrattParser::new(source, &tokens).parse().unwrap();
            let Expr::FunctionCall(call) = expression else {
                panic!("ROW constructor must remain a JSON operation, not lambda: {source}");
            };
            assert_eq!(call.name.parts[0].value, "json_query");
            assert_eq!(call.arguments.len(), 2);
            assert!(matches!(
                &call.arguments[0].value,
                Expr::Row(_) | Expr::LegacyParenthesized(_)
            ));
            assert!(
                matches!(&call.arguments[1].value,Expr::Identifier(name) if name.value=="path")
            );
        }
        let source = "(a, b) -> a";
        let tokens = lex(source).unwrap();
        let expression = super::PrattParser::new(source, &tokens).parse().unwrap();
        let Expr::Lambda(lambda) = expression else {
            panic!("parenthesized parameters retain lambda grammar");
        };
        assert_eq!(
            lambda
                .arguments
                .iter()
                .map(|argument| argument.name.value.as_str())
                .collect::<Vec<_>>(),
            ["a", "b"]
        );
        assert!(matches!(&lambda.body, Expr::Identifier(name) if name.value == "a"));
        let source = "ROW(generate_series(end => 1), x)";
        let tokens = lex(source).unwrap();
        assert!(
            super::PrattParser::new(source, &tokens)
                .with_table_function_named_arguments()
                .parse()
                .is_err()
        );
        let source = "generate_series(end => 1)";
        let tokens = lex(source).unwrap();
        assert!(matches!(
            super::PrattParser::new(source, &tokens)
                .with_table_function_named_arguments()
                .parse()
                .unwrap(),
            Expr::FunctionCall(_)
        ));
    }

    #[test]
    fn ordinary_special_children_do_not_inherit_table_named_argument_privilege() {
        for source in [
            "POSITION(generate_series(end => 1) IN text_value)",
            "POSITION(needle IN generate_series(end => 1))",
            "NORMALIZE(generate_series(end => 1))",
            "POSITION(generate_series(end => 1), text_value)",
            "`POSITION`(generate_series(end => 1), text_value)",
            "ns.POSITION(generate_series(end => 1), text_value)",
            "`NORMALIZE`(generate_series(end => 1))",
            "ns.NORMALIZE(generate_series(end => 1))",
        ] {
            let tokens = lex(source).unwrap();
            assert!(
                super::PrattParser::new(source, &tokens)
                    .with_table_function_named_arguments()
                    .parse()
                    .is_err(),
                "{source}"
            );
        }
        let source = "generate_series(end => 1)";
        let tokens = lex(source).unwrap();
        let expression = super::PrattParser::new(source, &tokens)
            .with_table_function_named_arguments()
            .parse()
            .unwrap();
        assert!(matches!(expression, crate::ast::Expr::FunctionCall(_)));
    }

    #[test]
    fn plain_grouping_moves_ordered_column_occurrences_into_the_core_operation() {
        use crate::{
            Span,
            ast::{Expr, syntax_eq_expr},
        };

        let source = "GROUPING(a, b, a)";
        let old_expression = parse_expression(source, &lex(source).unwrap()).unwrap();
        let Expr::GroupingOperation(node) = &old_expression else {
            panic!("expected the standard GROUPING operation");
        };
        assert_eq!(node.span, Span::new(0, source.len()));
        assert_eq!(node.grouping_columns.len(), 3);
        for (argument, (name, start)) in
            node.grouping_columns
                .iter()
                .zip([("a", 9), ("b", 12), ("a", 15)])
        {
            assert!(matches!(argument, Expr::Identifier(ident)
                if ident.value == name && !ident.quoted && ident.span == Span::new(start, start + 1)));
        }
        let new_expression = crate::trino_parser::parse_expression(source).unwrap();
        assert!(syntax_eq_expr(&old_expression, &new_expression));
    }

    #[test]
    fn grouping_moves_only_identifiers_unchanged_by_canonical_construction() {
        use crate::{
            Span,
            ast::{Expr, syntax_eq_expr},
        };
        let legacy = "GROUPING(`sum(a)`)";
        let original = parse_expression(legacy, &lex(legacy).unwrap()).unwrap();
        let target = crate::trino_parser::parse_expression("GROUPING(\"sum(a)\")").unwrap();
        let Expr::GroupingOperation(node) = &original else {
            panic!("canonical quoted punctuation must keep the GROUPING operation");
        };
        assert!(
            matches!(node.grouping_columns.as_slice(), [Expr::Identifier(part)]
            if part.value == "sum(a)" && part.quoted && part.span == Span::new(9, 17))
        );
        assert!(syntax_eq_expr(&original, &target));

        let legacy = "GROUPING(`a`)";
        let original = parse_expression(legacy, &lex(legacy).unwrap()).unwrap();
        assert!(matches!(original, Expr::FunctionCall(_)));
        let Expr::GroupingOperation(canonical) =
            crate::trino_parser::parse_expression("GROUPING(\"a\")").unwrap()
        else {
            panic!("expected the pinned GROUPING operation");
        };
        assert!(
            matches!(canonical.grouping_columns.as_slice(), [Expr::Identifier(part)]
            if part.value == "a" && !part.quoted)
        );
    }

    #[test]
    fn grouping_extensions_and_table_function_grammar_keep_the_actual_call() {
        use crate::ast::Expr;

        for source in [
            "grouping(a + 1)",
            "grouping()",
            "grouping(ALL a)",
            "grouping(DISTINCT a)",
            "grouping(a ORDER BY b)",
            "grouping(a) FILTER (WHERE TRUE)",
            "grouping(a) OVER (PARTITION BY b)",
            "grouping(A)",
            "grouping(`a`)",
            "grouping(`Sum(a)`)",
            "grouping(t.a)",
            "grouping_id(a)",
            "`grouping`(a)",
            "ns.grouping(a)",
        ] {
            assert!(
                matches!(
                    parse_expression(source, &lex(source).unwrap()).unwrap(),
                    Expr::FunctionCall(_)
                ),
                "{source}"
            );
        }
        let source = "grouping(a)";
        let tokens = lex(source).unwrap();
        assert!(matches!(
            super::PrattParser::new(source, &tokens)
                .with_table_function_named_arguments()
                .parse()
                .unwrap(),
            Expr::FunctionCall(_)
        ));
    }

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
        assert_eq!(canonical_expression, "map(1, [2, 3], NULL, map(4, 5))");

        let source = format!("SELECT {canonical_expression} AS value_map");
        let statements = crate::parse(&source).expect("map literal should parse in SELECT");
        let canonical_query = Printer::new().statements(&statements);
        assert_eq!(
            canonical_query,
            "SELECT map(1, [2, 3], NULL, map(4, 5)) AS value_map"
        );
        crate::parse(&canonical_query).expect("canonical map query should reparse");
    }

    #[test]
    fn substring_from_for_form_normalizes_to_substr_and_reparses() {
        for (source, canonical) in [
            (
                "SUBSTRING('STARROCKS' FROM 2 FOR 3)",
                "substr('STARROCKS', 2, 3)",
            ),
            ("SUBSTRING('STARROCKS' FROM 5)", "substr('STARROCKS', 5)"),
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
    #[test]
    fn parsed_ranges_keep_filter_legacy_endpoints_inside_arguments_and_binary_operands() {
        use super::PrattParser;
        use crate::{Span, ast::Expr, lex};
        for source in [
            "agg(2) FILTER (WHERE (TRUE))",
            "1 + agg(2) FILTER (WHERE (TRUE))",
        ] {
            let tokens = lex(source).unwrap();
            let parsed = PrattParser::new(source, &tokens).parse_spanned().unwrap();
            let selected_end = source.find("(TRUE)").unwrap() + "(TRUE)".len();
            assert_eq!(parsed.syntax_span, Span::new(0, source.len()));
            assert_eq!(parsed.legacy_metadata_span, Span::new(0, selected_end));
            assert_eq!(parsed.expression.span(), Span::new(0, selected_end));
            let call = match &parsed.expression {
                Expr::FunctionCall(call) => call,
                Expr::Binary(binary) => {
                    assert_eq!(binary.left.span(), Span::new(0, 1));
                    let Expr::FunctionCall(call) = binary.right.as_ref() else {
                        panic!("the actual RHS is the filtered call")
                    };
                    call
                }
                _ => panic!("the source is one call or an addition"),
            };
            assert_eq!(
                call.span,
                Span::new(source.find("agg").unwrap(), selected_end)
            );
            assert_eq!(
                call.filter.as_ref().unwrap().span(),
                Span::new(source.find("(TRUE)").unwrap(), selected_end)
            );
        }
        let source = "f(agg(2) FILTER (WHERE (TRUE)))";
        let tokens = lex(source).unwrap();
        let parsed = PrattParser::new(source, &tokens).parse_spanned().unwrap();
        let Expr::FunctionCall(outer) = &parsed.expression else {
            panic!("outer call")
        };
        let selected_end = source.find("(TRUE)").unwrap() + "(TRUE)".len();
        assert_eq!(outer.span, Span::new(0, source.len()));
        assert_eq!(
            outer.arguments[0].span,
            Span::new(source.find("agg").unwrap(), selected_end)
        );
        let Expr::FunctionCall(inner) = &outer.arguments[0].value else {
            panic!("actual filtered argument")
        };
        assert_eq!(inner.span, outer.arguments[0].span);
        let source = "agg(2) IGNORE NULLS";
        let tokens = lex(source).unwrap();
        let parsed = PrattParser::new(source, &tokens).parse_spanned().unwrap();
        assert_eq!(parsed.syntax_span, Span::new(0, source.len()));
        assert_eq!(parsed.legacy_metadata_span, Span::new(0, "agg(2)".len()));
        assert_eq!(parsed.expression.span(), parsed.legacy_metadata_span);
        let source = "agg((2)) FILTER (WHERE (TRUE)) OVER (ORDER BY (3))";
        let tokens = lex(source).unwrap();
        let parsed = PrattParser::new(source, &tokens).parse_spanned().unwrap();
        assert_eq!(parsed.syntax_span, Span::new(0, source.len()));
        assert_eq!(parsed.legacy_metadata_span, parsed.syntax_span);
        let Expr::FunctionCall(call) = &parsed.expression else {
            panic!("window call")
        };
        assert_eq!(call.span, Span::new(0, source.len()));
        let value_start = source.find("(2)").unwrap();
        assert_eq!(
            call.arguments[0].span,
            Span::new(value_start, value_start + 3)
        );
        let crate::ast::Window::Specification(window) = call.window.as_ref().unwrap() else {
            panic!("OVER specification")
        };
        let key_start = source.find("(3)").unwrap();
        assert_eq!(
            window.order_by.as_ref().unwrap().items[0].span,
            Span::new(key_start, key_start + 3)
        );
    }

    #[test]
    fn parser_frames_keep_logical_grouping_and_unknown_parenthesis_nodes() {
        use crate::ast::{Expr, LogicalOperator};
        for (source, terms, grouped) in [
            ("a OR b OR c", 3, None),
            ("(a OR b) OR c", 2, Some(0)),
            ("a OR (b OR c)", 2, Some(1)),
        ] {
            let tree = parse_expression(source, &lex(source).unwrap()).unwrap();
            let Expr::Logical(logical) = &tree else {
                panic!("logical source")
            };
            assert_eq!(logical.operator, LogicalOperator::Or);
            assert_eq!(logical.terms.len(), terms);
            if let Some(index) = grouped {
                let Expr::Logical(inner) = &logical.terms[index] else {
                    panic!("ordered inner logical chain")
                };
                assert_eq!(inner.operator, LogicalOperator::Or);
                assert_eq!(inner.terms.len(), 2);
            }
        }
        let source = "((1 + 2))";
        let tree = parse_expression(source, &lex(source).unwrap()).unwrap();
        let Expr::LegacyParenthesized(outer) = tree else {
            panic!("outer parentheses remain")
        };
        assert_eq!(outer.span, crate::Span::new(0, source.len()));
        let Expr::LegacyParenthesized(inner) = outer.expression.as_ref() else {
            panic!("inner parentheses remain")
        };
        assert_eq!(inner.span, crate::Span::new(1, source.len() - 1));
        let Expr::Binary(sum) = inner.expression.as_ref() else {
            panic!("original arithmetic child")
        };
        assert_eq!(sum.left.span(), crate::Span::new(2, 3));
        assert_eq!(sum.right.span(), crate::Span::new(6, 7));
    }

    #[test]
    fn parsed_metadata_retains_lambda_predicate_case_sort_and_row_source_ranges() {
        use crate::{Span, ast::Expr};
        let source = "f((x) -> (x + 1), (a) BETWEEN (1) AND (2), CASE WHEN (a) THEN (3) ELSE (4) END ORDER BY (b)) OVER (PARTITION BY (p) ORDER BY (w))";
        let tree = parse_expression(source, &lex(source).unwrap()).unwrap();
        let Expr::FunctionCall(call) = &tree else {
            panic!("one actual function call")
        };
        for (index, fragment) in [
            "(x) -> (x + 1)",
            "(a) BETWEEN (1) AND (2)",
            "CASE WHEN (a) THEN (3) ELSE (4) END",
        ]
        .iter()
        .enumerate()
        {
            let start = source.find(fragment).unwrap();
            assert_eq!(
                call.arguments[index].span,
                Span::new(start, start + fragment.len())
            );
        }
        let Expr::Lambda(lambda) = &call.arguments[0].value else {
            panic!("original lambda")
        };
        assert_eq!(lambda.span, call.arguments[0].span);
        let Expr::Predicated(predicate) = &call.arguments[1].value else {
            panic!("original predicate")
        };
        assert_eq!(predicate.span, call.arguments[1].span);
        let Expr::SearchedCase(case) = &call.arguments[2].value else {
            panic!("original CASE")
        };
        let fragment = "WHEN (a) THEN (3)";
        let start = source.find(fragment).unwrap();
        assert_eq!(
            case.when_clauses[0].span,
            Span::new(start, start + fragment.len())
        );
        let key_start = source.find("(b)").unwrap();
        assert_eq!(
            call.order_by.as_ref().unwrap().items[0].span,
            Span::new(key_start, key_start + 3)
        );
        let crate::ast::Window::Specification(window) = call.window.as_ref().unwrap() else {
            panic!("actual OVER specification")
        };
        let key_start = source.find("(w)").unwrap();
        assert_eq!(
            window.order_by.as_ref().unwrap().items[0].span,
            Span::new(key_start, key_start + 3)
        );
        let source = "ROW((1), ((2)))";
        let tree = parse_expression(source, &lex(source).unwrap()).unwrap();
        let Expr::Row(row) = tree else {
            panic!("real ROW constructor")
        };
        for (index, fragment) in ["(1)", "((2))"].iter().enumerate() {
            let start = source.find(fragment).unwrap();
            assert_eq!(
                row.fields[index].span,
                Span::new(start, start + fragment.len())
            );
            assert!(matches!(
                row.fields[index].expression,
                Expr::LegacyParenthesized(_)
            ));
        }
    }
    #[test]
    fn computed_operands_keep_real_ranges_and_ordered_children() {
        use crate::{
            Span,
            ast::{BinaryOperator, Expr},
        };
        let source = "((a+b))*((c+d))";
        let tree = parse_expression(source, &lex(source).unwrap()).unwrap();
        let Expr::Binary(product) = &tree else {
            panic!("product")
        };
        assert_eq!(product.operator, BinaryOperator::Multiply);
        let separator = source.find('*').unwrap();
        assert_eq!(product.left_operand_span, Span::new(0, separator));
        assert_eq!(product.span, Span::new(0, source.len()));
        for (operand, start, names) in [
            (
                product.left.as_ref(),
                source.find("a+b").unwrap(),
                ["a", "b"],
            ),
            (
                product.right.as_ref(),
                source.find("c+d").unwrap(),
                ["c", "d"],
            ),
        ] {
            let Expr::Binary(sum) = operand else {
                panic!("direct computed operand")
            };
            assert_eq!(sum.operator, BinaryOperator::Add);
            assert_eq!(sum.span, Span::new(start, start + 3));
            assert_eq!(sum.left_operand_span, Span::new(start, start + 1));
            for (child, name, offset) in [
                (sum.left.as_ref(), names[0], 0),
                (sum.right.as_ref(), names[1], 2),
            ] {
                let Expr::Identifier(identifier) = child else {
                    panic!("real identifier")
                };
                assert_eq!(identifier.value, name);
                assert_eq!(
                    identifier.span,
                    Span::new(start + offset, start + offset + 1)
                );
            }
        }
        let source = "(a OR b) AND (c OR d)";
        let tree = parse_expression(source, &lex(source).unwrap()).unwrap();
        let Expr::Logical(and) = tree else {
            panic!("conjunction")
        };
        for term in &and.terms {
            let Expr::Logical(or) = term else {
                panic!("direct grouped logical term")
            };
            assert_eq!(or.operator, crate::ast::LogicalOperator::Or);
            assert_eq!(or.terms.len(), 2);
        }
    }

    #[test]
    fn arithmetic_unary_and_cast_keep_computed_structure_without_legacy_parentheses() {
        use crate::ast::{BinaryOperator, Expr, SyntaxEq};

        for source in ["-(a+b)", "+(a+b)", "CAST((a+b) AS BIGINT)"] {
            let tree = parse_expression(source, &lex(source).unwrap()).unwrap();
            let child = match &tree {
                Expr::Unary(node) => &node.expression,
                Expr::Cast(node) => &node.expression,
                _ => panic!("computed operand owner"),
            };
            assert!(
                matches!(child, Expr::Binary(sum) if sum.operator == BinaryOperator::Add)
            );
            let canonical = crate::printer::print_expr(&tree);
            let reparsed = parse_expression(&canonical, &lex(&canonical).unwrap()).unwrap();
            assert!(tree.syntax_eq(&reparsed), "{canonical}");
        }
    }

    #[test]
    fn unknown_parents_and_noncomputed_roots_keep_original_parentheses() {
        use crate::ast::{Expr, Predicate};
        for source in [
            "(a)+b",
            "(1)+b",
            "(-1)+b",
            "(f(a))+b",
            "(a=b) AND TRUE",
            "(a||b)+c",
        ] {
            let tree = parse_expression(source, &lex(source).unwrap()).unwrap();
            let first = match &tree {
                Expr::Binary(binary) => binary.left.as_ref(),
                Expr::Logical(logical) => &logical.terms[0],
                _ => panic!("known actual parent"),
            };
            assert!(matches!(first, Expr::LegacyParenthesized(_)), "{source}");
        }
        for source in [
            "f((a+b))",
            "-(a)",
            "+(f(a))",
            "~(a+b)",
            "(a+b).field",
            "(a+b)=c",
            "a||(b+c)",
            "ROW((a+b))",
        ] {
            let tree = parse_expression(source, &lex(source).unwrap()).unwrap();
            let retained = match &tree {
                Expr::FunctionCall(call) => &call.arguments[0].value,
                Expr::Unary(unary) => unary.expression.as_ref(),
                Expr::Dereference(node) => &node.base,
                Expr::Predicated(node) => {
                    assert!(matches!(node.predicate, Predicate::Comparison(_)));
                    &node.value
                }
                Expr::Binary(binary) => binary.right.as_ref(),
                Expr::Row(row) => &row.fields[0].expression,
                _ => panic!("original opaque parent"),
            };
            assert!(matches!(retained, Expr::LegacyParenthesized(_)), "{source}");
        }
        let source = "((a+b))";
        let tree = parse_expression(source, &lex(source).unwrap()).unwrap();
        let Expr::LegacyParenthesized(outer) = tree else {
            panic!("opaque public root")
        };
        let Expr::LegacyParenthesized(inner) = outer.expression.as_ref() else {
            panic!("second real layer")
        };
        assert_eq!(outer.span, crate::Span::new(0, source.len()));
        assert_eq!(inner.span, crate::Span::new(1, source.len() - 1));
        assert!(matches!(inner.expression.as_ref(), Expr::Binary(_)));
    }

    #[test]
    fn computed_left_ranges_preserve_filter_endpoints_and_span_independent_syntax() {
        use crate::{
            Span,
            ast::{Expr, Fold, SyntaxEq},
        };
        let source = "((1 + agg(2) FILTER (WHERE (TRUE)))) * 4";
        let tree = parse_expression(source, &lex(source).unwrap()).unwrap();
        let Expr::Binary(product) = &tree else {
            panic!("product")
        };
        let end = source.find(" * ").unwrap();
        assert_eq!(product.left_operand_span, Span::new(0, end));
        let Expr::Binary(sum) = product.left.as_ref() else {
            panic!("computed sum")
        };
        let Expr::FunctionCall(call) = sum.right.as_ref() else {
            panic!("original call")
        };
        let selected_end = source.find("(TRUE)").unwrap() + "(TRUE)".len();
        assert_eq!(call.span.end(), selected_end);
        assert_eq!(sum.span.end(), selected_end);
        struct Identity;
        impl Fold for Identity {}
        let folded = Identity.fold_expr(tree.clone());
        assert_eq!(
            folded, tree,
            "fold preserves the authored diagnostic metadata"
        );
        let mut other = tree.clone();
        let Expr::Binary(other_product) = &mut other else {
            unreachable!()
        };
        other_product.left_operand_span = Span::new(2, end - 2);
        assert!(tree.syntax_eq(&other));
        assert_ne!(tree, other, "ordinary equality retains real span facts");
        let source = "((a+b)";
        let error = parse_expression(source, &lex(source).unwrap()).unwrap_err();
        let crate::error::ParseError::UnexpectedToken { span, expected, .. } = error else {
            panic!("original missing-delimiter error")
        };
        assert_eq!(expected, "')'");
        assert_eq!(span, Span::new(source.len(), source.len()));
    }

    #[test]
    fn deeply_grouped_computed_operand_moves_one_real_subtree() {
        use crate::ast::{BinaryOperator, Expr};
        let depth = 2_000;
        let source = format!("{}a+b{}*c", "(".repeat(depth), ")".repeat(depth));
        let tree = parse_expression(&source, &lex(&source).unwrap()).unwrap();
        let Expr::Binary(product) = &tree else {
            panic!("product")
        };
        assert_eq!(product.operator, BinaryOperator::Multiply);
        assert_eq!(
            product.left_operand_span,
            crate::Span::new(0, depth * 2 + 3)
        );
        let Expr::Binary(sum) = product.left.as_ref() else {
            panic!("real computed child")
        };
        assert_eq!(sum.operator, BinaryOperator::Add);
        for (child, expected) in [
            (sum.left.as_ref(), "a"),
            (sum.right.as_ref(), "b"),
            (product.right.as_ref(), "c"),
        ] {
            let Expr::Identifier(name) = child else {
                panic!("original ordered leaf")
            };
            assert_eq!(name.value, expected);
        }
    }
}

/// The final ordinary FunctionCall directly owns the original producer children.
fn ordinary_special_call(
    name: &'static str,
    quoted: bool,
    arguments: Vec<ParsedExpression>,
    span: Span,
) -> Expr {
    Expr::FunctionCall(AstBox::new(FunctionCall {
        name: ObjectName {
            parts: vec![Ident {
                value: name.to_owned(),
                quoted,
                span: crate::SYNTHETIC,
            }],
            span: crate::SYNTHETIC,
        },
        arguments: arguments
            .into_iter()
            .map(ParsedExpression::into_call_argument)
            .collect(),
        distinct: false,
        order_by: None,
        filter: None,
        null_treatment: None,
        window: None,
        processing_mode: None,
        span,
    }))
}
