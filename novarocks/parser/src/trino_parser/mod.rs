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

//! Query syntax built directly from the Trino lexical vocabulary.

mod access;
mod cursor;
mod expression;
mod failure;
mod json;
mod literal;
mod parameters;
mod query;
mod relation;
mod special;
mod types;
mod window;

use crate::ast::{Expr, Fold, Query, TypeName};
use crate::trino_token::{Token, TokenKind};
use crate::{ParserError, Span, trino_lexer};
use failure::GrammarError;

struct Parser<'source> {
    source: &'source str,
    tokens: Vec<Token>,
    position: usize,
    next_parameter: usize,
    // Only committed parameter productions enter this construction ledger.
    // Pattern quantifiers and trivia question marks are not parameter events.
    parameter_events: Vec<Span>,
    // An enclosing grammar production may own a specific delimiter token.
    // Keep that token's real kind and range; never insert another End token.
    grammar_boundary: Option<usize>,
    // SELECT items and ROW fields admit an implicit alias after the expression.
    alias_follow: bool,
}

impl<'source> Parser<'source> {
    fn new(source: &'source str) -> Result<Self, GrammarError> {
        Ok(Self {
            source,
            tokens: trino_lexer::lex(source)?,
            position: 0,
            next_parameter: 0,
            parameter_events: Vec::new(),
            grammar_boundary: None,
            alias_follow: false,
        })
    }

    fn finish(&self) -> Result<(), GrammarError> {
        if self.peek(0).kind == TokenKind::End {
            Ok(())
        } else {
            Err(self.error("end of input"))
        }
    }

    pub(super) fn with_grammar_boundary<T>(
        &mut self,
        boundary: usize,
        parse: impl FnOnce(&mut Self) -> Result<T, GrammarError>,
    ) -> Result<T, GrammarError> {
        assert!(boundary < self.tokens.len());
        let previous = self.grammar_boundary.replace(boundary);
        let result = parse(self);
        self.grammar_boundary = previous;
        result
    }

    pub(super) fn grammar_boundary_at_current(&self) -> bool {
        self.grammar_boundary
            .is_some_and(|index| self.peek(0).span == self.tokens[index].span)
    }

    fn with_alias_follow<T>(
        &mut self,
        allowed: bool,
        parse: impl FnOnce(&mut Self) -> Result<T, GrammarError>,
    ) -> Result<T, GrammarError> {
        let previous = std::mem::replace(&mut self.alias_follow, allowed);
        let result = parse(self);
        self.alias_follow = previous;
        result
    }
}

/// Parses one complete root query, including its session and function prefixes.
/// Script separators belong to the statement consumer, not this grammar rule.
pub fn parse_query(source: &str) -> Result<Query, ParserError> {
    let mut parser = Parser::new(source)?;
    let query = parser.parse_root_query_with_session()?;
    parser.finish()?;
    Ok(parser.parameter_numbers().fold_query(query))
}

/// Parses one complete expression with a single parameter construction owner.
pub fn parse_expression(source: &str) -> Result<Expr, ParserError> {
    let mut parser = Parser::new(source)?;
    let expression = parser.parse_expression()?;
    parser.finish()?;
    Ok(parser.parameter_numbers().fold_expr(expression))
}

/// Parses syntax only. Supported engine types are determined by the analyzer.
pub fn parse_type(source: &str) -> Result<TypeName, ParserError> {
    let mut parser = Parser::new(source)?;
    let data_type = parser.parse_type()?;
    parser.finish()?;
    Ok(data_type)
}
