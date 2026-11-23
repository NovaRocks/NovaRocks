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
mod advanced_relation;
mod branch_statement;
mod catalog_schema;
mod ctas_statement;
mod cursor;
mod expression;
mod failure;
mod json;
mod literal;
mod merge_call;
mod metadata_session;
mod parameters;
mod pattern;
mod permission_statement;
mod prepared_statement;
mod query;
mod relation;
mod routine;
mod script;
mod show_statement;
mod special;
mod statement;
mod table_alter;
mod table_statement;
mod types;
mod validation;
mod view_statement;
mod window;
mod write_statement;

use crate::ast::{
    Expr, Fold, FunctionSpecification, Query, RowPattern, TrinoPathSpecification, TrinoStatement,
    TypeName,
};
use crate::token::{Token, TokenKind};
use crate::{ParserError, Span, lexer};
use failure::GrammarError;

pub(crate) use literal::numeric_literal_kind;
pub use script::{StatementFrame, frame_statements};
pub use validation::{StatementValidationError, validate_statement_syntax};

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
    // A script frame can have an internal semicolon only when its real
    // grammar entered a function specification.
    routine_started: bool,
}

impl<'source> Parser<'source> {
    fn new(source: &'source str) -> Result<Self, GrammarError> {
        Ok(Self::from_tokens(source, lexer::lex(source)?))
    }

    fn from_tokens(source: &'source str, tokens: Vec<Token>) -> Self {
        Self {
            source,
            tokens,
            position: 0,
            next_parameter: 0,
            parameter_events: Vec::new(),
            grammar_boundary: None,
            alias_follow: false,
            routine_started: false,
        }
    }

    fn parse_complete_statement(&mut self) -> Result<TrinoStatement, GrammarError> {
        let statement = self.parse_statement()?;
        self.finish()?;
        Ok(statement)
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

    /// Prefer the complete quantified grammar alternative. Only a syntax
    /// mismatch may restore its real cursor and parameter-event checkpoint.
    fn with_optional_set_quantifier<T>(
        &mut self,
        mut parse: impl FnMut(&mut Self, Option<bool>) -> Result<T, GrammarError>,
    ) -> Result<T, GrammarError> {
        use crate::token::Keyword;
        let quantifier = match self.peek(0).kind {
            TokenKind::Keyword(Keyword::Distinct) => Some(true),
            TokenKind::Keyword(Keyword::All) => Some(false),
            _ => None,
        };
        if let Some(distinct) = quantifier {
            let checkpoint = self.checkpoint();
            self.bump();
            match parse(self, Some(distinct)) {
                Ok(value) => return Ok(value),
                Err(GrammarError::Syntax(_)) => self.restore(checkpoint),
                Err(error @ GrammarError::Construction(_)) => return Err(error),
            }
        }
        parse(self, None)
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

/// Parses one complete Trino statement. Script separators belong to its caller.
pub fn parse_statement(source: &str) -> Result<TrinoStatement, ParserError> {
    let mut parser = Parser::new(source)?;
    let statement = parser.parse_complete_statement()?;
    Ok(parser.parameter_numbers().fold_trino_statement(statement))
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

/// Parses the standalone functionSpecification grammar, without a query wrapper.
pub fn parse_function_specification(source: &str) -> Result<FunctionSpecification, ParserError> {
    let mut parser = Parser::new(source)?;
    let function = parser.parse_function_specification()?;
    parser.finish()?;
    Ok(parser
        .parameter_numbers()
        .fold_function_specification(function))
}

/// Parses the standalone rowPattern grammar; quantifier question marks are not parameters.
pub fn parse_row_pattern(source: &str) -> Result<RowPattern, ParserError> {
    let mut parser = Parser::new(source)?;
    let pattern = parser.parse_row_pattern()?;
    parser.finish()?;
    Ok(pattern)
}

/// Parses a complete standalone SQL path, with each name kept as an identifier.
pub fn parse_path_specification(source: &str) -> Result<TrinoPathSpecification, ParserError> {
    let mut parser = Parser::new(source)?;
    let path = parser.parse_path_specification()?;
    parser.finish()?;
    Ok(path)
}
