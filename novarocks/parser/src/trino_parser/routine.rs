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
use crate::{ParseError, Span};

impl Parser<'_> {
    #[recursive::recursive]
    pub(super) fn parse_function_specification(
        &mut self,
    ) -> Result<FunctionSpecification, ParserError> {
        let start = self.expect_keyword(K::Function)?;
        self.routine_started = true;
        let parameter_start = self.next_parameter;
        let name = self.qualified_name()?;
        self.expect_symbol(S::LParen)?;
        let mut parameters = Vec::new();
        if !self.at_symbol(S::RParen) {
            loop {
                parameters.push(self.parse_function_parameter()?);
                if self.eat_symbol(S::Comma).is_none() {
                    break;
                }
            }
        }
        self.expect_symbol(S::RParen)?;
        self.expect_keyword(K::Returns)?;
        let returns = self.parse_type()?;
        let characteristics_start = self.next_parameter;
        let mut characteristics = Vec::new();
        while self.routine_characteristic_ahead() {
            characteristics.push(self.parse_routine_characteristic()?);
        }
        let characteristics_end = self.next_parameter;
        let body_start = self.next_parameter;
        let body = if self.eat_keyword(K::As).is_some() {
            let (definition, span) = self.parse_function_definition()?;
            RoutineBody::External { definition, span }
        } else {
            let statement_start = self.current().span;
            let statement = self.parse_routine_control_statement()?;
            if !matches!(
                &statement,
                ControlStatement::Return(_) | ControlStatement::Compound(_)
            ) {
                return Err(routine_invariant(
                    "a function SQL body beginning with RETURN or BEGIN",
                    self.source,
                    self.span_since(statement_start),
                ));
            }
            RoutineBody::Sql(statement)
        };
        let body_end = self.next_parameter;
        // Types cannot contain parameters. The reference visits SQL control
        // first, then the earlier-text characteristics and their properties.
        self.reorder_parameter_groups(
            parameter_start,
            &[
                body_start..body_end,
                characteristics_start..characteristics_end,
            ],
        );
        Ok(FunctionSpecification {
            name,
            parameters,
            returns,
            characteristics,
            body,
            span: self.span_since(start),
        })
    }

    #[recursive::recursive]
    pub(super) fn parse_function_parameter(&mut self) -> Result<FunctionParameter, ParserError> {
        let start = self.current().span;
        let mut named_error = None;
        // Unlike a rowField's `type | identifier type`, this grammar has a
        // greedy optional identifier before type. Try the named form first.
        if self.is_identifier(0) {
            let checkpoint = self.checkpoint();
            let named = (|| {
                let name = self.ident()?;
                let data_type = self.parse_type()?;
                self.require_function_parameter_end()?;
                Ok::<_, ParserError>(FunctionParameter {
                    name: Some(name),
                    data_type,
                    span: self.span_since(start),
                })
            })();
            match named {
                Ok(parameter) => return Ok(parameter),
                Err(error @ ParserError::Construction(_)) => return Err(error),
                Err(ParserError::Syntax(error)) => {
                    named_error = Some(error);
                    self.restore(checkpoint);
                }
            }
        }
        let data_type = self.parse_type()?;
        if !self.at_symbol(S::Comma) && !self.at_symbol(S::RParen) {
            return Err(named_error
                .map(ParserError::Syntax)
                .unwrap_or_else(|| self.error("',' or ')' after a function parameter")));
        }
        Ok(FunctionParameter {
            name: None,
            data_type,
            span: self.span_since(start),
        })
    }

    fn require_function_parameter_end(&self) -> Result<(), ParserError> {
        if self.at_symbol(S::Comma) || self.at_symbol(S::RParen) {
            Ok(())
        } else {
            Err(self.error("',' or ')' after a function parameter"))
        }
    }

    fn routine_characteristic_ahead(&self) -> bool {
        matches!(
            self.peek(0).kind,
            T::Keyword(
                K::Language
                    | K::Deterministic
                    | K::Not
                    | K::Returns
                    | K::Called
                    | K::Security
                    | K::Comment
                    | K::With
            )
        )
    }

    #[recursive::recursive]
    fn parse_routine_characteristic(&mut self) -> Result<RoutineCharacteristic, ParserError> {
        let start = self.current().span;
        let characteristic = match self.peek(0).kind {
            T::Keyword(K::Language) => {
                self.bump();
                let language = self.ident()?;
                RoutineCharacteristic::Language {
                    language,
                    span: self.span_since(start),
                }
            }
            T::Keyword(K::Deterministic | K::Not) => {
                let deterministic = self.eat_keyword(K::Not).is_none();
                self.expect_keyword(K::Deterministic)?;
                RoutineCharacteristic::Deterministic {
                    deterministic,
                    span: self.span_since(start),
                }
            }
            T::Keyword(K::Returns | K::Called) => {
                let called_on_null = self.eat_keyword(K::Called).is_some();
                if !called_on_null {
                    self.expect_keyword(K::Returns)?;
                    self.expect_keyword(K::Null)?;
                }
                self.expect_keyword(K::On)?;
                self.expect_keyword(K::Null)?;
                self.expect_keyword(K::Input)?;
                RoutineCharacteristic::NullInput {
                    called_on_null,
                    span: self.span_since(start),
                }
            }
            T::Keyword(K::Security) => {
                self.bump();
                let security = if self.eat_keyword(K::Invoker).is_some() {
                    RoutineSecurity::Invoker
                } else {
                    self.expect_keyword(K::Definer)?;
                    RoutineSecurity::Definer
                };
                RoutineCharacteristic::Security {
                    security,
                    span: self.span_since(start),
                }
            }
            T::Keyword(K::Comment) => {
                self.bump();
                let (comment, _) = self.parse_string()?;
                RoutineCharacteristic::Comment {
                    comment,
                    span: self.span_since(start),
                }
            }
            T::Keyword(K::With) => {
                self.bump();
                self.expect_symbol(S::LParen)?;
                let mut properties = Vec::new();
                loop {
                    let property_start = self.current().span;
                    let name = self.ident()?;
                    self.expect_symbol(S::Eq)?;
                    // DEFAULT is also a non-reserved expression identifier.
                    // Select the default-value production only at its boundary.
                    let value = if self.at_keyword(K::Default)
                        && matches!(self.peek(1).kind, T::Symbol(S::Comma | S::RParen))
                    {
                        self.bump();
                        None
                    } else {
                        Some(self.parse_expression()?)
                    };
                    properties.push(RoutineProperty {
                        name,
                        value,
                        span: self.span_since(property_start),
                    });
                    if self.eat_symbol(S::Comma).is_none() {
                        break;
                    }
                }
                self.expect_symbol(S::RParen)?;
                RoutineCharacteristic::Properties {
                    properties,
                    span: self.span_since(start),
                }
            }
            _ => return Err(self.error("a routine characteristic")),
        };
        Ok(characteristic)
    }

    #[recursive::recursive]
    fn parse_routine_control_statement(&mut self) -> Result<ControlStatement, ParserError> {
        let start = self.current().span;
        let label = if self.is_identifier(0) && self.peek(1).kind == T::Symbol(S::Colon) {
            let label = self.ident()?;
            self.expect_symbol(S::Colon)?;
            Some(label)
        } else {
            None
        };
        if label.is_some()
            && !matches!(
                self.peek(0).kind,
                T::Keyword(K::Loop | K::While | K::Repeat)
            )
        {
            return Err(self.error("LOOP, WHILE, or REPEAT after a label"));
        }
        match self.peek(0).kind {
            T::Keyword(K::Return) => {
                self.bump();
                let value = self.parse_value_expression()?;
                Ok(ControlStatement::Return(ReturnStatement {
                    value,
                    span: self.span_since(start),
                }))
            }
            T::Keyword(K::Set) => {
                self.bump();
                let target = self.ident()?;
                self.expect_symbol(S::Eq)?;
                let value = self.parse_expression()?;
                Ok(ControlStatement::Assignment(AssignmentStatement {
                    target,
                    value,
                    span: self.span_since(start),
                }))
            }
            T::Keyword(K::Case) => self.parse_routine_case(start),
            T::Keyword(K::If) => self.parse_routine_if(start),
            T::Keyword(K::Iterate) => {
                self.bump();
                let label = self.ident()?;
                Ok(ControlStatement::Iterate(IterateStatement {
                    label,
                    span: self.span_since(start),
                }))
            }
            T::Keyword(K::Leave) => {
                self.bump();
                let label = self.ident()?;
                Ok(ControlStatement::Leave(LeaveStatement {
                    label,
                    span: self.span_since(start),
                }))
            }
            T::Keyword(K::Begin) => self.parse_routine_compound(start),
            T::Keyword(K::Loop) => {
                self.bump();
                let statements = self.parse_routine_statement_list(&[K::End])?;
                self.expect_keyword(K::End)?;
                self.expect_keyword(K::Loop)?;
                Ok(ControlStatement::Loop(LoopStatement {
                    label,
                    statements,
                    span: self.span_since(start),
                }))
            }
            T::Keyword(K::While) => {
                self.bump();
                let expression = self.parse_expression()?;
                self.expect_keyword(K::Do)?;
                let statements = self.parse_routine_statement_list(&[K::End])?;
                self.expect_keyword(K::End)?;
                self.expect_keyword(K::While)?;
                Ok(ControlStatement::While(WhileStatement {
                    label,
                    expression,
                    statements,
                    span: self.span_since(start),
                }))
            }
            T::Keyword(K::Repeat) => {
                self.bump();
                let statements = self.parse_routine_statement_list(&[K::Until])?;
                self.expect_keyword(K::Until)?;
                let condition = self.parse_expression()?;
                self.expect_keyword(K::End)?;
                self.expect_keyword(K::Repeat)?;
                Ok(ControlStatement::Repeat(RepeatStatement {
                    label,
                    statements,
                    condition,
                    span: self.span_since(start),
                }))
            }
            _ => Err(self.error("a routine control statement")),
        }
    }

    #[recursive::recursive]
    fn parse_routine_statement_list(
        &mut self,
        stop: &[K],
    ) -> Result<AstVec<ControlStatement>, ParserError> {
        let mut statements = Vec::new();
        loop {
            statements.push(self.parse_routine_control_statement()?);
            self.expect_symbol(S::Semicolon)?;
            if matches!(self.peek(0).kind, T::Keyword(keyword) if stop.contains(&keyword)) {
                break;
            }
        }
        Ok(statements.into())
    }

    #[recursive::recursive]
    fn parse_routine_case(&mut self, start: Span) -> Result<ControlStatement, ParserError> {
        self.expect_keyword(K::Case)?;
        let expression = if self.at_keyword(K::When) {
            None
        } else {
            Some(self.parse_expression()?)
        };
        let mut when_clauses = Vec::new();
        loop {
            let clause_start = self.expect_keyword(K::When)?;
            let expression = self.parse_expression()?;
            self.expect_keyword(K::Then)?;
            let statements = self.parse_routine_statement_list(&[K::When, K::Else, K::End])?;
            when_clauses.push(CaseStatementWhenClause {
                expression,
                statements,
                span: self.span_since(clause_start),
            });
            if !self.at_keyword(K::When) {
                break;
            }
        }
        let else_clause = self.parse_routine_else()?;
        self.expect_keyword(K::End)?;
        self.expect_keyword(K::Case)?;
        Ok(ControlStatement::Case(CaseStatement {
            expression,
            when_clauses,
            else_clause,
            span: self.span_since(start),
        }))
    }

    #[recursive::recursive]
    fn parse_routine_if(&mut self, start: Span) -> Result<ControlStatement, ParserError> {
        self.expect_keyword(K::If)?;
        let expression = self.parse_expression()?;
        self.expect_keyword(K::Then)?;
        let statements = self.parse_routine_statement_list(&[K::Elseif, K::Else, K::End])?;
        let mut else_if_clauses = Vec::new();
        while let Some(clause_start) = self.eat_keyword(K::Elseif) {
            let expression = self.parse_expression()?;
            self.expect_keyword(K::Then)?;
            let statements = self.parse_routine_statement_list(&[K::Elseif, K::Else, K::End])?;
            else_if_clauses.push(ElseIfClause {
                expression,
                statements,
                span: self.span_since(clause_start),
            });
        }
        let else_clause = self.parse_routine_else()?;
        self.expect_keyword(K::End)?;
        self.expect_keyword(K::If)?;
        Ok(ControlStatement::If(IfStatement {
            expression,
            statements,
            else_if_clauses,
            else_clause,
            span: self.span_since(start),
        }))
    }

    #[recursive::recursive]
    fn parse_routine_else(&mut self) -> Result<Option<ElseClause>, ParserError> {
        let Some(start) = self.eat_keyword(K::Else) else {
            return Ok(None);
        };
        let statements = self.parse_routine_statement_list(&[K::End])?;
        Ok(Some(ElseClause {
            statements,
            span: self.span_since(start),
        }))
    }

    #[recursive::recursive]
    fn parse_routine_compound(&mut self, start: Span) -> Result<ControlStatement, ParserError> {
        self.expect_keyword(K::Begin)?;
        let mut variable_declarations = Vec::new();
        while self.at_keyword(K::Declare) {
            variable_declarations.push(self.parse_routine_variable()?);
            self.expect_symbol(S::Semicolon)?;
        }
        let statements = if self.at_keyword(K::End) {
            AstVec::new()
        } else {
            self.parse_routine_statement_list(&[K::End])?
        };
        self.expect_keyword(K::End)?;
        Ok(ControlStatement::Compound(CompoundStatement {
            variable_declarations,
            statements,
            span: self.span_since(start),
        }))
    }

    #[recursive::recursive]
    fn parse_routine_variable(&mut self) -> Result<VariableDeclaration, ParserError> {
        let start = self.expect_keyword(K::Declare)?;
        let mut names = vec![self.ident()?];
        while self.eat_symbol(S::Comma).is_some() {
            names.push(self.ident()?);
        }
        let data_type = self.parse_type()?;
        let default_value = if self.eat_keyword(K::Default).is_some() {
            Some(self.parse_value_expression()?)
        } else {
            None
        };
        Ok(VariableDeclaration {
            names,
            data_type,
            default_value,
            span: self.span_since(start),
        })
    }
}

fn routine_invariant(expected: &'static str, source: &str, span: Span) -> ParserError {
    ParserError::Construction(
        ParseError::UnexpectedToken {
            expected,
            found: source[span.start()..span.end()].to_owned(),
            span,
        }
        .into(),
    )
}
