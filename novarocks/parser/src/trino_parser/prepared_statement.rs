// SPDX-License-Identifier: Apache-2.0

use super::{GrammarError, Parser};
use crate::ast::*;
use crate::trino_token::{Keyword as K, Symbol as S, TokenKind};

impl Parser<'_> {
    pub(super) fn parse_create_function(&mut self) -> Result<TrinoCreateFunction, GrammarError> {
        let start = self.expect_keyword(K::Create)?;
        let replace = self.parse_statement_or_replace()?;
        let specification = AstBox::new(self.parse_function_specification()?);
        Ok(TrinoCreateFunction {
            specification,
            replace,
            span: self.span_since(start),
        })
    }

    pub(super) fn parse_drop_function(&mut self) -> Result<TrinoDropFunction, GrammarError> {
        let start = self.expect_keyword(K::Drop)?;
        self.expect_keyword(K::Function)?;
        let exists = self.parse_statement_if_exists();
        let name = self.qualified_name()?;
        self.expect_symbol(S::LParen)?;
        let mut parameters = Vec::new();
        if !self.at_symbol(S::RParen) {
            parameters.push(self.parse_function_parameter()?);
            while self.eat_symbol(S::Comma).is_some() {
                parameters.push(self.parse_function_parameter()?);
            }
        }
        self.expect_symbol(S::RParen)?;
        Ok(TrinoDropFunction {
            name,
            parameters: parameters.into(),
            exists,
            span: self.span_since(start),
        })
    }

    pub(super) fn parse_prepare(&mut self) -> Result<TrinoPrepare, GrammarError> {
        let start = self.expect_keyword(K::Prepare)?;
        let name = self.ident()?;
        self.expect_keyword(K::From)?;
        let statement = AstBox::new(self.parse_statement()?);
        Ok(TrinoPrepare {
            name,
            statement,
            span: self.span_since(start),
        })
    }

    pub(super) fn parse_deallocate(&mut self) -> Result<TrinoDeallocate, GrammarError> {
        let start = self.expect_keyword(K::Deallocate)?;
        self.expect_keyword(K::Prepare)?;
        let name = self.ident()?;
        Ok(TrinoDeallocate {
            name,
            span: self.span_since(start),
        })
    }

    pub(super) fn parse_execute(&mut self) -> Result<TrinoStatement, GrammarError> {
        let start = self.expect_keyword(K::Execute)?;
        if self.at_keyword(K::Immediate)
            && matches!(
                self.peek(1).kind,
                TokenKind::String | TokenKind::UnicodeString
            )
        {
            self.bump();
            let (statement_text, _) = self.parse_string()?;
            let parameters = self.parse_execute_parameters()?;
            Ok(TrinoStatement::ExecuteImmediate(TrinoExecuteImmediate {
                statement_text,
                parameters,
                span: self.span_since(start),
            }))
        } else {
            let name = self.ident()?;
            let parameters = self.parse_execute_parameters()?;
            Ok(TrinoStatement::Execute(TrinoExecute {
                name,
                parameters,
                span: self.span_since(start),
            }))
        }
    }

    fn parse_execute_parameters(&mut self) -> Result<AstVec<Expr>, GrammarError> {
        let mut parameters = Vec::new();
        if self.eat_keyword(K::Using).is_some() {
            parameters.push(self.parse_expression()?);
            while self.eat_symbol(S::Comma).is_some() {
                parameters.push(self.parse_expression()?);
            }
        }
        Ok(parameters.into())
    }

    pub(super) fn parse_describe_input(&mut self) -> Result<TrinoDescribeInput, GrammarError> {
        let start = self.expect_keyword(K::Describe)?;
        self.expect_keyword(K::Input)?;
        let name = self.ident()?;
        Ok(TrinoDescribeInput {
            name,
            span: self.span_since(start),
        })
    }

    pub(super) fn parse_describe_output(&mut self) -> Result<TrinoDescribeOutput, GrammarError> {
        let start = self.expect_keyword(K::Describe)?;
        self.expect_keyword(K::Output)?;
        let input = if self.eat_symbol(S::LParen).is_some() {
            let query = AstBox::new(self.parse_root_query()?);
            self.expect_symbol(S::RParen)?;
            TrinoDescribeOutputInput::Query(query)
        } else {
            TrinoDescribeOutputInput::Prepared(self.ident()?)
        };
        Ok(TrinoDescribeOutput {
            input,
            span: self.span_since(start),
        })
    }
}
