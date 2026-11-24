// SPDX-License-Identifier: Apache-2.0

use super::{GrammarError, Parser};
use crate::Span;
use crate::ast::{
    AstBox, Expr, Table, TrinoDelete, TrinoInsert, TrinoUpdate, TrinoUpdateAssignment,
};
use crate::token::{Keyword, Symbol};

impl Parser<'_> {
    pub(super) fn parse_insert_statement(&mut self) -> Result<TrinoInsert, GrammarError> {
        let start = self.expect_keyword(Keyword::Insert)?;
        self.expect_keyword(Keyword::Into)?;
        let table = self.parse_write_statement_target(start)?;
        // After the opening parenthesis, a column name is an identifier and a
        // root query starts with a reserved query keyword or another parenthesis.
        let columns = if self.at_symbol(Symbol::LParen) && self.is_identifier(1) {
            Some(self.parse_column_aliases()?)
        } else {
            None
        };
        let query = AstBox::new(self.parse_root_query()?);
        Ok(TrinoInsert {
            table,
            columns,
            span: self.span_since(start),
            query,
        })
    }

    pub(super) fn parse_delete_statement(&mut self) -> Result<TrinoDelete, GrammarError> {
        let start = self.expect_keyword(Keyword::Delete)?;
        self.expect_keyword(Keyword::From)?;
        let table = self.parse_write_statement_target(start)?;
        let predicate = self.parse_write_statement_predicate()?;
        Ok(TrinoDelete {
            table,
            predicate,
            span: self.span_since(start),
        })
    }

    pub(super) fn parse_update_statement(&mut self) -> Result<TrinoUpdate, GrammarError> {
        let start = self.expect_keyword(Keyword::Update)?;
        let table = self.parse_write_statement_target(start)?;
        self.expect_keyword(Keyword::Set)?;
        let mut assignments = vec![self.parse_update_assignment()?];
        while self.eat_symbol(Symbol::Comma).is_some() {
            assignments.push(self.parse_update_assignment()?);
        }
        let predicate = self.parse_write_statement_predicate()?;
        Ok(TrinoUpdate {
            table,
            assignments: assignments.into(),
            predicate,
            span: self.span_since(start),
        })
    }

    pub(super) fn parse_write_statement_target(
        &mut self,
        statement_start: Span,
    ) -> Result<Table, GrammarError> {
        let name = self.qualified_name()?;
        let branch = if self.eat_symbol(Symbol::At).is_some() {
            Some(self.ident()?)
        } else {
            None
        };
        Ok(Table {
            name,
            query_period: None,
            branch,
            // The reference Table's location is the write statement start.
            span: self.span_since(statement_start),
        })
    }

    fn parse_update_assignment(&mut self) -> Result<TrinoUpdateAssignment, GrammarError> {
        let name = self.ident()?;
        let start = name.span;
        self.expect_symbol(Symbol::Eq)?;
        let value = self.parse_expression()?;
        Ok(TrinoUpdateAssignment {
            name,
            value,
            span: self.span_since(start),
        })
    }

    fn parse_write_statement_predicate(&mut self) -> Result<Option<Expr>, GrammarError> {
        if self.eat_keyword(Keyword::Where).is_some() {
            return self.parse_expression().map(Some);
        }
        Ok(None)
    }
}
