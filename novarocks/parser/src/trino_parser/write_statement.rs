// SPDX-License-Identifier: Apache-2.0

use super::{GrammarError, Parser};
use crate::Span;
use crate::ast::{
    AstBox, Expr, Table, TrinoDelete, TrinoInsert, TrinoInsertMode, TrinoUpdate,
    TrinoUpdateAssignment,
};
use crate::trino_token::{Keyword, Symbol, TokenKind};

impl Parser<'_> {
    pub(super) fn parse_insert_statement(&mut self) -> Result<TrinoInsert, GrammarError> {
        let start = self.expect_keyword(Keyword::Insert)?;
        let mode = self.parse_insert_mode()?;
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
            mode,
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

    fn parse_insert_mode(&mut self) -> Result<TrinoInsertMode, GrammarError> {
        if self.eat_keyword(Keyword::Into).is_some() {
            return Ok(TrinoInsertMode::Into);
        }
        // OVERWRITE is contextual to the two approved insert extensions.
        let token = self.peek(0);
        if token.kind != TokenKind::Identifier
            || !self.source[token.span.start()..token.span.end()].eq_ignore_ascii_case("OVERWRITE")
        {
            return Err(self.error("INTO or OVERWRITE"));
        }
        self.bump();
        // PARTITIONS is also a target identifier. The extension introducer
        // requires INTO or a target name after it.
        let mode = if self.at_keyword(Keyword::Partitions)
            && (self.is_identifier(1) || self.peek(1).kind == TokenKind::Keyword(Keyword::Into))
        {
            self.bump();
            TrinoInsertMode::OverwritePartitions
        } else {
            TrinoInsertMode::Overwrite
        };
        self.eat_keyword(Keyword::Into);
        Ok(mode)
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
