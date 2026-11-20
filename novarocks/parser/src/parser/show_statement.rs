// SPDX-License-Identifier: Apache-2.0

use super::{GrammarError, Parser};
use crate::SYNTHETIC;
use crate::ast::{
    AstBox, Relation, ShowCreateKind, Table, TableSubquery, TrinoShowCreate, TrinoShowStats,
};
use crate::token::{Keyword, Symbol, TokenKind};

impl Parser<'_> {
    pub(super) fn parse_show_create(&mut self) -> Result<TrinoShowCreate, GrammarError> {
        let start = self.expect_keyword(Keyword::Show)?;
        self.expect_keyword(Keyword::Create)?;
        let kind = match self.peek(0).kind {
            TokenKind::Keyword(Keyword::Table) => {
                self.bump();
                ShowCreateKind::Table
            }
            TokenKind::Keyword(Keyword::Schema) => {
                self.bump();
                ShowCreateKind::Schema
            }
            TokenKind::Keyword(Keyword::View) => {
                self.bump();
                ShowCreateKind::View
            }
            TokenKind::Keyword(Keyword::Materialized) => {
                self.bump();
                self.expect_keyword(Keyword::View)?;
                ShowCreateKind::MaterializedView
            }
            TokenKind::Keyword(Keyword::Function) => {
                self.bump();
                ShowCreateKind::Function
            }
            _ => return Err(self.error("TABLE, SCHEMA, VIEW, MATERIALIZED VIEW or FUNCTION")),
        };
        let name = self.qualified_name()?;
        Ok(TrinoShowCreate {
            kind,
            name,
            span: self.span_since(start),
        })
    }

    pub(super) fn parse_show_stats(&mut self) -> Result<TrinoShowStats, GrammarError> {
        let start = self.expect_keyword(Keyword::Show)?;
        self.expect_keyword(Keyword::Stats)?;
        self.expect_keyword(Keyword::For)?;
        // AstBuilder creates a locationless relation around the real named
        // or query child. Its source facts belong to that child.
        let relation = if self.eat_symbol(Symbol::LParen).is_some() {
            let query = self.parse_root_query()?;
            self.expect_symbol(Symbol::RParen)?;
            Relation::TableSubquery(AstBox::new(TableSubquery {
                query: AstBox::new(query),
                span: SYNTHETIC,
            }))
        } else {
            Relation::Table(AstBox::new(Table {
                name: self.qualified_name()?,
                query_period: None,
                branch: None,
                span: SYNTHETIC,
            }))
        };
        Ok(TrinoShowStats {
            relation,
            span: self.span_since(start),
        })
    }
}
