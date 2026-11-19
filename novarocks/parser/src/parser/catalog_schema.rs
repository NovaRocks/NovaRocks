// SPDX-License-Identifier: Apache-2.0

use super::{GrammarError, Parser};
use crate::ast::{
    AstVec, StatementPrincipal, StatementPrincipalKind, StatementProperty, TrinoCreateCatalog,
    TrinoCreateSchema, TrinoDropCatalog, TrinoDropSchema, TrinoRenameSchema,
};
use crate::token::{Keyword, Symbol, TokenKind};

impl Parser<'_> {
    pub(super) fn parse_create_catalog(&mut self) -> Result<TrinoCreateCatalog, GrammarError> {
        let start = self.expect_keyword(Keyword::Create)?;
        self.expect_keyword(Keyword::Catalog)?;
        let not_exists = self.parse_statement_if_not_exists();
        let catalog_name = self.ident()?;
        self.expect_keyword(Keyword::Using)?;
        let connector_name = self.ident()?;
        let comment = if self.eat_keyword(Keyword::Comment).is_some() {
            Some(self.parse_string()?.0)
        } else {
            None
        };
        let principal = self.parse_statement_authorization()?;
        let properties = self.parse_optional_statement_properties()?;
        Ok(TrinoCreateCatalog {
            catalog_name,
            not_exists,
            connector_name,
            properties,
            principal,
            comment,
            span: self.span_since(start),
        })
    }

    pub(super) fn parse_drop_catalog(&mut self) -> Result<TrinoDropCatalog, GrammarError> {
        let start = self.expect_keyword(Keyword::Drop)?;
        self.expect_keyword(Keyword::Catalog)?;
        let exists = self.parse_statement_if_exists();
        let catalog_name = self.ident()?;
        let cascade = self.parse_drop_cascade();
        Ok(TrinoDropCatalog {
            catalog_name,
            exists,
            cascade,
            span: self.span_since(start),
        })
    }

    pub(super) fn parse_create_schema(&mut self) -> Result<TrinoCreateSchema, GrammarError> {
        let start = self.expect_keyword(Keyword::Create)?;
        self.expect_keyword(Keyword::Schema)?;
        let not_exists = self.parse_statement_if_not_exists();
        let schema_name = self.qualified_name()?;
        let principal = self.parse_statement_authorization()?;
        let properties = self.parse_optional_statement_properties()?;
        Ok(TrinoCreateSchema {
            schema_name,
            not_exists,
            properties,
            principal,
            span: self.span_since(start),
        })
    }

    pub(super) fn parse_drop_schema(&mut self) -> Result<TrinoDropSchema, GrammarError> {
        let start = self.expect_keyword(Keyword::Drop)?;
        self.expect_keyword(Keyword::Schema)?;
        let exists = self.parse_statement_if_exists();
        let schema_name = self.qualified_name()?;
        let cascade = self.parse_drop_cascade();
        Ok(TrinoDropSchema {
            schema_name,
            exists,
            cascade,
            span: self.span_since(start),
        })
    }

    pub(super) fn parse_rename_schema(&mut self) -> Result<TrinoRenameSchema, GrammarError> {
        let start = self.expect_keyword(Keyword::Alter)?;
        self.expect_keyword(Keyword::Schema)?;
        let source = self.qualified_name()?;
        self.expect_keyword(Keyword::Rename)?;
        self.expect_keyword(Keyword::To)?;
        let target = self.ident()?;
        Ok(TrinoRenameSchema {
            source,
            target,
            span: self.span_since(start),
        })
    }

    pub(super) fn parse_statement_if_not_exists(&mut self) -> bool {
        if self.at_keyword(Keyword::If)
            && self.peek(1).kind == TokenKind::Keyword(Keyword::Not)
            && self.peek(2).kind == TokenKind::Keyword(Keyword::Exists)
        {
            self.bump();
            self.bump();
            self.bump();
            true
        } else {
            false
        }
    }

    pub(super) fn parse_statement_if_exists(&mut self) -> bool {
        if self.at_keyword(Keyword::If) && self.peek(1).kind == TokenKind::Keyword(Keyword::Exists)
        {
            self.bump();
            self.bump();
            true
        } else {
            false
        }
    }

    fn parse_drop_cascade(&mut self) -> bool {
        if self.eat_keyword(Keyword::Cascade).is_some() {
            true
        } else {
            self.eat_keyword(Keyword::Restrict);
            false
        }
    }

    fn parse_statement_authorization(
        &mut self,
    ) -> Result<Option<StatementPrincipal>, GrammarError> {
        if self.eat_keyword(Keyword::Authorization).is_none() {
            return Ok(None);
        }
        Ok(Some(self.parse_statement_principal()?))
    }

    pub(super) fn parse_statement_principal(&mut self) -> Result<StatementPrincipal, GrammarError> {
        let kind = match self.peek(0).kind {
            TokenKind::Keyword(Keyword::User) if self.is_identifier(1) => {
                self.bump();
                StatementPrincipalKind::User
            }
            TokenKind::Keyword(Keyword::Role) if self.is_identifier(1) => {
                self.bump();
                StatementPrincipalKind::Role
            }
            _ => StatementPrincipalKind::Unspecified,
        };
        Ok(StatementPrincipal {
            kind,
            name: self.ident()?,
        })
    }

    pub(super) fn parse_optional_statement_properties(
        &mut self,
    ) -> Result<AstVec<StatementProperty>, GrammarError> {
        if self.eat_keyword(Keyword::With).is_none() {
            return Ok(AstVec::new());
        }
        self.parse_parenthesized_statement_properties()
    }

    pub(super) fn parse_parenthesized_statement_properties(
        &mut self,
    ) -> Result<AstVec<StatementProperty>, GrammarError> {
        self.expect_symbol(Symbol::LParen)?;
        let properties = self.parse_statement_property_assignments()?;
        self.expect_symbol(Symbol::RParen)?;
        Ok(properties)
    }

    pub(super) fn parse_statement_property_assignments(
        &mut self,
    ) -> Result<AstVec<StatementProperty>, GrammarError> {
        let mut properties = Vec::new();
        loop {
            let name = self.ident()?;
            let start = name.span;
            self.expect_symbol(Symbol::Eq)?;
            // DEFAULT can also be an expression identifier. The dedicated
            // production consumes it only at the end of this assignment.
            let value = if self.at_keyword(Keyword::Default)
                && matches!(
                    self.peek(1).kind,
                    TokenKind::End | TokenKind::Symbol(Symbol::Comma | Symbol::RParen),
                ) {
                self.bump();
                None
            } else {
                Some(self.parse_expression()?)
            };
            properties.push(StatementProperty {
                name,
                value,
                span: self.span_since(start),
            });
            if self.eat_symbol(Symbol::Comma).is_none() {
                return Ok(properties.into());
            }
        }
    }
}
