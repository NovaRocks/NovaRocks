// SPDX-License-Identifier: Apache-2.0

use super::table_statement::statement_save_mode;
use super::{GrammarError, Parser};
use crate::SYNTHETIC;
use crate::ast::*;
use crate::token::Keyword;

impl Parser<'_> {
    pub(super) fn parse_create_view(&mut self) -> Result<TrinoCreateView, GrammarError> {
        let start = self.expect_keyword(Keyword::Create)?;
        let replace = self.parse_statement_or_replace()?;
        self.expect_keyword(Keyword::View)?;
        let not_exists = self.parse_statement_if_not_exists();
        let name = self.qualified_name()?;
        let comment = self.parse_view_comment()?;
        let security = if self.eat_keyword(Keyword::Security).is_some() {
            Some(if self.eat_keyword(Keyword::Definer).is_some() {
                ViewSecurity::Definer
            } else {
                self.expect_keyword(Keyword::Invoker)?;
                ViewSecurity::Invoker
            })
        } else {
            None
        };
        let properties = self.parse_optional_statement_properties()?;
        statement_save_mode(replace, not_exists, start)?;
        self.expect_keyword(Keyword::As)?;
        let query = self.parse_root_query()?;
        Ok(TrinoCreateView {
            name,
            replace,
            not_exists,
            comment,
            security,
            properties,
            query,
            span: self.span_since(start),
        })
    }

    pub(super) fn parse_create_materialized_view(
        &mut self,
    ) -> Result<TrinoCreateMaterializedView, GrammarError> {
        let start = self.expect_keyword(Keyword::Create)?;
        let replace = self.parse_statement_or_replace()?;
        self.expect_keyword(Keyword::Materialized)?;
        self.expect_keyword(Keyword::View)?;
        let not_exists = self.parse_statement_if_not_exists();
        let name = self.qualified_name()?;
        let grace_period = if self.eat_keyword(Keyword::Grace).is_some() {
            self.expect_keyword(Keyword::Period)?;
            if !self.at_keyword(Keyword::Interval) {
                return Err(self.error("an INTERVAL literal"));
            }
            let interval = self.parse_literal()?;
            if !matches!(interval.kind, LiteralKind::Interval(_)) {
                return Err(self.error("an interval qualifier"));
            }
            Some(interval)
        } else {
            None
        };
        let when_stale = if self.eat_keyword(Keyword::When).is_some() {
            self.expect_keyword(Keyword::Stale)?;
            Some(if self.eat_keyword(Keyword::Inline).is_some() {
                MaterializedViewWhenStale::Inline
            } else {
                self.expect_keyword(Keyword::Fail)?;
                MaterializedViewWhenStale::Fail
            })
        } else {
            None
        };
        let comment = self.parse_view_comment()?;
        let properties = self.parse_optional_statement_properties()?;
        self.expect_keyword(Keyword::As)?;
        let query = self.parse_root_query()?;
        Ok(TrinoCreateMaterializedView {
            name,
            replace,
            not_exists,
            grace_period,
            when_stale,
            comment,
            properties,
            query,
            span: self.span_since(start),
        })
    }

    fn parse_view_comment(&mut self) -> Result<Option<String>, GrammarError> {
        self.eat_keyword(Keyword::Comment)
            .map(|_| self.parse_string().map(|(value, _)| value))
            .transpose()
    }

    pub(super) fn parse_drop_view(&mut self) -> Result<TrinoDropView, GrammarError> {
        let start = self.expect_keyword(Keyword::Drop)?;
        self.expect_keyword(Keyword::View)?;
        let exists = self.parse_statement_if_exists();
        let name = self.qualified_name()?;
        Ok(TrinoDropView {
            name,
            exists,
            span: self.span_since(start),
        })
    }

    pub(super) fn parse_drop_materialized_view(
        &mut self,
    ) -> Result<TrinoDropMaterializedView, GrammarError> {
        let start = self.expect_keyword(Keyword::Drop)?;
        self.expect_keyword(Keyword::Materialized)?;
        self.expect_keyword(Keyword::View)?;
        let exists = self.parse_statement_if_exists();
        let name = self.qualified_name()?;
        Ok(TrinoDropMaterializedView {
            name,
            exists,
            span: self.span_since(start),
        })
    }

    pub(super) fn parse_refresh_materialized_view(
        &mut self,
    ) -> Result<TrinoRefreshMaterializedView, GrammarError> {
        let start = self.expect_keyword(Keyword::Refresh)?;
        self.expect_keyword(Keyword::Materialized)?;
        self.expect_keyword(Keyword::View)?;
        let name = self.qualified_name()?;
        Ok(TrinoRefreshMaterializedView {
            table: Table {
                name,
                branch: None,
                query_period: None,
                span: SYNTHETIC,
            },
            span: self.span_since(start),
        })
    }

    pub(super) fn parse_alter_view(&mut self) -> Result<TrinoStatement, GrammarError> {
        let start = self.expect_keyword(Keyword::Alter)?;
        self.expect_keyword(Keyword::View)?;
        let source = self.qualified_name()?;
        if self.eat_keyword(Keyword::Refresh).is_some() {
            return Ok(TrinoStatement::RefreshView(TrinoRefreshView {
                name: source,
                span: self.span_since(start),
            }));
        }
        self.expect_keyword(Keyword::Rename)?;
        self.expect_keyword(Keyword::To)?;
        let target = self.qualified_name()?;
        Ok(TrinoStatement::RenameView(TrinoRenameView {
            source,
            target,
            span: self.span_since(start),
        }))
    }

    pub(super) fn parse_alter_materialized_view(&mut self) -> Result<TrinoStatement, GrammarError> {
        let start = self.expect_keyword(Keyword::Alter)?;
        let table_start = self.expect_keyword(Keyword::Materialized)?;
        self.expect_keyword(Keyword::View)?;
        let exists = self.parse_statement_if_exists();
        let name = self.qualified_name()?;
        let table_span = self.span_since(table_start);
        if exists || self.at_keyword(Keyword::Rename) {
            self.expect_keyword(Keyword::Rename)?;
            self.expect_keyword(Keyword::To)?;
            let target = self.qualified_name()?;
            return Ok(TrinoStatement::RenameMaterializedView(
                TrinoRenameMaterializedView {
                    source: name,
                    target,
                    exists,
                    span: self.span_since(start),
                },
            ));
        }
        if self.eat_keyword(Keyword::Set).is_some() {
            self.expect_keyword(Keyword::Properties)?;
            let properties = self.parse_statement_property_assignments()?;
            return Ok(TrinoStatement::SetProperties(TrinoSetProperties {
                kind: SetPropertiesKind::MaterializedView,
                name,
                properties,
                span: self.span_since(start),
            }));
        }
        let (procedure_name, arguments, selection) = self.parse_statement_execute_tail()?;
        Ok(TrinoStatement::MaterializedViewExecute(
            TrinoMaterializedViewExecute {
                table: Table {
                    name,
                    branch: None,
                    query_period: None,
                    span: table_span,
                },
                procedure_name,
                arguments,
                selection,
                span: self.span_since(start),
            },
        ))
    }
}
