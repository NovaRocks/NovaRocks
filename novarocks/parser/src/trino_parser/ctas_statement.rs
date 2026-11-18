// SPDX-License-Identifier: Apache-2.0

use super::{GrammarError, Parser, table_statement::statement_save_mode};
use crate::ast::{AstBox, Query, TrinoCreateTableAsSelect};
use crate::trino_token::{Keyword, Symbol};

impl Parser<'_> {
    pub(super) fn parse_create_table_as_select(
        &mut self,
    ) -> Result<TrinoCreateTableAsSelect, GrammarError> {
        let start = self.expect_keyword(Keyword::Create)?;
        let replace = self.parse_statement_or_replace()?;
        self.expect_keyword(Keyword::Table)?;
        let not_exists = self.parse_statement_if_not_exists();
        let name = self.qualified_name()?;
        let column_aliases = if self.at_symbol(Symbol::LParen) {
            Some(self.parse_column_aliases()?)
        } else {
            None
        };
        let comment = if self.eat_keyword(Keyword::Comment).is_some() {
            Some(self.parse_string()?.0)
        } else {
            None
        };
        let properties = self.parse_optional_statement_properties()?;
        self.expect_keyword(Keyword::As)?;
        // The grammar admits the query syntax before AstBuilder checks
        // save mode. Among construction errors, the mode precedes the query.
        let save_mode = statement_save_mode(replace, not_exists, start);
        let query = match self.parse_ctas_root_query() {
            Ok(query) => query,
            Err(error @ GrammarError::Construction(_)) => {
                return Err(save_mode.err().unwrap_or(error));
            }
            Err(error @ GrammarError::Syntax(_)) => return Err(error),
        };
        let with_data = if self.eat_keyword(Keyword::With).is_some() {
            let with_data = self.eat_keyword(Keyword::No).is_none();
            self.expect_keyword(Keyword::Data)?;
            with_data
        } else {
            true
        };
        let save_mode = save_mode?;
        Ok(TrinoCreateTableAsSelect {
            name,
            query: AstBox::new(query),
            save_mode,
            properties,
            with_data,
            column_aliases,
            comment,
            span: self.span_since(start),
        })
    }

    /// CTAS puts rootQuery before the explicit parenthesized root alternative.
    /// A successful queryPrimary subquery is part of the query's actual tree.
    fn parse_ctas_root_query(&mut self) -> Result<Query, GrammarError> {
        if !self.at_symbol(Symbol::LParen) {
            return self.parse_root_query();
        }
        let checkpoint = self.checkpoint();
        match self.parse_root_query() {
            Ok(query) => Ok(query),
            Err(GrammarError::Syntax(_)) => {
                self.restore(checkpoint);
                self.expect_symbol(Symbol::LParen)?;
                let query = self.parse_root_query()?;
                self.expect_symbol(Symbol::RParen)?;
                Ok(query)
            }
            Err(error @ GrammarError::Construction(_)) => Err(error),
        }
    }
}
