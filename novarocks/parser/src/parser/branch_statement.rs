// SPDX-License-Identifier: Apache-2.0

use super::{GrammarError, Parser, table_statement::statement_save_mode};
use crate::ast::{TrinoCreateBranch, TrinoDropBranch, TrinoFastForwardBranch, TrinoShowBranches};
use crate::token::Keyword;

impl Parser<'_> {
    pub(super) fn parse_create_branch(&mut self) -> Result<TrinoCreateBranch, GrammarError> {
        let start = self.expect_keyword(Keyword::Create)?;
        let replace = self.parse_statement_or_replace()?;
        self.expect_keyword(Keyword::Branch)?;
        let not_exists = self.parse_statement_if_not_exists();
        let branch_name = self.ident()?;
        // The branch builder checks the incompatible save clauses before
        // constructing properties. A grammar error still wins.
        let save_mode = statement_save_mode(replace, not_exists, start);
        let properties = match self.parse_optional_statement_properties() {
            Ok(properties) => properties,
            Err(error @ GrammarError::Construction(_)) => {
                return Err(save_mode.err().unwrap_or(error));
            }
            Err(error @ GrammarError::Syntax(_)) => return Err(error),
        };
        self.expect_keyword(Keyword::In)?;
        self.expect_keyword(Keyword::Table)?;
        let table_name = self.qualified_name()?;
        let from_branch = if self.eat_keyword(Keyword::From).is_some() {
            Some(self.ident()?)
        } else {
            None
        };
        Ok(TrinoCreateBranch {
            table_name,
            branch_name,
            from_branch,
            save_mode: save_mode?,
            properties,
            span: self.span_since(start),
        })
    }

    pub(super) fn parse_drop_branch(&mut self) -> Result<TrinoDropBranch, GrammarError> {
        let start = self.expect_keyword(Keyword::Drop)?;
        self.expect_keyword(Keyword::Branch)?;
        let exists = self.parse_statement_if_exists();
        let branch_name = self.ident()?;
        self.expect_keyword(Keyword::In)?;
        self.expect_keyword(Keyword::Table)?;
        let table_name = self.qualified_name()?;
        Ok(TrinoDropBranch {
            table_name,
            branch_name,
            exists,
            span: self.span_since(start),
        })
    }

    pub(super) fn parse_fast_forward_branch(
        &mut self,
    ) -> Result<TrinoFastForwardBranch, GrammarError> {
        let start = self.expect_keyword(Keyword::Alter)?;
        self.expect_keyword(Keyword::Branch)?;
        let source_branch_name = self.ident()?;
        self.expect_keyword(Keyword::In)?;
        self.expect_keyword(Keyword::Table)?;
        let table_name = self.qualified_name()?;
        self.expect_keyword(Keyword::Fast)?;
        self.expect_keyword(Keyword::Forward)?;
        self.expect_keyword(Keyword::To)?;
        let target_branch_name = self.ident()?;
        Ok(TrinoFastForwardBranch {
            table_name,
            source_branch_name,
            target_branch_name,
            span: self.span_since(start),
        })
    }

    pub(super) fn parse_show_branches(&mut self) -> Result<TrinoShowBranches, GrammarError> {
        let start = self.expect_keyword(Keyword::Show)?;
        self.expect_keyword(Keyword::Branches)?;
        if self.eat_keyword(Keyword::From).is_none() {
            self.expect_keyword(Keyword::In)?;
        }
        self.expect_keyword(Keyword::Table)?;
        let table_name = self.qualified_name()?;
        Ok(TrinoShowBranches {
            table_name,
            span: self.span_since(start),
        })
    }
}
