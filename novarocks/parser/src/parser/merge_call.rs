// SPDX-License-Identifier: Apache-2.0

use super::{GrammarError, Parser};
use crate::ast::{
    AliasedRelation, AstBox, Relation, TrinoCall, TrinoMerge, TrinoMergeAssignment, TrinoMergeCase,
};
use crate::token::{Keyword, Symbol};

impl Parser<'_> {
    pub(super) fn parse_merge_statement(&mut self) -> Result<TrinoMerge, GrammarError> {
        let start = self.expect_keyword(Keyword::Merge)?;
        self.expect_keyword(Keyword::Into)?;
        let table = self.parse_write_statement_target(start)?;
        let name_start = table.name.span;
        let mut target = Relation::Table(AstBox::new(table));
        let explicit_alias = self.eat_keyword(Keyword::As).is_some();
        if explicit_alias || self.is_identifier(0) {
            let alias = self.ident()?;
            target = Relation::Aliased(AstBox::new(AliasedRelation {
                relation: AstBox::new(target),
                alias,
                column_names: None,
                span: self.span_since(name_start),
            }));
        }
        self.expect_keyword(Keyword::Using)?;
        let source = self.parse_relation()?;
        self.expect_keyword(Keyword::On)?;
        let predicate = self.parse_expression()?;
        let mut cases = vec![self.parse_merge_case()?];
        while self.at_keyword(Keyword::When) {
            cases.push(self.parse_merge_case()?);
        }
        Ok(TrinoMerge {
            target,
            source,
            predicate,
            cases: cases.into(),
            span: self.span_since(start),
        })
    }

    pub(super) fn parse_call_statement(&mut self) -> Result<TrinoCall, GrammarError> {
        let start = self.expect_keyword(Keyword::Call)?;
        let name = self.qualified_name()?;
        self.expect_symbol(Symbol::LParen)?;
        let mut arguments = Vec::new();
        if !self.at_symbol(Symbol::RParen) {
            arguments.push(self.parse_call_argument()?);
            while self.eat_symbol(Symbol::Comma).is_some() {
                arguments.push(self.parse_call_argument()?);
            }
        }
        self.expect_symbol(Symbol::RParen)?;
        Ok(TrinoCall {
            name,
            arguments: arguments.into(),
            span: self.span_since(start),
        })
    }

    fn parse_merge_case(&mut self) -> Result<TrinoMergeCase, GrammarError> {
        let start = self.expect_keyword(Keyword::When)?;
        let matched = self.eat_keyword(Keyword::Not).is_none();
        self.expect_keyword(Keyword::Matched)?;
        let parameter_start = self.next_parameter;
        let condition = if self.eat_keyword(Keyword::And).is_some() {
            Some(self.parse_expression()?)
        } else {
            None
        };
        let condition_end = self.next_parameter;
        self.expect_keyword(Keyword::Then)?;
        if !matched {
            self.expect_keyword(Keyword::Insert)?;
            let columns = if self.at_symbol(Symbol::LParen) {
                self.parse_column_aliases()?
            } else {
                Vec::new()
            };
            self.expect_keyword(Keyword::Values)?;
            self.expect_symbol(Symbol::LParen)?;
            let mut values = vec![self.parse_expression()?];
            while self.eat_symbol(Symbol::Comma).is_some() {
                values.push(self.parse_expression()?);
            }
            self.expect_symbol(Symbol::RParen)?;
            return Ok(TrinoMergeCase::Insert {
                condition,
                columns,
                values: values.into(),
                span: self.span_since(start),
            });
        }
        if self.eat_keyword(Keyword::Delete).is_some() {
            return Ok(TrinoMergeCase::Delete {
                condition,
                span: self.span_since(start),
            });
        }
        self.expect_keyword(Keyword::Update)?;
        self.expect_keyword(Keyword::Set)?;
        let mut assignments = vec![self.parse_merge_assignment()?];
        while self.eat_symbol(Symbol::Comma).is_some() {
            assignments.push(self.parse_merge_assignment()?);
        }
        let parameter_end = self.next_parameter;
        // AstBuilder constructs assignment values before the earlier WHEN condition.
        self.reorder_parameter_groups(
            parameter_start,
            &[condition_end..parameter_end, parameter_start..condition_end],
        );
        Ok(TrinoMergeCase::Update {
            condition,
            assignments: assignments.into(),
            span: self.span_since(start),
        })
    }

    fn parse_merge_assignment(&mut self) -> Result<TrinoMergeAssignment, GrammarError> {
        let target = self.ident()?;
        let start = target.span;
        self.expect_symbol(Symbol::Eq)?;
        let value = self.parse_expression()?;
        Ok(TrinoMergeAssignment {
            target,
            value,
            span: self.span_since(start),
        })
    }
}
