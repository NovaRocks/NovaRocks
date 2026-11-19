// SPDX-License-Identifier: Apache-2.0

use super::{GrammarError, Parser};
use crate::ast::{
    AstBox, Expr, LikePropertiesOption, StatementSaveMode, TrinoAnalyze, TrinoColumnDefinition,
    TrinoCreateTable, TrinoDropTable, TrinoLikeClause, TrinoTableElement,
};
use crate::token::{Keyword, Symbol};
use crate::{ParseError, Span};

impl Parser<'_> {
    pub(super) fn parse_create_table(&mut self) -> Result<TrinoCreateTable, GrammarError> {
        let start = self.expect_keyword(Keyword::Create)?;
        let replace = self.parse_statement_or_replace()?;
        self.expect_keyword(Keyword::Table)?;
        let not_exists = self.parse_statement_if_not_exists();
        let name = self.qualified_name()?;
        self.expect_symbol(Symbol::LParen)?;
        let parameter_start = self.next_parameter;
        let mut elements = vec![self.parse_trino_table_element()?];
        while self.eat_symbol(Symbol::Comma).is_some() {
            elements.push(self.parse_trino_table_element()?);
        }
        self.expect_symbol(Symbol::RParen)?;
        let elements_end = self.next_parameter;
        let comment = if self.eat_keyword(Keyword::Comment).is_some() {
            Some(self.parse_string()?.0)
        } else {
            None
        };
        let properties = self.parse_optional_statement_properties()?;
        let end = self.next_parameter;
        let save_mode = statement_save_mode(replace, not_exists, start)?;
        // AstBuilder constructs the trailing properties before visiting
        // table elements, although the SQL spells them in the opposite order.
        self.reorder_parameter_groups(
            parameter_start,
            &[elements_end..end, parameter_start..elements_end],
        );
        Ok(TrinoCreateTable {
            name,
            elements: elements.into(),
            save_mode,
            properties,
            comment,
            span: self.span_since(start),
        })
    }

    pub(super) fn parse_statement_or_replace(&mut self) -> Result<bool, GrammarError> {
        if self.eat_keyword(Keyword::Or).is_none() {
            return Ok(false);
        }
        self.expect_keyword(Keyword::Replace)?;
        Ok(true)
    }

    fn parse_trino_table_element(&mut self) -> Result<TrinoTableElement, GrammarError> {
        if let Some(start) = self.eat_keyword(Keyword::Like) {
            let table_name = self.qualified_name()?;
            let properties_option = if self.eat_keyword(Keyword::Including).is_some() {
                self.expect_keyword(Keyword::Properties)?;
                Some(LikePropertiesOption::Including)
            } else if self.eat_keyword(Keyword::Excluding).is_some() {
                self.expect_keyword(Keyword::Properties)?;
                Some(LikePropertiesOption::Excluding)
            } else {
                None
            };
            Ok(TrinoTableElement::Like(TrinoLikeClause {
                table_name,
                properties_option,
                span: self.span_since(start),
            }))
        } else {
            self.parse_trino_column_definition()
                .map(|column| TrinoTableElement::Column(AstBox::new(column)))
        }
    }

    pub(super) fn parse_trino_column_definition(
        &mut self,
    ) -> Result<TrinoColumnDefinition, GrammarError> {
        let name = self.qualified_name()?;
        let start = name.span;
        let data_type = self.parse_type()?;
        let default_value = if self.eat_keyword(Keyword::Default).is_some() {
            Some(Expr::Literal(self.parse_literal()?))
        } else {
            None
        };
        let nullable = if self.eat_keyword(Keyword::Not).is_some() {
            self.expect_keyword(Keyword::Null)?;
            false
        } else {
            true
        };
        let comment = if self.eat_keyword(Keyword::Comment).is_some() {
            Some(self.parse_string()?.0)
        } else {
            None
        };
        let properties = self.parse_optional_statement_properties()?;
        Ok(TrinoColumnDefinition {
            name,
            data_type,
            default_value,
            nullable,
            properties,
            comment,
            span: self.span_since(start),
        })
    }

    pub(super) fn parse_drop_table(&mut self) -> Result<TrinoDropTable, GrammarError> {
        let start = self.expect_keyword(Keyword::Drop)?;
        self.expect_keyword(Keyword::Table)?;
        let exists = self.parse_statement_if_exists();
        let table_name = self.qualified_name()?;
        Ok(TrinoDropTable {
            table_name,
            exists,
            span: self.span_since(start),
        })
    }

    pub(super) fn parse_analyze(&mut self) -> Result<TrinoAnalyze, GrammarError> {
        let start = self.expect_keyword(Keyword::Analyze)?;
        let table_name = self.qualified_name()?;
        let properties = self.parse_optional_statement_properties()?;
        Ok(TrinoAnalyze {
            table_name,
            properties,
            span: self.span_since(start),
        })
    }
}

/// Convert the two grammar clauses only after reading their actual alternative.
pub(super) fn statement_save_mode(
    replace: bool,
    not_exists: bool,
    start: Span,
) -> Result<StatementSaveMode, GrammarError> {
    match (replace, not_exists) {
        (false, false) => Ok(StatementSaveMode::Fail),
        (false, true) => Ok(StatementSaveMode::Ignore),
        (true, false) => Ok(StatementSaveMode::Replace),
        (true, true) => Err(GrammarError::Construction(
            ParseError::UnexpectedToken {
                expected: "at most one save-mode clause",
                found: "OR REPLACE and IF NOT EXISTS".to_owned(),
                span: start,
            }
            .into(),
        )),
    }
}
