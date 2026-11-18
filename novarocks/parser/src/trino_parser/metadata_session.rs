// SPDX-License-Identifier: Apache-2.0

use super::{GrammarError, Parser};
use crate::ast::{
    TrinoPathElement, TrinoPathSpecification, TrinoSetPath, TrinoSetTimeZone, TrinoShowCatalogs,
    TrinoShowColumns, TrinoShowFunctions, TrinoShowSchemas, TrinoShowSession, TrinoShowTables,
    TrinoStatement,
};
use crate::trino_token::{Keyword, Symbol, TokenKind};

impl Parser<'_> {
    pub(super) fn parse_show_metadata(&mut self) -> Result<TrinoStatement, GrammarError> {
        let start = self.expect_keyword(Keyword::Show)?;
        let statement = match self.peek(0).kind {
            TokenKind::Keyword(Keyword::Tables) => {
                self.bump();
                let schema = if self.eat_show_from_or_in() {
                    Some(self.qualified_name()?)
                } else {
                    None
                };
                let (like_pattern, escape) = self.parse_show_like()?;
                TrinoStatement::ShowTables(TrinoShowTables {
                    schema,
                    like_pattern,
                    escape,
                    span: self.span_since(start),
                })
            }
            TokenKind::Keyword(Keyword::Schemas) => {
                self.bump();
                let catalog = if self.eat_show_from_or_in() {
                    Some(self.ident()?)
                } else {
                    None
                };
                let (like_pattern, escape) = self.parse_show_like()?;
                TrinoStatement::ShowSchemas(TrinoShowSchemas {
                    catalog,
                    like_pattern,
                    escape,
                    span: self.span_since(start),
                })
            }
            TokenKind::Keyword(Keyword::Catalogs) => {
                self.bump();
                let (like_pattern, escape) = self.parse_show_like()?;
                TrinoStatement::ShowCatalogs(TrinoShowCatalogs {
                    like_pattern,
                    escape,
                    span: self.span_since(start),
                })
            }
            TokenKind::Keyword(Keyword::Columns) => {
                self.bump();
                if !self.eat_show_from_or_in() {
                    return Err(self.error("FROM or IN"));
                }
                let table = self.qualified_name()?;
                let (like_pattern, escape) = self.parse_show_like()?;
                TrinoStatement::ShowColumns(TrinoShowColumns {
                    table,
                    like_pattern,
                    escape,
                    span: self.span_since(start),
                })
            }
            TokenKind::Keyword(Keyword::Functions) => {
                self.bump();
                let schema = if self.eat_show_from_or_in() {
                    Some(self.qualified_name()?)
                } else {
                    None
                };
                let (like_pattern, escape) = self.parse_show_like()?;
                TrinoStatement::ShowFunctions(TrinoShowFunctions {
                    schema,
                    like_pattern,
                    escape,
                    span: self.span_since(start),
                })
            }
            TokenKind::Keyword(Keyword::Session) => {
                self.bump();
                let (like_pattern, escape) = self.parse_show_like()?;
                TrinoStatement::ShowSession(TrinoShowSession {
                    like_pattern,
                    escape,
                    span: self.span_since(start),
                })
            }

            _ => return Err(self.error("TABLES, SCHEMAS, CATALOGS, COLUMNS, FUNCTIONS or SESSION")),
        };
        Ok(statement)
    }

    pub(super) fn parse_describe_table(&mut self) -> Result<TrinoStatement, GrammarError> {
        let start = self
            .eat_keyword(Keyword::Describe)
            .or_else(|| self.eat_keyword(Keyword::Desc))
            .ok_or_else(|| self.error("DESCRIBE or DESC"))?;
        let table = self.qualified_name()?;
        Ok(TrinoStatement::ShowColumns(TrinoShowColumns {
            table,
            like_pattern: None,
            escape: None,
            span: self.span_since(start),
        }))
    }

    fn eat_show_from_or_in(&mut self) -> bool {
        self.eat_keyword(Keyword::From).is_some() || self.eat_keyword(Keyword::In).is_some()
    }

    fn parse_show_like(&mut self) -> Result<(Option<String>, Option<String>), GrammarError> {
        if self.eat_keyword(Keyword::Like).is_none() {
            return Ok((None, None));
        }
        let (pattern, _) = self.parse_string()?;
        let escape = if self.eat_keyword(Keyword::Escape).is_some() {
            Some(self.parse_string()?.0)
        } else {
            None
        };
        Ok((Some(pattern), escape))
    }

    pub(super) fn parse_set_path(&mut self) -> Result<TrinoSetPath, GrammarError> {
        let start = self.expect_keyword(Keyword::Set)?;
        self.expect_keyword(Keyword::Path)?;
        let path_specification = self.parse_path_specification()?;
        Ok(TrinoSetPath {
            path_specification,
            span: self.span_since(start),
        })
    }

    pub(super) fn parse_path_specification(
        &mut self,
    ) -> Result<TrinoPathSpecification, GrammarError> {
        let start = self.peek(0).span;
        let mut path = Vec::new();
        loop {
            let first = self.ident()?;
            let element_start = first.span;
            let (catalog, schema) = if self.eat_symbol(Symbol::Dot).is_some() {
                (Some(first), self.ident()?)
            } else {
                (None, first)
            };
            path.push(TrinoPathElement {
                catalog,
                schema,
                span: self.span_since(element_start),
            });
            if self.eat_symbol(Symbol::Comma).is_none() {
                break;
            }
        }
        Ok(TrinoPathSpecification {
            path: path.into(),
            span: self.span_since(start),
        })
    }

    pub(super) fn parse_set_time_zone(&mut self) -> Result<TrinoSetTimeZone, GrammarError> {
        let start = self.expect_keyword(Keyword::Set)?;
        self.expect_keyword(Keyword::Time)?;
        self.expect_keyword(Keyword::Zone)?;
        // LOCAL is also a nonreserved identifier in the expression alternative.
        let time_zone = if self.at_keyword(Keyword::Local) && self.peek(1).kind == TokenKind::End {
            self.bump();
            None
        } else {
            Some(self.parse_expression()?)
        };
        Ok(TrinoSetTimeZone {
            time_zone,
            span: self.span_since(start),
        })
    }
}
