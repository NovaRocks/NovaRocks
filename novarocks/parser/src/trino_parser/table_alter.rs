// SPDX-License-Identifier: Apache-2.0

use super::{GrammarError, Parser};
use crate::Span;
use crate::ast::{
    AstBox, AstVec, CallArgument, CommentKind, Expr, Ident, ObjectName, SetPropertiesKind, Table,
    TrinoAddColumn, TrinoColumnPosition, TrinoComment, TrinoDropColumn, TrinoDropDefaultValue,
    TrinoDropNotNullConstraint, TrinoRenameColumn, TrinoRenameTable, TrinoSetColumnType,
    TrinoSetDefaultValue, TrinoSetProperties, TrinoStatement, TrinoTableExecute,
    TrinoTruncateTable,
};
use crate::trino_token::{Keyword, Symbol, TokenKind};

impl Parser<'_> {
    pub(super) fn parse_alter_table(&mut self) -> Result<TrinoStatement, GrammarError> {
        let start = self.expect_keyword(Keyword::Alter)?;
        let table_start = self.expect_keyword(Keyword::Table)?;
        let exists = self.parse_statement_if_exists();
        let table = self.qualified_name()?;
        let statement = match self.peek(0).kind {
            TokenKind::Keyword(Keyword::Rename) => {
                self.bump();
                if self.eat_keyword(Keyword::Column).is_some() {
                    let column_exists = self.parse_statement_if_exists();
                    let source = self.qualified_name()?;
                    self.expect_keyword(Keyword::To)?;
                    let target = self.ident()?;
                    TrinoStatement::RenameColumn(TrinoRenameColumn {
                        table,
                        source,
                        target,
                        table_exists: exists,
                        column_exists,
                        span: self.span_since(start),
                    })
                } else {
                    self.expect_keyword(Keyword::To)?;
                    let target = self.qualified_name()?;
                    TrinoStatement::RenameTable(TrinoRenameTable {
                        source: table,
                        target,
                        exists,
                        span: self.span_since(start),
                    })
                }
            }
            TokenKind::Keyword(Keyword::Add) => {
                self.bump();
                self.expect_keyword(Keyword::Column)?;
                let column_not_exists = self.parse_statement_if_not_exists();
                let column = AstBox::new(self.parse_trino_column_definition()?);
                let position = match self.peek(0).kind {
                    TokenKind::Keyword(Keyword::First) => {
                        self.bump();
                        Some(TrinoColumnPosition::First)
                    }
                    TokenKind::Keyword(Keyword::Last) => {
                        self.bump();
                        Some(TrinoColumnPosition::Last)
                    }
                    TokenKind::Keyword(Keyword::After) => {
                        self.bump();
                        Some(TrinoColumnPosition::After(self.ident()?))
                    }
                    _ => None,
                };
                TrinoStatement::AddColumn(TrinoAddColumn {
                    name: table,
                    column,
                    position,
                    table_exists: exists,
                    column_not_exists,
                    span: self.span_since(start),
                })
            }
            TokenKind::Keyword(Keyword::Drop) => {
                self.bump();
                self.expect_keyword(Keyword::Column)?;
                let column_exists = self.parse_statement_if_exists();
                let field = self.qualified_name()?;
                TrinoStatement::DropColumn(TrinoDropColumn {
                    table,
                    field,
                    table_exists: exists,
                    column_exists,
                    span: self.span_since(start),
                })
            }
            TokenKind::Keyword(Keyword::Alter) => {
                self.bump();
                self.expect_keyword(Keyword::Column)?;
                return self.parse_alter_table_column(start, table, exists);
            }
            TokenKind::Keyword(Keyword::Set) if !exists => {
                self.bump();
                self.expect_keyword(Keyword::Properties)?;
                let properties = self.parse_statement_property_assignments()?;
                TrinoStatement::SetProperties(TrinoSetProperties {
                    kind: SetPropertiesKind::Table,
                    name: table,
                    properties,
                    span: self.span_since(start),
                })
            }
            TokenKind::Keyword(Keyword::Execute) if !exists => {
                let table_span = self.span_since(table_start);
                let (procedure_name, arguments, selection) = self.parse_statement_execute_tail()?;
                TrinoStatement::TableExecute(TrinoTableExecute {
                    table: Table {
                        name: table,
                        query_period: None,
                        branch: None,
                        span: table_span,
                    },
                    procedure_name,
                    arguments,
                    selection,
                    span: self.span_since(start),
                })
            }
            _ => return Err(self.error("a table alteration")),
        };
        Ok(statement)
    }

    fn parse_alter_table_column(
        &mut self,
        start: Span,
        table_name: ObjectName,
        table_exists: bool,
    ) -> Result<TrinoStatement, GrammarError> {
        // DROP NOT NULL is the only ALTER COLUMN alternative whose name is
        // an identifier rather than a qualifiedName.
        if self.is_identifier(0)
            && self.peek(1).kind == TokenKind::Keyword(Keyword::Drop)
            && self.peek(2).kind == TokenKind::Keyword(Keyword::Not)
        {
            let column = self.ident()?;
            self.expect_keyword(Keyword::Drop)?;
            self.expect_keyword(Keyword::Not)?;
            self.expect_keyword(Keyword::Null)?;
            return Ok(TrinoStatement::DropNotNullConstraint(
                TrinoDropNotNullConstraint {
                    table: table_name,
                    column,
                    table_exists,
                    span: self.span_since(start),
                },
            ));
        }
        let column_name = self.qualified_name()?;
        if self.eat_keyword(Keyword::Drop).is_some() {
            self.expect_keyword(Keyword::Default)?;
            return Ok(TrinoStatement::DropDefaultValue(TrinoDropDefaultValue {
                table_name,
                column_name,
                table_exists,
                span: self.span_since(start),
            }));
        }
        self.expect_keyword(Keyword::Set)?;
        if self.eat_keyword(Keyword::Default).is_some() {
            let default_value = Expr::Literal(self.parse_literal()?);
            return Ok(TrinoStatement::SetDefaultValue(TrinoSetDefaultValue {
                table_name,
                column_name,
                default_value,
                table_exists,
                span: self.span_since(start),
            }));
        }
        self.expect_keyword(Keyword::Data)?;
        self.expect_keyword(Keyword::Type)?;
        let data_type = self.parse_type()?;
        Ok(TrinoStatement::SetColumnType(TrinoSetColumnType {
            table_name,
            column_name,
            data_type,
            table_exists,
            span: self.span_since(start),
        }))
    }

    pub(super) fn parse_statement_execute_tail(
        &mut self,
    ) -> Result<(Ident, AstVec<CallArgument>, Option<Expr>), GrammarError> {
        self.expect_keyword(Keyword::Execute)?;
        let procedure_name = self.ident()?;
        let mut arguments = Vec::new();
        if self.eat_symbol(Symbol::LParen).is_some() {
            if !self.at_symbol(Symbol::RParen) {
                arguments.push(self.parse_call_argument()?);
                while self.eat_symbol(Symbol::Comma).is_some() {
                    arguments.push(self.parse_call_argument()?);
                }
            }
            self.expect_symbol(Symbol::RParen)?;
        }
        let selection = if self.eat_keyword(Keyword::Where).is_some() {
            Some(self.parse_expression()?)
        } else {
            None
        };
        Ok((procedure_name, arguments.into(), selection))
    }

    pub(super) fn parse_truncate_table(&mut self) -> Result<TrinoTruncateTable, GrammarError> {
        let start = self.expect_keyword(Keyword::Truncate)?;
        self.expect_keyword(Keyword::Table)?;
        let table_name = self.qualified_name()?;
        Ok(TrinoTruncateTable {
            table_name,
            span: self.span_since(start),
        })
    }

    pub(super) fn parse_comment(&mut self) -> Result<TrinoComment, GrammarError> {
        let start = self.expect_keyword(Keyword::Comment)?;
        self.expect_keyword(Keyword::On)?;
        let kind = match self.peek(0).kind {
            TokenKind::Keyword(Keyword::Table) => {
                self.bump();
                CommentKind::Table
            }
            TokenKind::Keyword(Keyword::View) => {
                self.bump();
                CommentKind::View
            }
            TokenKind::Keyword(Keyword::Materialized) => {
                self.bump();
                self.expect_keyword(Keyword::View)?;
                CommentKind::MaterializedView
            }
            TokenKind::Keyword(Keyword::Column) => {
                self.bump();
                CommentKind::Column
            }
            _ => return Err(self.error("TABLE, VIEW, MATERIALIZED VIEW or COLUMN")),
        };
        let name = self.qualified_name()?;
        self.expect_keyword(Keyword::Is)?;
        let comment = if self.eat_keyword(Keyword::Null).is_some() {
            None
        } else {
            Some(self.parse_string()?.0)
        };
        Ok(TrinoComment {
            kind,
            name,
            comment,
            span: self.span_since(start),
        })
    }
}
