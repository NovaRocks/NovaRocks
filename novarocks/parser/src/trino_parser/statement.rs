// SPDX-License-Identifier: Apache-2.0

use super::{GrammarError, Parser};
use crate::ast::*;
use crate::trino_token::{Keyword, Symbol, TokenKind};

impl Parser<'_> {
    #[recursive::recursive]
    pub(super) fn parse_statement(&mut self) -> Result<TrinoStatement, GrammarError> {
        use Keyword::{
            Authorization, Commit, Explain, Isolation, Read, Reset, Rollback, Session, Set, Start,
            Transaction, Use, Work,
        };

        if let Some(authorization) = self.try_parse_set_authorization_statement()? {
            return Ok(TrinoStatement::SetAuthorization(authorization));
        }
        let statement = match self.peek(0).kind {
            TokenKind::Keyword(Keyword::Create) => {
                let replace = self.peek(1).kind == TokenKind::Keyword(Keyword::Or)
                    && self.peek(2).kind == TokenKind::Keyword(Keyword::Replace);
                match self.peek(if replace { 3 } else { 1 }).kind {
                    TokenKind::Keyword(Keyword::Catalog) if !replace => {
                        TrinoStatement::CreateCatalog(self.parse_create_catalog()?)
                    }
                    TokenKind::Keyword(Keyword::Schema) if !replace => {
                        TrinoStatement::CreateSchema(self.parse_create_schema()?)
                    }
                    TokenKind::Keyword(Keyword::Table) => return self.parse_table_creation(),
                    TokenKind::Keyword(Keyword::View) => {
                        TrinoStatement::CreateView(self.parse_create_view()?)
                    }
                    TokenKind::Keyword(Keyword::Materialized) => {
                        TrinoStatement::CreateMaterializedView(
                            self.parse_create_materialized_view()?,
                        )
                    }
                    TokenKind::Keyword(Keyword::Branch) => {
                        TrinoStatement::CreateBranch(self.parse_create_branch()?)
                    }
                    TokenKind::Keyword(Keyword::Function) => {
                        TrinoStatement::CreateFunction(self.parse_create_function()?)
                    }
                    TokenKind::Keyword(Keyword::Role) if !replace => {
                        TrinoStatement::CreateRole(self.parse_create_role_statement()?)
                    }
                    _ => return Err(self.error("a CREATE statement kind")),
                }
            }
            TokenKind::Keyword(Keyword::Drop) => match self.peek(1).kind {
                TokenKind::Keyword(Keyword::Catalog) => {
                    TrinoStatement::DropCatalog(self.parse_drop_catalog()?)
                }
                TokenKind::Keyword(Keyword::Schema) => {
                    TrinoStatement::DropSchema(self.parse_drop_schema()?)
                }
                TokenKind::Keyword(Keyword::Table) => {
                    TrinoStatement::DropTable(self.parse_drop_table()?)
                }
                TokenKind::Keyword(Keyword::View) => {
                    TrinoStatement::DropView(self.parse_drop_view()?)
                }
                TokenKind::Keyword(Keyword::Materialized) => {
                    TrinoStatement::DropMaterializedView(self.parse_drop_materialized_view()?)
                }
                TokenKind::Keyword(Keyword::Branch) => {
                    TrinoStatement::DropBranch(self.parse_drop_branch()?)
                }
                TokenKind::Keyword(Keyword::Function) => {
                    TrinoStatement::DropFunction(self.parse_drop_function()?)
                }
                TokenKind::Keyword(Keyword::Role) => {
                    TrinoStatement::DropRole(self.parse_drop_role_statement()?)
                }
                _ => return Err(self.error("a DROP statement kind")),
            },
            TokenKind::Keyword(Keyword::Alter)
                if self.peek(1).kind == TokenKind::Keyword(Keyword::Schema) =>
            {
                TrinoStatement::RenameSchema(self.parse_rename_schema()?)
            }
            TokenKind::Keyword(Keyword::Refresh)
                if self.peek(1).kind == TokenKind::Keyword(Keyword::Materialized) =>
            {
                TrinoStatement::RefreshMaterializedView(self.parse_refresh_materialized_view()?)
            }
            TokenKind::Keyword(Keyword::Alter)
                if self.peek(1).kind == TokenKind::Keyword(Keyword::View) =>
            {
                return self.parse_alter_view();
            }
            TokenKind::Keyword(Keyword::Alter)
                if self.peek(1).kind == TokenKind::Keyword(Keyword::Materialized) =>
            {
                return self.parse_alter_materialized_view();
            }
            TokenKind::Keyword(Keyword::Alter)
                if self.peek(1).kind == TokenKind::Keyword(Keyword::Table) =>
            {
                return self.parse_alter_table();
            }
            TokenKind::Keyword(Keyword::Truncate) => {
                TrinoStatement::TruncateTable(self.parse_truncate_table()?)
            }
            TokenKind::Keyword(Keyword::Comment) => TrinoStatement::Comment(self.parse_comment()?),
            TokenKind::Keyword(Keyword::Alter)
                if self.peek(1).kind == TokenKind::Keyword(Keyword::Branch) =>
            {
                TrinoStatement::FastForwardBranch(self.parse_fast_forward_branch()?)
            }
            TokenKind::Keyword(Keyword::Show) => match self.peek(1).kind {
                TokenKind::Keyword(Keyword::Create) => {
                    TrinoStatement::ShowCreate(self.parse_show_create()?)
                }
                TokenKind::Keyword(Keyword::Stats) => {
                    TrinoStatement::ShowStats(self.parse_show_stats()?)
                }
                TokenKind::Keyword(Keyword::Branches) => {
                    TrinoStatement::ShowBranches(self.parse_show_branches()?)
                }
                TokenKind::Keyword(Keyword::Grants) => {
                    TrinoStatement::ShowGrants(self.parse_show_grants_statement()?)
                }
                TokenKind::Keyword(Keyword::Roles | Keyword::Current) => {
                    TrinoStatement::ShowRoles(self.parse_show_roles_statement()?)
                }
                TokenKind::Keyword(Keyword::Role) => {
                    TrinoStatement::ShowRoleGrants(self.parse_show_role_grants_statement()?)
                }
                _ => return self.parse_show_metadata(),
            },
            TokenKind::Keyword(Keyword::Insert) => {
                TrinoStatement::Insert(self.parse_insert_statement()?)
            }
            TokenKind::Keyword(Keyword::Delete) => {
                TrinoStatement::Delete(self.parse_delete_statement()?)
            }
            TokenKind::Keyword(Keyword::Update) => {
                TrinoStatement::Update(self.parse_update_statement()?)
            }
            TokenKind::Keyword(Keyword::Prepare) => TrinoStatement::Prepare(self.parse_prepare()?),
            TokenKind::Keyword(Keyword::Deallocate) => {
                TrinoStatement::Deallocate(self.parse_deallocate()?)
            }
            TokenKind::Keyword(Keyword::Execute) => return self.parse_execute(),
            TokenKind::Keyword(Keyword::Describe)
                if self.peek(1).kind == TokenKind::Keyword(Keyword::Input)
                    && self.is_identifier(2) =>
            {
                TrinoStatement::DescribeInput(self.parse_describe_input()?)
            }
            TokenKind::Keyword(Keyword::Describe)
                if self.peek(1).kind == TokenKind::Keyword(Keyword::Output)
                    && (self.is_identifier(2)
                        || self.peek(2).kind == TokenKind::Symbol(Symbol::LParen)) =>
            {
                TrinoStatement::DescribeOutput(self.parse_describe_output()?)
            }
            TokenKind::Keyword(Keyword::Describe | Keyword::Desc) => {
                return self.parse_describe_table();
            }
            TokenKind::Keyword(Keyword::Grant) => return self.parse_grant_statement(),
            TokenKind::Keyword(Keyword::Revoke) => return self.parse_revoke_statement(),
            TokenKind::Keyword(Keyword::Deny) => TrinoStatement::Deny(self.parse_deny_statement()?),
            TokenKind::Keyword(Set) if self.peek(1).kind == TokenKind::Keyword(Keyword::Role) => {
                TrinoStatement::SetRole(self.parse_set_role_statement()?)
            }
            TokenKind::Keyword(Set) if self.peek(1).kind == TokenKind::Keyword(Keyword::Path) => {
                TrinoStatement::SetPath(self.parse_set_path()?)
            }
            TokenKind::Keyword(Set)
                if self.peek(1).kind == TokenKind::Keyword(Keyword::Time)
                    && self.peek(2).kind == TokenKind::Keyword(Keyword::Zone) =>
            {
                TrinoStatement::SetTimeZone(self.parse_set_time_zone()?)
            }
            TokenKind::Keyword(Keyword::Merge) => {
                TrinoStatement::Merge(self.parse_merge_statement()?)
            }
            TokenKind::Keyword(Keyword::Call) => TrinoStatement::Call(self.parse_call_statement()?),
            TokenKind::Keyword(Keyword::Analyze) => TrinoStatement::Analyze(self.parse_analyze()?),
            TokenKind::Keyword(Use) => {
                let start = self.bump().span;
                let first = self.ident()?;
                let (catalog, schema) = if self.eat_symbol(Symbol::Dot).is_some() {
                    (Some(first), self.ident()?)
                } else {
                    (None, first)
                };
                TrinoStatement::Use(TrinoUse {
                    catalog,
                    schema,
                    span: self.span_since(start),
                })
            }
            TokenKind::Keyword(Set) if self.peek(1).kind == TokenKind::Keyword(Session) => {
                let start = self.bump().span;
                self.expect_keyword(Session)?;
                if self.at_keyword(Authorization)
                    && !matches!(
                        self.peek(1).kind,
                        TokenKind::Symbol(Symbol::Dot | Symbol::Eq)
                    )
                {
                    self.bump();
                    let user = if self.is_identifier(0) {
                        TrinoAuthorizationUser::Identifier(self.ident()?)
                    } else {
                        let (value, span) = self.parse_string()?;
                        TrinoAuthorizationUser::String { value, span }
                    };
                    return Ok(TrinoStatement::SetSessionAuthorization(
                        TrinoSetSessionAuthorization {
                            user,
                            span: self.span_since(start),
                        },
                    ));
                }
                let name = self.qualified_name()?;
                self.expect_symbol(Symbol::Eq)?;
                let value = self.parse_expression()?;
                TrinoStatement::SetSession(TrinoSetSession {
                    name,
                    value,
                    span: self.span_since(start),
                })
            }
            TokenKind::Keyword(Reset) if self.peek(1).kind == TokenKind::Keyword(Session) => {
                let start = self.bump().span;
                self.expect_keyword(Session)?;
                if self.at_keyword(Authorization) && self.peek(1).kind == TokenKind::End {
                    self.bump();
                    return Ok(TrinoStatement::ResetSessionAuthorization(
                        self.span_since(start),
                    ));
                }
                let name = self.qualified_name()?;
                TrinoStatement::ResetSession(TrinoResetSession {
                    name,
                    span: self.span_since(start),
                })
            }
            TokenKind::Keyword(Explain) => return self.parse_explain_statement(),
            TokenKind::Keyword(Start) => {
                let start = self.bump().span;
                self.expect_keyword(Transaction)?;
                let mut modes = Vec::new();
                if self.at_keyword(Isolation) || self.at_keyword(Read) {
                    loop {
                        modes.push(self.parse_transaction_mode()?);
                        if self.eat_symbol(Symbol::Comma).is_none() {
                            break;
                        }
                    }
                }
                TrinoStatement::StartTransaction(TrinoStartTransaction {
                    modes: modes.into(),
                    span: self.span_since(start),
                })
            }
            TokenKind::Keyword(Commit) => {
                let start = self.bump().span;
                self.eat_keyword(Work);
                TrinoStatement::Commit(self.span_since(start))
            }
            TokenKind::Keyword(Rollback) => {
                let start = self.bump().span;
                self.eat_keyword(Work);
                TrinoStatement::Rollback(self.span_since(start))
            }
            _ => TrinoStatement::Query(self.parse_root_query_with_session()?),
        };
        Ok(statement)
    }

    fn parse_table_creation(&mut self) -> Result<TrinoStatement, GrammarError> {
        let checkpoint = self.checkpoint();
        match self.parse_create_table_as_select() {
            Ok(statement) => Ok(TrinoStatement::CreateTableAsSelect(statement)),
            Err(GrammarError::Syntax(_)) => {
                self.restore(checkpoint);
                self.parse_create_table().map(TrinoStatement::CreateTable)
            }
            Err(error @ GrammarError::Construction(_)) => Err(error),
        }
    }

    fn parse_explain_statement(&mut self) -> Result<TrinoStatement, GrammarError> {
        use Keyword::*;

        let start = self.expect_keyword(Explain)?;
        if self.at_keyword(Analyze) {
            // ANALYZE may itself begin a statement, in which case the
            // reference grammar's earlier EXPLAIN statement alternative wins.
            let checkpoint = self.checkpoint();
            match self.parse_statement() {
                Ok(statement) if self.peek(0).kind == TokenKind::End => {
                    return Ok(TrinoStatement::Explain(TrinoExplain {
                        statement: AstBox::new(statement),
                        options: AstVec::new(),
                        span: self.span_since(start),
                    }));
                }
                Ok(_) | Err(GrammarError::Syntax(_)) => self.restore(checkpoint),
                Err(error @ GrammarError::Construction(_)) => return Err(error),
            }
            self.expect_keyword(Analyze)?;
            let verbose = self.eat_keyword(Verbose).is_some();
            let statement = AstBox::new(self.parse_statement()?);
            return Ok(TrinoStatement::ExplainAnalyze(TrinoExplainAnalyze {
                statement,
                verbose,
                span: self.span_since(start),
            }));
        }
        let mut options = Vec::new();
        if self.at_symbol(Symbol::LParen)
            && matches!(self.peek(1).kind, TokenKind::Keyword(Format | Type))
        {
            self.bump();
            loop {
                options.push(self.parse_explain_option()?);
                if self.eat_symbol(Symbol::Comma).is_none() {
                    break;
                }
            }
            self.expect_symbol(Symbol::RParen)?;
        }
        let statement = AstBox::new(self.parse_statement()?);
        Ok(TrinoStatement::Explain(TrinoExplain {
            statement,
            options: options.into(),
            span: self.span_since(start),
        }))
    }

    fn parse_explain_option(&mut self) -> Result<TrinoExplainOption, GrammarError> {
        use Keyword::*;

        let start = self.current().span;
        let kind = if self.eat_keyword(Format).is_some() {
            let value = match self.peek(0).kind {
                TokenKind::Keyword(Text) => TrinoExplainFormat::Text,
                TokenKind::Keyword(Graphviz) => TrinoExplainFormat::Graphviz,
                TokenKind::Keyword(Json) => TrinoExplainFormat::Json,
                _ => return Err(self.error("TEXT, GRAPHVIZ or JSON")),
            };
            TrinoExplainOptionKind::Format(value)
        } else {
            self.expect_keyword(Type)?;
            let value = match self.peek(0).kind {
                TokenKind::Keyword(Logical) => TrinoExplainType::Logical,
                TokenKind::Keyword(Distributed) => TrinoExplainType::Distributed,
                TokenKind::Keyword(Validate) => TrinoExplainType::Validate,
                TokenKind::Keyword(Io) => TrinoExplainType::Io,
                _ => return Err(self.error("LOGICAL, DISTRIBUTED, VALIDATE or IO")),
            };
            TrinoExplainOptionKind::Type(value)
        };
        self.bump();
        Ok(TrinoExplainOption {
            kind,
            span: self.span_since(start),
        })
    }

    fn parse_transaction_mode(&mut self) -> Result<TrinoTransactionMode, GrammarError> {
        use Keyword::*;

        let start = self.current().span;
        let kind = if self.eat_keyword(Isolation).is_some() {
            self.expect_keyword(Level)?;
            let isolation = if self.eat_keyword(Read).is_some() {
                if self.eat_keyword(Uncommitted).is_some() {
                    TrinoIsolationLevel::ReadUncommitted
                } else {
                    self.expect_keyword(Committed)?;
                    TrinoIsolationLevel::ReadCommitted
                }
            } else if self.eat_keyword(Repeatable).is_some() {
                self.expect_keyword(Read)?;
                TrinoIsolationLevel::RepeatableRead
            } else {
                self.expect_keyword(Serializable)?;
                TrinoIsolationLevel::Serializable
            };
            TrinoTransactionModeKind::Isolation(isolation)
        } else {
            self.expect_keyword(Read)?;
            if self.eat_keyword(Only).is_some() {
                TrinoTransactionModeKind::ReadOnly
            } else {
                self.expect_keyword(Write)?;
                TrinoTransactionModeKind::ReadWrite
            }
        };
        Ok(TrinoTransactionMode {
            kind,
            span: self.span_since(start),
        })
    }
}
