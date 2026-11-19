// SPDX-License-Identifier: Apache-2.0

use super::{GrammarError, Parser};
use crate::ast::*;
use crate::token::{Keyword as K, Symbol, TokenKind};
use crate::{ParseError, Span};
use std::collections::HashSet;
use std::mem::discriminant;

enum PermissionName {
    Identifier { name: Ident, spelling: String },
    Keyword { spelling: String, span: Span },
}

impl PermissionName {
    fn privilege(self) -> String {
        match self {
            Self::Identifier { spelling, .. } | Self::Keyword { spelling, .. } => spelling,
        }
    }

    fn role(self) -> Result<Ident, GrammarError> {
        match self {
            Self::Identifier { name, .. } => Ok(name),
            Self::Keyword { spelling, span } => Err(GrammarError::Construction(
                ParseError::UnexpectedToken {
                    expected: "a role name (may not use a privilege name as a role name)",
                    found: spelling,
                    span,
                }
                .into(),
            )),
        }
    }
}

impl Parser<'_> {
    pub(super) fn parse_create_role_statement(&mut self) -> Result<TrinoCreateRole, GrammarError> {
        let start = self.expect_keyword(K::Create)?;
        self.expect_keyword(K::Role)?;
        let name = self.ident()?;
        let grantor = if self.eat_keyword(K::With).is_some() {
            self.expect_keyword(K::Admin)?;
            Some(self.parse_permission_grantor()?)
        } else {
            None
        };
        let catalog = self.parse_permission_catalog()?;
        Ok(TrinoCreateRole {
            name,
            grantor,
            catalog,
            span: self.span_since(start),
        })
    }

    pub(super) fn parse_drop_role_statement(&mut self) -> Result<TrinoDropRole, GrammarError> {
        let start = self.expect_keyword(K::Drop)?;
        self.expect_keyword(K::Role)?;
        let if_exists = self.parse_statement_if_exists();
        let name = self.ident()?;
        let catalog = self.parse_permission_catalog()?;
        Ok(TrinoDropRole {
            name,
            if_exists,
            catalog,
            span: self.span_since(start),
        })
    }

    pub(super) fn parse_grant_statement(&mut self) -> Result<TrinoStatement, GrammarError> {
        let start = self.expect_keyword(K::Grant)?;
        let names = self.parse_permission_names()?;
        if self.eat_keyword(K::On).is_some() {
            let (object, (grantee, grant_option)) = self.parse_scoped_grant(start, |parser| {
                parser.expect_keyword(K::To)?;
                let grantee = parser.parse_statement_principal()?;
                let grant_option = parser.eat_keyword(K::With).is_some();
                if grant_option {
                    parser.expect_keyword(K::Grant)?;
                    parser.expect_keyword(K::Option)?;
                }
                Ok((grantee, grant_option))
            })?;
            return Ok(TrinoStatement::Grant(TrinoGrant {
                privileges: names
                    .map(|names| names.into_iter().map(PermissionName::privilege).collect()),
                object,
                grantee,
                grant_option,
                span: self.span_since(start),
            }));
        }

        let names = names.ok_or_else(|| self.error("ON after ALL PRIVILEGES"))?;
        self.expect_keyword(K::To)?;
        let grantees = self.parse_permission_principals()?;
        let admin_option = self.eat_keyword(K::With).is_some();
        if admin_option {
            self.expect_keyword(K::Admin)?;
            self.expect_keyword(K::Option)?;
        }
        let grantor = self.parse_granted_by()?;
        let catalog = self.parse_permission_catalog()?;
        self.expect_permission_boundary()?;
        let roles = unique_permission_roles(names)?;
        Ok(TrinoStatement::GrantRoles(TrinoGrantRoles {
            roles,
            grantees,
            admin_option,
            grantor,
            catalog,
            span: self.span_since(start),
        }))
    }

    pub(super) fn parse_revoke_statement(&mut self) -> Result<TrinoStatement, GrammarError> {
        let start = self.expect_keyword(K::Revoke)?;
        // These prefix words can also be identifiers. Consume only a complete
        // option phrase, leaving a role named ADMIN or GRANT available.
        let option = match (self.peek(0).kind, self.peek(1).kind, self.peek(2).kind) {
            (
                TokenKind::Keyword(keyword @ (K::Admin | K::Grant)),
                TokenKind::Keyword(K::Option),
                TokenKind::Keyword(K::For),
            ) => {
                self.bump();
                self.bump();
                self.bump();
                Some(keyword)
            }
            _ => None,
        };
        let names = self.parse_permission_names()?;
        if self.eat_keyword(K::On).is_some() {
            if option == Some(K::Admin) {
                return Err(self.error("a role revocation after ADMIN OPTION FOR"));
            }
            let (object, grantee) = self.parse_scoped_grant(start, |parser| {
                parser.expect_keyword(K::From)?;
                parser.parse_statement_principal()
            })?;
            return Ok(TrinoStatement::Revoke(TrinoRevoke {
                privileges: names
                    .map(|names| names.into_iter().map(PermissionName::privilege).collect()),
                object,
                grantee,
                grant_option_for: option.is_some(),
                span: self.span_since(start),
            }));
        }

        if option == Some(K::Grant) {
            return Err(self.error("ON after a privilege list"));
        }
        let names = names.ok_or_else(|| self.error("ON after ALL PRIVILEGES"))?;
        self.expect_keyword(K::From)?;
        let grantees = self.parse_permission_principals()?;
        let grantor = self.parse_granted_by()?;
        let catalog = self.parse_permission_catalog()?;
        self.expect_permission_boundary()?;
        let roles = unique_permission_roles(names)?;
        Ok(TrinoStatement::RevokeRoles(TrinoRevokeRoles {
            roles,
            grantees,
            admin_option: option.is_some(),
            grantor,
            catalog,
            span: self.span_since(start),
        }))
    }

    pub(super) fn parse_deny_statement(&mut self) -> Result<TrinoDeny, GrammarError> {
        let start = self.expect_keyword(K::Deny)?;
        let privileges = self
            .parse_permission_names()?
            .map(|names| names.into_iter().map(PermissionName::privilege).collect());
        self.expect_keyword(K::On)?;
        let (object, grantee) = self.parse_scoped_grant(start, |parser| {
            parser.expect_keyword(K::To)?;
            parser.parse_statement_principal()
        })?;
        Ok(TrinoDeny {
            privileges,
            object,
            grantee,
            span: self.span_since(start),
        })
    }

    pub(super) fn parse_set_role_statement(&mut self) -> Result<TrinoSetRole, GrammarError> {
        let start = self.expect_keyword(K::Set)?;
        self.expect_keyword(K::Role)?;
        let selection = if self.eat_keyword(K::All).is_some() {
            TrinoRoleSelection::All
        } else if self.eat_keyword(K::None).is_some() {
            TrinoRoleSelection::None
        } else {
            TrinoRoleSelection::Named(self.ident()?)
        };
        let catalog = self.parse_permission_catalog()?;
        Ok(TrinoSetRole {
            selection,
            catalog,
            span: self.span_since(start),
        })
    }

    pub(super) fn parse_show_grants_statement(&mut self) -> Result<TrinoShowGrants, GrammarError> {
        let start = self.expect_keyword(K::Show)?;
        self.expect_keyword(K::Grants)?;
        let object = if self.eat_keyword(K::On).is_some() {
            Some(self.parse_scoped_grant(start, |_| Ok(()))?.0)
        } else {
            None
        };
        Ok(TrinoShowGrants {
            object,
            span: self.span_since(start),
        })
    }

    pub(super) fn parse_show_roles_statement(&mut self) -> Result<TrinoShowRoles, GrammarError> {
        let start = self.expect_keyword(K::Show)?;
        let current = self.eat_keyword(K::Current).is_some();
        self.expect_keyword(K::Roles)?;
        let catalog = self.parse_show_role_catalog()?;
        Ok(TrinoShowRoles {
            catalog,
            current,
            span: self.span_since(start),
        })
    }

    pub(super) fn parse_show_role_grants_statement(
        &mut self,
    ) -> Result<TrinoShowRoleGrants, GrammarError> {
        let start = self.expect_keyword(K::Show)?;
        self.expect_keyword(K::Role)?;
        self.expect_keyword(K::Grants)?;
        let catalog = self.parse_show_role_catalog()?;
        Ok(TrinoShowRoleGrants {
            catalog,
            span: self.span_since(start),
        })
    }

    /// Commit only once the generic ALTER prefix has selected the
    /// SET AUTHORIZATION production. Other ALTER families retain the cursor.
    pub(super) fn try_parse_set_authorization_statement(
        &mut self,
    ) -> Result<Option<TrinoSetAuthorization>, GrammarError> {
        if !self.at_keyword(K::Alter) {
            return Ok(None);
        }
        let original = self.checkpoint();
        let start = self.bump().span;
        if self.at_keyword(K::Materialized) && self.peek(1).kind == TokenKind::Keyword(K::View) {
            let checkpoint = self.checkpoint();
            if let Some((kind, source)) = self.parse_owned_authorization_head(true)? {
                let principal = self.parse_statement_principal()?;
                return Ok(Some(TrinoSetAuthorization {
                    owned_entity_kind: kind,
                    source,
                    principal,
                    span: self.span_since(start),
                }));
            }
            self.restore(checkpoint);
        }
        if let Some((kind, source)) = self.parse_owned_authorization_head(false)? {
            let principal = self.parse_statement_principal()?;
            return Ok(Some(TrinoSetAuthorization {
                owned_entity_kind: kind,
                source,
                principal,
                span: self.span_since(start),
            }));
        }
        self.restore(original);
        Ok(None)
    }

    fn parse_owned_authorization_head(
        &mut self,
        materialized_view: bool,
    ) -> Result<Option<(String, ObjectName)>, GrammarError> {
        let kind = if materialized_view {
            self.bump();
            self.bump();
            "MATERIALIZED VIEW".to_owned()
        } else if self.at_keyword(K::Table) || self.is_identifier(0) {
            let token = self.bump();
            self.permission_spelling(token.span).to_uppercase()
        } else {
            return Ok(None);
        };
        let source = match self.qualified_name() {
            Ok(source) => source,
            Err(GrammarError::Syntax(_)) => return Ok(None),
            Err(error @ GrammarError::Construction(_)) => return Err(error),
        };
        if self.at_keyword(K::Set) && self.peek(1).kind == TokenKind::Keyword(K::Authorization) {
            self.bump();
            self.bump();
            Ok(Some((kind, source)))
        } else {
            Ok(None)
        }
    }

    fn parse_permission_names(&mut self) -> Result<Option<Vec<PermissionName>>, GrammarError> {
        if self.at_keyword(K::All) && self.peek(1).kind == TokenKind::Keyword(K::Privileges) {
            self.bump();
            self.bump();
            return Ok(None);
        }
        let mut names = vec![self.parse_permission_name()?];
        while self.eat_symbol(Symbol::Comma).is_some() {
            names.push(self.parse_permission_name()?);
        }
        Ok(Some(names))
    }

    fn parse_permission_name(&mut self) -> Result<PermissionName, GrammarError> {
        let token = self.peek(0);
        if matches!(
            token.kind,
            TokenKind::Keyword(K::Create | K::Select | K::Delete | K::Insert | K::Update)
        ) {
            self.bump();
            let mut spelling = self.permission_spelling(token.span).to_owned();
            let mut span = token.span;
            if token.kind == TokenKind::Keyword(K::Create)
                && let Some(branch) = self.eat_keyword(K::Branch)
            {
                spelling.push(' ');
                spelling.push_str(self.permission_spelling(branch));
                span = token.span.cover(branch);
            }
            return Ok(PermissionName::Keyword { spelling, span });
        }
        let name = self.ident()?;
        let spelling = self.permission_spelling(name.span).to_owned();
        Ok(PermissionName::Identifier { name, spelling })
    }

    fn parse_permission_principals(&mut self) -> Result<Vec<StatementPrincipal>, GrammarError> {
        let mut principals = vec![self.parse_statement_principal()?];
        while self.eat_symbol(Symbol::Comma).is_some() {
            principals.push(self.parse_statement_principal()?);
        }
        let mut seen = HashSet::new();
        principals
            .retain(|value| seen.insert((discriminant(&value.kind), value.name.value.clone())));
        Ok(principals)
    }

    fn parse_permission_grantor(&mut self) -> Result<TrinoGrantor, GrammarError> {
        if self.eat_keyword(K::CurrentUser).is_some() {
            Ok(TrinoGrantor::CurrentUser)
        } else if self.eat_keyword(K::CurrentRole).is_some() {
            Ok(TrinoGrantor::CurrentRole)
        } else {
            Ok(TrinoGrantor::Principal(self.parse_statement_principal()?))
        }
    }

    fn parse_granted_by(&mut self) -> Result<Option<TrinoGrantor>, GrammarError> {
        if self.eat_keyword(K::Granted).is_some() {
            self.expect_keyword(K::By)?;
            Ok(Some(self.parse_permission_grantor()?))
        } else {
            Ok(None)
        }
    }

    fn parse_permission_catalog(&mut self) -> Result<Option<Ident>, GrammarError> {
        if self.eat_keyword(K::In).is_some() {
            self.ident().map(Some)
        } else {
            Ok(None)
        }
    }

    fn parse_show_role_catalog(&mut self) -> Result<Option<Ident>, GrammarError> {
        if self.eat_keyword(K::From).is_some() {
            self.ident().map(Some)
        } else {
            self.parse_permission_catalog()
        }
    }

    fn parse_scoped_grant<T>(
        &mut self,
        statement_start: Span,
        mut tail: impl FnMut(&mut Self) -> Result<T, GrammarError>,
    ) -> Result<(TrinoGrantObject, T), GrammarError> {
        let branch = if self.at_keyword(K::Branch)
            && self.is_identifier(1)
            && self.peek(2).kind == TokenKind::Keyword(K::In)
        {
            self.bump();
            let branch = self.ident()?;
            self.expect_keyword(K::In)?;
            Some(branch)
        } else {
            None
        };

        if (self.at_keyword(K::Table) || self.is_identifier(0)) && self.is_identifier(1) {
            let can_omit_kind = self.is_identifier(0);
            let checkpoint = self.checkpoint();
            let kind = self.bump().span;
            let entity_kind = self.permission_spelling(kind).to_owned();
            let with_kind = (|| {
                let name = self.qualified_name()?;
                let span = self.span_since(statement_start);
                let value = tail(self)?;
                self.expect_permission_boundary()?;
                Ok((name, span, value))
            })();
            match with_kind {
                Ok((name, span, value)) => {
                    return Ok((
                        TrinoGrantObject {
                            entity_kind: Some(entity_kind),
                            name,
                            branch,
                            span,
                        },
                        value,
                    ));
                }
                Err(error @ GrammarError::Construction(_)) => return Err(error),
                Err(error @ GrammarError::Syntax(_)) if !can_omit_kind => {
                    return Err(error);
                }
                Err(GrammarError::Syntax(_)) => self.restore(checkpoint),
            }
        }

        let name = self.qualified_name()?;
        let object = TrinoGrantObject {
            entity_kind: None,
            name,
            branch,
            span: self.span_since(statement_start),
        };
        let value = tail(self)?;
        self.expect_permission_boundary()?;
        Ok((object, value))
    }

    fn permission_spelling(&self, span: Span) -> &str {
        &self.source[span.start()..span.end()]
    }

    fn expect_permission_boundary(&self) -> Result<(), GrammarError> {
        if self.grammar_boundary_at_current()
            || matches!(
                self.peek(0).kind,
                TokenKind::End | TokenKind::Symbol(Symbol::Semicolon)
            )
        {
            Ok(())
        } else {
            Err(self.error("the end of the permission statement"))
        }
    }
}

fn unique_permission_roles(names: Vec<PermissionName>) -> Result<Vec<Ident>, GrammarError> {
    let mut seen = HashSet::new();
    let mut roles = Vec::with_capacity(names.len());
    for value in names {
        let role = value.role()?;
        if seen.insert(role.value.clone()) {
            roles.push(role);
        }
    }
    Ok(roles)
}
