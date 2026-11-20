// SPDX-License-Identifier: Apache-2.0

use super::{PrintResult, Writer, unsupported};
use crate::ast::*;
use crate::token::{Keyword as K, Token, TokenKind};
use crate::{KeywordClass, keyword, lexer};

impl Writer {
    pub(super) fn create_role_statement(&mut self, node: &TrinoCreateRole) {
        self.text("CREATE ROLE ");
        self.identifier(&node.name);
        if let Some(grantor) = &node.grantor {
            self.text(" WITH ADMIN ");
            self.permission_grantor(grantor);
        }
        self.permission_catalog(node.catalog.as_ref(), " IN ");
    }

    pub(super) fn drop_role_statement(&mut self, node: &TrinoDropRole) {
        self.text("DROP ROLE ");
        if node.if_exists {
            self.text("IF EXISTS ");
        }
        self.identifier(&node.name);
        self.permission_catalog(node.catalog.as_ref(), " IN ");
    }

    pub(super) fn grant_roles_statement(&mut self, node: &TrinoGrantRoles) -> PrintResult {
        self.text("GRANT ");
        self.permission_roles(&node.roles, &node.grantees, " TO ")?;
        if node.admin_option {
            self.text(" WITH ADMIN OPTION");
        }
        self.permission_grantor_catalog(node.grantor.as_ref(), node.catalog.as_ref());
        Ok(())
    }

    pub(super) fn revoke_roles_statement(&mut self, node: &TrinoRevokeRoles) -> PrintResult {
        self.text("REVOKE ");
        if node.admin_option {
            self.text("ADMIN OPTION FOR ");
        }
        self.permission_roles(&node.roles, &node.grantees, " FROM ")?;
        self.permission_grantor_catalog(node.grantor.as_ref(), node.catalog.as_ref());
        Ok(())
    }

    pub(super) fn grant_statement(&mut self, node: &TrinoGrant) -> PrintResult {
        self.text("GRANT ");
        self.permission_privileges(node.privileges.as_deref())?;
        self.text(" ON ");
        self.permission_grant_object(&node.object)?;
        self.text(" TO ");
        self.statement_principal(&node.grantee);
        if node.grant_option {
            self.text(" WITH GRANT OPTION");
        }
        Ok(())
    }

    pub(super) fn revoke_statement(&mut self, node: &TrinoRevoke) -> PrintResult {
        self.text("REVOKE ");
        if node.grant_option_for {
            self.text("GRANT OPTION FOR ");
        }
        self.permission_privileges(node.privileges.as_deref())?;
        self.text(" ON ");
        self.permission_grant_object(&node.object)?;
        self.text(" FROM ");
        self.statement_principal(&node.grantee);
        Ok(())
    }

    pub(super) fn deny_statement(&mut self, node: &TrinoDeny) -> PrintResult {
        self.text("DENY ");
        self.permission_privileges(node.privileges.as_deref())?;
        self.text(" ON ");
        self.permission_grant_object(&node.object)?;
        self.text(" TO ");
        self.statement_principal(&node.grantee);
        Ok(())
    }

    pub(super) fn set_role_statement(&mut self, node: &TrinoSetRole) -> PrintResult {
        self.text("SET ROLE ");
        match &node.selection {
            TrinoRoleSelection::All => self.text("ALL"),
            TrinoRoleSelection::None => self.text("NONE"),
            TrinoRoleSelection::Named(role) => {
                if !role.quoted
                    && (role.value.eq_ignore_ascii_case("all")
                        || role.value.eq_ignore_ascii_case("none"))
                {
                    return Err(unsupported(
                        "an unquoted ALL or NONE used as a SET ROLE name",
                    ));
                }
                self.identifier(role);
            }
        }
        self.permission_catalog(node.catalog.as_ref(), " IN ");
        Ok(())
    }

    pub(super) fn show_grants_statement(&mut self, node: &TrinoShowGrants) -> PrintResult {
        self.text("SHOW GRANTS");
        if let Some(object) = &node.object {
            self.text(" ON ");
            self.permission_grant_object(object)?;
        }
        Ok(())
    }

    pub(super) fn show_roles_statement(&mut self, node: &TrinoShowRoles) {
        self.text(if node.current {
            "SHOW CURRENT ROLES"
        } else {
            "SHOW ROLES"
        });
        self.permission_catalog(node.catalog.as_ref(), " FROM ");
    }

    pub(super) fn show_role_grants_statement(&mut self, node: &TrinoShowRoleGrants) {
        self.text("SHOW ROLE GRANTS");
        self.permission_catalog(node.catalog.as_ref(), " FROM ");
    }

    pub(super) fn set_authorization_statement(
        &mut self,
        node: &TrinoSetAuthorization,
    ) -> PrintResult {
        let kind = &node.owned_entity_kind;
        if kind != "MATERIALIZED VIEW"
            && (!permission_entity_kind(kind) || *kind != kind.to_uppercase())
        {
            return Err(unsupported("a non-canonical authorization entity kind"));
        }
        self.text("ALTER ");
        self.text(kind);
        self.character(' ');
        self.name(&node.source);
        self.text(" SET AUTHORIZATION ");
        self.statement_principal(&node.principal);
        Ok(())
    }

    fn permission_grantor(&mut self, grantor: &TrinoGrantor) {
        match grantor {
            TrinoGrantor::CurrentUser => self.text("CURRENT_USER"),
            TrinoGrantor::CurrentRole => self.text("CURRENT_ROLE"),
            TrinoGrantor::Principal(principal) => self.statement_principal(principal),
        }
    }

    fn permission_catalog(&mut self, catalog: Option<&Ident>, prefix: &str) {
        if let Some(catalog) = catalog {
            self.text(prefix);
            self.identifier(catalog);
        }
    }

    fn permission_grantor_catalog(
        &mut self,
        grantor: Option<&TrinoGrantor>,
        catalog: Option<&Ident>,
    ) {
        if let Some(grantor) = grantor {
            self.text(" GRANTED BY ");
            self.permission_grantor(grantor);
        }
        self.permission_catalog(catalog, " IN ");
    }

    fn permission_roles(
        &mut self,
        roles: &[Ident],
        grantees: &[StatementPrincipal],
        separator: &str,
    ) -> PrintResult {
        if roles.is_empty() || grantees.is_empty() {
            return Err(unsupported("an empty permission role or grantee list"));
        }
        if roles.iter().any(|role| {
            !role.quoted
                && ["CREATE", "SELECT", "DELETE", "INSERT", "UPDATE"]
                    .iter()
                    .any(|value| role.value.eq_ignore_ascii_case(value))
        }) {
            return Err(unsupported("a privilege keyword used as a role name"));
        }
        self.identifiers(roles);
        self.text(separator);
        for (index, grantee) in grantees.iter().enumerate() {
            if index != 0 {
                self.text(", ");
            }
            self.statement_principal(grantee);
        }
        Ok(())
    }

    fn permission_grant_object(&mut self, object: &TrinoGrantObject) -> PrintResult {
        if let Some(branch) = &object.branch {
            self.text("BRANCH ");
            self.identifier(branch);
            self.text(" IN ");
        }
        if let Some(kind) = &object.entity_kind {
            if !permission_entity_kind(kind) {
                return Err(unsupported("a non-token grant entity kind"));
            }
            self.text(kind);
            self.character(' ');
        }
        self.name(&object.name);
        Ok(())
    }

    fn permission_privileges(&mut self, privileges: Option<&[String]>) -> PrintResult {
        let Some(privileges) = privileges else {
            self.text("ALL PRIVILEGES");
            return Ok(());
        };
        if privileges.is_empty() || privileges.iter().any(|name| !permission_privilege(name)) {
            return Err(unsupported(
                "a privilege list without spellable privilege names",
            ));
        }
        for (index, name) in privileges.iter().enumerate() {
            if index != 0 {
                self.text(", ");
            }
            self.text(name);
        }
        Ok(())
    }
}

fn permission_tokens(source: &str) -> Option<Vec<Token>> {
    Some(
        lexer::lex(source)
            .ok()?
            .into_iter()
            .filter(|token| !matches!(token.kind, TokenKind::Trivia(_) | TokenKind::End))
            .collect(),
    )
}

fn permission_identifier(token: Token) -> bool {
    match token.kind {
        TokenKind::Identifier => true,
        TokenKind::QuotedIdentifier => token.span.end() - token.span.start() > 2,
        TokenKind::Keyword(keyword) => keyword::class(keyword) == KeywordClass::NonReserved,
        _ => false,
    }
}

fn permission_entity_kind(source: &str) -> bool {
    let Some(tokens) = permission_tokens(source) else {
        return false;
    };
    let [token] = tokens.as_slice() else {
        return false;
    };
    token.span.start() == 0
        && token.span.end() == source.len()
        && (token.kind == TokenKind::Keyword(K::Table) || permission_identifier(*token))
}

fn permission_privilege(source: &str) -> bool {
    let Some(tokens) = permission_tokens(source) else {
        return false;
    };
    match tokens.as_slice() {
        [token] => {
            token.span.start() == 0
                && token.span.end() == source.len()
                && (matches!(
                    token.kind,
                    TokenKind::Keyword(K::Create | K::Select | K::Delete | K::Insert | K::Update)
                ) || permission_identifier(*token))
        }
        [create, branch] => {
            create.kind == TokenKind::Keyword(K::Create)
                && branch.kind == TokenKind::Keyword(K::Branch)
                && create.span.start() == 0
                && branch.span.end() == source.len()
                && &source[create.span.end()..branch.span.start()] == " "
        }
        _ => false,
    }
}
