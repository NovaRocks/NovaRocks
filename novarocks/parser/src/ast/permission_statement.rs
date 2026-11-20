// SPDX-License-Identifier: Apache-2.0

use super::{Fold, Ident, ObjectName, StatementPrincipal, SyntaxEq, Visit};
use crate::Span;
use std::collections::HashSet;
use std::mem::discriminant;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TrinoGrantor {
    Principal(StatementPrincipal),
    CurrentUser,
    CurrentRole,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TrinoRoleSelection {
    All,
    None,
    Named(Ident),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TrinoGrantObject {
    /// Raw spelling of the single entity-kind token in AstBuilder.
    pub entity_kind: Option<String>,
    pub name: ObjectName,
    pub branch: Option<Ident>,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TrinoCreateRole {
    pub name: Ident,
    pub grantor: Option<TrinoGrantor>,
    pub catalog: Option<Ident>,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TrinoDropRole {
    pub name: Ident,
    pub if_exists: bool,
    pub catalog: Option<Ident>,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TrinoGrantRoles {
    pub roles: Vec<Ident>,
    pub grantees: Vec<StatementPrincipal>,
    pub admin_option: bool,
    pub grantor: Option<TrinoGrantor>,
    pub catalog: Option<Ident>,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TrinoRevokeRoles {
    pub roles: Vec<Ident>,
    pub grantees: Vec<StatementPrincipal>,
    pub admin_option: bool,
    pub grantor: Option<TrinoGrantor>,
    pub catalog: Option<Ident>,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TrinoGrant {
    /// Absence denotes ALL PRIVILEGES.
    pub privileges: Option<Vec<String>>,
    pub object: TrinoGrantObject,
    pub grantee: StatementPrincipal,
    pub grant_option: bool,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TrinoRevoke {
    /// Absence denotes ALL PRIVILEGES.
    pub privileges: Option<Vec<String>>,
    pub object: TrinoGrantObject,
    pub grantee: StatementPrincipal,
    pub grant_option_for: bool,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TrinoDeny {
    /// Absence denotes ALL PRIVILEGES.
    pub privileges: Option<Vec<String>>,
    pub object: TrinoGrantObject,
    pub grantee: StatementPrincipal,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TrinoSetRole {
    pub selection: TrinoRoleSelection,
    pub catalog: Option<Ident>,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TrinoShowGrants {
    pub object: Option<TrinoGrantObject>,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TrinoShowRoles {
    pub catalog: Option<Ident>,
    pub current: bool,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TrinoShowRoleGrants {
    pub catalog: Option<Ident>,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TrinoSetAuthorization {
    /// The uppercase kind spelling produced by AstBuilder.
    pub owned_entity_kind: String,
    pub source: ObjectName,
    pub principal: StatementPrincipal,
    pub span: Span,
}

pub fn walk_trino_grantor<V: Visit + ?Sized>(visitor: &mut V, node: &TrinoGrantor) {
    if let TrinoGrantor::Principal(principal) = node {
        visitor.visit_statement_principal(principal);
    }
}

pub fn fold_trino_grantor<F: Fold + ?Sized>(folder: &mut F, node: TrinoGrantor) -> TrinoGrantor {
    match node {
        TrinoGrantor::Principal(principal) => {
            TrinoGrantor::Principal(folder.fold_statement_principal(principal))
        }
        other => other,
    }
}

impl SyntaxEq for TrinoGrantor {
    fn syntax_eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Principal(left), Self::Principal(right)) => left.syntax_eq(right),
            (Self::CurrentUser, Self::CurrentUser) | (Self::CurrentRole, Self::CurrentRole) => true,
            _ => false,
        }
    }
}

impl SyntaxEq for TrinoRoleSelection {
    fn syntax_eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Named(left), Self::Named(right)) => left.syntax_eq(right),
            (Self::All, Self::All) | (Self::None, Self::None) => true,
            _ => false,
        }
    }
}

pub fn walk_trino_grant_object<V: Visit + ?Sized>(visitor: &mut V, node: &TrinoGrantObject) {
    visitor.visit_object_name(&node.name);
    if let Some(value) = &node.branch {
        visitor.visit_ident(value);
    }
}

pub fn fold_trino_grant_object<F: Fold + ?Sized>(
    folder: &mut F,
    mut node: TrinoGrantObject,
) -> TrinoGrantObject {
    node.name = folder.fold_object_name(node.name);
    node.branch = node.branch.map(|value| folder.fold_ident(value));
    node
}

impl SyntaxEq for TrinoGrantObject {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.entity_kind == other.entity_kind
            && self.name.syntax_eq(&other.name)
            && same_option(&self.branch, &other.branch)
    }
}

pub fn walk_trino_create_role<V: Visit + ?Sized>(visitor: &mut V, node: &TrinoCreateRole) {
    visitor.visit_ident(&node.name);
    if let Some(value) = &node.grantor {
        visitor.visit_trino_grantor(value);
    }
    if let Some(value) = &node.catalog {
        visitor.visit_ident(value);
    }
}

pub fn fold_trino_create_role<F: Fold + ?Sized>(
    folder: &mut F,
    mut node: TrinoCreateRole,
) -> TrinoCreateRole {
    node.name = folder.fold_ident(node.name);
    node.grantor = node.grantor.map(|value| folder.fold_trino_grantor(value));
    node.catalog = node.catalog.map(|value| folder.fold_ident(value));
    node
}

impl SyntaxEq for TrinoCreateRole {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.name.syntax_eq(&other.name)
            && same_option(&self.grantor, &other.grantor)
            && same_option(&self.catalog, &other.catalog)
    }
}

pub fn walk_trino_drop_role<V: Visit + ?Sized>(visitor: &mut V, node: &TrinoDropRole) {
    visitor.visit_ident(&node.name);
    if let Some(value) = &node.catalog {
        visitor.visit_ident(value);
    }
}

pub fn fold_trino_drop_role<F: Fold + ?Sized>(
    folder: &mut F,
    mut node: TrinoDropRole,
) -> TrinoDropRole {
    node.name = folder.fold_ident(node.name);
    node.catalog = node.catalog.map(|value| folder.fold_ident(value));
    node
}

impl SyntaxEq for TrinoDropRole {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.name.syntax_eq(&other.name)
            && self.if_exists == other.if_exists
            && same_option(&self.catalog, &other.catalog)
    }
}

pub fn walk_trino_grant_roles<V: Visit + ?Sized>(visitor: &mut V, node: &TrinoGrantRoles) {
    for value in &node.roles {
        visitor.visit_ident(value);
    }
    for value in &node.grantees {
        visitor.visit_statement_principal(value);
    }
    if let Some(value) = &node.grantor {
        visitor.visit_trino_grantor(value);
    }
    if let Some(value) = &node.catalog {
        visitor.visit_ident(value);
    }
}

pub fn fold_trino_grant_roles<F: Fold + ?Sized>(
    folder: &mut F,
    mut node: TrinoGrantRoles,
) -> TrinoGrantRoles {
    node.roles = node
        .roles
        .into_iter()
        .map(|value| folder.fold_ident(value))
        .collect();
    node.grantees = node
        .grantees
        .into_iter()
        .map(|value| folder.fold_statement_principal(value))
        .collect();
    node.grantor = node.grantor.map(|value| folder.fold_trino_grantor(value));
    node.catalog = node.catalog.map(|value| folder.fold_ident(value));
    node
}

impl SyntaxEq for TrinoGrantRoles {
    fn syntax_eq(&self, other: &Self) -> bool {
        ident_set(&self.roles) == ident_set(&other.roles)
            && principal_set(&self.grantees) == principal_set(&other.grantees)
            && self.admin_option == other.admin_option
            && same_option(&self.grantor, &other.grantor)
            && same_option(&self.catalog, &other.catalog)
    }
}

pub fn walk_trino_revoke_roles<V: Visit + ?Sized>(visitor: &mut V, node: &TrinoRevokeRoles) {
    for value in &node.roles {
        visitor.visit_ident(value);
    }
    for value in &node.grantees {
        visitor.visit_statement_principal(value);
    }
    if let Some(value) = &node.grantor {
        visitor.visit_trino_grantor(value);
    }
    if let Some(value) = &node.catalog {
        visitor.visit_ident(value);
    }
}

pub fn fold_trino_revoke_roles<F: Fold + ?Sized>(
    folder: &mut F,
    mut node: TrinoRevokeRoles,
) -> TrinoRevokeRoles {
    node.roles = node
        .roles
        .into_iter()
        .map(|value| folder.fold_ident(value))
        .collect();
    node.grantees = node
        .grantees
        .into_iter()
        .map(|value| folder.fold_statement_principal(value))
        .collect();
    node.grantor = node.grantor.map(|value| folder.fold_trino_grantor(value));
    node.catalog = node.catalog.map(|value| folder.fold_ident(value));
    node
}

impl SyntaxEq for TrinoRevokeRoles {
    fn syntax_eq(&self, other: &Self) -> bool {
        ident_set(&self.roles) == ident_set(&other.roles)
            && principal_set(&self.grantees) == principal_set(&other.grantees)
            && self.admin_option == other.admin_option
            && same_option(&self.grantor, &other.grantor)
            && same_option(&self.catalog, &other.catalog)
    }
}

pub fn walk_trino_grant<V: Visit + ?Sized>(visitor: &mut V, node: &TrinoGrant) {
    visitor.visit_trino_grant_object(&node.object);
    visitor.visit_statement_principal(&node.grantee);
}

pub fn fold_trino_grant<F: Fold + ?Sized>(folder: &mut F, mut node: TrinoGrant) -> TrinoGrant {
    node.object = folder.fold_trino_grant_object(node.object);
    node.grantee = folder.fold_statement_principal(node.grantee);
    node
}

impl SyntaxEq for TrinoGrant {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.privileges == other.privileges
            && self.object.syntax_eq(&other.object)
            && self.grantee.syntax_eq(&other.grantee)
            && self.grant_option == other.grant_option
    }
}

pub fn walk_trino_revoke<V: Visit + ?Sized>(visitor: &mut V, node: &TrinoRevoke) {
    visitor.visit_trino_grant_object(&node.object);
    visitor.visit_statement_principal(&node.grantee);
}

pub fn fold_trino_revoke<F: Fold + ?Sized>(folder: &mut F, mut node: TrinoRevoke) -> TrinoRevoke {
    node.object = folder.fold_trino_grant_object(node.object);
    node.grantee = folder.fold_statement_principal(node.grantee);
    node
}

impl SyntaxEq for TrinoRevoke {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.privileges == other.privileges
            && self.object.syntax_eq(&other.object)
            && self.grantee.syntax_eq(&other.grantee)
            && self.grant_option_for == other.grant_option_for
    }
}

pub fn walk_trino_deny<V: Visit + ?Sized>(visitor: &mut V, node: &TrinoDeny) {
    visitor.visit_trino_grant_object(&node.object);
    visitor.visit_statement_principal(&node.grantee);
}

pub fn fold_trino_deny<F: Fold + ?Sized>(folder: &mut F, mut node: TrinoDeny) -> TrinoDeny {
    node.object = folder.fold_trino_grant_object(node.object);
    node.grantee = folder.fold_statement_principal(node.grantee);
    node
}

impl SyntaxEq for TrinoDeny {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.privileges == other.privileges
            && self.object.syntax_eq(&other.object)
            && self.grantee.syntax_eq(&other.grantee)
    }
}

pub fn walk_trino_set_role<V: Visit + ?Sized>(visitor: &mut V, node: &TrinoSetRole) {
    if let TrinoRoleSelection::Named(value) = &node.selection {
        visitor.visit_ident(value);
    }
    if let Some(value) = &node.catalog {
        visitor.visit_ident(value);
    }
}

pub fn fold_trino_set_role<F: Fold + ?Sized>(
    folder: &mut F,
    mut node: TrinoSetRole,
) -> TrinoSetRole {
    node.selection = match node.selection {
        TrinoRoleSelection::Named(value) => TrinoRoleSelection::Named(folder.fold_ident(value)),
        other => other,
    };
    node.catalog = node.catalog.map(|value| folder.fold_ident(value));
    node
}

impl SyntaxEq for TrinoSetRole {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.selection.syntax_eq(&other.selection) && same_option(&self.catalog, &other.catalog)
    }
}

pub fn walk_trino_show_grants<V: Visit + ?Sized>(visitor: &mut V, node: &TrinoShowGrants) {
    if let Some(value) = &node.object {
        visitor.visit_trino_grant_object(value);
    }
}

pub fn fold_trino_show_grants<F: Fold + ?Sized>(
    folder: &mut F,
    mut node: TrinoShowGrants,
) -> TrinoShowGrants {
    node.object = node
        .object
        .map(|value| folder.fold_trino_grant_object(value));
    node
}

impl SyntaxEq for TrinoShowGrants {
    fn syntax_eq(&self, other: &Self) -> bool {
        same_option(&self.object, &other.object)
    }
}

pub fn walk_trino_show_roles<V: Visit + ?Sized>(visitor: &mut V, node: &TrinoShowRoles) {
    if let Some(value) = &node.catalog {
        visitor.visit_ident(value);
    }
}

pub fn fold_trino_show_roles<F: Fold + ?Sized>(
    folder: &mut F,
    mut node: TrinoShowRoles,
) -> TrinoShowRoles {
    node.catalog = node.catalog.map(|value| folder.fold_ident(value));
    node
}

impl SyntaxEq for TrinoShowRoles {
    fn syntax_eq(&self, other: &Self) -> bool {
        same_option(&self.catalog, &other.catalog) && self.current == other.current
    }
}

pub fn walk_trino_show_role_grants<V: Visit + ?Sized>(visitor: &mut V, node: &TrinoShowRoleGrants) {
    if let Some(value) = &node.catalog {
        visitor.visit_ident(value);
    }
}

pub fn fold_trino_show_role_grants<F: Fold + ?Sized>(
    folder: &mut F,
    mut node: TrinoShowRoleGrants,
) -> TrinoShowRoleGrants {
    node.catalog = node.catalog.map(|value| folder.fold_ident(value));
    node
}

impl SyntaxEq for TrinoShowRoleGrants {
    fn syntax_eq(&self, other: &Self) -> bool {
        same_option(&self.catalog, &other.catalog)
    }
}

pub fn walk_trino_set_authorization<V: Visit + ?Sized>(
    visitor: &mut V,
    node: &TrinoSetAuthorization,
) {
    visitor.visit_object_name(&node.source);
    visitor.visit_statement_principal(&node.principal);
}

pub fn fold_trino_set_authorization<F: Fold + ?Sized>(
    folder: &mut F,
    mut node: TrinoSetAuthorization,
) -> TrinoSetAuthorization {
    node.source = folder.fold_object_name(node.source);
    node.principal = folder.fold_statement_principal(node.principal);
    node
}

impl SyntaxEq for TrinoSetAuthorization {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.owned_entity_kind == other.owned_entity_kind
            && self.source.syntax_eq(&other.source)
            && self.principal.syntax_eq(&other.principal)
    }
}

fn same_option<T: SyntaxEq>(left: &Option<T>, right: &Option<T>) -> bool {
    match (left, right) {
        (Some(left), Some(right)) => left.syntax_eq(right),
        (None, None) => true,
        _ => false,
    }
}

fn ident_set(values: &[Ident]) -> HashSet<(&str, bool)> {
    values
        .iter()
        .map(|value| (value.value.as_str(), value.quoted))
        .collect()
}

fn principal_set(
    values: &[StatementPrincipal],
) -> HashSet<(
    std::mem::Discriminant<super::StatementPrincipalKind>,
    &str,
    bool,
)> {
    values
        .iter()
        .map(|value| {
            (
                discriminant(&value.kind),
                value.name.value.as_str(),
                value.name.quoted,
            )
        })
        .collect()
}
