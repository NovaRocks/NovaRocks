// SPDX-License-Identifier: Apache-2.0

use super::{AstVec, Expr, Fold, Ident, ObjectName, SyntaxEq, Visit};
use crate::Span;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StatementProperty {
    pub name: Ident,
    /// Absence is the DEFAULT production. Explicit SQL NULL has an expression.
    pub value: Option<Expr>,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StatementPrincipal {
    pub kind: StatementPrincipalKind,
    pub name: Ident,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StatementPrincipalKind {
    Unspecified,
    User,
    Role,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TrinoCreateCatalog {
    pub catalog_name: Ident,
    pub not_exists: bool,
    pub connector_name: Ident,
    pub properties: AstVec<StatementProperty>,
    pub principal: Option<StatementPrincipal>,
    pub comment: Option<String>,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TrinoDropCatalog {
    pub catalog_name: Ident,
    pub exists: bool,
    pub cascade: bool,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TrinoCreateSchema {
    pub schema_name: ObjectName,
    pub not_exists: bool,
    pub properties: AstVec<StatementProperty>,
    pub principal: Option<StatementPrincipal>,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TrinoDropSchema {
    pub schema_name: ObjectName,
    pub exists: bool,
    pub cascade: bool,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TrinoRenameSchema {
    pub source: ObjectName,
    pub target: Ident,
    pub span: Span,
}

#[recursive::recursive]
pub fn walk_statement_property<V: Visit + ?Sized>(visitor: &mut V, node: &StatementProperty) {
    visitor.visit_ident(&node.name);
    if let Some(value) = &node.value {
        visitor.visit_expr(value);
    }
}

#[recursive::recursive]
pub fn fold_statement_property<F: Fold + ?Sized>(
    folder: &mut F,
    mut node: StatementProperty,
) -> StatementProperty {
    node.name = folder.fold_ident(node.name);
    node.value = node.value.map(|value| folder.fold_expr(value));
    node
}

pub fn walk_statement_principal<V: Visit + ?Sized>(visitor: &mut V, node: &StatementPrincipal) {
    visitor.visit_ident(&node.name);
}

pub fn fold_statement_principal<F: Fold + ?Sized>(
    folder: &mut F,
    mut node: StatementPrincipal,
) -> StatementPrincipal {
    node.name = folder.fold_ident(node.name);
    node
}

pub fn walk_trino_create_catalog<V: Visit + ?Sized>(visitor: &mut V, node: &TrinoCreateCatalog) {
    visitor.visit_ident(&node.catalog_name);
    visitor.visit_ident(&node.connector_name);
    if let Some(principal) = &node.principal {
        visitor.visit_statement_principal(principal);
    }
    for property in &node.properties {
        visitor.visit_statement_property(property);
    }
}

pub fn fold_trino_create_catalog<F: Fold + ?Sized>(
    folder: &mut F,
    mut node: TrinoCreateCatalog,
) -> TrinoCreateCatalog {
    node.catalog_name = folder.fold_ident(node.catalog_name);
    node.connector_name = folder.fold_ident(node.connector_name);
    node.principal = node
        .principal
        .map(|principal| folder.fold_statement_principal(principal));
    node.properties = node
        .properties
        .into_iter()
        .map(|property| folder.fold_statement_property(property))
        .collect();
    node
}

pub fn walk_trino_drop_catalog<V: Visit + ?Sized>(visitor: &mut V, node: &TrinoDropCatalog) {
    visitor.visit_ident(&node.catalog_name);
}

pub fn fold_trino_drop_catalog<F: Fold + ?Sized>(
    folder: &mut F,
    mut node: TrinoDropCatalog,
) -> TrinoDropCatalog {
    node.catalog_name = folder.fold_ident(node.catalog_name);
    node
}

pub fn walk_trino_create_schema<V: Visit + ?Sized>(visitor: &mut V, node: &TrinoCreateSchema) {
    visitor.visit_object_name(&node.schema_name);
    if let Some(principal) = &node.principal {
        visitor.visit_statement_principal(principal);
    }
    for property in &node.properties {
        visitor.visit_statement_property(property);
    }
}

pub fn fold_trino_create_schema<F: Fold + ?Sized>(
    folder: &mut F,
    mut node: TrinoCreateSchema,
) -> TrinoCreateSchema {
    node.schema_name = folder.fold_object_name(node.schema_name);
    node.principal = node
        .principal
        .map(|principal| folder.fold_statement_principal(principal));
    node.properties = node
        .properties
        .into_iter()
        .map(|property| folder.fold_statement_property(property))
        .collect();
    node
}

pub fn walk_trino_drop_schema<V: Visit + ?Sized>(visitor: &mut V, node: &TrinoDropSchema) {
    visitor.visit_object_name(&node.schema_name);
}

pub fn fold_trino_drop_schema<F: Fold + ?Sized>(
    folder: &mut F,
    mut node: TrinoDropSchema,
) -> TrinoDropSchema {
    node.schema_name = folder.fold_object_name(node.schema_name);
    node
}

pub fn walk_trino_rename_schema<V: Visit + ?Sized>(visitor: &mut V, node: &TrinoRenameSchema) {
    visitor.visit_object_name(&node.source);
    visitor.visit_ident(&node.target);
}

pub fn fold_trino_rename_schema<F: Fold + ?Sized>(
    folder: &mut F,
    mut node: TrinoRenameSchema,
) -> TrinoRenameSchema {
    node.source = folder.fold_object_name(node.source);
    node.target = folder.fold_ident(node.target);
    node
}

impl SyntaxEq for StatementProperty {
    #[recursive::recursive]
    fn syntax_eq(&self, other: &Self) -> bool {
        self.name.syntax_eq(&other.name) && optional_eq(&self.value, &other.value)
    }
}

impl SyntaxEq for StatementPrincipal {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.kind == other.kind && self.name.syntax_eq(&other.name)
    }
}

impl SyntaxEq for TrinoCreateCatalog {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.catalog_name.syntax_eq(&other.catalog_name)
            && self.not_exists == other.not_exists
            && self.connector_name.syntax_eq(&other.connector_name)
            && properties_eq(&self.properties, &other.properties)
            && optional_eq(&self.principal, &other.principal)
            && self.comment == other.comment
    }
}

impl SyntaxEq for TrinoDropCatalog {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.catalog_name.syntax_eq(&other.catalog_name)
            && self.exists == other.exists
            && self.cascade == other.cascade
    }
}

impl SyntaxEq for TrinoCreateSchema {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.schema_name.syntax_eq(&other.schema_name)
            && self.not_exists == other.not_exists
            && properties_eq(&self.properties, &other.properties)
            && optional_eq(&self.principal, &other.principal)
    }
}

impl SyntaxEq for TrinoDropSchema {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.schema_name.syntax_eq(&other.schema_name)
            && self.exists == other.exists
            && self.cascade == other.cascade
    }
}

impl SyntaxEq for TrinoRenameSchema {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.source.syntax_eq(&other.source) && self.target.syntax_eq(&other.target)
    }
}

fn optional_eq<T: SyntaxEq>(left: &Option<T>, right: &Option<T>) -> bool {
    match (left, right) {
        (None, None) => true,
        (Some(left), Some(right)) => left.syntax_eq(right),
        _ => false,
    }
}

fn properties_eq(left: &[StatementProperty], right: &[StatementProperty]) -> bool {
    left.len() == right.len()
        && left
            .iter()
            .zip(right)
            .all(|(left, right)| left.syntax_eq(right))
}
