// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::Span;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TrinoCreateView {
    pub name: ObjectName,
    pub replace: bool,
    pub not_exists: bool,
    pub comment: Option<String>,
    pub security: Option<ViewSecurity>,
    pub properties: AstVec<StatementProperty>,
    pub query: Query,
    pub span: Span,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ViewSecurity {
    Definer,
    Invoker,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TrinoCreateMaterializedView {
    pub name: ObjectName,
    pub replace: bool,
    pub not_exists: bool,
    pub grace_period: Option<Literal>,
    pub when_stale: Option<MaterializedViewWhenStale>,
    pub comment: Option<String>,
    pub properties: AstVec<StatementProperty>,
    pub query: Query,
    pub span: Span,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MaterializedViewWhenStale {
    Inline,
    Fail,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TrinoDropView {
    pub name: ObjectName,
    pub exists: bool,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TrinoDropMaterializedView {
    pub name: ObjectName,
    pub exists: bool,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TrinoRenameView {
    pub source: ObjectName,
    pub target: ObjectName,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TrinoRenameMaterializedView {
    pub source: ObjectName,
    pub target: ObjectName,
    pub exists: bool,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TrinoRefreshView {
    pub name: ObjectName,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TrinoRefreshMaterializedView {
    pub table: Table,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TrinoMaterializedViewExecute {
    pub table: Table,
    pub procedure_name: Ident,
    pub arguments: AstVec<CallArgument>,
    pub selection: Option<Expr>,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TrinoSetProperties {
    pub kind: SetPropertiesKind,
    pub name: ObjectName,
    pub properties: AstVec<StatementProperty>,
    pub span: Span,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SetPropertiesKind {
    Table,
    MaterializedView,
}

impl SyntaxEq for TrinoCreateView {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.name.syntax_eq(&other.name)
            && self.replace == other.replace
            && self.not_exists == other.not_exists
            && self.comment == other.comment
            && self.security == other.security
            && self.properties.len() == other.properties.len()
            && self
                .properties
                .iter()
                .zip(&other.properties)
                .all(|(a, b)| a.syntax_eq(b))
            && self.query.syntax_eq(&other.query)
    }
}

impl SyntaxEq for TrinoCreateMaterializedView {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.name.syntax_eq(&other.name)
            && self.replace == other.replace
            && self.not_exists == other.not_exists
            && match (&self.grace_period, &other.grace_period) {
                (Some(a), Some(b)) => a.syntax_eq(b),
                (None, None) => true,
                _ => false,
            }
            && self.when_stale == other.when_stale
            && self.comment == other.comment
            && self.properties.len() == other.properties.len()
            && self
                .properties
                .iter()
                .zip(&other.properties)
                .all(|(a, b)| a.syntax_eq(b))
            && self.query.syntax_eq(&other.query)
    }
}

impl SyntaxEq for TrinoMaterializedViewExecute {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.table.syntax_eq(&other.table)
            && self.procedure_name.syntax_eq(&other.procedure_name)
            && self.arguments.len() == other.arguments.len()
            && self
                .arguments
                .iter()
                .zip(&other.arguments)
                .all(|(a, b)| a.syntax_eq(b))
            && match (&self.selection, &other.selection) {
                (Some(a), Some(b)) => a.syntax_eq(b),
                (None, None) => true,
                _ => false,
            }
    }
}

impl SyntaxEq for TrinoSetProperties {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.kind == other.kind
            && self.name.syntax_eq(&other.name)
            && self.properties.len() == other.properties.len()
            && self
                .properties
                .iter()
                .zip(&other.properties)
                .all(|(a, b)| a.syntax_eq(b))
    }
}

pub fn walk_trino_create_view<V: Visit + ?Sized>(visitor: &mut V, node: &TrinoCreateView) {
    visitor.visit_object_name(&node.name);
    for property in &node.properties {
        visitor.visit_statement_property(property);
    }
    visitor.visit_query(&node.query);
}

pub fn fold_trino_create_view<F: Fold + ?Sized>(
    folder: &mut F,
    mut node: TrinoCreateView,
) -> TrinoCreateView {
    node.name = folder.fold_object_name(node.name);
    node.properties = node
        .properties
        .into_iter()
        .map(|property| folder.fold_statement_property(property))
        .collect();
    node.query = folder.fold_query(node.query);
    node
}

pub fn walk_trino_create_materialized_view<V: Visit + ?Sized>(
    visitor: &mut V,
    node: &TrinoCreateMaterializedView,
) {
    visitor.visit_object_name(&node.name);
    if let Some(grace_period) = &node.grace_period {
        visitor.visit_literal(grace_period);
    }
    for property in &node.properties {
        visitor.visit_statement_property(property);
    }
    visitor.visit_query(&node.query);
}

pub fn fold_trino_create_materialized_view<F: Fold + ?Sized>(
    folder: &mut F,
    mut node: TrinoCreateMaterializedView,
) -> TrinoCreateMaterializedView {
    node.name = folder.fold_object_name(node.name);
    node.grace_period = node.grace_period.map(|period| folder.fold_literal(period));
    node.properties = node
        .properties
        .into_iter()
        .map(|property| folder.fold_statement_property(property))
        .collect();
    node.query = folder.fold_query(node.query);
    node
}

pub fn walk_trino_materialized_view_execute<V: Visit + ?Sized>(
    visitor: &mut V,
    node: &TrinoMaterializedViewExecute,
) {
    visitor.visit_table(&node.table);
    visitor.visit_ident(&node.procedure_name);
    for argument in &node.arguments {
        visitor.visit_call_argument(argument);
    }
    if let Some(selection) = &node.selection {
        visitor.visit_expr(selection);
    }
}

pub fn fold_trino_materialized_view_execute<F: Fold + ?Sized>(
    folder: &mut F,
    mut node: TrinoMaterializedViewExecute,
) -> TrinoMaterializedViewExecute {
    node.table = folder.fold_table(node.table);
    node.procedure_name = folder.fold_ident(node.procedure_name);
    node.arguments = node
        .arguments
        .into_iter()
        .map(|argument| folder.fold_call_argument(argument))
        .collect();
    node.selection = node.selection.map(|selection| folder.fold_expr(selection));
    node
}

pub fn walk_trino_set_properties<V: Visit + ?Sized>(visitor: &mut V, node: &TrinoSetProperties) {
    visitor.visit_object_name(&node.name);
    for property in &node.properties {
        visitor.visit_statement_property(property);
    }
}

pub fn fold_trino_set_properties<F: Fold + ?Sized>(
    folder: &mut F,
    mut node: TrinoSetProperties,
) -> TrinoSetProperties {
    node.name = folder.fold_object_name(node.name);
    node.properties = node
        .properties
        .into_iter()
        .map(|property| folder.fold_statement_property(property))
        .collect();
    node
}

impl SyntaxEq for TrinoDropView {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.name.syntax_eq(&other.name) && self.exists == other.exists
    }
}

pub fn walk_trino_drop_view<V: Visit + ?Sized>(visitor: &mut V, node: &TrinoDropView) {
    visitor.visit_object_name(&node.name);
}

pub fn fold_trino_drop_view<F: Fold + ?Sized>(
    folder: &mut F,
    mut node: TrinoDropView,
) -> TrinoDropView {
    node.name = folder.fold_object_name(node.name);
    node
}

impl SyntaxEq for TrinoDropMaterializedView {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.name.syntax_eq(&other.name) && self.exists == other.exists
    }
}

pub fn walk_trino_drop_materialized_view<V: Visit + ?Sized>(
    visitor: &mut V,
    node: &TrinoDropMaterializedView,
) {
    visitor.visit_object_name(&node.name);
}

pub fn fold_trino_drop_materialized_view<F: Fold + ?Sized>(
    folder: &mut F,
    mut node: TrinoDropMaterializedView,
) -> TrinoDropMaterializedView {
    node.name = folder.fold_object_name(node.name);
    node
}

impl SyntaxEq for TrinoRenameView {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.source.syntax_eq(&other.source) && self.target.syntax_eq(&other.target)
    }
}

pub fn walk_trino_rename_view<V: Visit + ?Sized>(visitor: &mut V, node: &TrinoRenameView) {
    visitor.visit_object_name(&node.source);
    visitor.visit_object_name(&node.target);
}

pub fn fold_trino_rename_view<F: Fold + ?Sized>(
    folder: &mut F,
    mut node: TrinoRenameView,
) -> TrinoRenameView {
    node.source = folder.fold_object_name(node.source);
    node.target = folder.fold_object_name(node.target);
    node
}

impl SyntaxEq for TrinoRenameMaterializedView {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.source.syntax_eq(&other.source)
            && self.target.syntax_eq(&other.target)
            && self.exists == other.exists
    }
}

pub fn walk_trino_rename_materialized_view<V: Visit + ?Sized>(
    visitor: &mut V,
    node: &TrinoRenameMaterializedView,
) {
    visitor.visit_object_name(&node.source);
    visitor.visit_object_name(&node.target);
}

pub fn fold_trino_rename_materialized_view<F: Fold + ?Sized>(
    folder: &mut F,
    mut node: TrinoRenameMaterializedView,
) -> TrinoRenameMaterializedView {
    node.source = folder.fold_object_name(node.source);
    node.target = folder.fold_object_name(node.target);
    node
}

impl SyntaxEq for TrinoRefreshView {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.name.syntax_eq(&other.name)
    }
}

pub fn walk_trino_refresh_view<V: Visit + ?Sized>(visitor: &mut V, node: &TrinoRefreshView) {
    visitor.visit_object_name(&node.name);
}

pub fn fold_trino_refresh_view<F: Fold + ?Sized>(
    folder: &mut F,
    mut node: TrinoRefreshView,
) -> TrinoRefreshView {
    node.name = folder.fold_object_name(node.name);
    node
}

impl SyntaxEq for TrinoRefreshMaterializedView {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.table.syntax_eq(&other.table)
    }
}

pub fn walk_trino_refresh_materialized_view<V: Visit + ?Sized>(
    visitor: &mut V,
    node: &TrinoRefreshMaterializedView,
) {
    visitor.visit_table(&node.table);
}

pub fn fold_trino_refresh_materialized_view<F: Fold + ?Sized>(
    folder: &mut F,
    mut node: TrinoRefreshMaterializedView,
) -> TrinoRefreshMaterializedView {
    node.table = folder.fold_table(node.table);
    node
}
