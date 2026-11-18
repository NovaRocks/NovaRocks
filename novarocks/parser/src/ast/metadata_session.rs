// SPDX-License-Identifier: Apache-2.0

use super::{AstVec, Expr, Fold, Ident, ObjectName, SyntaxEq, Visit};
use crate::Span;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TrinoShowTables {
    pub schema: Option<ObjectName>,
    pub like_pattern: Option<String>,
    pub escape: Option<String>,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TrinoShowSchemas {
    pub catalog: Option<Ident>,
    pub like_pattern: Option<String>,
    pub escape: Option<String>,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TrinoShowCatalogs {
    pub like_pattern: Option<String>,
    pub escape: Option<String>,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TrinoShowColumns {
    pub table: ObjectName,
    pub like_pattern: Option<String>,
    pub escape: Option<String>,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TrinoShowFunctions {
    pub schema: Option<ObjectName>,
    pub like_pattern: Option<String>,
    pub escape: Option<String>,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TrinoShowSession {
    pub like_pattern: Option<String>,
    pub escape: Option<String>,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TrinoSetPath {
    pub path_specification: TrinoPathSpecification,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TrinoPathSpecification {
    pub path: AstVec<TrinoPathElement>,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TrinoPathElement {
    pub catalog: Option<Ident>,
    pub schema: Ident,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TrinoSetTimeZone {
    pub time_zone: Option<Expr>,
    pub span: Span,
}

fn optional_syntax_eq<T: SyntaxEq>(left: &Option<T>, right: &Option<T>) -> bool {
    match (left, right) {
        (Some(left), Some(right)) => left.syntax_eq(right),
        (None, None) => true,
        _ => false,
    }
}

impl SyntaxEq for TrinoShowTables {
    fn syntax_eq(&self, other: &Self) -> bool {
        optional_syntax_eq(&self.schema, &other.schema)
            && self.like_pattern == other.like_pattern
            && self.escape == other.escape
    }
}

pub fn walk_trino_show_tables<V: Visit + ?Sized>(visitor: &mut V, node: &TrinoShowTables) {
    if let Some(name) = &node.schema {
        visitor.visit_object_name(name);
    }
}

pub fn fold_trino_show_tables<F: Fold + ?Sized>(
    folder: &mut F,
    mut node: TrinoShowTables,
) -> TrinoShowTables {
    node.schema = node.schema.map(|name| folder.fold_object_name(name));
    node
}

impl SyntaxEq for TrinoShowSchemas {
    fn syntax_eq(&self, other: &Self) -> bool {
        optional_syntax_eq(&self.catalog, &other.catalog)
            && self.like_pattern == other.like_pattern
            && self.escape == other.escape
    }
}

pub fn walk_trino_show_schemas<V: Visit + ?Sized>(visitor: &mut V, node: &TrinoShowSchemas) {
    if let Some(name) = &node.catalog {
        visitor.visit_ident(name);
    }
}

pub fn fold_trino_show_schemas<F: Fold + ?Sized>(
    folder: &mut F,
    mut node: TrinoShowSchemas,
) -> TrinoShowSchemas {
    node.catalog = node.catalog.map(|name| folder.fold_ident(name));
    node
}

impl SyntaxEq for TrinoShowCatalogs {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.like_pattern == other.like_pattern && self.escape == other.escape
    }
}

impl SyntaxEq for TrinoShowColumns {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.table.syntax_eq(&other.table)
            && self.like_pattern == other.like_pattern
            && self.escape == other.escape
    }
}

pub fn walk_trino_show_columns<V: Visit + ?Sized>(visitor: &mut V, node: &TrinoShowColumns) {
    visitor.visit_object_name(&node.table);
}

pub fn fold_trino_show_columns<F: Fold + ?Sized>(
    folder: &mut F,
    mut node: TrinoShowColumns,
) -> TrinoShowColumns {
    node.table = folder.fold_object_name(node.table);
    node
}

impl SyntaxEq for TrinoShowFunctions {
    fn syntax_eq(&self, other: &Self) -> bool {
        optional_syntax_eq(&self.schema, &other.schema)
            && self.like_pattern == other.like_pattern
            && self.escape == other.escape
    }
}

pub fn walk_trino_show_functions<V: Visit + ?Sized>(visitor: &mut V, node: &TrinoShowFunctions) {
    if let Some(name) = &node.schema {
        visitor.visit_object_name(name);
    }
}

pub fn fold_trino_show_functions<F: Fold + ?Sized>(
    folder: &mut F,
    mut node: TrinoShowFunctions,
) -> TrinoShowFunctions {
    node.schema = node.schema.map(|name| folder.fold_object_name(name));
    node
}

impl SyntaxEq for TrinoShowSession {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.like_pattern == other.like_pattern && self.escape == other.escape
    }
}

impl SyntaxEq for TrinoSetPath {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.path_specification.syntax_eq(&other.path_specification)
    }
}

impl SyntaxEq for TrinoPathSpecification {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.path.len() == other.path.len()
            && self
                .path
                .iter()
                .zip(other.path.iter())
                .all(|(left, right)| left.syntax_eq(right))
    }
}

impl SyntaxEq for TrinoPathElement {
    fn syntax_eq(&self, other: &Self) -> bool {
        optional_syntax_eq(&self.catalog, &other.catalog) && self.schema.syntax_eq(&other.schema)
    }
}

pub fn walk_trino_set_path<V: Visit + ?Sized>(visitor: &mut V, node: &TrinoSetPath) {
    for element in &node.path_specification.path {
        if let Some(catalog) = &element.catalog {
            visitor.visit_ident(catalog);
        }
        visitor.visit_ident(&element.schema);
    }
}

pub fn fold_trino_set_path<F: Fold + ?Sized>(
    folder: &mut F,
    mut node: TrinoSetPath,
) -> TrinoSetPath {
    node.path_specification.path = node
        .path_specification
        .path
        .into_iter()
        .map(|mut element| {
            element.catalog = element.catalog.map(|catalog| folder.fold_ident(catalog));
            element.schema = folder.fold_ident(element.schema);
            element
        })
        .collect();
    node
}

impl SyntaxEq for TrinoSetTimeZone {
    fn syntax_eq(&self, other: &Self) -> bool {
        optional_syntax_eq(&self.time_zone, &other.time_zone)
    }
}

pub fn walk_trino_set_time_zone<V: Visit + ?Sized>(visitor: &mut V, node: &TrinoSetTimeZone) {
    if let Some(time_zone) = &node.time_zone {
        visitor.visit_expr(time_zone);
    }
}

pub fn fold_trino_set_time_zone<F: Fold + ?Sized>(
    folder: &mut F,
    mut node: TrinoSetTimeZone,
) -> TrinoSetTimeZone {
    node.time_zone = node.time_zone.map(|time_zone| folder.fold_expr(time_zone));
    node
}
