// SPDX-License-Identifier: Apache-2.0

use super::{AstBox, AstVec, Expr, Fold, Ident, Query, SyntaxEq, Table, Visit};
use crate::Span;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TrinoInsert {
    pub table: Table,
    pub columns: Option<Vec<Ident>>,
    pub query: AstBox<Query>,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TrinoDelete {
    pub table: Table,
    pub predicate: Option<Expr>,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TrinoUpdate {
    pub table: Table,
    pub assignments: AstVec<TrinoUpdateAssignment>,
    pub predicate: Option<Expr>,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TrinoUpdateAssignment {
    pub name: Ident,
    pub value: Expr,
    pub span: Span,
}

pub fn walk_trino_insert<V: Visit + ?Sized>(visitor: &mut V, node: &TrinoInsert) {
    visitor.visit_table(&node.table);
    if let Some(columns) = &node.columns {
        for column in columns {
            visitor.visit_ident(column);
        }
    }
    visitor.visit_query(&node.query);
}

pub fn walk_trino_delete<V: Visit + ?Sized>(visitor: &mut V, node: &TrinoDelete) {
    visitor.visit_table(&node.table);
    if let Some(predicate) = &node.predicate {
        visitor.visit_expr(predicate);
    }
}

pub fn walk_trino_update<V: Visit + ?Sized>(visitor: &mut V, node: &TrinoUpdate) {
    visitor.visit_table(&node.table);
    for assignment in &node.assignments {
        visitor.visit_trino_update_assignment(assignment);
    }
    if let Some(predicate) = &node.predicate {
        visitor.visit_expr(predicate);
    }
}

pub fn walk_trino_update_assignment<V: Visit + ?Sized>(
    visitor: &mut V,
    node: &TrinoUpdateAssignment,
) {
    visitor.visit_ident(&node.name);
    visitor.visit_expr(&node.value);
}

pub fn fold_trino_insert<F: Fold + ?Sized>(folder: &mut F, mut node: TrinoInsert) -> TrinoInsert {
    node.table = folder.fold_table(node.table);
    node.columns = node.columns.map(|columns| {
        columns
            .into_iter()
            .map(|column| folder.fold_ident(column))
            .collect()
    });
    node.query = AstBox::new(folder.fold_query(node.query.into_inner()));
    node
}

pub fn fold_trino_delete<F: Fold + ?Sized>(folder: &mut F, mut node: TrinoDelete) -> TrinoDelete {
    node.table = folder.fold_table(node.table);
    node.predicate = node.predicate.map(|value| folder.fold_expr(value));
    node
}

pub fn fold_trino_update<F: Fold + ?Sized>(folder: &mut F, mut node: TrinoUpdate) -> TrinoUpdate {
    node.table = folder.fold_table(node.table);
    node.assignments = node
        .assignments
        .into_iter()
        .map(|assignment| folder.fold_trino_update_assignment(assignment))
        .collect();
    node.predicate = node.predicate.map(|value| folder.fold_expr(value));
    node
}

pub fn fold_trino_update_assignment<F: Fold + ?Sized>(
    folder: &mut F,
    mut node: TrinoUpdateAssignment,
) -> TrinoUpdateAssignment {
    node.name = folder.fold_ident(node.name);
    node.value = folder.fold_expr(node.value);
    node
}

impl SyntaxEq for TrinoInsert {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.table.syntax_eq(&other.table)
            && self.query.syntax_eq(&other.query)
            && match (&self.columns, &other.columns) {
                (None, None) => true,
                (Some(left), Some(right)) => same_slots(left, right),
                _ => false,
            }
    }
}

impl SyntaxEq for TrinoDelete {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.table.syntax_eq(&other.table) && same_predicate(&self.predicate, &other.predicate)
    }
}

impl SyntaxEq for TrinoUpdate {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.table.syntax_eq(&other.table)
            && same_slots(&self.assignments, &other.assignments)
            && same_predicate(&self.predicate, &other.predicate)
    }
}

impl SyntaxEq for TrinoUpdateAssignment {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.name.syntax_eq(&other.name) && self.value.syntax_eq(&other.value)
    }
}

fn same_slots<T: SyntaxEq>(left: &[T], right: &[T]) -> bool {
    left.len() == right.len()
        && left
            .iter()
            .zip(right)
            .all(|(left, right)| left.syntax_eq(right))
}

fn same_predicate(left: &Option<Expr>, right: &Option<Expr>) -> bool {
    match (left, right) {
        (Some(left), Some(right)) => left.syntax_eq(right),
        (None, None) => true,
        _ => false,
    }
}
