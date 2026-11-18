// SPDX-License-Identifier: Apache-2.0

use super::{Fold, ObjectName, Relation, SyntaxEq, Visit};
use crate::Span;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TrinoShowCreate {
    pub kind: ShowCreateKind,
    pub name: ObjectName,
    pub span: Span,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ShowCreateKind {
    Table,
    Schema,
    View,
    MaterializedView,
    Function,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TrinoShowStats {
    pub relation: Relation,
    pub span: Span,
}

pub fn walk_trino_show_create<V: Visit + ?Sized>(visitor: &mut V, node: &TrinoShowCreate) {
    visitor.visit_object_name(&node.name);
}

pub fn fold_trino_show_create<F: Fold + ?Sized>(
    folder: &mut F,
    mut node: TrinoShowCreate,
) -> TrinoShowCreate {
    node.name = folder.fold_object_name(node.name);
    node
}

impl SyntaxEq for TrinoShowCreate {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.kind == other.kind && self.name.syntax_eq(&other.name)
    }
}

pub fn walk_trino_show_stats<V: Visit + ?Sized>(visitor: &mut V, node: &TrinoShowStats) {
    visitor.visit_relation(&node.relation);
}

pub fn fold_trino_show_stats<F: Fold + ?Sized>(
    folder: &mut F,
    mut node: TrinoShowStats,
) -> TrinoShowStats {
    node.relation = folder.fold_relation(node.relation);
    node
}

impl SyntaxEq for TrinoShowStats {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.relation.syntax_eq(&other.relation)
    }
}
