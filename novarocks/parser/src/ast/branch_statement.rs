// SPDX-License-Identifier: Apache-2.0

use super::{
    AstVec, Fold, Ident, ObjectName, StatementProperty, StatementSaveMode, SyntaxEq, Visit,
};
use crate::Span;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TrinoCreateBranch {
    pub table_name: ObjectName,
    pub branch_name: Ident,
    pub from_branch: Option<Ident>,
    pub save_mode: StatementSaveMode,
    pub properties: AstVec<StatementProperty>,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TrinoDropBranch {
    pub table_name: ObjectName,
    pub branch_name: Ident,
    pub exists: bool,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TrinoFastForwardBranch {
    pub table_name: ObjectName,
    pub source_branch_name: Ident,
    pub target_branch_name: Ident,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TrinoShowBranches {
    pub table_name: ObjectName,
    pub span: Span,
}

pub fn walk_trino_create_branch<V: Visit + ?Sized>(visitor: &mut V, node: &TrinoCreateBranch) {
    visitor.visit_object_name(&node.table_name);
    visitor.visit_ident(&node.branch_name);
    if let Some(from_branch) = &node.from_branch {
        visitor.visit_ident(from_branch);
    }
    for property in &node.properties {
        visitor.visit_statement_property(property);
    }
}

pub fn fold_trino_create_branch<F: Fold + ?Sized>(
    folder: &mut F,
    mut node: TrinoCreateBranch,
) -> TrinoCreateBranch {
    node.table_name = folder.fold_object_name(node.table_name);
    node.branch_name = folder.fold_ident(node.branch_name);
    node.from_branch = node.from_branch.map(|branch| folder.fold_ident(branch));
    node.properties = node
        .properties
        .into_iter()
        .map(|property| folder.fold_statement_property(property))
        .collect();
    node
}

impl SyntaxEq for TrinoCreateBranch {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.table_name.syntax_eq(&other.table_name)
            && self.branch_name.syntax_eq(&other.branch_name)
            && self.save_mode == other.save_mode
            && self.properties.len() == other.properties.len()
            && self
                .properties
                .iter()
                .zip(&other.properties)
                .all(|(left, right)| left.syntax_eq(right))
            && match (&self.from_branch, &other.from_branch) {
                (None, None) => true,
                (Some(left), Some(right)) => left.syntax_eq(right),
                _ => false,
            }
    }
}

pub fn walk_trino_drop_branch<V: Visit + ?Sized>(visitor: &mut V, node: &TrinoDropBranch) {
    visitor.visit_object_name(&node.table_name);
    visitor.visit_ident(&node.branch_name);
}

pub fn fold_trino_drop_branch<F: Fold + ?Sized>(
    folder: &mut F,
    mut node: TrinoDropBranch,
) -> TrinoDropBranch {
    node.table_name = folder.fold_object_name(node.table_name);
    node.branch_name = folder.fold_ident(node.branch_name);
    node
}

impl SyntaxEq for TrinoDropBranch {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.table_name.syntax_eq(&other.table_name)
            && self.branch_name.syntax_eq(&other.branch_name)
            && self.exists == other.exists
    }
}

pub fn walk_trino_fast_forward_branch<V: Visit + ?Sized>(
    visitor: &mut V,
    node: &TrinoFastForwardBranch,
) {
    visitor.visit_object_name(&node.table_name);
    visitor.visit_ident(&node.source_branch_name);
    visitor.visit_ident(&node.target_branch_name);
}

pub fn fold_trino_fast_forward_branch<F: Fold + ?Sized>(
    folder: &mut F,
    mut node: TrinoFastForwardBranch,
) -> TrinoFastForwardBranch {
    node.table_name = folder.fold_object_name(node.table_name);
    node.source_branch_name = folder.fold_ident(node.source_branch_name);
    node.target_branch_name = folder.fold_ident(node.target_branch_name);
    node
}

impl SyntaxEq for TrinoFastForwardBranch {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.table_name.syntax_eq(&other.table_name)
            && self.source_branch_name.syntax_eq(&other.source_branch_name)
            && self.target_branch_name.syntax_eq(&other.target_branch_name)
    }
}

pub fn walk_trino_show_branches<V: Visit + ?Sized>(visitor: &mut V, node: &TrinoShowBranches) {
    visitor.visit_object_name(&node.table_name);
}

pub fn fold_trino_show_branches<F: Fold + ?Sized>(
    folder: &mut F,
    mut node: TrinoShowBranches,
) -> TrinoShowBranches {
    node.table_name = folder.fold_object_name(node.table_name);
    node
}

impl SyntaxEq for TrinoShowBranches {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.table_name.syntax_eq(&other.table_name)
    }
}
