// SPDX-License-Identifier: Apache-2.0

use super::{
    AstBox, AstVec, CallArgument, Expr, Fold, Ident, ObjectName, SyntaxEq, Table,
    TrinoColumnDefinition, TypeName, Visit,
};
use crate::Span;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TrinoRenameTable {
    pub source: ObjectName,
    pub target: ObjectName,
    pub exists: bool,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TrinoAddColumn {
    pub name: ObjectName,
    pub column: AstBox<TrinoColumnDefinition>,
    pub position: Option<TrinoColumnPosition>,
    pub table_exists: bool,
    pub column_not_exists: bool,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TrinoColumnPosition {
    First,
    Last,
    After(Ident),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TrinoRenameColumn {
    pub table: ObjectName,
    pub source: ObjectName,
    pub target: Ident,
    pub table_exists: bool,
    pub column_exists: bool,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TrinoDropColumn {
    pub table: ObjectName,
    pub field: ObjectName,
    pub table_exists: bool,
    pub column_exists: bool,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TrinoSetDefaultValue {
    pub table_name: ObjectName,
    pub column_name: ObjectName,
    pub default_value: Expr,
    pub table_exists: bool,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TrinoDropDefaultValue {
    pub table_name: ObjectName,
    pub column_name: ObjectName,
    pub table_exists: bool,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TrinoSetColumnType {
    pub table_name: ObjectName,
    pub column_name: ObjectName,
    pub data_type: TypeName,
    pub table_exists: bool,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TrinoDropNotNullConstraint {
    pub table: ObjectName,
    pub column: Ident,
    pub table_exists: bool,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TrinoTableExecute {
    pub table: Table,
    pub procedure_name: Ident,
    pub arguments: AstVec<CallArgument>,
    pub selection: Option<Expr>,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TrinoTruncateTable {
    pub table_name: ObjectName,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TrinoComment {
    pub kind: CommentKind,
    pub name: ObjectName,
    pub comment: Option<String>,
    pub span: Span,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CommentKind {
    Table,
    View,
    MaterializedView,
    Column,
}

pub fn walk_trino_add_column<V: Visit + ?Sized>(visitor: &mut V, node: &TrinoAddColumn) {
    visitor.visit_object_name(&node.name);
    visitor.visit_trino_column_definition(&node.column);
    if let Some(TrinoColumnPosition::After(after)) = &node.position {
        visitor.visit_ident(after);
    }
}

pub fn fold_trino_add_column<F: Fold + ?Sized>(
    folder: &mut F,
    mut node: TrinoAddColumn,
) -> TrinoAddColumn {
    node.name = folder.fold_object_name(node.name);
    node.column = AstBox::new(folder.fold_trino_column_definition(node.column.into_inner()));
    node.position = node.position.map(|position| match position {
        TrinoColumnPosition::After(after) => TrinoColumnPosition::After(folder.fold_ident(after)),
        other => other,
    });
    node
}

impl SyntaxEq for TrinoAddColumn {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.name.syntax_eq(&other.name)
            && self.column.syntax_eq(&other.column)
            && self.table_exists == other.table_exists
            && self.column_not_exists == other.column_not_exists
            && match (&self.position, &other.position) {
                (None, None)
                | (Some(TrinoColumnPosition::First), Some(TrinoColumnPosition::First))
                | (Some(TrinoColumnPosition::Last), Some(TrinoColumnPosition::Last)) => true,
                (
                    Some(TrinoColumnPosition::After(left)),
                    Some(TrinoColumnPosition::After(right)),
                ) => left.syntax_eq(right),
                _ => false,
            }
    }
}

pub fn walk_trino_table_execute<V: Visit + ?Sized>(visitor: &mut V, node: &TrinoTableExecute) {
    visitor.visit_table(&node.table);
    visitor.visit_ident(&node.procedure_name);
    for argument in &node.arguments {
        visitor.visit_call_argument(argument);
    }
    if let Some(selection) = &node.selection {
        visitor.visit_expr(selection);
    }
}

pub fn fold_trino_table_execute<F: Fold + ?Sized>(
    folder: &mut F,
    mut node: TrinoTableExecute,
) -> TrinoTableExecute {
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

impl SyntaxEq for TrinoTableExecute {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.table.syntax_eq(&other.table)
            && self.procedure_name.syntax_eq(&other.procedure_name)
            && self.arguments.len() == other.arguments.len()
            && self
                .arguments
                .iter()
                .zip(&other.arguments)
                .all(|(left, right)| left.syntax_eq(right))
            && match (&self.selection, &other.selection) {
                (None, None) => true,
                (Some(left), Some(right)) => left.syntax_eq(right),
                _ => false,
            }
    }
}

pub fn walk_trino_rename_table<V: Visit + ?Sized>(visitor: &mut V, node: &TrinoRenameTable) {
    visitor.visit_object_name(&node.source);
    visitor.visit_object_name(&node.target);
}

pub fn fold_trino_rename_table<F: Fold + ?Sized>(
    folder: &mut F,
    mut node: TrinoRenameTable,
) -> TrinoRenameTable {
    node.source = folder.fold_object_name(node.source);
    node.target = folder.fold_object_name(node.target);
    node
}

impl SyntaxEq for TrinoRenameTable {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.source.syntax_eq(&other.source)
            && self.target.syntax_eq(&other.target)
            && self.exists == other.exists
    }
}

pub fn walk_trino_rename_column<V: Visit + ?Sized>(visitor: &mut V, node: &TrinoRenameColumn) {
    visitor.visit_object_name(&node.table);
    visitor.visit_object_name(&node.source);
    visitor.visit_ident(&node.target);
}

pub fn fold_trino_rename_column<F: Fold + ?Sized>(
    folder: &mut F,
    mut node: TrinoRenameColumn,
) -> TrinoRenameColumn {
    node.table = folder.fold_object_name(node.table);
    node.source = folder.fold_object_name(node.source);
    node.target = folder.fold_ident(node.target);
    node
}

impl SyntaxEq for TrinoRenameColumn {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.table.syntax_eq(&other.table)
            && self.source.syntax_eq(&other.source)
            && self.target.syntax_eq(&other.target)
            && self.table_exists == other.table_exists
            && self.column_exists == other.column_exists
    }
}

pub fn walk_trino_drop_column<V: Visit + ?Sized>(visitor: &mut V, node: &TrinoDropColumn) {
    visitor.visit_object_name(&node.table);
    visitor.visit_object_name(&node.field);
}

pub fn fold_trino_drop_column<F: Fold + ?Sized>(
    folder: &mut F,
    mut node: TrinoDropColumn,
) -> TrinoDropColumn {
    node.table = folder.fold_object_name(node.table);
    node.field = folder.fold_object_name(node.field);
    node
}

impl SyntaxEq for TrinoDropColumn {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.table.syntax_eq(&other.table)
            && self.field.syntax_eq(&other.field)
            && self.table_exists == other.table_exists
            && self.column_exists == other.column_exists
    }
}

pub fn walk_trino_set_default_value<V: Visit + ?Sized>(
    visitor: &mut V,
    node: &TrinoSetDefaultValue,
) {
    visitor.visit_object_name(&node.table_name);
    visitor.visit_object_name(&node.column_name);
    visitor.visit_expr(&node.default_value);
}

pub fn fold_trino_set_default_value<F: Fold + ?Sized>(
    folder: &mut F,
    mut node: TrinoSetDefaultValue,
) -> TrinoSetDefaultValue {
    node.table_name = folder.fold_object_name(node.table_name);
    node.column_name = folder.fold_object_name(node.column_name);
    node.default_value = folder.fold_expr(node.default_value);
    node
}

impl SyntaxEq for TrinoSetDefaultValue {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.table_name.syntax_eq(&other.table_name)
            && self.column_name.syntax_eq(&other.column_name)
            && self.default_value.syntax_eq(&other.default_value)
            && self.table_exists == other.table_exists
    }
}

pub fn walk_trino_drop_default_value<V: Visit + ?Sized>(
    visitor: &mut V,
    node: &TrinoDropDefaultValue,
) {
    visitor.visit_object_name(&node.table_name);
    visitor.visit_object_name(&node.column_name);
}

pub fn fold_trino_drop_default_value<F: Fold + ?Sized>(
    folder: &mut F,
    mut node: TrinoDropDefaultValue,
) -> TrinoDropDefaultValue {
    node.table_name = folder.fold_object_name(node.table_name);
    node.column_name = folder.fold_object_name(node.column_name);
    node
}

impl SyntaxEq for TrinoDropDefaultValue {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.table_name.syntax_eq(&other.table_name)
            && self.column_name.syntax_eq(&other.column_name)
            && self.table_exists == other.table_exists
    }
}

pub fn walk_trino_set_column_type<V: Visit + ?Sized>(visitor: &mut V, node: &TrinoSetColumnType) {
    visitor.visit_object_name(&node.table_name);
    visitor.visit_object_name(&node.column_name);
    visitor.visit_type_name(&node.data_type);
}

pub fn fold_trino_set_column_type<F: Fold + ?Sized>(
    folder: &mut F,
    mut node: TrinoSetColumnType,
) -> TrinoSetColumnType {
    node.table_name = folder.fold_object_name(node.table_name);
    node.column_name = folder.fold_object_name(node.column_name);
    node.data_type = folder.fold_type_name(node.data_type);
    node
}

impl SyntaxEq for TrinoSetColumnType {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.table_name.syntax_eq(&other.table_name)
            && self.column_name.syntax_eq(&other.column_name)
            && self.data_type.syntax_eq(&other.data_type)
            && self.table_exists == other.table_exists
    }
}

pub fn walk_trino_drop_not_null_constraint<V: Visit + ?Sized>(
    visitor: &mut V,
    node: &TrinoDropNotNullConstraint,
) {
    visitor.visit_object_name(&node.table);
    visitor.visit_ident(&node.column);
}

pub fn fold_trino_drop_not_null_constraint<F: Fold + ?Sized>(
    folder: &mut F,
    mut node: TrinoDropNotNullConstraint,
) -> TrinoDropNotNullConstraint {
    node.table = folder.fold_object_name(node.table);
    node.column = folder.fold_ident(node.column);
    node
}

impl SyntaxEq for TrinoDropNotNullConstraint {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.table.syntax_eq(&other.table)
            && self.column.syntax_eq(&other.column)
            && self.table_exists == other.table_exists
    }
}

pub fn walk_trino_truncate_table<V: Visit + ?Sized>(visitor: &mut V, node: &TrinoTruncateTable) {
    visitor.visit_object_name(&node.table_name);
}

pub fn fold_trino_truncate_table<F: Fold + ?Sized>(
    folder: &mut F,
    mut node: TrinoTruncateTable,
) -> TrinoTruncateTable {
    node.table_name = folder.fold_object_name(node.table_name);
    node
}

impl SyntaxEq for TrinoTruncateTable {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.table_name.syntax_eq(&other.table_name)
    }
}

pub fn walk_trino_comment<V: Visit + ?Sized>(visitor: &mut V, node: &TrinoComment) {
    visitor.visit_object_name(&node.name);
}

pub fn fold_trino_comment<F: Fold + ?Sized>(
    folder: &mut F,
    mut node: TrinoComment,
) -> TrinoComment {
    node.name = folder.fold_object_name(node.name);
    node
}

impl SyntaxEq for TrinoComment {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.name.syntax_eq(&other.name) && self.kind == other.kind && self.comment == other.comment
    }
}
