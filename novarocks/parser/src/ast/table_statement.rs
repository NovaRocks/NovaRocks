// SPDX-License-Identifier: Apache-2.0

use super::{AstBox, AstVec, Expr, Fold, ObjectName, StatementProperty, SyntaxEq, TypeName, Visit};
use crate::Span;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StatementSaveMode {
    Fail,
    Ignore,
    Replace,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TrinoCreateTable {
    pub name: ObjectName,
    pub elements: AstVec<TrinoTableElement>,
    pub save_mode: StatementSaveMode,
    pub properties: AstVec<StatementProperty>,
    pub comment: Option<String>,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TrinoTableElement {
    Column(AstBox<TrinoColumnDefinition>),
    Like(TrinoLikeClause),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TrinoColumnDefinition {
    pub name: ObjectName,
    pub data_type: TypeName,
    /// The grammar constructs a literal expression here.
    pub default_value: Option<Expr>,
    pub nullable: bool,
    pub properties: AstVec<StatementProperty>,
    pub comment: Option<String>,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TrinoLikeClause {
    pub table_name: ObjectName,
    pub properties_option: Option<LikePropertiesOption>,
    pub span: Span,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LikePropertiesOption {
    Including,
    Excluding,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TrinoDropTable {
    pub table_name: ObjectName,
    pub exists: bool,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TrinoAnalyze {
    pub table_name: ObjectName,
    pub properties: AstVec<StatementProperty>,
    pub span: Span,
}

pub fn walk_trino_create_table<V: Visit + ?Sized>(visitor: &mut V, node: &TrinoCreateTable) {
    visitor.visit_object_name(&node.name);
    for element in &node.elements {
        visitor.visit_trino_table_element(element);
    }
    for property in &node.properties {
        visitor.visit_statement_property(property);
    }
}

pub fn fold_trino_create_table<F: Fold + ?Sized>(
    folder: &mut F,
    mut node: TrinoCreateTable,
) -> TrinoCreateTable {
    node.name = folder.fold_object_name(node.name);
    node.elements = node
        .elements
        .into_iter()
        .map(|element| folder.fold_trino_table_element(element))
        .collect();
    node.properties = node
        .properties
        .into_iter()
        .map(|property| folder.fold_statement_property(property))
        .collect();
    node
}

pub fn walk_trino_table_element<V: Visit + ?Sized>(visitor: &mut V, node: &TrinoTableElement) {
    match node {
        TrinoTableElement::Column(column) => visitor.visit_trino_column_definition(column),
        TrinoTableElement::Like(like) => visitor.visit_trino_like_clause(like),
    }
}

pub fn fold_trino_table_element<F: Fold + ?Sized>(
    folder: &mut F,
    node: TrinoTableElement,
) -> TrinoTableElement {
    match node {
        TrinoTableElement::Column(column) => TrinoTableElement::Column(AstBox::new(
            folder.fold_trino_column_definition(column.into_inner()),
        )),
        TrinoTableElement::Like(like) => {
            TrinoTableElement::Like(folder.fold_trino_like_clause(like))
        }
    }
}

pub fn walk_trino_column_definition<V: Visit + ?Sized>(
    visitor: &mut V,
    node: &TrinoColumnDefinition,
) {
    visitor.visit_object_name(&node.name);
    visitor.visit_type_name(&node.data_type);
    if let Some(default) = &node.default_value {
        visitor.visit_expr(default);
    }
    for property in &node.properties {
        visitor.visit_statement_property(property);
    }
}

pub fn fold_trino_column_definition<F: Fold + ?Sized>(
    folder: &mut F,
    mut node: TrinoColumnDefinition,
) -> TrinoColumnDefinition {
    node.name = folder.fold_object_name(node.name);
    node.data_type = folder.fold_type_name(node.data_type);
    node.default_value = node.default_value.map(|default| folder.fold_expr(default));
    node.properties = node
        .properties
        .into_iter()
        .map(|property| folder.fold_statement_property(property))
        .collect();
    node
}

pub fn walk_trino_like_clause<V: Visit + ?Sized>(visitor: &mut V, node: &TrinoLikeClause) {
    visitor.visit_object_name(&node.table_name);
}

pub fn fold_trino_like_clause<F: Fold + ?Sized>(
    folder: &mut F,
    mut node: TrinoLikeClause,
) -> TrinoLikeClause {
    node.table_name = folder.fold_object_name(node.table_name);
    node
}

pub fn walk_trino_drop_table<V: Visit + ?Sized>(visitor: &mut V, node: &TrinoDropTable) {
    visitor.visit_object_name(&node.table_name);
}

pub fn fold_trino_drop_table<F: Fold + ?Sized>(
    folder: &mut F,
    mut node: TrinoDropTable,
) -> TrinoDropTable {
    node.table_name = folder.fold_object_name(node.table_name);
    node
}

pub fn walk_trino_analyze<V: Visit + ?Sized>(visitor: &mut V, node: &TrinoAnalyze) {
    visitor.visit_object_name(&node.table_name);
    for property in &node.properties {
        visitor.visit_statement_property(property);
    }
}

pub fn fold_trino_analyze<F: Fold + ?Sized>(
    folder: &mut F,
    mut node: TrinoAnalyze,
) -> TrinoAnalyze {
    node.table_name = folder.fold_object_name(node.table_name);
    node.properties = node
        .properties
        .into_iter()
        .map(|property| folder.fold_statement_property(property))
        .collect();
    node
}

impl SyntaxEq for TrinoCreateTable {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.name.syntax_eq(&other.name)
            && self.save_mode == other.save_mode
            && self.comment == other.comment
            && syntax_eq_slots(&self.elements, &other.elements)
            && syntax_eq_slots(&self.properties, &other.properties)
    }
}

impl SyntaxEq for TrinoTableElement {
    fn syntax_eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Column(left), Self::Column(right)) => left.syntax_eq(right),
            (Self::Like(left), Self::Like(right)) => left.syntax_eq(right),
            _ => false,
        }
    }
}

impl SyntaxEq for TrinoColumnDefinition {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.name.syntax_eq(&other.name)
            && self.data_type.syntax_eq(&other.data_type)
            && self.nullable == other.nullable
            && self.comment == other.comment
            && syntax_eq_slots(&self.properties, &other.properties)
            && match (&self.default_value, &other.default_value) {
                (None, None) => true,
                (Some(left), Some(right)) => left.syntax_eq(right),
                _ => false,
            }
    }
}

impl SyntaxEq for TrinoLikeClause {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.table_name.syntax_eq(&other.table_name)
            && self.properties_option == other.properties_option
    }
}

impl SyntaxEq for TrinoDropTable {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.table_name.syntax_eq(&other.table_name) && self.exists == other.exists
    }
}

impl SyntaxEq for TrinoAnalyze {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.table_name.syntax_eq(&other.table_name)
            && syntax_eq_slots(&self.properties, &other.properties)
    }
}

fn syntax_eq_slots<T: SyntaxEq>(left: &[T], right: &[T]) -> bool {
    left.len() == right.len()
        && left
            .iter()
            .zip(right)
            .all(|(left, right)| left.syntax_eq(right))
}
