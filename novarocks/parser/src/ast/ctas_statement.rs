// SPDX-License-Identifier: Apache-2.0

use super::{
    AstBox, AstVec, Fold, Ident, ObjectName, Query, StatementProperty, StatementSaveMode, SyntaxEq,
    Visit,
};
use crate::Span;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TrinoCreateTableAsSelect {
    pub name: ObjectName,
    pub query: AstBox<Query>,
    pub save_mode: StatementSaveMode,
    pub properties: AstVec<StatementProperty>,
    pub with_data: bool,
    pub column_aliases: Option<Vec<Ident>>,
    pub comment: Option<String>,
    pub span: Span,
}

pub fn walk_trino_create_table_as_select<V: Visit + ?Sized>(
    visitor: &mut V,
    node: &TrinoCreateTableAsSelect,
) {
    visitor.visit_object_name(&node.name);
    if let Some(aliases) = &node.column_aliases {
        for alias in aliases {
            visitor.visit_ident(alias);
        }
    }
    for property in &node.properties {
        visitor.visit_statement_property(property);
    }
    visitor.visit_query(&node.query);
}

pub fn fold_trino_create_table_as_select<F: Fold + ?Sized>(
    folder: &mut F,
    mut node: TrinoCreateTableAsSelect,
) -> TrinoCreateTableAsSelect {
    node.name = folder.fold_object_name(node.name);
    node.column_aliases = node.column_aliases.map(|aliases| {
        aliases
            .into_iter()
            .map(|alias| folder.fold_ident(alias))
            .collect()
    });
    node.properties = node
        .properties
        .into_iter()
        .map(|property| folder.fold_statement_property(property))
        .collect();
    node.query = AstBox::new(folder.fold_query(node.query.into_inner()));
    node
}

impl SyntaxEq for TrinoCreateTableAsSelect {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.name.syntax_eq(&other.name)
            && self.query.syntax_eq(&other.query)
            && self.save_mode == other.save_mode
            && same_slots(&self.properties, &other.properties)
            && self.with_data == other.with_data
            && self.comment == other.comment
            && match (&self.column_aliases, &other.column_aliases) {
                (None, None) => true,
                (Some(left), Some(right)) => same_slots(left, right),
                _ => false,
            }
    }
}

fn same_slots<T: SyntaxEq>(left: &[T], right: &[T]) -> bool {
    left.len() == right.len()
        && left
            .iter()
            .zip(right)
            .all(|(left, right)| left.syntax_eq(right))
}
