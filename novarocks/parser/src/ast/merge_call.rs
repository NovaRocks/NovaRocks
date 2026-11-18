// SPDX-License-Identifier: Apache-2.0

use super::{AstVec, CallArgument, Expr, Fold, Ident, ObjectName, Relation, SyntaxEq, Visit};
use crate::Span;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TrinoMerge {
    pub target: Relation,
    pub source: Relation,
    pub predicate: Expr,
    pub cases: AstVec<TrinoMergeCase>,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TrinoMergeCase {
    Update {
        condition: Option<Expr>,
        assignments: AstVec<TrinoMergeAssignment>,
        span: Span,
    },
    Delete {
        condition: Option<Expr>,
        span: Span,
    },
    Insert {
        condition: Option<Expr>,
        columns: Vec<Ident>,
        values: AstVec<Expr>,
        span: Span,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TrinoMergeAssignment {
    pub target: Ident,
    pub value: Expr,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TrinoCall {
    pub name: ObjectName,
    pub arguments: AstVec<CallArgument>,
    pub span: Span,
}

impl TrinoMergeCase {
    pub const fn span(&self) -> Span {
        match self {
            Self::Update { span, .. } | Self::Delete { span, .. } | Self::Insert { span, .. } => {
                *span
            }
        }
    }
}

pub fn walk_trino_merge<V: Visit + ?Sized>(visitor: &mut V, node: &TrinoMerge) {
    visitor.visit_relation(&node.target);
    visitor.visit_relation(&node.source);
    visitor.visit_expr(&node.predicate);
    for case in &node.cases {
        visitor.visit_trino_merge_case(case);
    }
}

pub fn walk_trino_merge_case<V: Visit + ?Sized>(visitor: &mut V, node: &TrinoMergeCase) {
    let condition = match node {
        TrinoMergeCase::Update {
            condition,
            assignments,
            ..
        } => {
            for assignment in assignments {
                visitor.visit_trino_merge_assignment(assignment);
            }
            condition
        }
        TrinoMergeCase::Delete { condition, .. } => condition,
        TrinoMergeCase::Insert {
            condition,
            columns,
            values,
            ..
        } => {
            if let Some(condition) = condition {
                visitor.visit_expr(condition);
            }
            for column in columns {
                visitor.visit_ident(column);
            }
            for value in values {
                visitor.visit_expr(value);
            }
            return;
        }
    };
    if let Some(condition) = condition {
        visitor.visit_expr(condition);
    }
}

pub fn walk_trino_merge_assignment<V: Visit + ?Sized>(
    visitor: &mut V,
    node: &TrinoMergeAssignment,
) {
    visitor.visit_ident(&node.target);
    visitor.visit_expr(&node.value);
}

pub fn walk_trino_call<V: Visit + ?Sized>(visitor: &mut V, node: &TrinoCall) {
    visitor.visit_object_name(&node.name);
    for argument in &node.arguments {
        visitor.visit_call_argument(argument);
    }
}

pub fn fold_trino_merge<F: Fold + ?Sized>(folder: &mut F, mut node: TrinoMerge) -> TrinoMerge {
    node.target = folder.fold_relation(node.target);
    node.source = folder.fold_relation(node.source);
    node.predicate = folder.fold_expr(node.predicate);
    node.cases = node
        .cases
        .into_iter()
        .map(|case| folder.fold_trino_merge_case(case))
        .collect();
    node
}

pub fn fold_trino_merge_case<F: Fold + ?Sized>(
    folder: &mut F,
    node: TrinoMergeCase,
) -> TrinoMergeCase {
    match node {
        TrinoMergeCase::Update {
            condition,
            assignments,
            span,
        } => TrinoMergeCase::Update {
            assignments: assignments
                .into_iter()
                .map(|assignment| folder.fold_trino_merge_assignment(assignment))
                .collect(),
            condition: condition.map(|value| folder.fold_expr(value)),
            span,
        },
        TrinoMergeCase::Delete { condition, span } => TrinoMergeCase::Delete {
            condition: condition.map(|value| folder.fold_expr(value)),
            span,
        },
        TrinoMergeCase::Insert {
            condition,
            columns,
            values,
            span,
        } => TrinoMergeCase::Insert {
            condition: condition.map(|value| folder.fold_expr(value)),
            columns: columns
                .into_iter()
                .map(|column| folder.fold_ident(column))
                .collect(),
            values: values
                .into_iter()
                .map(|value| folder.fold_expr(value))
                .collect(),
            span,
        },
    }
}

pub fn fold_trino_merge_assignment<F: Fold + ?Sized>(
    folder: &mut F,
    mut node: TrinoMergeAssignment,
) -> TrinoMergeAssignment {
    node.target = folder.fold_ident(node.target);
    node.value = folder.fold_expr(node.value);
    node
}

pub fn fold_trino_call<F: Fold + ?Sized>(folder: &mut F, mut node: TrinoCall) -> TrinoCall {
    node.name = folder.fold_object_name(node.name);
    node.arguments = node
        .arguments
        .into_iter()
        .map(|argument| folder.fold_call_argument(argument))
        .collect();
    node
}

impl SyntaxEq for TrinoMerge {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.target.syntax_eq(&other.target)
            && self.source.syntax_eq(&other.source)
            && self.predicate.syntax_eq(&other.predicate)
            && same_slots(&self.cases, &other.cases)
    }
}

impl SyntaxEq for TrinoMergeCase {
    fn syntax_eq(&self, other: &Self) -> bool {
        match (self, other) {
            (
                Self::Update {
                    condition: left_condition,
                    assignments: left,
                    ..
                },
                Self::Update {
                    condition: right_condition,
                    assignments: right,
                    ..
                },
            ) => same_condition(left_condition, right_condition) && same_slots(left, right),
            (
                Self::Delete {
                    condition: left, ..
                },
                Self::Delete {
                    condition: right, ..
                },
            ) => same_condition(left, right),
            (
                Self::Insert {
                    condition: left_condition,
                    columns: left_columns,
                    values: left_values,
                    ..
                },
                Self::Insert {
                    condition: right_condition,
                    columns: right_columns,
                    values: right_values,
                    ..
                },
            ) => {
                same_condition(left_condition, right_condition)
                    && same_slots(left_columns, right_columns)
                    && same_slots(left_values, right_values)
            }
            _ => false,
        }
    }
}

impl SyntaxEq for TrinoMergeAssignment {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.target.syntax_eq(&other.target) && self.value.syntax_eq(&other.value)
    }
}

impl SyntaxEq for TrinoCall {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.name.syntax_eq(&other.name) && same_slots(&self.arguments, &other.arguments)
    }
}

fn same_slots<T: SyntaxEq>(left: &[T], right: &[T]) -> bool {
    left.len() == right.len()
        && left
            .iter()
            .zip(right)
            .all(|(left, right)| left.syntax_eq(right))
}

fn same_condition(left: &Option<Expr>, right: &Option<Expr>) -> bool {
    match (left, right) {
        (Some(left), Some(right)) => left.syntax_eq(right),
        (None, None) => true,
        _ => false,
    }
}
