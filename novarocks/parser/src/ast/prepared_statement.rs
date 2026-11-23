// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::Span;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TrinoPrepare {
    pub name: Ident,
    pub statement: AstBox<TrinoStatement>,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TrinoDeallocate {
    pub name: Ident,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TrinoExecute {
    pub name: Ident,
    pub parameters: AstVec<Expr>,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TrinoExecuteImmediate {
    pub statement_text: String,
    pub statement_text_span: Span,
    pub parameters: AstVec<Expr>,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TrinoDescribeInput {
    pub name: Ident,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TrinoDescribeOutput {
    pub input: TrinoDescribeOutputInput,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TrinoDescribeOutputInput {
    Prepared(Ident),
    Query(AstBox<Query>),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TrinoCreateFunction {
    pub specification: AstBox<FunctionSpecification>,
    pub replace: bool,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TrinoDropFunction {
    pub name: ObjectName,
    pub parameters: AstVec<FunctionParameter>,
    pub exists: bool,
    pub span: Span,
}

impl SyntaxEq for TrinoDescribeOutput {
    fn syntax_eq(&self, other: &Self) -> bool {
        match (&self.input, &other.input) {
            (TrinoDescribeOutputInput::Prepared(a), TrinoDescribeOutputInput::Prepared(b)) => {
                a.syntax_eq(b)
            }
            (TrinoDescribeOutputInput::Query(a), TrinoDescribeOutputInput::Query(b)) => {
                a.syntax_eq(b)
            }
            _ => false,
        }
    }
}

pub fn walk_trino_describe_output<V: Visit + ?Sized>(visitor: &mut V, node: &TrinoDescribeOutput) {
    match &node.input {
        TrinoDescribeOutputInput::Prepared(name) => visitor.visit_ident(name),
        TrinoDescribeOutputInput::Query(query) => visitor.visit_query(query),
    }
}

pub fn fold_trino_describe_output<F: Fold + ?Sized>(
    folder: &mut F,
    mut node: TrinoDescribeOutput,
) -> TrinoDescribeOutput {
    node.input = match node.input {
        TrinoDescribeOutputInput::Prepared(name) => {
            TrinoDescribeOutputInput::Prepared(folder.fold_ident(name))
        }
        TrinoDescribeOutputInput::Query(query) => {
            TrinoDescribeOutputInput::Query(AstBox::new(folder.fold_query(query.into_inner())))
        }
    };
    node
}

impl SyntaxEq for TrinoPrepare {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.name.syntax_eq(&other.name) && self.statement.syntax_eq(&other.statement)
    }
}

pub fn walk_trino_prepare<V: Visit + ?Sized>(visitor: &mut V, node: &TrinoPrepare) {
    visitor.visit_ident(&node.name);
    visitor.visit_trino_statement(&node.statement);
}

pub fn fold_trino_prepare<F: Fold + ?Sized>(
    folder: &mut F,
    mut node: TrinoPrepare,
) -> TrinoPrepare {
    node.name = folder.fold_ident(node.name);
    node.statement = AstBox::new(folder.fold_trino_statement(node.statement.into_inner()));
    node
}

impl SyntaxEq for TrinoDeallocate {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.name.syntax_eq(&other.name)
    }
}

pub fn walk_trino_deallocate<V: Visit + ?Sized>(visitor: &mut V, node: &TrinoDeallocate) {
    visitor.visit_ident(&node.name);
}

pub fn fold_trino_deallocate<F: Fold + ?Sized>(
    folder: &mut F,
    mut node: TrinoDeallocate,
) -> TrinoDeallocate {
    node.name = folder.fold_ident(node.name);
    node
}

impl SyntaxEq for TrinoExecute {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.name.syntax_eq(&other.name)
            && self.parameters.len() == other.parameters.len()
            && self
                .parameters
                .iter()
                .zip(&other.parameters)
                .all(|(a, b)| a.syntax_eq(b))
    }
}

pub fn walk_trino_execute<V: Visit + ?Sized>(visitor: &mut V, node: &TrinoExecute) {
    visitor.visit_ident(&node.name);
    for value in &node.parameters {
        visitor.visit_expr(value);
    }
}

pub fn fold_trino_execute<F: Fold + ?Sized>(
    folder: &mut F,
    mut node: TrinoExecute,
) -> TrinoExecute {
    node.name = folder.fold_ident(node.name);
    node.parameters = node
        .parameters
        .into_iter()
        .map(|value| folder.fold_expr(value))
        .collect();
    node
}

impl SyntaxEq for TrinoExecuteImmediate {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.statement_text == other.statement_text
            && self.parameters.len() == other.parameters.len()
            && self
                .parameters
                .iter()
                .zip(&other.parameters)
                .all(|(a, b)| a.syntax_eq(b))
    }
}

pub fn walk_trino_execute_immediate<V: Visit + ?Sized>(
    visitor: &mut V,
    node: &TrinoExecuteImmediate,
) {
    for value in &node.parameters {
        visitor.visit_expr(value);
    }
}

pub fn fold_trino_execute_immediate<F: Fold + ?Sized>(
    folder: &mut F,
    mut node: TrinoExecuteImmediate,
) -> TrinoExecuteImmediate {
    node.parameters = node
        .parameters
        .into_iter()
        .map(|value| folder.fold_expr(value))
        .collect();
    node
}

impl SyntaxEq for TrinoDescribeInput {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.name.syntax_eq(&other.name)
    }
}

pub fn walk_trino_describe_input<V: Visit + ?Sized>(visitor: &mut V, node: &TrinoDescribeInput) {
    visitor.visit_ident(&node.name);
}

pub fn fold_trino_describe_input<F: Fold + ?Sized>(
    folder: &mut F,
    mut node: TrinoDescribeInput,
) -> TrinoDescribeInput {
    node.name = folder.fold_ident(node.name);
    node
}

impl SyntaxEq for TrinoCreateFunction {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.specification.syntax_eq(&other.specification) && self.replace == other.replace
    }
}

pub fn walk_trino_create_function<V: Visit + ?Sized>(visitor: &mut V, node: &TrinoCreateFunction) {
    visitor.visit_function_specification(&node.specification);
}

pub fn fold_trino_create_function<F: Fold + ?Sized>(
    folder: &mut F,
    mut node: TrinoCreateFunction,
) -> TrinoCreateFunction {
    node.specification =
        AstBox::new(folder.fold_function_specification(node.specification.into_inner()));
    node
}

impl SyntaxEq for TrinoDropFunction {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.name.syntax_eq(&other.name)
            && self.parameters.len() == other.parameters.len()
            && self
                .parameters
                .iter()
                .zip(&other.parameters)
                .all(|(a, b)| a.syntax_eq(b))
            && self.exists == other.exists
    }
}

pub fn walk_trino_drop_function<V: Visit + ?Sized>(visitor: &mut V, node: &TrinoDropFunction) {
    visitor.visit_object_name(&node.name);
    for value in &node.parameters {
        visitor.visit_function_parameter(value);
    }
}

pub fn fold_trino_drop_function<F: Fold + ?Sized>(
    folder: &mut F,
    mut node: TrinoDropFunction,
) -> TrinoDropFunction {
    node.name = folder.fold_object_name(node.name);
    node.parameters = node
        .parameters
        .into_iter()
        .map(|value| folder.fold_function_parameter(value))
        .collect();
    node
}
