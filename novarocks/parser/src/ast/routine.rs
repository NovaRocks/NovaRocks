// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.

//! Typed inline routine syntax facts.
//!
//! This module owns no execution capability or raw SQL fallback. Characteristic
//! order, duplicate characteristics, optional ELSE clauses, and optional labels
//! are retained exactly. Grammar and parser construction enforce cardinality.

use super::{AstVec, Expr, Fold, Ident, ObjectName, SyntaxEq, TypeName, Visit};
use crate::Span;

/// One FUNCTION declaration, with exactly one SQL or external body.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FunctionSpecification {
    pub name: ObjectName,
    pub parameters: Vec<FunctionParameter>,
    pub returns: TypeName,
    pub characteristics: Vec<RoutineCharacteristic>,
    pub body: RoutineBody,
    pub span: Span,
}

/// A parameter may omit its name; its type is always present.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FunctionParameter {
    pub name: Option<Ident>,
    pub data_type: TypeName,
    pub span: Span,
}

/// One routine WITH property; None means DEFAULT, never a missing value.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RoutineProperty {
    pub name: Ident,
    pub value: Option<Expr>,
    pub span: Span,
}

/// RETURN owns one value expression.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReturnStatement {
    pub value: Expr,
    pub span: Span,
}

/// SET changes the named local target using one expression.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AssignmentStatement {
    pub target: Ident,
    pub value: Expr,
    pub span: Span,
}

/// A missing operand distinguishes searched CASE from simple CASE.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CaseStatement {
    pub expression: Option<Expr>,
    pub when_clauses: Vec<CaseStatementWhenClause>,
    pub else_clause: Option<ElseClause>,
    pub span: Span,
}

/// A CASE arm keeps its condition/value and ordered control statements.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CaseStatementWhenClause {
    pub expression: Expr,
    pub statements: AstVec<ControlStatement>,
    pub span: Span,
}

/// IF keeps its THEN body, ordered ELSEIF clauses, and optional ELSE.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IfStatement {
    pub expression: Expr,
    pub statements: AstVec<ControlStatement>,
    pub else_if_clauses: Vec<ElseIfClause>,
    pub else_clause: Option<ElseClause>,
    pub span: Span,
}

/// An ELSEIF clause is a node, not a nested IF abbreviation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ElseIfClause {
    pub expression: Expr,
    pub statements: AstVec<ControlStatement>,
    pub span: Span,
}

/// ELSE presence is distinct from absence, even for programmatic empty lists.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ElseClause {
    pub statements: AstVec<ControlStatement>,
    pub span: Span,
}

/// ITERATE requires an explicit label.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IterateStatement {
    pub label: Ident,
    pub span: Span,
}

/// LEAVE requires an explicit label.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LeaveStatement {
    pub label: Ident,
    pub span: Span,
}

/// DECLARE keeps all names, one shared type, and an optional default.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VariableDeclaration {
    pub names: Vec<Ident>,
    pub data_type: TypeName,
    pub default_value: Option<Expr>,
    pub span: Span,
}

/// BEGIN keeps declarations separately from executable statements; no label.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompoundStatement {
    pub variable_declarations: Vec<VariableDeclaration>,
    pub statements: AstVec<ControlStatement>,
    pub span: Span,
}

/// A LOOP retains its optional opening label.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LoopStatement {
    pub label: Option<Ident>,
    pub statements: AstVec<ControlStatement>,
    pub span: Span,
}

/// WHILE checks its condition before its ordered body.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WhileStatement {
    pub label: Option<Ident>,
    pub expression: Expr,
    pub statements: AstVec<ControlStatement>,
    pub span: Span,
}

/// REPEAT keeps its ordered body and trailing UNTIL condition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RepeatStatement {
    pub label: Option<Ident>,
    pub statements: AstVec<ControlStatement>,
    pub condition: Expr,
    pub span: Span,
}

/// The Java optional statement/definition pair has an exclusive-or invariant.
/// The external string is decoded content after AstBuilder's leading-newline
/// removal. It is never a raw SQL representation of a control-statement tree.
#[derive(Clone, Debug, Eq, PartialEq)]
#[expect(
    clippy::large_enum_variant,
    reason = "the SQL body owns the original typed statement and its recursion guard"
)]
pub enum RoutineBody {
    Sql(ControlStatement),
    External { definition: String, span: Span },
}

/// Ordered routine attributes. Repetition is retained for later validation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RoutineCharacteristic {
    Language {
        language: Ident,
        span: Span,
    },
    Deterministic {
        deterministic: bool,
        span: Span,
    },
    NullInput {
        called_on_null: bool,
        span: Span,
    },
    Security {
        security: RoutineSecurity,
        span: Span,
    },
    Comment {
        comment: String,
        span: Span,
    },
    Properties {
        properties: Vec<RoutineProperty>,
        span: Span,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RoutineSecurity {
    Invoker,
    Definer,
}

/// The complete routine control vocabulary. Variable declarations are retained
/// as control nodes, while the grammar emits them only in the BEGIN prefix.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ControlStatement {
    Return(ReturnStatement),
    Assignment(AssignmentStatement),
    Case(CaseStatement),
    If(IfStatement),
    Iterate(IterateStatement),
    Leave(LeaveStatement),
    VariableDeclaration(VariableDeclaration),
    Compound(CompoundStatement),
    Loop(LoopStatement),
    While(WhileStatement),
    Repeat(RepeatStatement),
}

impl FunctionSpecification {
    pub const fn span(&self) -> Span {
        self.span
    }
}

impl FunctionParameter {
    pub const fn span(&self) -> Span {
        self.span
    }
}

impl RoutineProperty {
    pub const fn span(&self) -> Span {
        self.span
    }
}

impl ReturnStatement {
    pub const fn span(&self) -> Span {
        self.span
    }
}

impl AssignmentStatement {
    pub const fn span(&self) -> Span {
        self.span
    }
}

impl CaseStatement {
    pub const fn span(&self) -> Span {
        self.span
    }
}

impl CaseStatementWhenClause {
    pub const fn span(&self) -> Span {
        self.span
    }
}

impl IfStatement {
    pub const fn span(&self) -> Span {
        self.span
    }
}

impl ElseIfClause {
    pub const fn span(&self) -> Span {
        self.span
    }
}

impl ElseClause {
    pub const fn span(&self) -> Span {
        self.span
    }
}

impl IterateStatement {
    pub const fn span(&self) -> Span {
        self.span
    }
}

impl LeaveStatement {
    pub const fn span(&self) -> Span {
        self.span
    }
}

impl VariableDeclaration {
    pub const fn span(&self) -> Span {
        self.span
    }
}

impl CompoundStatement {
    pub const fn span(&self) -> Span {
        self.span
    }
}

impl LoopStatement {
    pub const fn span(&self) -> Span {
        self.span
    }
}

impl WhileStatement {
    pub const fn span(&self) -> Span {
        self.span
    }
}

impl RepeatStatement {
    pub const fn span(&self) -> Span {
        self.span
    }
}

impl RoutineBody {
    pub const fn span(&self) -> Span {
        match self {
            Self::Sql(statement) => statement.span(),
            Self::External { span, .. } => *span,
        }
    }
}

impl RoutineCharacteristic {
    pub const fn span(&self) -> Span {
        match self {
            Self::Language { span, .. }
            | Self::Deterministic { span, .. }
            | Self::NullInput { span, .. }
            | Self::Security { span, .. }
            | Self::Comment { span, .. }
            | Self::Properties { span, .. } => *span,
        }
    }
}

impl ControlStatement {
    pub const fn span(&self) -> Span {
        match self {
            Self::Return(node) => node.span(),
            Self::Assignment(node) => node.span(),
            Self::Case(node) => node.span(),
            Self::If(node) => node.span(),
            Self::Iterate(node) => node.span(),
            Self::Leave(node) => node.span(),
            Self::VariableDeclaration(node) => node.span(),
            Self::Compound(node) => node.span(),
            Self::Loop(node) => node.span(),
            Self::While(node) => node.span(),
            Self::Repeat(node) => node.span(),
        }
    }
}

pub fn walk_function_specification<V: Visit + ?Sized>(
    visitor: &mut V,
    node: &FunctionSpecification,
) {
    visitor.visit_object_name(&node.name);
    for child in &node.parameters {
        visitor.visit_function_parameter(child);
    }
    visitor.visit_type_name(&node.returns);
    for child in &node.characteristics {
        visitor.visit_routine_characteristic(child);
    }
    visitor.visit_routine_body(&node.body);
}

pub fn walk_function_parameter<V: Visit + ?Sized>(visitor: &mut V, node: &FunctionParameter) {
    if let Some(child) = &node.name {
        visitor.visit_ident(child);
    }
    visitor.visit_type_name(&node.data_type);
}

pub fn walk_routine_property<V: Visit + ?Sized>(visitor: &mut V, node: &RoutineProperty) {
    visitor.visit_ident(&node.name);
    if let Some(child) = &node.value {
        visitor.visit_expr(child);
    }
}

pub fn walk_return_statement<V: Visit + ?Sized>(visitor: &mut V, node: &ReturnStatement) {
    visitor.visit_expr(&node.value);
}

pub fn walk_assignment_statement<V: Visit + ?Sized>(visitor: &mut V, node: &AssignmentStatement) {
    visitor.visit_ident(&node.target);
    visitor.visit_expr(&node.value);
}

pub fn walk_case_statement<V: Visit + ?Sized>(visitor: &mut V, node: &CaseStatement) {
    if let Some(child) = &node.expression {
        visitor.visit_expr(child);
    }
    for child in &node.when_clauses {
        visitor.visit_case_statement_when_clause(child);
    }
    if let Some(child) = &node.else_clause {
        visitor.visit_else_clause(child);
    }
}

pub fn walk_case_statement_when_clause<V: Visit + ?Sized>(
    visitor: &mut V,
    node: &CaseStatementWhenClause,
) {
    visitor.visit_expr(&node.expression);
    for child in &node.statements {
        visitor.visit_control_statement(child);
    }
}

pub fn walk_if_statement<V: Visit + ?Sized>(visitor: &mut V, node: &IfStatement) {
    visitor.visit_expr(&node.expression);
    for child in &node.statements {
        visitor.visit_control_statement(child);
    }
    for child in &node.else_if_clauses {
        visitor.visit_else_if_clause(child);
    }
    if let Some(child) = &node.else_clause {
        visitor.visit_else_clause(child);
    }
}

pub fn walk_else_if_clause<V: Visit + ?Sized>(visitor: &mut V, node: &ElseIfClause) {
    visitor.visit_expr(&node.expression);
    for child in &node.statements {
        visitor.visit_control_statement(child);
    }
}

pub fn walk_else_clause<V: Visit + ?Sized>(visitor: &mut V, node: &ElseClause) {
    for child in &node.statements {
        visitor.visit_control_statement(child);
    }
}

pub fn walk_iterate_statement<V: Visit + ?Sized>(visitor: &mut V, node: &IterateStatement) {
    visitor.visit_ident(&node.label);
}

pub fn walk_leave_statement<V: Visit + ?Sized>(visitor: &mut V, node: &LeaveStatement) {
    visitor.visit_ident(&node.label);
}

pub fn walk_variable_declaration<V: Visit + ?Sized>(visitor: &mut V, node: &VariableDeclaration) {
    for child in &node.names {
        visitor.visit_ident(child);
    }
    visitor.visit_type_name(&node.data_type);
    if let Some(child) = &node.default_value {
        visitor.visit_expr(child);
    }
}

pub fn walk_compound_statement<V: Visit + ?Sized>(visitor: &mut V, node: &CompoundStatement) {
    for child in &node.variable_declarations {
        visitor.visit_variable_declaration(child);
    }
    for child in &node.statements {
        visitor.visit_control_statement(child);
    }
}

pub fn walk_loop_statement<V: Visit + ?Sized>(visitor: &mut V, node: &LoopStatement) {
    if let Some(child) = &node.label {
        visitor.visit_ident(child);
    }
    for child in &node.statements {
        visitor.visit_control_statement(child);
    }
}

pub fn walk_while_statement<V: Visit + ?Sized>(visitor: &mut V, node: &WhileStatement) {
    if let Some(child) = &node.label {
        visitor.visit_ident(child);
    }
    visitor.visit_expr(&node.expression);
    for child in &node.statements {
        visitor.visit_control_statement(child);
    }
}

pub fn walk_repeat_statement<V: Visit + ?Sized>(visitor: &mut V, node: &RepeatStatement) {
    if let Some(child) = &node.label {
        visitor.visit_ident(child);
    }
    for child in &node.statements {
        visitor.visit_control_statement(child);
    }
    visitor.visit_expr(&node.condition);
}

pub fn walk_routine_body<V: Visit + ?Sized>(visitor: &mut V, node: &RoutineBody) {
    match node {
        RoutineBody::Sql(statement) => visitor.visit_control_statement(statement),
        RoutineBody::External { .. } => {}
    }
}

pub fn walk_routine_characteristic<V: Visit + ?Sized>(
    visitor: &mut V,
    node: &RoutineCharacteristic,
) {
    match node {
        RoutineCharacteristic::Language { language, .. } => visitor.visit_ident(language),
        RoutineCharacteristic::Properties { properties, .. } => {
            for property in properties {
                visitor.visit_routine_property(property);
            }
        }
        RoutineCharacteristic::Deterministic { .. }
        | RoutineCharacteristic::NullInput { .. }
        | RoutineCharacteristic::Security { .. }
        | RoutineCharacteristic::Comment { .. } => {}
    }
}

#[recursive::recursive]
pub fn walk_control_statement<V: Visit + ?Sized>(visitor: &mut V, node: &ControlStatement) {
    match node {
        ControlStatement::Return(child) => visitor.visit_return_statement(child),
        ControlStatement::Assignment(child) => visitor.visit_assignment_statement(child),
        ControlStatement::Case(child) => visitor.visit_case_statement(child),
        ControlStatement::If(child) => visitor.visit_if_statement(child),
        ControlStatement::Iterate(child) => visitor.visit_iterate_statement(child),
        ControlStatement::Leave(child) => visitor.visit_leave_statement(child),
        ControlStatement::VariableDeclaration(child) => visitor.visit_variable_declaration(child),
        ControlStatement::Compound(child) => visitor.visit_compound_statement(child),
        ControlStatement::Loop(child) => visitor.visit_loop_statement(child),
        ControlStatement::While(child) => visitor.visit_while_statement(child),
        ControlStatement::Repeat(child) => visitor.visit_repeat_statement(child),
    }
}

pub fn fold_function_specification<F: Fold + ?Sized>(
    folder: &mut F,
    mut node: FunctionSpecification,
) -> FunctionSpecification {
    node.name = folder.fold_object_name(node.name);
    node.parameters = node
        .parameters
        .into_iter()
        .map(|child| folder.fold_function_parameter(child))
        .collect();
    node.returns = folder.fold_type_name(node.returns);
    node.characteristics = node
        .characteristics
        .into_iter()
        .map(|child| folder.fold_routine_characteristic(child))
        .collect();
    node.body = folder.fold_routine_body(node.body);
    node
}

pub fn fold_function_parameter<F: Fold + ?Sized>(
    folder: &mut F,
    mut node: FunctionParameter,
) -> FunctionParameter {
    node.name = node.name.map(|child| folder.fold_ident(child));
    node.data_type = folder.fold_type_name(node.data_type);
    node
}

pub fn fold_routine_property<F: Fold + ?Sized>(
    folder: &mut F,
    mut node: RoutineProperty,
) -> RoutineProperty {
    node.name = folder.fold_ident(node.name);
    node.value = node.value.map(|child| folder.fold_expr(child));
    node
}

pub fn fold_return_statement<F: Fold + ?Sized>(
    folder: &mut F,
    mut node: ReturnStatement,
) -> ReturnStatement {
    node.value = folder.fold_expr(node.value);
    node
}

pub fn fold_assignment_statement<F: Fold + ?Sized>(
    folder: &mut F,
    mut node: AssignmentStatement,
) -> AssignmentStatement {
    node.target = folder.fold_ident(node.target);
    node.value = folder.fold_expr(node.value);
    node
}

pub fn fold_case_statement<F: Fold + ?Sized>(
    folder: &mut F,
    mut node: CaseStatement,
) -> CaseStatement {
    node.expression = node.expression.map(|child| folder.fold_expr(child));
    node.when_clauses = node
        .when_clauses
        .into_iter()
        .map(|child| folder.fold_case_statement_when_clause(child))
        .collect();
    node.else_clause = node.else_clause.map(|child| folder.fold_else_clause(child));
    node
}

pub fn fold_case_statement_when_clause<F: Fold + ?Sized>(
    folder: &mut F,
    mut node: CaseStatementWhenClause,
) -> CaseStatementWhenClause {
    node.expression = folder.fold_expr(node.expression);
    node.statements = node
        .statements
        .into_iter()
        .map(|child| folder.fold_control_statement(child))
        .collect();
    node
}

pub fn fold_if_statement<F: Fold + ?Sized>(folder: &mut F, mut node: IfStatement) -> IfStatement {
    node.expression = folder.fold_expr(node.expression);
    node.statements = node
        .statements
        .into_iter()
        .map(|child| folder.fold_control_statement(child))
        .collect();
    node.else_if_clauses = node
        .else_if_clauses
        .into_iter()
        .map(|child| folder.fold_else_if_clause(child))
        .collect();
    node.else_clause = node.else_clause.map(|child| folder.fold_else_clause(child));
    node
}

pub fn fold_else_if_clause<F: Fold + ?Sized>(
    folder: &mut F,
    mut node: ElseIfClause,
) -> ElseIfClause {
    node.expression = folder.fold_expr(node.expression);
    node.statements = node
        .statements
        .into_iter()
        .map(|child| folder.fold_control_statement(child))
        .collect();
    node
}

pub fn fold_else_clause<F: Fold + ?Sized>(folder: &mut F, mut node: ElseClause) -> ElseClause {
    node.statements = node
        .statements
        .into_iter()
        .map(|child| folder.fold_control_statement(child))
        .collect();
    node
}

pub fn fold_iterate_statement<F: Fold + ?Sized>(
    folder: &mut F,
    mut node: IterateStatement,
) -> IterateStatement {
    node.label = folder.fold_ident(node.label);
    node
}

pub fn fold_leave_statement<F: Fold + ?Sized>(
    folder: &mut F,
    mut node: LeaveStatement,
) -> LeaveStatement {
    node.label = folder.fold_ident(node.label);
    node
}

pub fn fold_variable_declaration<F: Fold + ?Sized>(
    folder: &mut F,
    mut node: VariableDeclaration,
) -> VariableDeclaration {
    node.names = node
        .names
        .into_iter()
        .map(|child| folder.fold_ident(child))
        .collect();
    node.data_type = folder.fold_type_name(node.data_type);
    node.default_value = node.default_value.map(|child| folder.fold_expr(child));
    node
}

pub fn fold_compound_statement<F: Fold + ?Sized>(
    folder: &mut F,
    mut node: CompoundStatement,
) -> CompoundStatement {
    node.variable_declarations = node
        .variable_declarations
        .into_iter()
        .map(|child| folder.fold_variable_declaration(child))
        .collect();
    node.statements = node
        .statements
        .into_iter()
        .map(|child| folder.fold_control_statement(child))
        .collect();
    node
}

pub fn fold_loop_statement<F: Fold + ?Sized>(
    folder: &mut F,
    mut node: LoopStatement,
) -> LoopStatement {
    node.label = node.label.map(|child| folder.fold_ident(child));
    node.statements = node
        .statements
        .into_iter()
        .map(|child| folder.fold_control_statement(child))
        .collect();
    node
}

pub fn fold_while_statement<F: Fold + ?Sized>(
    folder: &mut F,
    mut node: WhileStatement,
) -> WhileStatement {
    node.label = node.label.map(|child| folder.fold_ident(child));
    node.expression = folder.fold_expr(node.expression);
    node.statements = node
        .statements
        .into_iter()
        .map(|child| folder.fold_control_statement(child))
        .collect();
    node
}

pub fn fold_repeat_statement<F: Fold + ?Sized>(
    folder: &mut F,
    mut node: RepeatStatement,
) -> RepeatStatement {
    node.label = node.label.map(|child| folder.fold_ident(child));
    node.statements = node
        .statements
        .into_iter()
        .map(|child| folder.fold_control_statement(child))
        .collect();
    node.condition = folder.fold_expr(node.condition);
    node
}

pub fn fold_routine_body<F: Fold + ?Sized>(folder: &mut F, node: RoutineBody) -> RoutineBody {
    match node {
        RoutineBody::Sql(statement) => RoutineBody::Sql(folder.fold_control_statement(statement)),
        RoutineBody::External { definition, span } => RoutineBody::External { definition, span },
    }
}

pub fn fold_routine_characteristic<F: Fold + ?Sized>(
    folder: &mut F,
    node: RoutineCharacteristic,
) -> RoutineCharacteristic {
    match node {
        RoutineCharacteristic::Language { language, span } => RoutineCharacteristic::Language {
            language: folder.fold_ident(language),
            span,
        },
        RoutineCharacteristic::Properties { properties, span } => {
            RoutineCharacteristic::Properties {
                properties: properties
                    .into_iter()
                    .map(|property| folder.fold_routine_property(property))
                    .collect(),
                span,
            }
        }
        other @ (RoutineCharacteristic::Deterministic { .. }
        | RoutineCharacteristic::NullInput { .. }
        | RoutineCharacteristic::Security { .. }
        | RoutineCharacteristic::Comment { .. }) => other,
    }
}

#[recursive::recursive]
pub fn fold_control_statement<F: Fold + ?Sized>(
    folder: &mut F,
    node: ControlStatement,
) -> ControlStatement {
    match node {
        ControlStatement::Return(child) => {
            ControlStatement::Return(folder.fold_return_statement(child))
        }
        ControlStatement::Assignment(child) => {
            ControlStatement::Assignment(folder.fold_assignment_statement(child))
        }
        ControlStatement::Case(child) => ControlStatement::Case(folder.fold_case_statement(child)),
        ControlStatement::If(child) => ControlStatement::If(folder.fold_if_statement(child)),
        ControlStatement::Iterate(child) => {
            ControlStatement::Iterate(folder.fold_iterate_statement(child))
        }
        ControlStatement::Leave(child) => {
            ControlStatement::Leave(folder.fold_leave_statement(child))
        }
        ControlStatement::VariableDeclaration(child) => {
            ControlStatement::VariableDeclaration(folder.fold_variable_declaration(child))
        }
        ControlStatement::Compound(child) => {
            ControlStatement::Compound(folder.fold_compound_statement(child))
        }
        ControlStatement::Loop(child) => ControlStatement::Loop(folder.fold_loop_statement(child)),
        ControlStatement::While(child) => {
            ControlStatement::While(folder.fold_while_statement(child))
        }
        ControlStatement::Repeat(child) => {
            ControlStatement::Repeat(folder.fold_repeat_statement(child))
        }
    }
}

// Local container comparisons avoid changing the shared SyntaxEq vocabulary.
fn eq_option<T: SyntaxEq>(left: &Option<T>, right: &Option<T>) -> bool {
    match (left, right) {
        (Some(left), Some(right)) => left.syntax_eq(right),
        (None, None) => true,
        _ => false,
    }
}

fn eq_slice<T: SyntaxEq>(left: &[T], right: &[T]) -> bool {
    left.len() == right.len()
        && left
            .iter()
            .zip(right)
            .all(|(left, right)| left.syntax_eq(right))
}

impl SyntaxEq for FunctionSpecification {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.name.syntax_eq(&other.name)
            && eq_slice(&self.parameters, &other.parameters)
            && self.returns.syntax_eq(&other.returns)
            && eq_slice(&self.characteristics, &other.characteristics)
            && self.body.syntax_eq(&other.body)
    }
}

impl SyntaxEq for FunctionParameter {
    fn syntax_eq(&self, other: &Self) -> bool {
        eq_option(&self.name, &other.name) && self.data_type.syntax_eq(&other.data_type)
    }
}

impl SyntaxEq for RoutineProperty {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.name.syntax_eq(&other.name) && eq_option(&self.value, &other.value)
    }
}

impl SyntaxEq for ReturnStatement {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.value.syntax_eq(&other.value)
    }
}

impl SyntaxEq for AssignmentStatement {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.target.syntax_eq(&other.target) && self.value.syntax_eq(&other.value)
    }
}

impl SyntaxEq for CaseStatement {
    fn syntax_eq(&self, other: &Self) -> bool {
        eq_option(&self.expression, &other.expression)
            && eq_slice(&self.when_clauses, &other.when_clauses)
            && eq_option(&self.else_clause, &other.else_clause)
    }
}

impl SyntaxEq for CaseStatementWhenClause {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.expression.syntax_eq(&other.expression)
            && eq_slice(&self.statements, &other.statements)
    }
}

impl SyntaxEq for IfStatement {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.expression.syntax_eq(&other.expression)
            && eq_slice(&self.statements, &other.statements)
            && eq_slice(&self.else_if_clauses, &other.else_if_clauses)
            && eq_option(&self.else_clause, &other.else_clause)
    }
}

impl SyntaxEq for ElseIfClause {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.expression.syntax_eq(&other.expression)
            && eq_slice(&self.statements, &other.statements)
    }
}

impl SyntaxEq for ElseClause {
    fn syntax_eq(&self, other: &Self) -> bool {
        eq_slice(&self.statements, &other.statements)
    }
}

impl SyntaxEq for IterateStatement {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.label.syntax_eq(&other.label)
    }
}

impl SyntaxEq for LeaveStatement {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.label.syntax_eq(&other.label)
    }
}

impl SyntaxEq for VariableDeclaration {
    fn syntax_eq(&self, other: &Self) -> bool {
        eq_slice(&self.names, &other.names)
            && self.data_type.syntax_eq(&other.data_type)
            && eq_option(&self.default_value, &other.default_value)
    }
}

impl SyntaxEq for CompoundStatement {
    fn syntax_eq(&self, other: &Self) -> bool {
        eq_slice(&self.variable_declarations, &other.variable_declarations)
            && eq_slice(&self.statements, &other.statements)
    }
}

impl SyntaxEq for LoopStatement {
    fn syntax_eq(&self, other: &Self) -> bool {
        eq_option(&self.label, &other.label) && eq_slice(&self.statements, &other.statements)
    }
}

impl SyntaxEq for WhileStatement {
    fn syntax_eq(&self, other: &Self) -> bool {
        eq_option(&self.label, &other.label)
            && self.expression.syntax_eq(&other.expression)
            && eq_slice(&self.statements, &other.statements)
    }
}

impl SyntaxEq for RepeatStatement {
    fn syntax_eq(&self, other: &Self) -> bool {
        eq_option(&self.label, &other.label)
            && eq_slice(&self.statements, &other.statements)
            && self.condition.syntax_eq(&other.condition)
    }
}

impl SyntaxEq for RoutineBody {
    fn syntax_eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Sql(left), Self::Sql(right)) => left.syntax_eq(right),
            (
                Self::External {
                    definition: left, ..
                },
                Self::External {
                    definition: right, ..
                },
            ) => left == right,
            _ => false,
        }
    }
}

impl SyntaxEq for RoutineCharacteristic {
    fn syntax_eq(&self, other: &Self) -> bool {
        match (self, other) {
            (
                Self::Language { language: left, .. },
                Self::Language {
                    language: right, ..
                },
            ) => left.syntax_eq(right),
            (
                Self::Deterministic {
                    deterministic: left,
                    ..
                },
                Self::Deterministic {
                    deterministic: right,
                    ..
                },
            ) => left == right,
            (
                Self::NullInput {
                    called_on_null: left,
                    ..
                },
                Self::NullInput {
                    called_on_null: right,
                    ..
                },
            ) => left == right,
            (
                Self::Security { security: left, .. },
                Self::Security {
                    security: right, ..
                },
            ) => left == right,
            (Self::Comment { comment: left, .. }, Self::Comment { comment: right, .. }) => {
                left == right
            }
            (
                Self::Properties {
                    properties: left, ..
                },
                Self::Properties {
                    properties: right, ..
                },
            ) => eq_slice(left, right),
            _ => false,
        }
    }
}

impl SyntaxEq for ControlStatement {
    #[recursive::recursive]
    fn syntax_eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Return(left), Self::Return(right)) => left.syntax_eq(right),
            (Self::Assignment(left), Self::Assignment(right)) => left.syntax_eq(right),
            (Self::Case(left), Self::Case(right)) => left.syntax_eq(right),
            (Self::If(left), Self::If(right)) => left.syntax_eq(right),
            (Self::Iterate(left), Self::Iterate(right)) => left.syntax_eq(right),
            (Self::Leave(left), Self::Leave(right)) => left.syntax_eq(right),
            (Self::VariableDeclaration(left), Self::VariableDeclaration(right)) => {
                left.syntax_eq(right)
            }
            (Self::Compound(left), Self::Compound(right)) => left.syntax_eq(right),
            (Self::Loop(left), Self::Loop(right)) => left.syntax_eq(right),
            (Self::While(left), Self::While(right)) => left.syntax_eq(right),
            (Self::Repeat(left), Self::Repeat(right)) => left.syntax_eq(right),
            _ => false,
        }
    }
}

#[cfg(test)]
#[path = "routine_tests.rs"]
mod tests;
