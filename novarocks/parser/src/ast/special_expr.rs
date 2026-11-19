// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information regarding copyright
// ownership. The ASF licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

//! Typed expressions whose grammar and execution capability have separate owners.
//! Optional precision and child order are syntax facts, not inferred defaults.

use crate::{
    Span,
    ast::{
        AstBox, AstVec, CallArgument, Expr, Fold, Ident, IntervalDataType, Literal, LiteralKind,
        ObjectName, SyntaxEq, Visit,
    },
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AtLocal {
    pub value: Expr,
    pub span: Span,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AtTimeZone {
    pub value: Expr,
    pub time_zone: Expr,
    pub span: Span,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GroupingOperation {
    pub grouping_columns: AstVec<Expr>,
    pub span: Span,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TrimSpecification {
    Both,
    Leading,
    Trailing,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Trim {
    pub specification: TrimSpecification,
    pub trim_source: Expr,
    pub trim_character: Option<Expr>,
    pub span: Span,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Overlay {
    pub value: Expr,
    pub replacement: Expr,
    pub start: Expr,
    pub length: Option<Expr>,
    pub span: Span,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IntervalValueExpression {
    pub left: Expr,
    pub right: Expr,
    pub data_type: IntervalDataType,
    pub span: Span,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MethodCall {
    pub receiver: Expr,
    pub method: Ident,
    pub arguments: AstVec<CallArgument>,
    pub span: Span,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StaticMethodCall {
    pub type_name: ObjectName,
    pub method: Ident,
    pub arguments: AstVec<CallArgument>,
    pub span: Span,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CurrentCatalog {
    pub span: Span,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CurrentDate {
    pub span: Span,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CurrentPath {
    pub span: Span,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CurrentSchema {
    pub span: Span,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CurrentUser {
    pub span: Span,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CurrentTime {
    pub precision: Option<i32>,
    pub span: Span,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CurrentTimestamp {
    pub precision: Option<i32>,
    pub span: Span,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LocalTime {
    pub precision: Option<i32>,
    pub span: Span,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LocalTimestamp {
    pub precision: Option<i32>,
    pub span: Span,
}

impl AtLocal {
    pub const fn span(&self) -> Span {
        self.span
    }
}
#[recursive::recursive]
pub fn walk_at_local<V: Visit + ?Sized>(visitor: &mut V, node: &AtLocal) {
    visitor.visit_expr(&node.value);
}
#[recursive::recursive]
pub fn fold_at_local<F: Fold + ?Sized>(folder: &mut F, mut node: AtLocal) -> AtLocal {
    node.value = folder.fold_expr(node.value);
    node
}
impl SyntaxEq for AtLocal {
    #[recursive::recursive]
    fn syntax_eq(&self, other: &Self) -> bool {
        self.value.syntax_eq(&other.value)
    }
}

impl AtTimeZone {
    pub const fn span(&self) -> Span {
        self.span
    }
}
#[recursive::recursive]
pub fn walk_at_time_zone<V: Visit + ?Sized>(visitor: &mut V, node: &AtTimeZone) {
    visitor.visit_expr(&node.value);
    visitor.visit_expr(&node.time_zone);
}
#[recursive::recursive]
pub fn fold_at_time_zone<F: Fold + ?Sized>(folder: &mut F, mut node: AtTimeZone) -> AtTimeZone {
    node.value = folder.fold_expr(node.value);
    node.time_zone = folder.fold_expr(node.time_zone);
    node
}
impl SyntaxEq for AtTimeZone {
    #[recursive::recursive]
    fn syntax_eq(&self, other: &Self) -> bool {
        (self.value.syntax_eq(&other.value)) && (self.time_zone.syntax_eq(&other.time_zone))
    }
}

impl GroupingOperation {
    pub const fn span(&self) -> Span {
        self.span
    }
}
#[recursive::recursive]
pub fn walk_grouping_operation<V: Visit + ?Sized>(visitor: &mut V, node: &GroupingOperation) {
    for child in &node.grouping_columns {
        visitor.visit_expr(child);
    }
}
#[recursive::recursive]
pub fn fold_grouping_operation<F: Fold + ?Sized>(
    folder: &mut F,
    mut node: GroupingOperation,
) -> GroupingOperation {
    node.grouping_columns = node
        .grouping_columns
        .into_iter()
        .map(|child| folder.fold_expr(child))
        .collect();
    node
}
impl SyntaxEq for GroupingOperation {
    #[recursive::recursive]
    fn syntax_eq(&self, other: &Self) -> bool {
        self.grouping_columns.len() == other.grouping_columns.len()
            && self
                .grouping_columns
                .iter()
                .zip(&other.grouping_columns)
                .all(|(a, b)| a.syntax_eq(b))
    }
}

impl Trim {
    pub const fn span(&self) -> Span {
        self.span
    }
}
#[recursive::recursive]
pub fn walk_trim<V: Visit + ?Sized>(visitor: &mut V, node: &Trim) {
    visitor.visit_expr(&node.trim_source);
    if let Some(child) = &node.trim_character {
        visitor.visit_expr(child);
    }
}
#[recursive::recursive]
pub fn fold_trim<F: Fold + ?Sized>(folder: &mut F, mut node: Trim) -> Trim {
    node.trim_source = folder.fold_expr(node.trim_source);
    node.trim_character = node.trim_character.map(|child| folder.fold_expr(child));
    node
}
impl SyntaxEq for Trim {
    #[recursive::recursive]
    fn syntax_eq(&self, other: &Self) -> bool {
        (self.specification == other.specification)
            && (self.trim_source.syntax_eq(&other.trim_source))
            && (match (&self.trim_character, &other.trim_character) {
                (Some(a), Some(b)) => a.syntax_eq(b),
                (None, None) => true,
                _ => false,
            })
    }
}

impl Overlay {
    pub const fn span(&self) -> Span {
        self.span
    }
}
#[recursive::recursive]
pub fn walk_overlay<V: Visit + ?Sized>(visitor: &mut V, node: &Overlay) {
    visitor.visit_expr(&node.value);
    visitor.visit_expr(&node.replacement);
    visitor.visit_expr(&node.start);
    if let Some(child) = &node.length {
        visitor.visit_expr(child);
    }
}
#[recursive::recursive]
pub fn fold_overlay<F: Fold + ?Sized>(folder: &mut F, mut node: Overlay) -> Overlay {
    node.value = folder.fold_expr(node.value);
    node.replacement = folder.fold_expr(node.replacement);
    node.start = folder.fold_expr(node.start);
    node.length = node.length.map(|child| folder.fold_expr(child));
    node
}
impl SyntaxEq for Overlay {
    #[recursive::recursive]
    fn syntax_eq(&self, other: &Self) -> bool {
        self.value.syntax_eq(&other.value)
            && (self.replacement.syntax_eq(&other.replacement))
            && (self.start.syntax_eq(&other.start))
            && (match (&self.length, &other.length) {
                (Some(a), Some(b)) => a.syntax_eq(b),
                (None, None) => true,
                _ => false,
            })
    }
}

impl IntervalValueExpression {
    pub const fn span(&self) -> Span {
        self.span
    }
}
#[recursive::recursive]
pub fn walk_interval_value_expression<V: Visit + ?Sized>(
    visitor: &mut V,
    node: &IntervalValueExpression,
) {
    visitor.visit_expr(&node.left);
    visitor.visit_expr(&node.right);
    visitor.visit_interval_data_type(&node.data_type);
}
#[recursive::recursive]
pub fn fold_interval_value_expression<F: Fold + ?Sized>(
    folder: &mut F,
    mut node: IntervalValueExpression,
) -> IntervalValueExpression {
    node.left = folder.fold_expr(node.left);
    node.right = folder.fold_expr(node.right);
    node.data_type = folder.fold_interval_data_type(node.data_type);
    node
}
impl SyntaxEq for IntervalValueExpression {
    #[recursive::recursive]
    fn syntax_eq(&self, other: &Self) -> bool {
        (self.left.syntax_eq(&other.left))
            && (self.right.syntax_eq(&other.right))
            && (self.data_type.syntax_eq(&other.data_type))
    }
}

impl MethodCall {
    pub const fn span(&self) -> Span {
        self.span
    }
}
#[recursive::recursive]
pub fn walk_method_call<V: Visit + ?Sized>(visitor: &mut V, node: &MethodCall) {
    visitor.visit_expr(&node.receiver);
    visitor.visit_ident(&node.method);
    for child in &node.arguments {
        visitor.visit_call_argument(child);
    }
}
#[recursive::recursive]
pub fn fold_method_call<F: Fold + ?Sized>(folder: &mut F, mut node: MethodCall) -> MethodCall {
    node.receiver = folder.fold_expr(node.receiver);
    node.method = folder.fold_ident(node.method);
    node.arguments = node
        .arguments
        .into_iter()
        .map(|child| folder.fold_call_argument(child))
        .collect();
    node
}
impl SyntaxEq for MethodCall {
    #[recursive::recursive]
    fn syntax_eq(&self, other: &Self) -> bool {
        (self.receiver.syntax_eq(&other.receiver))
            && (self.method.syntax_eq(&other.method))
            && (self.arguments.len() == other.arguments.len()
                && self
                    .arguments
                    .iter()
                    .zip(&other.arguments)
                    .all(|(a, b)| a.syntax_eq(b)))
    }
}

impl StaticMethodCall {
    pub const fn span(&self) -> Span {
        self.span
    }
}
#[recursive::recursive]
pub fn walk_static_method_call<V: Visit + ?Sized>(visitor: &mut V, node: &StaticMethodCall) {
    visitor.visit_object_name(&node.type_name);
    visitor.visit_ident(&node.method);
    for child in &node.arguments {
        visitor.visit_call_argument(child);
    }
}
#[recursive::recursive]
pub fn fold_static_method_call<F: Fold + ?Sized>(
    folder: &mut F,
    mut node: StaticMethodCall,
) -> StaticMethodCall {
    node.type_name = folder.fold_object_name(node.type_name);
    node.method = folder.fold_ident(node.method);
    node.arguments = node
        .arguments
        .into_iter()
        .map(|child| folder.fold_call_argument(child))
        .collect();
    node
}
impl SyntaxEq for StaticMethodCall {
    #[recursive::recursive]
    fn syntax_eq(&self, other: &Self) -> bool {
        (self.type_name.syntax_eq(&other.type_name))
            && (self.method.syntax_eq(&other.method))
            && (self.arguments.len() == other.arguments.len()
                && self
                    .arguments
                    .iter()
                    .zip(&other.arguments)
                    .all(|(a, b)| a.syntax_eq(b)))
    }
}

impl CurrentCatalog {
    pub const fn span(&self) -> Span {
        self.span
    }
}
pub fn walk_current_catalog<V: Visit + ?Sized>(_visitor: &mut V, _node: &CurrentCatalog) {}
pub fn fold_current_catalog<F: Fold + ?Sized>(
    _folder: &mut F,
    node: CurrentCatalog,
) -> CurrentCatalog {
    node
}
impl SyntaxEq for CurrentCatalog {
    fn syntax_eq(&self, _other: &Self) -> bool {
        true
    }
}

impl CurrentDate {
    pub const fn span(&self) -> Span {
        self.span
    }
}
pub fn walk_current_date<V: Visit + ?Sized>(_visitor: &mut V, _node: &CurrentDate) {}
pub fn fold_current_date<F: Fold + ?Sized>(_folder: &mut F, node: CurrentDate) -> CurrentDate {
    node
}
impl SyntaxEq for CurrentDate {
    fn syntax_eq(&self, _other: &Self) -> bool {
        true
    }
}

impl CurrentPath {
    pub const fn span(&self) -> Span {
        self.span
    }
}
pub fn walk_current_path<V: Visit + ?Sized>(_visitor: &mut V, _node: &CurrentPath) {}
pub fn fold_current_path<F: Fold + ?Sized>(_folder: &mut F, node: CurrentPath) -> CurrentPath {
    node
}
impl SyntaxEq for CurrentPath {
    fn syntax_eq(&self, _other: &Self) -> bool {
        true
    }
}

impl CurrentSchema {
    pub const fn span(&self) -> Span {
        self.span
    }
}
pub fn walk_current_schema<V: Visit + ?Sized>(_visitor: &mut V, _node: &CurrentSchema) {}
pub fn fold_current_schema<F: Fold + ?Sized>(
    _folder: &mut F,
    node: CurrentSchema,
) -> CurrentSchema {
    node
}
impl SyntaxEq for CurrentSchema {
    fn syntax_eq(&self, _other: &Self) -> bool {
        true
    }
}

impl CurrentUser {
    pub const fn span(&self) -> Span {
        self.span
    }
}
pub fn walk_current_user<V: Visit + ?Sized>(_visitor: &mut V, _node: &CurrentUser) {}
pub fn fold_current_user<F: Fold + ?Sized>(_folder: &mut F, node: CurrentUser) -> CurrentUser {
    node
}
impl SyntaxEq for CurrentUser {
    fn syntax_eq(&self, _other: &Self) -> bool {
        true
    }
}

impl CurrentTime {
    pub const fn span(&self) -> Span {
        self.span
    }
}
pub fn walk_current_time<V: Visit + ?Sized>(_visitor: &mut V, _node: &CurrentTime) {}
pub fn fold_current_time<F: Fold + ?Sized>(_folder: &mut F, node: CurrentTime) -> CurrentTime {
    node
}
impl SyntaxEq for CurrentTime {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.precision == other.precision
    }
}

impl CurrentTimestamp {
    pub const fn span(&self) -> Span {
        self.span
    }
}
pub fn walk_current_timestamp<V: Visit + ?Sized>(_visitor: &mut V, _node: &CurrentTimestamp) {}
pub fn fold_current_timestamp<F: Fold + ?Sized>(
    _folder: &mut F,
    node: CurrentTimestamp,
) -> CurrentTimestamp {
    node
}
impl SyntaxEq for CurrentTimestamp {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.precision == other.precision
    }
}

impl LocalTime {
    pub const fn span(&self) -> Span {
        self.span
    }
}
pub fn walk_local_time<V: Visit + ?Sized>(_visitor: &mut V, _node: &LocalTime) {}
pub fn fold_local_time<F: Fold + ?Sized>(_folder: &mut F, node: LocalTime) -> LocalTime {
    node
}
impl SyntaxEq for LocalTime {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.precision == other.precision
    }
}

impl LocalTimestamp {
    pub const fn span(&self) -> Span {
        self.span
    }
}
pub fn walk_local_timestamp<V: Visit + ?Sized>(_visitor: &mut V, _node: &LocalTimestamp) {}
pub fn fold_local_timestamp<F: Fold + ?Sized>(
    _folder: &mut F,
    node: LocalTimestamp,
) -> LocalTimestamp {
    node
}
impl SyntaxEq for LocalTimestamp {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.precision == other.precision
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SpecialExpressionInvariantError {
    InvalidTimeZoneNode,
    EmptyGroupingColumns,
    InvalidGroupingColumn,
}
impl std::fmt::Display for SpecialExpressionInvariantError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::InvalidTimeZoneNode => "AT TIME ZONE requires a string or interval literal",
            Self::EmptyGroupingColumns => "GROUPING requires at least one name",
            Self::InvalidGroupingColumn => "GROUPING columns must be identifier/dereference names",
        })
    }
}
impl std::error::Error for SpecialExpressionInvariantError {}
impl AtTimeZone {
    pub fn new(
        value: Expr,
        time_zone: Expr,
        span: Span,
    ) -> Result<Self, SpecialExpressionInvariantError> {
        let node = Self {
            value,
            time_zone,
            span,
        };
        node.validate_constructor_invariants()?;
        Ok(node)
    }
    pub fn validate_constructor_invariants(&self) -> Result<(), SpecialExpressionInvariantError> {
        if matches!(
            &self.time_zone,
            Expr::Literal(Literal {
                kind: LiteralKind::String(_) | LiteralKind::Interval(_),
                ..
            })
        ) {
            Ok(())
        } else {
            Err(SpecialExpressionInvariantError::InvalidTimeZoneNode)
        }
    }
}
impl GroupingOperation {
    pub fn validate_constructor_invariants(&self) -> Result<(), SpecialExpressionInvariantError> {
        if self.grouping_columns.is_empty() {
            return Err(SpecialExpressionInvariantError::EmptyGroupingColumns);
        }
        for column in &self.grouping_columns {
            if super::identifier_chain_observed(column, |_| Ok::<(), std::convert::Infallible>(()))
                .unwrap_or_else(|never| match never {})
                .is_none()
            {
                return Err(SpecialExpressionInvariantError::InvalidGroupingColumn);
            }
        }
        Ok(())
    }
}
#[cfg(test)]
#[path = "special_expr_tests.rs"]
mod tests;

impl GroupingOperation {
    /// Apply this node's QualifiedName-to-expression canonical construction.
    /// This is not the general ObjectName parser or name-resolution policy.
    pub fn new(
        names: Vec<ObjectName>,
        span: Span,
    ) -> Result<Self, SpecialExpressionInvariantError> {
        if names.is_empty() {
            return Err(SpecialExpressionInvariantError::EmptyGroupingColumns);
        }
        let grouping_columns = names
            .into_iter()
            .map(|name| {
                let mut parts = name.parts.into_iter();
                let first = parts
                    .next()
                    .ok_or(SpecialExpressionInvariantError::InvalidGroupingColumn)?;
                let mut expression = Expr::Identifier(grouping_identifier(first)?);
                for part in parts {
                    expression = Expr::Dereference(AstBox::new(super::DereferenceExpr {
                        base: expression,
                        field: Some(grouping_identifier(part)?),
                        span: crate::SYNTHETIC,
                    }));
                }
                Ok(expression)
            })
            .collect::<Result<AstVec<_>, SpecialExpressionInvariantError>>()?;
        Ok(Self {
            grouping_columns,
            span,
        })
    }
}
fn grouping_identifier(part: Ident) -> Result<Ident, SpecialExpressionInvariantError> {
    let value = part.value.to_lowercase();
    if value.is_empty() {
        return Err(SpecialExpressionInvariantError::InvalidGroupingColumn);
    }
    Ok(Ident {
        quoted: !grouping_identifier_is_unquoted(&value),
        value,
        span: crate::SYNTHETIC,
    })
}

fn grouping_identifier_is_unquoted(value: &str) -> bool {
    value
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
        && value
            .as_bytes()
            .first()
            .is_some_and(|byte| !byte.is_ascii_digit())
}
