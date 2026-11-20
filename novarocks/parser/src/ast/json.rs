// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.

//! Complete SQL/JSON syntax facts.
//! Parsing and execution policy belong to their original owners.
//! JSON paths are decoded StringLiteral payloads, never raw-SQL placeholders.

use super::{AstBox, AstVec, Expr, Fold, Ident, SyntaxEq, TypeName, Visit};
use crate::Span;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum JsonFormat {
    Json,
    Utf8,
    Utf16,
    Utf32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum JsonExistsErrorBehavior {
    False,
    True,
    Unknown,
    Error,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum JsonValueBehavior {
    Null,
    Error,
    Default,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum JsonQueryWrapperBehavior {
    Without,
    Conditional,
    Unconditional,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum JsonQueryQuotesBehavior {
    Keep,
    Omit,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum JsonQueryBehavior {
    Null,
    Error,
    EmptyArray,
    EmptyObject,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum JsonSerializeErrorBehavior {
    Error,
    Null,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum JsonTableErrorBehavior {
    Error,
    Empty,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum JsonTableParentChildPlanType {
    Outer,
    Inner,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum JsonTableSiblingsPlanType {
    Union,
    Cross,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JsonPathString {
    pub value: String,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JsonPathInvocation {
    pub input_expression: Expr,
    pub input_format: JsonFormat,
    pub json_path: JsonPathString,
    pub path_name: Option<Ident>,
    pub path_parameters: Vec<JsonPathParameter>,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JsonPathParameter {
    pub name: Ident,
    pub parameter: Expr,
    pub format: Option<JsonFormat>,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JsonConstructor {
    pub expression: Expr,
    pub format: JsonFormat,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JsonExists {
    pub path_invocation: JsonPathInvocation,
    pub error_behavior: JsonExistsErrorBehavior,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JsonValue {
    pub path_invocation: JsonPathInvocation,
    pub returned_type: Option<TypeName>,
    pub empty_behavior: JsonValueBehavior,
    pub empty_default: Option<Expr>,
    pub error_behavior: JsonValueBehavior,
    pub error_default: Option<Expr>,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JsonQuery {
    pub path_invocation: JsonPathInvocation,
    pub returned_type: Option<TypeName>,
    pub output_format: Option<JsonFormat>,
    pub wrapper_behavior: JsonQueryWrapperBehavior,
    pub quotes_behavior: Option<JsonQueryQuotesBehavior>,
    pub empty_behavior: JsonQueryBehavior,
    pub error_behavior: JsonQueryBehavior,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JsonSerialize {
    pub expression: Expr,
    pub input_format: JsonFormat,
    pub returned_type: Option<TypeName>,
    pub output_format: Option<JsonFormat>,
    pub error_behavior: JsonSerializeErrorBehavior,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JsonObject {
    pub members: Vec<JsonObjectMember>,
    pub null_on_null: bool,
    pub unique_keys: bool,
    pub returned_type: Option<TypeName>,
    pub output_format: Option<JsonFormat>,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JsonObjectMember {
    pub key: Expr,
    pub value: Expr,
    pub format: Option<JsonFormat>,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JsonArray {
    pub elements: Vec<JsonArrayElement>,
    pub null_on_null: bool,
    pub returned_type: Option<TypeName>,
    pub output_format: Option<JsonFormat>,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JsonArrayElement {
    pub value: Expr,
    pub format: Option<JsonFormat>,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JsonTable {
    pub path_invocation: JsonPathInvocation,
    pub columns: Vec<JsonTableColumn>,
    pub plan: Option<JsonTablePlan>,
    pub error_behavior: Option<JsonTableErrorBehavior>,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JsonTableOrdinalityColumn {
    pub name: Ident,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JsonTableValueColumn {
    pub name: Ident,
    pub data_type: TypeName,
    pub json_path: Option<JsonPathString>,
    pub empty_behavior: JsonValueBehavior,
    pub empty_default: Option<Expr>,
    pub error_behavior: Option<JsonValueBehavior>,
    pub error_default: Option<Expr>,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JsonTableQueryColumn {
    pub name: Ident,
    pub data_type: TypeName,
    pub format: JsonFormat,
    pub json_path: Option<JsonPathString>,
    pub wrapper_behavior: JsonQueryWrapperBehavior,
    pub quotes_behavior: Option<JsonQueryQuotesBehavior>,
    pub empty_behavior: JsonQueryBehavior,
    pub error_behavior: Option<JsonQueryBehavior>,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JsonTableNestedColumns {
    pub json_path: JsonPathString,
    pub path_name: Option<Ident>,
    pub columns: AstVec<JsonTableColumn>,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JsonTableDefaultPlan {
    pub parent_child: JsonTableParentChildPlanType,
    pub siblings: JsonTableSiblingsPlanType,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JsonTablePlanLeaf {
    pub name: Ident,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JsonTablePlanParentChild {
    pub plan_type: JsonTableParentChildPlanType,
    pub parent: JsonTablePlanLeaf,
    pub child: AstBox<JsonTableSpecificPlan>,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JsonTablePlanSiblings {
    pub plan_type: JsonTableSiblingsPlanType,
    pub siblings: AstVec<JsonTableSpecificPlan>,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[expect(
    clippy::large_enum_variant,
    reason = "JSON_TABLE columns are kept contiguous in AstVec with guarded recursive owners"
)]
pub enum JsonTableColumn {
    Ordinality(JsonTableOrdinalityColumn),
    Value(JsonTableValueColumn),
    Query(JsonTableQueryColumn),
    Nested(JsonTableNestedColumns),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum JsonTablePlan {
    Default(JsonTableDefaultPlan),
    Specific(JsonTableSpecificPlan),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum JsonTableSpecificPlan {
    Leaf(JsonTablePlanLeaf),
    ParentChild(JsonTablePlanParentChild),
    Siblings(JsonTablePlanSiblings),
}

impl JsonPathString {
    pub const fn span(&self) -> Span {
        self.span
    }
}

impl JsonPathInvocation {
    pub const fn span(&self) -> Span {
        self.span
    }
}

impl JsonPathParameter {
    pub const fn span(&self) -> Span {
        self.span
    }
}

impl JsonConstructor {
    pub const fn span(&self) -> Span {
        self.span
    }
}

impl JsonExists {
    pub const fn span(&self) -> Span {
        self.span
    }
}

impl JsonValue {
    pub const fn span(&self) -> Span {
        self.span
    }
}

impl JsonQuery {
    pub const fn span(&self) -> Span {
        self.span
    }
}

impl JsonSerialize {
    pub const fn span(&self) -> Span {
        self.span
    }
}

impl JsonObject {
    pub const fn span(&self) -> Span {
        self.span
    }
}

impl JsonObjectMember {
    pub const fn span(&self) -> Span {
        self.span
    }
}

impl JsonArray {
    pub const fn span(&self) -> Span {
        self.span
    }
}

impl JsonArrayElement {
    pub const fn span(&self) -> Span {
        self.span
    }
}

impl JsonTable {
    pub const fn span(&self) -> Span {
        self.span
    }
}

impl JsonTableOrdinalityColumn {
    pub const fn span(&self) -> Span {
        self.span
    }
}

impl JsonTableValueColumn {
    pub const fn span(&self) -> Span {
        self.span
    }
}

impl JsonTableQueryColumn {
    pub const fn span(&self) -> Span {
        self.span
    }
}

impl JsonTableNestedColumns {
    pub const fn span(&self) -> Span {
        self.span
    }
}

impl JsonTableDefaultPlan {
    pub const fn span(&self) -> Span {
        self.span
    }
}

impl JsonTablePlanLeaf {
    pub const fn span(&self) -> Span {
        self.span
    }
}

impl JsonTablePlanParentChild {
    pub const fn span(&self) -> Span {
        self.span
    }
}

impl JsonTablePlanSiblings {
    pub const fn span(&self) -> Span {
        self.span
    }
}

impl JsonTableColumn {
    pub const fn span(&self) -> Span {
        match self {
            Self::Ordinality(node) => node.span(),
            Self::Value(node) => node.span(),
            Self::Query(node) => node.span(),
            Self::Nested(node) => node.span(),
        }
    }
}

impl JsonTablePlan {
    pub const fn span(&self) -> Span {
        match self {
            Self::Default(node) => node.span(),
            Self::Specific(node) => node.span(),
        }
    }
}

impl JsonTableSpecificPlan {
    pub const fn span(&self) -> Span {
        match self {
            Self::Leaf(node) => node.span(),
            Self::ParentChild(node) => node.span(),
            Self::Siblings(node) => node.span(),
        }
    }
}

pub fn walk_json_path_string<V: Visit + ?Sized>(visitor: &mut V, node: &JsonPathString) {
    let _ = (visitor, node);
}

pub fn walk_json_path_invocation<V: Visit + ?Sized>(visitor: &mut V, node: &JsonPathInvocation) {
    visitor.visit_expr(&node.input_expression);
    visitor.visit_json_path_string(&node.json_path);
    if let Some(child) = &node.path_name {
        visitor.visit_ident(child);
    }
    for child in &node.path_parameters {
        visitor.visit_json_path_parameter(child);
    }
}

pub fn walk_json_path_parameter<V: Visit + ?Sized>(visitor: &mut V, node: &JsonPathParameter) {
    visitor.visit_ident(&node.name);
    visitor.visit_expr(&node.parameter);
}

pub fn walk_json_constructor<V: Visit + ?Sized>(visitor: &mut V, node: &JsonConstructor) {
    visitor.visit_expr(&node.expression);
}

pub fn walk_json_exists<V: Visit + ?Sized>(visitor: &mut V, node: &JsonExists) {
    visitor.visit_json_path_invocation(&node.path_invocation);
}

pub fn walk_json_value<V: Visit + ?Sized>(visitor: &mut V, node: &JsonValue) {
    visitor.visit_json_path_invocation(&node.path_invocation);
    if let Some(child) = &node.returned_type {
        visitor.visit_type_name(child);
    }
    if let Some(child) = &node.empty_default {
        visitor.visit_expr(child);
    }
    if let Some(child) = &node.error_default {
        visitor.visit_expr(child);
    }
}

#[recursive::recursive]
pub fn walk_json_query<V: Visit + ?Sized>(visitor: &mut V, node: &JsonQuery) {
    visitor.visit_json_path_invocation(&node.path_invocation);
    if let Some(child) = &node.returned_type {
        visitor.visit_type_name(child);
    }
}

pub fn walk_json_serialize<V: Visit + ?Sized>(visitor: &mut V, node: &JsonSerialize) {
    visitor.visit_expr(&node.expression);
    if let Some(child) = &node.returned_type {
        visitor.visit_type_name(child);
    }
}

pub fn walk_json_object<V: Visit + ?Sized>(visitor: &mut V, node: &JsonObject) {
    for child in &node.members {
        visitor.visit_json_object_member(child);
    }
    if let Some(child) = &node.returned_type {
        visitor.visit_type_name(child);
    }
}

pub fn walk_json_object_member<V: Visit + ?Sized>(visitor: &mut V, node: &JsonObjectMember) {
    visitor.visit_expr(&node.key);
    visitor.visit_expr(&node.value);
}

pub fn walk_json_array<V: Visit + ?Sized>(visitor: &mut V, node: &JsonArray) {
    for child in &node.elements {
        visitor.visit_json_array_element(child);
    }
    if let Some(child) = &node.returned_type {
        visitor.visit_type_name(child);
    }
}

pub fn walk_json_array_element<V: Visit + ?Sized>(visitor: &mut V, node: &JsonArrayElement) {
    visitor.visit_expr(&node.value);
}

pub fn walk_json_table<V: Visit + ?Sized>(visitor: &mut V, node: &JsonTable) {
    visitor.visit_json_path_invocation(&node.path_invocation);
    for child in &node.columns {
        visitor.visit_json_table_column(child);
    }
    if let Some(child) = &node.plan {
        visitor.visit_json_table_plan(child);
    }
}

pub fn walk_json_table_ordinality_column<V: Visit + ?Sized>(
    visitor: &mut V,
    node: &JsonTableOrdinalityColumn,
) {
    visitor.visit_ident(&node.name);
}

pub fn walk_json_table_value_column<V: Visit + ?Sized>(
    visitor: &mut V,
    node: &JsonTableValueColumn,
) {
    visitor.visit_ident(&node.name);
    visitor.visit_type_name(&node.data_type);
    if let Some(child) = &node.json_path {
        visitor.visit_json_path_string(child);
    }
    if let Some(child) = &node.empty_default {
        visitor.visit_expr(child);
    }
    if let Some(child) = &node.error_default {
        visitor.visit_expr(child);
    }
}

pub fn walk_json_table_query_column<V: Visit + ?Sized>(
    visitor: &mut V,
    node: &JsonTableQueryColumn,
) {
    visitor.visit_ident(&node.name);
    visitor.visit_type_name(&node.data_type);
    if let Some(child) = &node.json_path {
        visitor.visit_json_path_string(child);
    }
}

pub fn walk_json_table_nested_columns<V: Visit + ?Sized>(
    visitor: &mut V,
    node: &JsonTableNestedColumns,
) {
    visitor.visit_json_path_string(&node.json_path);
    if let Some(child) = &node.path_name {
        visitor.visit_ident(child);
    }
    for child in &node.columns {
        visitor.visit_json_table_column(child);
    }
}

pub fn walk_json_table_default_plan<V: Visit + ?Sized>(
    visitor: &mut V,
    node: &JsonTableDefaultPlan,
) {
    let _ = (visitor, node);
}

pub fn walk_json_table_plan_leaf<V: Visit + ?Sized>(visitor: &mut V, node: &JsonTablePlanLeaf) {
    visitor.visit_ident(&node.name);
}

pub fn walk_json_table_plan_parent_child<V: Visit + ?Sized>(
    visitor: &mut V,
    node: &JsonTablePlanParentChild,
) {
    visitor.visit_json_table_plan_leaf(&node.parent);
    visitor.visit_json_table_specific_plan(&node.child);
}

pub fn walk_json_table_plan_siblings<V: Visit + ?Sized>(
    visitor: &mut V,
    node: &JsonTablePlanSiblings,
) {
    for child in &node.siblings {
        visitor.visit_json_table_specific_plan(child);
    }
}

#[recursive::recursive]
pub fn walk_json_table_column<V: Visit + ?Sized>(visitor: &mut V, node: &JsonTableColumn) {
    match node {
        JsonTableColumn::Ordinality(child) => visitor.visit_json_table_ordinality_column(child),
        JsonTableColumn::Value(child) => visitor.visit_json_table_value_column(child),
        JsonTableColumn::Query(child) => visitor.visit_json_table_query_column(child),
        JsonTableColumn::Nested(child) => visitor.visit_json_table_nested_columns(child),
    }
}

pub fn walk_json_table_plan<V: Visit + ?Sized>(visitor: &mut V, node: &JsonTablePlan) {
    match node {
        JsonTablePlan::Default(child) => visitor.visit_json_table_default_plan(child),
        JsonTablePlan::Specific(child) => visitor.visit_json_table_specific_plan(child),
    }
}

#[recursive::recursive]
pub fn walk_json_table_specific_plan<V: Visit + ?Sized>(
    visitor: &mut V,
    node: &JsonTableSpecificPlan,
) {
    match node {
        JsonTableSpecificPlan::Leaf(child) => visitor.visit_json_table_plan_leaf(child),
        JsonTableSpecificPlan::ParentChild(child) => {
            visitor.visit_json_table_plan_parent_child(child)
        }
        JsonTableSpecificPlan::Siblings(child) => visitor.visit_json_table_plan_siblings(child),
    }
}

pub fn fold_json_path_string<F: Fold + ?Sized>(
    folder: &mut F,
    node: JsonPathString,
) -> JsonPathString {
    let _ = folder;
    node
}

pub fn fold_json_path_invocation<F: Fold + ?Sized>(
    folder: &mut F,
    mut node: JsonPathInvocation,
) -> JsonPathInvocation {
    node.input_expression = folder.fold_expr(node.input_expression);
    node.json_path = folder.fold_json_path_string(node.json_path);
    node.path_name = node.path_name.map(|child| folder.fold_ident(child));
    node.path_parameters = node
        .path_parameters
        .into_iter()
        .map(|child| folder.fold_json_path_parameter(child))
        .collect();
    node
}

pub fn fold_json_path_parameter<F: Fold + ?Sized>(
    folder: &mut F,
    mut node: JsonPathParameter,
) -> JsonPathParameter {
    node.name = folder.fold_ident(node.name);
    node.parameter = folder.fold_expr(node.parameter);
    node
}

pub fn fold_json_constructor<F: Fold + ?Sized>(
    folder: &mut F,
    mut node: JsonConstructor,
) -> JsonConstructor {
    node.expression = folder.fold_expr(node.expression);
    node
}

pub fn fold_json_exists<F: Fold + ?Sized>(folder: &mut F, mut node: JsonExists) -> JsonExists {
    node.path_invocation = folder.fold_json_path_invocation(node.path_invocation);
    node
}

pub fn fold_json_value<F: Fold + ?Sized>(folder: &mut F, mut node: JsonValue) -> JsonValue {
    node.path_invocation = folder.fold_json_path_invocation(node.path_invocation);
    node.returned_type = node.returned_type.map(|child| folder.fold_type_name(child));
    node.empty_default = node.empty_default.map(|child| folder.fold_expr(child));
    node.error_default = node.error_default.map(|child| folder.fold_expr(child));
    node
}

#[recursive::recursive]
pub fn fold_json_query<F: Fold + ?Sized>(folder: &mut F, mut node: JsonQuery) -> JsonQuery {
    node.path_invocation = folder.fold_json_path_invocation(node.path_invocation);
    node.returned_type = node.returned_type.map(|child| folder.fold_type_name(child));
    node
}

pub fn fold_json_serialize<F: Fold + ?Sized>(
    folder: &mut F,
    mut node: JsonSerialize,
) -> JsonSerialize {
    node.expression = folder.fold_expr(node.expression);
    node.returned_type = node.returned_type.map(|child| folder.fold_type_name(child));
    node
}

pub fn fold_json_object<F: Fold + ?Sized>(folder: &mut F, mut node: JsonObject) -> JsonObject {
    node.members = node
        .members
        .into_iter()
        .map(|child| folder.fold_json_object_member(child))
        .collect();
    node.returned_type = node.returned_type.map(|child| folder.fold_type_name(child));
    node
}

pub fn fold_json_object_member<F: Fold + ?Sized>(
    folder: &mut F,
    mut node: JsonObjectMember,
) -> JsonObjectMember {
    node.key = folder.fold_expr(node.key);
    node.value = folder.fold_expr(node.value);
    node
}

pub fn fold_json_array<F: Fold + ?Sized>(folder: &mut F, mut node: JsonArray) -> JsonArray {
    node.elements = node
        .elements
        .into_iter()
        .map(|child| folder.fold_json_array_element(child))
        .collect();
    node.returned_type = node.returned_type.map(|child| folder.fold_type_name(child));
    node
}

pub fn fold_json_array_element<F: Fold + ?Sized>(
    folder: &mut F,
    mut node: JsonArrayElement,
) -> JsonArrayElement {
    node.value = folder.fold_expr(node.value);
    node
}

pub fn fold_json_table<F: Fold + ?Sized>(folder: &mut F, mut node: JsonTable) -> JsonTable {
    node.path_invocation = folder.fold_json_path_invocation(node.path_invocation);
    node.columns = node
        .columns
        .into_iter()
        .map(|child| folder.fold_json_table_column(child))
        .collect();
    node.plan = node.plan.map(|child| folder.fold_json_table_plan(child));
    node
}

pub fn fold_json_table_ordinality_column<F: Fold + ?Sized>(
    folder: &mut F,
    mut node: JsonTableOrdinalityColumn,
) -> JsonTableOrdinalityColumn {
    node.name = folder.fold_ident(node.name);
    node
}

pub fn fold_json_table_value_column<F: Fold + ?Sized>(
    folder: &mut F,
    mut node: JsonTableValueColumn,
) -> JsonTableValueColumn {
    node.name = folder.fold_ident(node.name);
    node.data_type = folder.fold_type_name(node.data_type);
    node.json_path = node
        .json_path
        .map(|child| folder.fold_json_path_string(child));
    node.empty_default = node.empty_default.map(|child| folder.fold_expr(child));
    node.error_default = node.error_default.map(|child| folder.fold_expr(child));
    node
}

pub fn fold_json_table_query_column<F: Fold + ?Sized>(
    folder: &mut F,
    mut node: JsonTableQueryColumn,
) -> JsonTableQueryColumn {
    node.name = folder.fold_ident(node.name);
    node.data_type = folder.fold_type_name(node.data_type);
    node.json_path = node
        .json_path
        .map(|child| folder.fold_json_path_string(child));
    node
}

pub fn fold_json_table_nested_columns<F: Fold + ?Sized>(
    folder: &mut F,
    mut node: JsonTableNestedColumns,
) -> JsonTableNestedColumns {
    node.json_path = folder.fold_json_path_string(node.json_path);
    node.path_name = node.path_name.map(|child| folder.fold_ident(child));
    node.columns = node
        .columns
        .into_iter()
        .map(|child| folder.fold_json_table_column(child))
        .collect();
    node
}

pub fn fold_json_table_default_plan<F: Fold + ?Sized>(
    folder: &mut F,
    node: JsonTableDefaultPlan,
) -> JsonTableDefaultPlan {
    let _ = folder;
    node
}

pub fn fold_json_table_plan_leaf<F: Fold + ?Sized>(
    folder: &mut F,
    mut node: JsonTablePlanLeaf,
) -> JsonTablePlanLeaf {
    node.name = folder.fold_ident(node.name);
    node
}

pub fn fold_json_table_plan_parent_child<F: Fold + ?Sized>(
    folder: &mut F,
    mut node: JsonTablePlanParentChild,
) -> JsonTablePlanParentChild {
    node.parent = folder.fold_json_table_plan_leaf(node.parent);
    node.child = AstBox::new(folder.fold_json_table_specific_plan(node.child.into_inner()));
    node
}

pub fn fold_json_table_plan_siblings<F: Fold + ?Sized>(
    folder: &mut F,
    mut node: JsonTablePlanSiblings,
) -> JsonTablePlanSiblings {
    node.siblings = node
        .siblings
        .into_iter()
        .map(|child| folder.fold_json_table_specific_plan(child))
        .collect();
    node
}

#[recursive::recursive]
pub fn fold_json_table_column<F: Fold + ?Sized>(
    folder: &mut F,
    node: JsonTableColumn,
) -> JsonTableColumn {
    match node {
        JsonTableColumn::Ordinality(child) => {
            JsonTableColumn::Ordinality(folder.fold_json_table_ordinality_column(child))
        }
        JsonTableColumn::Value(child) => {
            JsonTableColumn::Value(folder.fold_json_table_value_column(child))
        }
        JsonTableColumn::Query(child) => {
            JsonTableColumn::Query(folder.fold_json_table_query_column(child))
        }
        JsonTableColumn::Nested(child) => {
            JsonTableColumn::Nested(folder.fold_json_table_nested_columns(child))
        }
    }
}

pub fn fold_json_table_plan<F: Fold + ?Sized>(
    folder: &mut F,
    node: JsonTablePlan,
) -> JsonTablePlan {
    match node {
        JsonTablePlan::Default(child) => {
            JsonTablePlan::Default(folder.fold_json_table_default_plan(child))
        }
        JsonTablePlan::Specific(child) => {
            JsonTablePlan::Specific(folder.fold_json_table_specific_plan(child))
        }
    }
}

#[recursive::recursive]
pub fn fold_json_table_specific_plan<F: Fold + ?Sized>(
    folder: &mut F,
    node: JsonTableSpecificPlan,
) -> JsonTableSpecificPlan {
    match node {
        JsonTableSpecificPlan::Leaf(child) => {
            JsonTableSpecificPlan::Leaf(folder.fold_json_table_plan_leaf(child))
        }
        JsonTableSpecificPlan::ParentChild(child) => {
            JsonTableSpecificPlan::ParentChild(folder.fold_json_table_plan_parent_child(child))
        }
        JsonTableSpecificPlan::Siblings(child) => {
            JsonTableSpecificPlan::Siblings(folder.fold_json_table_plan_siblings(child))
        }
    }
}

fn eq_option<T: SyntaxEq>(left: &Option<T>, right: &Option<T>) -> bool {
    match (left, right) {
        (Some(a), Some(b)) => a.syntax_eq(b),
        (None, None) => true,
        _ => false,
    }
}
fn eq_slice<T: SyntaxEq>(left: &[T], right: &[T]) -> bool {
    left.len() == right.len() && left.iter().zip(right).all(|(a, b)| a.syntax_eq(b))
}

impl SyntaxEq for JsonPathString {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.value == other.value
    }
}

impl SyntaxEq for JsonPathInvocation {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.input_expression.syntax_eq(&other.input_expression)
            && self.input_format == other.input_format
            && self.json_path.syntax_eq(&other.json_path)
            && eq_option(&self.path_name, &other.path_name)
            && eq_slice(&self.path_parameters, &other.path_parameters)
    }
}

impl SyntaxEq for JsonPathParameter {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.name.syntax_eq(&other.name)
            && self.parameter.syntax_eq(&other.parameter)
            && self.format == other.format
    }
}

impl SyntaxEq for JsonConstructor {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.expression.syntax_eq(&other.expression) && self.format == other.format
    }
}

impl SyntaxEq for JsonExists {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.path_invocation.syntax_eq(&other.path_invocation)
            && self.error_behavior == other.error_behavior
    }
}

impl SyntaxEq for JsonValue {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.path_invocation.syntax_eq(&other.path_invocation)
            && eq_option(&self.returned_type, &other.returned_type)
            && self.empty_behavior == other.empty_behavior
            && eq_option(&self.empty_default, &other.empty_default)
            && self.error_behavior == other.error_behavior
            && eq_option(&self.error_default, &other.error_default)
    }
}

impl SyntaxEq for JsonQuery {
    #[recursive::recursive]
    fn syntax_eq(&self, other: &Self) -> bool {
        self.path_invocation.syntax_eq(&other.path_invocation)
            && eq_option(&self.returned_type, &other.returned_type)
            && self.output_format == other.output_format
            && self.wrapper_behavior == other.wrapper_behavior
            && self.quotes_behavior == other.quotes_behavior
            && self.empty_behavior == other.empty_behavior
            && self.error_behavior == other.error_behavior
    }
}

impl SyntaxEq for JsonSerialize {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.expression.syntax_eq(&other.expression)
            && self.input_format == other.input_format
            && eq_option(&self.returned_type, &other.returned_type)
            && self.output_format == other.output_format
            && self.error_behavior == other.error_behavior
    }
}

impl SyntaxEq for JsonObject {
    fn syntax_eq(&self, other: &Self) -> bool {
        eq_slice(&self.members, &other.members)
            && self.null_on_null == other.null_on_null
            && self.unique_keys == other.unique_keys
            && eq_option(&self.returned_type, &other.returned_type)
            && self.output_format == other.output_format
    }
}

impl SyntaxEq for JsonObjectMember {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.key.syntax_eq(&other.key)
            && self.value.syntax_eq(&other.value)
            && self.format == other.format
    }
}

impl SyntaxEq for JsonArray {
    fn syntax_eq(&self, other: &Self) -> bool {
        eq_slice(&self.elements, &other.elements)
            && self.null_on_null == other.null_on_null
            && eq_option(&self.returned_type, &other.returned_type)
            && self.output_format == other.output_format
    }
}

impl SyntaxEq for JsonArrayElement {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.value.syntax_eq(&other.value) && self.format == other.format
    }
}

impl SyntaxEq for JsonTable {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.path_invocation.syntax_eq(&other.path_invocation)
            && eq_slice(&self.columns, &other.columns)
            && eq_option(&self.plan, &other.plan)
            && self.error_behavior == other.error_behavior
    }
}

impl SyntaxEq for JsonTableOrdinalityColumn {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.name.syntax_eq(&other.name)
    }
}

impl SyntaxEq for JsonTableValueColumn {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.name.syntax_eq(&other.name)
            && self.data_type.syntax_eq(&other.data_type)
            && eq_option(&self.json_path, &other.json_path)
            && self.empty_behavior == other.empty_behavior
            && eq_option(&self.empty_default, &other.empty_default)
            && self.error_behavior == other.error_behavior
            && eq_option(&self.error_default, &other.error_default)
    }
}

impl SyntaxEq for JsonTableQueryColumn {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.name.syntax_eq(&other.name)
            && self.data_type.syntax_eq(&other.data_type)
            && self.format == other.format
            && eq_option(&self.json_path, &other.json_path)
            && self.wrapper_behavior == other.wrapper_behavior
            && self.quotes_behavior == other.quotes_behavior
            && self.empty_behavior == other.empty_behavior
            && self.error_behavior == other.error_behavior
    }
}

impl SyntaxEq for JsonTableNestedColumns {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.json_path.syntax_eq(&other.json_path)
            && eq_option(&self.path_name, &other.path_name)
            && eq_slice(&self.columns, &other.columns)
    }
}

impl SyntaxEq for JsonTableDefaultPlan {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.parent_child == other.parent_child && self.siblings == other.siblings
    }
}

impl SyntaxEq for JsonTablePlanLeaf {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.name.syntax_eq(&other.name)
    }
}

impl SyntaxEq for JsonTablePlanParentChild {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.plan_type == other.plan_type
            && self.parent.syntax_eq(&other.parent)
            && self.child.syntax_eq(&other.child)
    }
}

impl SyntaxEq for JsonTablePlanSiblings {
    fn syntax_eq(&self, other: &Self) -> bool {
        self.plan_type == other.plan_type && eq_slice(&self.siblings, &other.siblings)
    }
}

impl SyntaxEq for JsonTableColumn {
    #[recursive::recursive]
    fn syntax_eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Ordinality(left), Self::Ordinality(right)) => left.syntax_eq(right),
            (Self::Value(left), Self::Value(right)) => left.syntax_eq(right),
            (Self::Query(left), Self::Query(right)) => left.syntax_eq(right),
            (Self::Nested(left), Self::Nested(right)) => left.syntax_eq(right),
            _ => false,
        }
    }
}

impl SyntaxEq for JsonTablePlan {
    fn syntax_eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Default(left), Self::Default(right)) => left.syntax_eq(right),
            (Self::Specific(left), Self::Specific(right)) => left.syntax_eq(right),
            _ => false,
        }
    }
}

impl SyntaxEq for JsonTableSpecificPlan {
    #[recursive::recursive]
    fn syntax_eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Leaf(left), Self::Leaf(right)) => left.syntax_eq(right),
            (Self::ParentChild(left), Self::ParentChild(right)) => left.syntax_eq(right),
            (Self::Siblings(left), Self::Siblings(right)) => left.syntax_eq(right),
            _ => false,
        }
    }
}

/// Exact local constructor invariants from the pinned Java AST. This is not
/// JSON path/type/capability admission and does not normalize SQL expressions.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum JsonAstInvariantError {
    EmptyDefaultMismatch,
    ErrorDefaultMismatch,
    EmptyTableColumns,
    EmptyNestedColumns,
    TooFewPlanSiblings,
}

impl std::fmt::Display for JsonAstInvariantError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::EmptyDefaultMismatch => {
                "ON EMPTY default expression must be present exactly for DEFAULT behavior"
            }
            Self::ErrorDefaultMismatch => {
                "ON ERROR default expression must be present exactly for DEFAULT behavior"
            }
            Self::EmptyTableColumns => "JSON_TABLE columns must not be empty",
            Self::EmptyNestedColumns => "JSON_TABLE nested columns must not be empty",
            Self::TooFewPlanSiblings => "JSON_TABLE sibling plan requires at least two siblings",
        })
    }
}
impl std::error::Error for JsonAstInvariantError {}

impl JsonPathString {
    /// The caller supplies the original SQL StringLiteral's decoded value.
    pub fn new(value: String, span: Span) -> Self {
        Self { value, span }
    }
}

impl JsonPathInvocation {
    /// Plain two-argument invocation. FORMAT JSON is the AstBuilder default;
    /// there is no path name or PASSING list to fabricate.
    pub fn plain(input_expression: Expr, json_path: JsonPathString, span: Span) -> Self {
        Self {
            input_expression,
            input_format: JsonFormat::Json,
            json_path,
            path_name: None,
            path_parameters: Vec::new(),
            span,
        }
    }
}

impl JsonQuery {
    /// The exact AstBuilder facts of JSON_QUERY(input, 'path') with no clauses.
    /// Explicit KEEP remains Some(Keep); the absent clause remains None here.
    pub fn plain(path_invocation: JsonPathInvocation, span: Span) -> Self {
        Self {
            path_invocation,
            returned_type: None,
            output_format: None,
            wrapper_behavior: JsonQueryWrapperBehavior::Without,
            quotes_behavior: None,
            empty_behavior: JsonQueryBehavior::Null,
            error_behavior: JsonQueryBehavior::Null,
            span,
        }
    }
}

impl JsonValue {
    pub fn new(
        path_invocation: JsonPathInvocation,
        returned_type: Option<TypeName>,
        empty_behavior: JsonValueBehavior,
        empty_default: Option<Expr>,
        error_behavior: JsonValueBehavior,
        error_default: Option<Expr>,
        span: Span,
    ) -> Result<Self, JsonAstInvariantError> {
        let node = Self {
            path_invocation,
            returned_type,
            empty_behavior,
            empty_default,
            error_behavior,
            error_default,
            span,
        };
        node.validate_constructor_invariants()?;
        Ok(node)
    }
    pub fn validate_constructor_invariants(&self) -> Result<(), JsonAstInvariantError> {
        if (self.empty_behavior == JsonValueBehavior::Default) != self.empty_default.is_some() {
            return Err(JsonAstInvariantError::EmptyDefaultMismatch);
        }
        if (self.error_behavior == JsonValueBehavior::Default) != self.error_default.is_some() {
            return Err(JsonAstInvariantError::ErrorDefaultMismatch);
        }
        Ok(())
    }
}

impl JsonTableValueColumn {
    pub fn new(
        name: Ident,
        data_type: TypeName,
        json_path: Option<JsonPathString>,
        empty_behavior: JsonValueBehavior,
        empty_default: Option<Expr>,
        error_behavior: Option<JsonValueBehavior>,
        error_default: Option<Expr>,
        span: Span,
    ) -> Result<Self, JsonAstInvariantError> {
        let node = Self {
            name,
            data_type,
            json_path,
            empty_behavior,
            empty_default,
            error_behavior,
            error_default,
            span,
        };
        node.validate_constructor_invariants()?;
        Ok(node)
    }
    pub fn validate_constructor_invariants(&self) -> Result<(), JsonAstInvariantError> {
        if (self.empty_behavior == JsonValueBehavior::Default) != self.empty_default.is_some() {
            return Err(JsonAstInvariantError::EmptyDefaultMismatch);
        }
        if (self.error_behavior == Some(JsonValueBehavior::Default)) != self.error_default.is_some()
        {
            return Err(JsonAstInvariantError::ErrorDefaultMismatch);
        }
        Ok(())
    }
}

impl JsonTable {
    pub fn new(
        path_invocation: JsonPathInvocation,
        columns: Vec<JsonTableColumn>,
        plan: Option<JsonTablePlan>,
        error_behavior: Option<JsonTableErrorBehavior>,
        span: Span,
    ) -> Result<Self, JsonAstInvariantError> {
        let node = Self {
            path_invocation,
            columns,
            plan,
            error_behavior,
            span,
        };
        node.validate_constructor_invariants()?;
        Ok(node)
    }
    pub fn validate_constructor_invariants(&self) -> Result<(), JsonAstInvariantError> {
        if self.columns.is_empty() {
            return Err(JsonAstInvariantError::EmptyTableColumns);
        }
        Ok(())
    }
}

impl JsonTableNestedColumns {
    pub fn new(
        json_path: JsonPathString,
        path_name: Option<Ident>,
        columns: Vec<JsonTableColumn>,
        span: Span,
    ) -> Result<Self, JsonAstInvariantError> {
        let node = Self {
            json_path,
            path_name,
            columns: columns.into(),
            span,
        };
        node.validate_constructor_invariants()?;
        Ok(node)
    }
    pub fn validate_constructor_invariants(&self) -> Result<(), JsonAstInvariantError> {
        if self.columns.is_empty() {
            return Err(JsonAstInvariantError::EmptyNestedColumns);
        }
        Ok(())
    }
}

impl JsonTablePlanSiblings {
    pub fn new(
        plan_type: JsonTableSiblingsPlanType,
        siblings: Vec<JsonTableSpecificPlan>,
        span: Span,
    ) -> Result<Self, JsonAstInvariantError> {
        let node = Self {
            plan_type,
            siblings: siblings.into(),
            span,
        };
        node.validate_constructor_invariants()?;
        Ok(node)
    }
    pub fn validate_constructor_invariants(&self) -> Result<(), JsonAstInvariantError> {
        if self.siblings.len() < 2 {
            return Err(JsonAstInvariantError::TooFewPlanSiblings);
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "json_tests.rs"]
mod tests;
