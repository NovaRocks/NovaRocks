// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Typed relations and shared Table/TableSubquery payloads.

use super::{
    AstBox, AstVec, Expr, Fold, GroupBy, Ident, JsonTable, MeasureDefinition, ObjectName, OrderBy,
    PatternSearchMode, Query, RowPattern, SkipTo, SubsetDefinition, SyntaxEq, TypeName,
    VariableDefinition, Visit,
};
use crate::Span;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Relation {
    Table(AstBox<Table>),
    TableSubquery(AstBox<TableSubquery>),
    Aliased(AstBox<AliasedRelation>),
    Lateral(AstBox<Lateral>),
    Unnest(AstBox<Unnest>),
    Join(AstBox<Join>),
    Sampled(AstBox<SampledRelation>),
    Pivot(AstBox<Pivot>),
    Nearest(AstBox<Nearest>),
    TableFunction(AstBox<TableFunctionInvocation>),
    JsonTable(AstBox<JsonTable>),
    PatternRecognition(AstBox<PatternRecognitionRelation>),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum JoinCriteria {
    On(JoinOn),
    Using(JoinUsing),
    Natural(NaturalJoin),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TableFunctionArgumentValue {
    Table(TableFunctionTableArgument),
    Descriptor(TableFunctionDescriptorArgument),
    Expression(Expr),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum JoinType {
    Cross,
    Inner,
    Left,
    Right,
    Full,
    Implicit,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SamplingType {
    Bernoulli,
    System,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QueryPeriodRangeType {
    Timestamp,
    Version,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EmptyTableTreatmentKind {
    Keep,
    Prune,
}

// Java Table extends QueryBody and is also a Relation. query_period and
// branch are mutually exclusive; FROM/table-query grammar currently sets no branch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Table {
    pub name: ObjectName,
    pub query_period: Option<QueryPeriod>,
    pub branch: Option<Ident>,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TableSubquery {
    pub query: AstBox<Query>,
    pub span: Span,
}

// Java columnNames is nullable. None and Some(empty) must remain distinct.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AliasedRelation {
    pub relation: AstBox<Relation>,
    pub alias: Ident,
    pub column_names: Option<Vec<Ident>>,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Lateral {
    pub query: AstBox<Query>,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Unnest {
    pub expressions: AstVec<Expr>,
    pub with_ordinality: bool,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Join {
    pub kind: JoinType,
    pub left: AstBox<Relation>,
    pub right: AstBox<Relation>,
    pub criteria: Option<JoinCriteria>,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JoinOn {
    pub expression: Expr,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JoinUsing {
    pub columns: Vec<Ident>,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NaturalJoin {
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SampledRelation {
    pub relation: AstBox<Relation>,
    pub kind: SamplingType,
    pub sample_percentage: Expr,
    pub span: Span,
}

// Java stores both optional endpoints. Current grammar/builder constructs
// start=None, end=Some(valueExpression); no new start-period syntax is implied.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryPeriod {
    pub start: Option<AstBox<Expr>>,
    pub end: Option<AstBox<Expr>>,
    pub range_type: QueryPeriodRangeType,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Pivot {
    pub input: AstBox<Relation>,
    pub aggregations: AstVec<PivotAggregation>,
    pub pivot_columns: AstVec<Expr>,
    pub value_groups: AstVec<PivotValueGroup>,
    pub group_by: Option<GroupBy>,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PivotAggregation {
    pub expression: Expr,
    pub alias: Option<Ident>,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PivotValueGroup {
    pub values: AstVec<Expr>,
    pub alias: Option<Ident>,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Nearest {
    pub relation: AstBox<Relation>,
    pub where_clause: Option<Expr>,
    pub match_expression: Expr,
    pub span: Span,
}

// Co-partition groups are ordered lists of QualifiedNames, not expressions.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TableFunctionInvocation {
    pub name: ObjectName,
    pub arguments: AstVec<TableFunctionArgument>,
    pub copartitioning: Vec<Vec<ObjectName>>,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TableFunctionArgument {
    pub name: Option<Ident>,
    pub value: TableFunctionArgumentValue,
    pub span: Span,
}

// Some(empty partition_by) means PARTITION BY (), not an absent clause.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TableFunctionTableArgument {
    pub table: AstBox<Relation>,
    pub partition_by: Option<AstVec<Expr>>,
    pub order_by: Option<OrderBy>,
    pub empty_table_treatment: Option<EmptyTableTreatment>,
    pub span: Span,
}

// None is the explicit CAST(NULL AS DESCRIPTOR) alternative.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TableFunctionDescriptorArgument {
    pub descriptor: Option<Descriptor>,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Descriptor {
    pub fields: Vec<DescriptorField>,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescriptorField {
    pub name: Ident,
    pub data_type: Option<TypeName>,
    pub span: Span,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EmptyTableTreatment {
    pub treatment: EmptyTableTreatmentKind,
    pub span: Span,
}

/// Complete MATCH_RECOGNIZE relation; reuses current shared row-pattern facts.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PatternRecognitionRelation {
    pub input: AstBox<Relation>,
    pub partition_by: AstVec<Expr>,
    pub order_by: Option<OrderBy>,
    pub measures: AstVec<MeasureDefinition>,
    pub rows_per_match: Option<RowsPerMatch>,
    pub after_match_skip_to: Option<SkipTo>,
    pub pattern_search_mode: Option<PatternSearchMode>,
    pub pattern: RowPattern,
    pub subsets: AstVec<SubsetDefinition>,
    pub variable_definitions: AstVec<VariableDefinition>,
    pub span: Span,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RowsPerMatch {
    One,
    AllShowEmpty,
    AllOmitEmpty,
    AllWithUnmatched,
    Window,
}

#[recursive::recursive]
pub fn walk_relation<V: Visit + ?Sized>(visitor: &mut V, node: &Relation) {
    match node {
        Relation::Table(value) => visitor.visit_table(value),
        Relation::TableSubquery(value) => visitor.visit_table_subquery(value),
        Relation::Aliased(value) => visitor.visit_aliased_relation(value),
        Relation::Lateral(value) => visitor.visit_lateral(value),
        Relation::Unnest(value) => visitor.visit_unnest(value),
        Relation::Join(value) => visitor.visit_join(value),
        Relation::Sampled(value) => visitor.visit_sampled_relation(value),
        Relation::Pivot(value) => visitor.visit_pivot(value),
        Relation::Nearest(value) => visitor.visit_nearest(value),
        Relation::TableFunction(value) => visitor.visit_table_function_invocation(value),
        Relation::JsonTable(value) => visitor.visit_json_table(value),
        Relation::PatternRecognition(value) => visitor.visit_pattern_recognition_relation(value),
    }
}

#[recursive::recursive]
pub fn walk_join_criteria<V: Visit + ?Sized>(visitor: &mut V, node: &JoinCriteria) {
    match node {
        JoinCriteria::On(value) => visitor.visit_join_on(value),
        JoinCriteria::Using(value) => visitor.visit_join_using(value),
        JoinCriteria::Natural(value) => visitor.visit_natural_join(value),
    }
}

#[recursive::recursive]
pub fn walk_table_function_argument_value<V: Visit + ?Sized>(
    visitor: &mut V,
    node: &TableFunctionArgumentValue,
) {
    match node {
        TableFunctionArgumentValue::Table(value) => {
            visitor.visit_table_function_table_argument(value)
        }
        TableFunctionArgumentValue::Descriptor(value) => {
            visitor.visit_table_function_descriptor_argument(value)
        }
        TableFunctionArgumentValue::Expression(value) => visitor.visit_expr(value),
    }
}

#[recursive::recursive]
pub fn walk_table<V: Visit + ?Sized>(visitor: &mut V, node: &Table) {
    visitor.visit_object_name(&node.name);
    if let Some(value) = &node.query_period {
        visitor.visit_query_period(value);
    }
    if let Some(value) = &node.branch {
        visitor.visit_ident(value);
    }
}

#[recursive::recursive]
pub fn walk_table_subquery<V: Visit + ?Sized>(visitor: &mut V, node: &TableSubquery) {
    visitor.visit_query(&node.query);
}

#[recursive::recursive]
pub fn walk_aliased_relation<V: Visit + ?Sized>(visitor: &mut V, node: &AliasedRelation) {
    visitor.visit_relation(&node.relation);
    visitor.visit_ident(&node.alias);
    if let Some(values) = &node.column_names {
        for value in values {
            visitor.visit_ident(value);
        }
    }
}

#[recursive::recursive]
pub fn walk_lateral<V: Visit + ?Sized>(visitor: &mut V, node: &Lateral) {
    visitor.visit_query(&node.query);
}

#[recursive::recursive]
pub fn walk_unnest<V: Visit + ?Sized>(visitor: &mut V, node: &Unnest) {
    for value in &node.expressions {
        visitor.visit_expr(value);
    }
}

#[recursive::recursive]
pub fn walk_join<V: Visit + ?Sized>(visitor: &mut V, node: &Join) {
    visitor.visit_relation(&node.left);
    visitor.visit_relation(&node.right);
    if let Some(value) = &node.criteria {
        visitor.visit_join_criteria(value);
    }
}

#[recursive::recursive]
pub fn walk_join_on<V: Visit + ?Sized>(visitor: &mut V, node: &JoinOn) {
    visitor.visit_expr(&node.expression);
}

#[recursive::recursive]
pub fn walk_join_using<V: Visit + ?Sized>(visitor: &mut V, node: &JoinUsing) {
    for value in &node.columns {
        visitor.visit_ident(value);
    }
}

#[recursive::recursive]
pub fn walk_natural_join<V: Visit + ?Sized>(_: &mut V, _: &NaturalJoin) {}

#[recursive::recursive]
pub fn walk_sampled_relation<V: Visit + ?Sized>(visitor: &mut V, node: &SampledRelation) {
    visitor.visit_relation(&node.relation);
    visitor.visit_expr(&node.sample_percentage);
}

#[recursive::recursive]
pub fn walk_query_period<V: Visit + ?Sized>(visitor: &mut V, node: &QueryPeriod) {
    if let Some(value) = &node.start {
        visitor.visit_expr(value);
    }
    if let Some(value) = &node.end {
        visitor.visit_expr(value);
    }
}

#[recursive::recursive]
pub fn walk_pivot<V: Visit + ?Sized>(visitor: &mut V, node: &Pivot) {
    visitor.visit_relation(&node.input);
    for value in &node.aggregations {
        visitor.visit_pivot_aggregation(value);
    }
    for value in &node.pivot_columns {
        visitor.visit_expr(value);
    }
    for value in &node.value_groups {
        visitor.visit_pivot_value_group(value);
    }
    if let Some(value) = &node.group_by {
        visitor.visit_group_by(value);
    }
}

#[recursive::recursive]
pub fn walk_pivot_aggregation<V: Visit + ?Sized>(visitor: &mut V, node: &PivotAggregation) {
    visitor.visit_expr(&node.expression);
    if let Some(value) = &node.alias {
        visitor.visit_ident(value);
    }
}

#[recursive::recursive]
pub fn walk_pivot_value_group<V: Visit + ?Sized>(visitor: &mut V, node: &PivotValueGroup) {
    for value in &node.values {
        visitor.visit_expr(value);
    }
    if let Some(value) = &node.alias {
        visitor.visit_ident(value);
    }
}

#[recursive::recursive]
pub fn walk_nearest<V: Visit + ?Sized>(visitor: &mut V, node: &Nearest) {
    visitor.visit_relation(&node.relation);
    if let Some(value) = &node.where_clause {
        visitor.visit_expr(value);
    }
    visitor.visit_expr(&node.match_expression);
}

#[recursive::recursive]
pub fn walk_table_function_invocation<V: Visit + ?Sized>(
    visitor: &mut V,
    node: &TableFunctionInvocation,
) {
    visitor.visit_object_name(&node.name);
    for value in &node.arguments {
        visitor.visit_table_function_argument(value);
    }
    for values in &node.copartitioning {
        for value in values {
            visitor.visit_object_name(value);
        }
    }
}

#[recursive::recursive]
pub fn walk_table_function_argument<V: Visit + ?Sized>(
    visitor: &mut V,
    node: &TableFunctionArgument,
) {
    if let Some(value) = &node.name {
        visitor.visit_ident(value);
    }
    visitor.visit_table_function_argument_value(&node.value);
}

#[recursive::recursive]
pub fn walk_table_function_table_argument<V: Visit + ?Sized>(
    visitor: &mut V,
    node: &TableFunctionTableArgument,
) {
    visitor.visit_relation(&node.table);
    if let Some(values) = &node.partition_by {
        for value in values {
            visitor.visit_expr(value);
        }
    }
    if let Some(value) = &node.order_by {
        visitor.visit_order_by(value);
    }
    if let Some(value) = &node.empty_table_treatment {
        visitor.visit_empty_table_treatment(value);
    }
}

#[recursive::recursive]
pub fn walk_table_function_descriptor_argument<V: Visit + ?Sized>(
    visitor: &mut V,
    node: &TableFunctionDescriptorArgument,
) {
    if let Some(value) = &node.descriptor {
        visitor.visit_descriptor(value);
    }
}

#[recursive::recursive]
pub fn walk_descriptor<V: Visit + ?Sized>(visitor: &mut V, node: &Descriptor) {
    for value in &node.fields {
        visitor.visit_descriptor_field(value);
    }
}

#[recursive::recursive]
pub fn walk_descriptor_field<V: Visit + ?Sized>(visitor: &mut V, node: &DescriptorField) {
    visitor.visit_ident(&node.name);
    if let Some(value) = &node.data_type {
        visitor.visit_type_name(value);
    }
}

#[recursive::recursive]
pub fn walk_empty_table_treatment<V: Visit + ?Sized>(_: &mut V, _: &EmptyTableTreatment) {}

#[recursive::recursive]
pub fn fold_relation<F: Fold + ?Sized>(folder: &mut F, node: Relation) -> Relation {
    match node {
        Relation::Table(value) => {
            Relation::Table(AstBox::new(folder.fold_table(value.into_inner())))
        }
        Relation::TableSubquery(value) => {
            Relation::TableSubquery(AstBox::new(folder.fold_table_subquery(value.into_inner())))
        }
        Relation::Aliased(value) => Relation::Aliased(AstBox::new(
            folder.fold_aliased_relation(value.into_inner()),
        )),
        Relation::Lateral(value) => {
            Relation::Lateral(AstBox::new(folder.fold_lateral(value.into_inner())))
        }
        Relation::Unnest(value) => {
            Relation::Unnest(AstBox::new(folder.fold_unnest(value.into_inner())))
        }
        Relation::Join(value) => Relation::Join(AstBox::new(folder.fold_join(value.into_inner()))),
        Relation::Sampled(value) => Relation::Sampled(AstBox::new(
            folder.fold_sampled_relation(value.into_inner()),
        )),
        Relation::Pivot(value) => {
            Relation::Pivot(AstBox::new(folder.fold_pivot(value.into_inner())))
        }
        Relation::Nearest(value) => {
            Relation::Nearest(AstBox::new(folder.fold_nearest(value.into_inner())))
        }
        Relation::TableFunction(value) => Relation::TableFunction(AstBox::new(
            folder.fold_table_function_invocation(value.into_inner()),
        )),
        Relation::JsonTable(value) => {
            Relation::JsonTable(AstBox::new(folder.fold_json_table(value.into_inner())))
        }
        Relation::PatternRecognition(value) => Relation::PatternRecognition(AstBox::new(
            folder.fold_pattern_recognition_relation(value.into_inner()),
        )),
    }
}

#[recursive::recursive]
pub fn fold_join_criteria<F: Fold + ?Sized>(folder: &mut F, node: JoinCriteria) -> JoinCriteria {
    match node {
        JoinCriteria::On(value) => JoinCriteria::On(folder.fold_join_on(value)),
        JoinCriteria::Using(value) => JoinCriteria::Using(folder.fold_join_using(value)),
        JoinCriteria::Natural(value) => JoinCriteria::Natural(folder.fold_natural_join(value)),
    }
}

#[recursive::recursive]
pub fn fold_table_function_argument_value<F: Fold + ?Sized>(
    folder: &mut F,
    node: TableFunctionArgumentValue,
) -> TableFunctionArgumentValue {
    match node {
        TableFunctionArgumentValue::Table(value) => {
            TableFunctionArgumentValue::Table(folder.fold_table_function_table_argument(value))
        }
        TableFunctionArgumentValue::Descriptor(value) => TableFunctionArgumentValue::Descriptor(
            folder.fold_table_function_descriptor_argument(value),
        ),
        TableFunctionArgumentValue::Expression(value) => {
            TableFunctionArgumentValue::Expression(folder.fold_expr(value))
        }
    }
}

#[recursive::recursive]
pub fn fold_table<F: Fold + ?Sized>(folder: &mut F, mut node: Table) -> Table {
    node.name = folder.fold_object_name(node.name);
    node.query_period = node
        .query_period
        .map(|value| folder.fold_query_period(value));
    node.branch = node.branch.map(|value| folder.fold_ident(value));
    node
}

#[recursive::recursive]
pub fn fold_table_subquery<F: Fold + ?Sized>(
    folder: &mut F,
    mut node: TableSubquery,
) -> TableSubquery {
    node.query = AstBox::new(folder.fold_query(node.query.into_inner()));
    node
}

#[recursive::recursive]
pub fn fold_aliased_relation<F: Fold + ?Sized>(
    folder: &mut F,
    mut node: AliasedRelation,
) -> AliasedRelation {
    node.relation = AstBox::new(folder.fold_relation(node.relation.into_inner()));
    node.alias = folder.fold_ident(node.alias);
    node.column_names = node.column_names.map(|values| {
        values
            .into_iter()
            .map(|value| folder.fold_ident(value))
            .collect()
    });
    node
}

#[recursive::recursive]
pub fn fold_lateral<F: Fold + ?Sized>(folder: &mut F, mut node: Lateral) -> Lateral {
    node.query = AstBox::new(folder.fold_query(node.query.into_inner()));
    node
}

#[recursive::recursive]
pub fn fold_unnest<F: Fold + ?Sized>(folder: &mut F, mut node: Unnest) -> Unnest {
    node.expressions = node
        .expressions
        .into_iter()
        .map(|value| folder.fold_expr(value))
        .collect();
    node
}

#[recursive::recursive]
pub fn fold_join<F: Fold + ?Sized>(folder: &mut F, mut node: Join) -> Join {
    node.left = AstBox::new(folder.fold_relation(node.left.into_inner()));
    node.right = AstBox::new(folder.fold_relation(node.right.into_inner()));
    node.criteria = node.criteria.map(|value| folder.fold_join_criteria(value));
    node
}

#[recursive::recursive]
pub fn fold_join_on<F: Fold + ?Sized>(folder: &mut F, mut node: JoinOn) -> JoinOn {
    node.expression = folder.fold_expr(node.expression);
    node
}

#[recursive::recursive]
pub fn fold_join_using<F: Fold + ?Sized>(folder: &mut F, mut node: JoinUsing) -> JoinUsing {
    node.columns = node
        .columns
        .into_iter()
        .map(|value| folder.fold_ident(value))
        .collect();
    node
}

#[recursive::recursive]
pub fn fold_natural_join<F: Fold + ?Sized>(_: &mut F, node: NaturalJoin) -> NaturalJoin {
    node
}

#[recursive::recursive]
pub fn fold_sampled_relation<F: Fold + ?Sized>(
    folder: &mut F,
    mut node: SampledRelation,
) -> SampledRelation {
    node.relation = AstBox::new(folder.fold_relation(node.relation.into_inner()));
    node.sample_percentage = folder.fold_expr(node.sample_percentage);
    node
}

#[recursive::recursive]
pub fn fold_query_period<F: Fold + ?Sized>(folder: &mut F, mut node: QueryPeriod) -> QueryPeriod {
    node.start = node
        .start
        .map(|value| AstBox::new(folder.fold_expr(value.into_inner())));
    node.end = node
        .end
        .map(|value| AstBox::new(folder.fold_expr(value.into_inner())));
    node
}

#[recursive::recursive]
pub fn fold_pivot<F: Fold + ?Sized>(folder: &mut F, mut node: Pivot) -> Pivot {
    node.input = AstBox::new(folder.fold_relation(node.input.into_inner()));
    node.aggregations = node
        .aggregations
        .into_iter()
        .map(|value| folder.fold_pivot_aggregation(value))
        .collect();
    node.pivot_columns = node
        .pivot_columns
        .into_iter()
        .map(|value| folder.fold_expr(value))
        .collect();
    node.value_groups = node
        .value_groups
        .into_iter()
        .map(|value| folder.fold_pivot_value_group(value))
        .collect();
    node.group_by = node.group_by.map(|value| folder.fold_group_by(value));
    node
}

#[recursive::recursive]
pub fn fold_pivot_aggregation<F: Fold + ?Sized>(
    folder: &mut F,
    mut node: PivotAggregation,
) -> PivotAggregation {
    node.expression = folder.fold_expr(node.expression);
    node.alias = node.alias.map(|value| folder.fold_ident(value));
    node
}

#[recursive::recursive]
pub fn fold_pivot_value_group<F: Fold + ?Sized>(
    folder: &mut F,
    mut node: PivotValueGroup,
) -> PivotValueGroup {
    node.values = node
        .values
        .into_iter()
        .map(|value| folder.fold_expr(value))
        .collect();
    node.alias = node.alias.map(|value| folder.fold_ident(value));
    node
}

#[recursive::recursive]
pub fn fold_nearest<F: Fold + ?Sized>(folder: &mut F, mut node: Nearest) -> Nearest {
    node.relation = AstBox::new(folder.fold_relation(node.relation.into_inner()));
    node.where_clause = node.where_clause.map(|value| folder.fold_expr(value));
    node.match_expression = folder.fold_expr(node.match_expression);
    node
}

#[recursive::recursive]
pub fn fold_table_function_invocation<F: Fold + ?Sized>(
    folder: &mut F,
    mut node: TableFunctionInvocation,
) -> TableFunctionInvocation {
    node.name = folder.fold_object_name(node.name);
    node.arguments = node
        .arguments
        .into_iter()
        .map(|value| folder.fold_table_function_argument(value))
        .collect();
    node.copartitioning = node
        .copartitioning
        .into_iter()
        .map(|values| {
            values
                .into_iter()
                .map(|value| folder.fold_object_name(value))
                .collect()
        })
        .collect();
    node
}

#[recursive::recursive]
pub fn fold_table_function_argument<F: Fold + ?Sized>(
    folder: &mut F,
    mut node: TableFunctionArgument,
) -> TableFunctionArgument {
    node.name = node.name.map(|value| folder.fold_ident(value));
    node.value = folder.fold_table_function_argument_value(node.value);
    node
}

#[recursive::recursive]
pub fn fold_table_function_table_argument<F: Fold + ?Sized>(
    folder: &mut F,
    mut node: TableFunctionTableArgument,
) -> TableFunctionTableArgument {
    node.table = AstBox::new(folder.fold_relation(node.table.into_inner()));
    node.partition_by = node.partition_by.map(|values| {
        values
            .into_iter()
            .map(|value| folder.fold_expr(value))
            .collect()
    });
    node.order_by = node.order_by.map(|value| folder.fold_order_by(value));
    node.empty_table_treatment = node
        .empty_table_treatment
        .map(|value| folder.fold_empty_table_treatment(value));
    node
}

#[recursive::recursive]
pub fn fold_table_function_descriptor_argument<F: Fold + ?Sized>(
    folder: &mut F,
    mut node: TableFunctionDescriptorArgument,
) -> TableFunctionDescriptorArgument {
    node.descriptor = node.descriptor.map(|value| folder.fold_descriptor(value));
    node
}

#[recursive::recursive]
pub fn fold_descriptor<F: Fold + ?Sized>(folder: &mut F, mut node: Descriptor) -> Descriptor {
    node.fields = node
        .fields
        .into_iter()
        .map(|value| folder.fold_descriptor_field(value))
        .collect();
    node
}

#[recursive::recursive]
pub fn fold_descriptor_field<F: Fold + ?Sized>(
    folder: &mut F,
    mut node: DescriptorField,
) -> DescriptorField {
    node.name = folder.fold_ident(node.name);
    node.data_type = node.data_type.map(|value| folder.fold_type_name(value));
    node
}

#[recursive::recursive]
pub fn fold_empty_table_treatment<F: Fold + ?Sized>(
    _: &mut F,
    node: EmptyTableTreatment,
) -> EmptyTableTreatment {
    node
}

fn eq_slice<T: SyntaxEq>(left: &[T], right: &[T]) -> bool {
    left.len() == right.len() && left.iter().zip(right).all(|(a, b)| a.syntax_eq(b))
}
fn eq_optional<T: SyntaxEq>(left: Option<&T>, right: Option<&T>) -> bool {
    match (left, right) {
        (Some(a), Some(b)) => a.syntax_eq(b),
        (None, None) => true,
        _ => false,
    }
}
fn eq_optional_slice<T: SyntaxEq>(left: Option<&[T]>, right: Option<&[T]>) -> bool {
    match (left, right) {
        (Some(a), Some(b)) => eq_slice(a, b),
        (None, None) => true,
        _ => false,
    }
}

impl SyntaxEq for Relation {
    #[recursive::recursive]
    fn syntax_eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Table(a), Self::Table(b)) => a.syntax_eq(b),
            (Self::TableSubquery(a), Self::TableSubquery(b)) => a.syntax_eq(b),
            (Self::Aliased(a), Self::Aliased(b)) => a.syntax_eq(b),
            (Self::Lateral(a), Self::Lateral(b)) => a.syntax_eq(b),
            (Self::Unnest(a), Self::Unnest(b)) => a.syntax_eq(b),
            (Self::Join(a), Self::Join(b)) => a.syntax_eq(b),
            (Self::Sampled(a), Self::Sampled(b)) => a.syntax_eq(b),
            (Self::Pivot(a), Self::Pivot(b)) => a.syntax_eq(b),
            (Self::Nearest(a), Self::Nearest(b)) => a.syntax_eq(b),
            (Self::TableFunction(a), Self::TableFunction(b)) => a.syntax_eq(b),
            (Self::JsonTable(a), Self::JsonTable(b)) => a.syntax_eq(b),
            (Self::PatternRecognition(a), Self::PatternRecognition(b)) => a.syntax_eq(b),
            _ => false,
        }
    }
}

impl SyntaxEq for JoinCriteria {
    #[recursive::recursive]
    fn syntax_eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::On(a), Self::On(b)) => a.syntax_eq(b),
            (Self::Using(a), Self::Using(b)) => a.syntax_eq(b),
            (Self::Natural(a), Self::Natural(b)) => a.syntax_eq(b),
            _ => false,
        }
    }
}

impl SyntaxEq for TableFunctionArgumentValue {
    #[recursive::recursive]
    fn syntax_eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Table(a), Self::Table(b)) => a.syntax_eq(b),
            (Self::Descriptor(a), Self::Descriptor(b)) => a.syntax_eq(b),
            (Self::Expression(a), Self::Expression(b)) => a.syntax_eq(b),
            _ => false,
        }
    }
}

impl SyntaxEq for Table {
    #[recursive::recursive]
    fn syntax_eq(&self, other: &Self) -> bool {
        self.name.syntax_eq(&other.name)
            && eq_optional(self.query_period.as_ref(), other.query_period.as_ref())
            && eq_optional(self.branch.as_ref(), other.branch.as_ref())
    }
}

impl SyntaxEq for TableSubquery {
    #[recursive::recursive]
    fn syntax_eq(&self, other: &Self) -> bool {
        self.query.syntax_eq(&other.query)
    }
}

impl SyntaxEq for AliasedRelation {
    #[recursive::recursive]
    fn syntax_eq(&self, other: &Self) -> bool {
        self.relation.syntax_eq(&other.relation)
            && self.alias.syntax_eq(&other.alias)
            && eq_optional_slice(self.column_names.as_deref(), other.column_names.as_deref())
    }
}

impl SyntaxEq for Lateral {
    #[recursive::recursive]
    fn syntax_eq(&self, other: &Self) -> bool {
        self.query.syntax_eq(&other.query)
    }
}

impl SyntaxEq for Unnest {
    #[recursive::recursive]
    fn syntax_eq(&self, other: &Self) -> bool {
        eq_slice(&self.expressions, &other.expressions)
            && self.with_ordinality == other.with_ordinality
    }
}

impl SyntaxEq for Join {
    #[recursive::recursive]
    fn syntax_eq(&self, other: &Self) -> bool {
        self.kind == other.kind
            && self.left.syntax_eq(&other.left)
            && self.right.syntax_eq(&other.right)
            && eq_optional(self.criteria.as_ref(), other.criteria.as_ref())
    }
}

impl SyntaxEq for JoinOn {
    #[recursive::recursive]
    fn syntax_eq(&self, other: &Self) -> bool {
        self.expression.syntax_eq(&other.expression)
    }
}

impl SyntaxEq for JoinUsing {
    #[recursive::recursive]
    fn syntax_eq(&self, other: &Self) -> bool {
        eq_slice(&self.columns, &other.columns)
    }
}

impl SyntaxEq for NaturalJoin {
    #[recursive::recursive]
    fn syntax_eq(&self, _: &Self) -> bool {
        true
    }
}

impl SyntaxEq for SampledRelation {
    #[recursive::recursive]
    fn syntax_eq(&self, other: &Self) -> bool {
        self.relation.syntax_eq(&other.relation)
            && self.kind == other.kind
            && self.sample_percentage.syntax_eq(&other.sample_percentage)
    }
}

impl SyntaxEq for QueryPeriod {
    #[recursive::recursive]
    fn syntax_eq(&self, other: &Self) -> bool {
        eq_optional(self.start.as_deref(), other.start.as_deref())
            && eq_optional(self.end.as_deref(), other.end.as_deref())
            && self.range_type == other.range_type
    }
}

impl SyntaxEq for Pivot {
    #[recursive::recursive]
    fn syntax_eq(&self, other: &Self) -> bool {
        self.input.syntax_eq(&other.input)
            && eq_slice(&self.aggregations, &other.aggregations)
            && eq_slice(&self.pivot_columns, &other.pivot_columns)
            && eq_slice(&self.value_groups, &other.value_groups)
            && eq_optional(self.group_by.as_ref(), other.group_by.as_ref())
    }
}

impl SyntaxEq for PivotAggregation {
    #[recursive::recursive]
    fn syntax_eq(&self, other: &Self) -> bool {
        self.expression.syntax_eq(&other.expression)
            && eq_optional(self.alias.as_ref(), other.alias.as_ref())
    }
}

impl SyntaxEq for PivotValueGroup {
    #[recursive::recursive]
    fn syntax_eq(&self, other: &Self) -> bool {
        eq_slice(&self.values, &other.values)
            && eq_optional(self.alias.as_ref(), other.alias.as_ref())
    }
}

impl SyntaxEq for Nearest {
    #[recursive::recursive]
    fn syntax_eq(&self, other: &Self) -> bool {
        self.relation.syntax_eq(&other.relation)
            && eq_optional(self.where_clause.as_ref(), other.where_clause.as_ref())
            && self.match_expression.syntax_eq(&other.match_expression)
    }
}

impl SyntaxEq for TableFunctionInvocation {
    #[recursive::recursive]
    fn syntax_eq(&self, other: &Self) -> bool {
        self.name.syntax_eq(&other.name)
            && eq_slice(&self.arguments, &other.arguments)
            && self.copartitioning.len() == other.copartitioning.len()
            && self
                .copartitioning
                .iter()
                .zip(&other.copartitioning)
                .all(|(a, b)| eq_slice(a, b))
    }
}

impl SyntaxEq for TableFunctionArgument {
    #[recursive::recursive]
    fn syntax_eq(&self, other: &Self) -> bool {
        eq_optional(self.name.as_ref(), other.name.as_ref()) && self.value.syntax_eq(&other.value)
    }
}

impl SyntaxEq for TableFunctionTableArgument {
    #[recursive::recursive]
    fn syntax_eq(&self, other: &Self) -> bool {
        self.table.syntax_eq(&other.table)
            && eq_optional_slice(
                self.partition_by.as_ref().map(|v| v.as_slice()),
                other.partition_by.as_ref().map(|v| v.as_slice()),
            )
            && eq_optional(self.order_by.as_ref(), other.order_by.as_ref())
            && eq_optional(
                self.empty_table_treatment.as_ref(),
                other.empty_table_treatment.as_ref(),
            )
    }
}

impl SyntaxEq for TableFunctionDescriptorArgument {
    #[recursive::recursive]
    fn syntax_eq(&self, other: &Self) -> bool {
        eq_optional(self.descriptor.as_ref(), other.descriptor.as_ref())
    }
}

impl SyntaxEq for Descriptor {
    #[recursive::recursive]
    fn syntax_eq(&self, other: &Self) -> bool {
        eq_slice(&self.fields, &other.fields)
    }
}

impl SyntaxEq for DescriptorField {
    #[recursive::recursive]
    fn syntax_eq(&self, other: &Self) -> bool {
        self.name.syntax_eq(&other.name)
            && eq_optional(self.data_type.as_ref(), other.data_type.as_ref())
    }
}

impl SyntaxEq for EmptyTableTreatment {
    #[recursive::recursive]
    fn syntax_eq(&self, other: &Self) -> bool {
        self.treatment == other.treatment
    }
}

#[recursive::recursive]
pub fn walk_pattern_recognition_relation<V: Visit + ?Sized>(
    visitor: &mut V,
    node: &PatternRecognitionRelation,
) {
    visitor.visit_relation(&node.input);
    for value in &node.partition_by {
        visitor.visit_expr(value);
    }
    if let Some(value) = &node.order_by {
        visitor.visit_order_by(value);
    }
    for value in &node.measures {
        visitor.visit_measure_definition(value);
    }
    if let Some(value) = &node.after_match_skip_to {
        visitor.visit_skip_to(value);
    }
    visitor.visit_row_pattern(&node.pattern);
    for value in &node.subsets {
        visitor.visit_subset_definition(value);
    }
    for value in &node.variable_definitions {
        visitor.visit_variable_definition(value);
    }
    if let Some(value) = &node.pattern_search_mode {
        visitor.visit_pattern_search_mode(value);
    }
}
#[recursive::recursive]
pub fn fold_pattern_recognition_relation<F: Fold + ?Sized>(
    folder: &mut F,
    mut node: PatternRecognitionRelation,
) -> PatternRecognitionRelation {
    node.input = AstBox::new(folder.fold_relation(node.input.into_inner()));
    node.partition_by = node
        .partition_by
        .into_iter()
        .map(|value| folder.fold_expr(value))
        .collect();
    node.order_by = node.order_by.map(|value| folder.fold_order_by(value));
    node.measures = node
        .measures
        .into_iter()
        .map(|value| folder.fold_measure_definition(value))
        .collect();
    node.after_match_skip_to = node
        .after_match_skip_to
        .map(|value| folder.fold_skip_to(value));
    node.pattern = folder.fold_row_pattern(node.pattern);
    node.subsets = node
        .subsets
        .into_iter()
        .map(|value| folder.fold_subset_definition(value))
        .collect();
    node.variable_definitions = node
        .variable_definitions
        .into_iter()
        .map(|value| folder.fold_variable_definition(value))
        .collect();
    node.pattern_search_mode = node
        .pattern_search_mode
        .map(|value| folder.fold_pattern_search_mode(value));
    node
}
impl SyntaxEq for PatternRecognitionRelation {
    #[recursive::recursive]
    fn syntax_eq(&self, other: &Self) -> bool {
        self.input.syntax_eq(&other.input)
            && eq_slice(&self.partition_by, &other.partition_by)
            && eq_optional(self.order_by.as_ref(), other.order_by.as_ref())
            && eq_slice(&self.measures, &other.measures)
            && self.rows_per_match == other.rows_per_match
            && eq_optional(
                self.after_match_skip_to.as_ref(),
                other.after_match_skip_to.as_ref(),
            )
            && eq_optional(
                self.pattern_search_mode.as_ref(),
                other.pattern_search_mode.as_ref(),
            )
            && self.pattern.syntax_eq(&other.pattern)
            && eq_slice(&self.subsets, &other.subsets)
            && eq_slice(&self.variable_definitions, &other.variable_definitions)
    }
}
impl Relation {
    pub fn span(&self) -> Span {
        match self {
            Self::Table(n) => n.span,
            Self::TableSubquery(n) => n.span,
            Self::Aliased(n) => n.span,
            Self::Lateral(n) => n.span,
            Self::Unnest(n) => n.span,
            Self::Join(n) => n.span,
            Self::Sampled(n) => n.span,
            Self::Pivot(n) => n.span,
            Self::Nearest(n) => n.span,
            Self::TableFunction(n) => n.span,
            Self::JsonTable(n) => n.span,
            Self::PatternRecognition(n) => n.span,
        }
    }
}
