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

//! Flat CREATE-time persistence facts projected from SQL analysis.
//!
//! These values preserve semantic order and occurrence qualification without
//! exposing or copying the analyzer tree. Provider-native stable identities
//! are intentionally absent: the application joins them from the exact
//! CREATE-time observations.

use std::collections::{BTreeMap, BTreeSet};

use arrow::datatypes::DataType;

use crate::analysis::{
    ExprKind, QueryBody, Relation, ResolvedQuery, SetOpKind, SortItem, TypedExpr,
};
use crate::column_id::ColumnId;
use crate::common::{BinOp, UnOp};
use crate::compiler::SqlMvRelationOccurrenceId;
use crate::planner::table::ScanSource;

/// Complete SQL-owned input for CREATE-time MV persistence mapping.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SqlMvCreatePersistenceFacts {
    relation_occurrences: Vec<SqlMvPersistenceRelationOccurrenceFacts>,
    outputs: Vec<SqlMvPersistenceOutputFacts>,
    aggregates: Vec<SqlMvPersistenceAggregateFacts>,
    union_branches: Vec<SqlMvPersistenceUnionBranchFacts>,
}

impl SqlMvCreatePersistenceFacts {
    pub fn relation_occurrences(&self) -> &[SqlMvPersistenceRelationOccurrenceFacts] {
        &self.relation_occurrences
    }

    pub fn outputs(&self) -> &[SqlMvPersistenceOutputFacts] {
        &self.outputs
    }

    pub fn aggregates(&self) -> &[SqlMvPersistenceAggregateFacts] {
        &self.aggregates
    }

    /// Ordered UNION ALL leaves. A non-UNION query has no branch records.
    pub fn union_branches(&self) -> &[SqlMvPersistenceUnionBranchFacts] {
        &self.union_branches
    }
}

/// One syntactic base-relation occurrence in left-to-right depth-first order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SqlMvPersistenceRelationOccurrenceFacts {
    occurrence_id: SqlMvRelationOccurrenceId,
    catalog: String,
    namespace: String,
    relation: String,
    qualifier: String,
    referenced_fields: Vec<SqlMvPersistenceSourceFieldFacts>,
}

impl SqlMvPersistenceRelationOccurrenceFacts {
    pub fn occurrence_id(&self) -> SqlMvRelationOccurrenceId {
        self.occurrence_id
    }

    pub fn catalog(&self) -> &str {
        &self.catalog
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    pub fn relation(&self) -> &str {
        &self.relation
    }

    pub fn qualifier(&self) -> &str {
        &self.qualifier
    }

    /// Referenced fields only, in source-schema order.
    pub fn referenced_fields(&self) -> &[SqlMvPersistenceSourceFieldFacts] {
        &self.referenced_fields
    }
}

/// SQL binding facts for one referenced field within one occurrence.
///
/// `field_ordinal` is an exact CREATE-time join key into the matching observed
/// schema. It is not a durable field identity and must not be persisted as one.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SqlMvPersistenceSourceFieldFacts {
    field_ordinal: u32,
    name: String,
}

impl SqlMvPersistenceSourceFieldFacts {
    pub fn field_ordinal(&self) -> u32 {
        self.field_ordinal
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}

/// One source-field use qualified by a syntactic relation occurrence.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct SqlMvPersistenceSourceFieldReference {
    occurrence_id: SqlMvRelationOccurrenceId,
    field_ordinal: u32,
    field_name: String,
}

impl SqlMvPersistenceSourceFieldReference {
    pub fn occurrence_id(&self) -> SqlMvRelationOccurrenceId {
        self.occurrence_id
    }

    pub fn field_ordinal(&self) -> u32 {
        self.field_ordinal
    }

    pub fn field_name(&self) -> &str {
        &self.field_name
    }
}

/// One visible output in SQL result order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SqlMvPersistenceOutputFacts {
    output_ordinal: u32,
    name: String,
    data_type: DataType,
    nullable: bool,
    expression: SqlMvPersistenceExpressionFacts,
}

impl SqlMvPersistenceOutputFacts {
    pub fn output_ordinal(&self) -> u32 {
        self.output_ordinal
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn data_type(&self) -> &DataType {
        &self.data_type
    }

    pub fn nullable(&self) -> bool {
        self.nullable
    }

    pub fn expression(&self) -> &SqlMvPersistenceExpressionFacts {
        &self.expression
    }
}

/// Persistence-relevant shape of an output expression.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SqlMvPersistenceExpressionFacts {
    kind: SqlMvPersistenceExpressionKind,
    function_identity: Option<String>,
    source_fields: Vec<SqlMvPersistenceSourceFieldReference>,
}

impl SqlMvPersistenceExpressionFacts {
    pub fn kind(&self) -> SqlMvPersistenceExpressionKind {
        self.kind
    }

    /// Stable SQL function/operator family spelling for Function and Mixed.
    pub fn function_identity(&self) -> Option<&str> {
        self.function_identity.as_deref()
    }

    /// Set-semantics references, sorted by occurrence then source ordinal.
    pub fn source_fields(&self) -> &[SqlMvPersistenceSourceFieldReference] {
        &self.source_fields
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SqlMvPersistenceExpressionKind {
    Field,
    Literal,
    Cast,
    Function,
    Mixed,
}

/// One analyzed aggregate call in branch/output traversal order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SqlMvPersistenceAggregateFacts {
    aggregate_ordinal: u32,
    branch_ordinal: Option<u32>,
    output_ordinal: Option<u32>,
    function_identity: String,
    source_fields: Vec<SqlMvPersistenceSourceFieldReference>,
}

impl SqlMvPersistenceAggregateFacts {
    pub fn aggregate_ordinal(&self) -> u32 {
        self.aggregate_ordinal
    }

    pub fn branch_ordinal(&self) -> Option<u32> {
        self.branch_ordinal
    }

    pub fn output_ordinal(&self) -> Option<u32> {
        self.output_ordinal
    }

    pub fn function_identity(&self) -> &str {
        &self.function_identity
    }

    pub fn source_fields(&self) -> &[SqlMvPersistenceSourceFieldReference] {
        &self.source_fields
    }
}

/// Membership of one ordered UNION ALL leaf.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SqlMvPersistenceUnionBranchFacts {
    branch_ordinal: u32,
    relation_occurrence_ids: Vec<SqlMvRelationOccurrenceId>,
    output_ordinals: Vec<u32>,
}

impl SqlMvPersistenceUnionBranchFacts {
    pub fn branch_ordinal(&self) -> u32 {
        self.branch_ordinal
    }

    pub fn relation_occurrence_ids(&self) -> &[SqlMvRelationOccurrenceId] {
        &self.relation_occurrence_ids
    }

    pub fn output_ordinals(&self) -> &[u32] {
        &self.output_ordinals
    }
}

struct ProjectionBuilder {
    occurrences: Vec<SqlMvPersistenceRelationOccurrenceFacts>,
    source_fields_by_column_id: BTreeMap<ColumnId, BTreeSet<SqlMvPersistenceSourceFieldReference>>,
    referenced_fields: BTreeSet<SqlMvPersistenceSourceFieldReference>,
    aggregates: Vec<SqlMvPersistenceAggregateFacts>,
}

pub(super) fn project_create_persistence_facts(
    query: &ResolvedQuery,
    names: &[String],
) -> Result<SqlMvCreatePersistenceFacts, String> {
    let mut output_leaves = Vec::new();
    let is_root_union = collect_union_leaves(query, &mut output_leaves)?;
    let mut builder = ProjectionBuilder {
        occurrences: Vec::new(),
        source_fields_by_column_id: BTreeMap::new(),
        referenced_fields: BTreeSet::new(),
        aggregates: Vec::new(),
    };
    register_query(query, &mut builder)?;

    // Branches are the leaves whose rows reach the target, which is the root
    // UNION and nothing else. A UNION under an aggregate is an input to one
    // aggregate, not a branch: the aggregate merges both leaves into one group
    // per key, so there is no per-branch row to tell apart and no per-branch
    // state to attribute. Recording it as a branch would demand a branch
    // discriminator column the output does not have, and would split one
    // durable aggregate into two that nothing distinguishes.
    let branch_occurrences = if is_root_union {
        output_leaves
            .iter()
            .map(|leaf| query_relation_occurrence_ids(leaf, &builder))
            .collect::<Result<Vec<_>, String>>()?
    } else {
        Vec::new()
    };

    let leaf_expressions = output_leaves
        .iter()
        .map(|leaf| project_leaf_expressions(leaf, &mut builder))
        .collect::<Result<Vec<_>, String>>()?;
    collect_query_semantics(query, &branch_occurrences, &mut builder)?;

    let output_columns = output_columns(query);
    if names.len() != output_columns.len() {
        return Err("MV persistence output declaration width differs from analyzed output".into());
    }
    if leaf_expressions
        .iter()
        .any(|expressions| expressions.len() != output_columns.len())
    {
        return Err("MV CREATE persistence output arity does not match UNION branches".to_string());
    }
    let outputs = output_columns
        .into_iter()
        .enumerate()
        .map(|(output_index, (_label, data_type, nullable))| {
            let expressions = leaf_expressions
                .iter()
                .map(|branch| branch[output_index].clone())
                .collect::<Vec<_>>();
            Ok(SqlMvPersistenceOutputFacts {
                output_ordinal: u32_from_usize("MV output ordinal", output_index)?,
                name: names[output_index].clone(),
                data_type,
                nullable,
                expression: merge_branch_expression_facts(expressions),
            })
        })
        .collect::<Result<Vec<_>, String>>()?;

    for reference in &builder.referenced_fields {
        let occurrence = builder
            .occurrences
            .iter_mut()
            .find(|occurrence| occurrence.occurrence_id == reference.occurrence_id)
            .ok_or_else(|| {
                "MV CREATE persistence field references unknown occurrence".to_string()
            })?;
        occurrence
            .referenced_fields
            .push(SqlMvPersistenceSourceFieldFacts {
                field_ordinal: reference.field_ordinal,
                name: reference.field_name.clone(),
            });
    }

    let union_branches = if !branch_occurrences.is_empty() {
        branch_occurrences
            .into_iter()
            .enumerate()
            .map(|(branch_index, relation_occurrence_ids)| {
                Ok(SqlMvPersistenceUnionBranchFacts {
                    branch_ordinal: u32_from_usize("UNION branch ordinal", branch_index)?,
                    relation_occurrence_ids,
                    output_ordinals: (0..outputs.len())
                        .map(|index| u32_from_usize("UNION output ordinal", index))
                        .collect::<Result<Vec<_>, String>>()?,
                })
            })
            .collect::<Result<Vec<_>, String>>()?
    } else if is_root_union {
        return Err("MV CREATE persistence UNION ALL has no branches".to_string());
    } else {
        Vec::new()
    };

    Ok(SqlMvCreatePersistenceFacts {
        relation_occurrences: builder.occurrences,
        outputs,
        aggregates: builder.aggregates,
        union_branches,
    })
}

/// Returns whether the root is a UNION ALL while always collecting its leaves.
fn collect_union_leaves<'a>(
    query: &'a ResolvedQuery,
    leaves: &mut Vec<&'a ResolvedQuery>,
) -> Result<bool, String> {
    match &query.body {
        QueryBody::SetOperation(set_op) => {
            if set_op.kind != SetOpKind::Union || !set_op.all {
                return Err(
                    "MV CREATE persistence facts require UNION ALL set operations".to_string(),
                );
            }
            collect_union_leaves(&set_op.left, leaves)?;
            collect_union_leaves(&set_op.right, leaves)?;
            Ok(true)
        }
        QueryBody::Select(_) => {
            leaves.push(query);
            Ok(false)
        }
        QueryBody::Values(_) => {
            Err("MV CREATE persistence facts do not support VALUES".to_string())
        }
    }
}

fn query_relation_occurrence_ids(
    query: &ResolvedQuery,
    builder: &ProjectionBuilder,
) -> Result<Vec<SqlMvRelationOccurrenceId>, String> {
    match &query.body {
        QueryBody::Select(select) => {
            let relation = select
                .from
                .as_ref()
                .ok_or_else(|| "MV CREATE persistence facts require a FROM relation".to_string())?;
            relation_occurrence_ids(relation, builder)
        }
        QueryBody::SetOperation(set_op) => {
            let mut ids = query_relation_occurrence_ids(&set_op.left, builder)?;
            ids.extend(query_relation_occurrence_ids(&set_op.right, builder)?);
            Ok(ids)
        }
        QueryBody::Values(_) => {
            Err("MV CREATE persistence facts do not support VALUES".to_string())
        }
    }
}

fn relation_occurrence_ids(
    relation: &Relation,
    builder: &ProjectionBuilder,
) -> Result<Vec<SqlMvRelationOccurrenceId>, String> {
    match relation {
        Relation::Scan(scan) => {
            let column_id = scan.column_ids.first().ok_or_else(|| {
                "MV CREATE persistence scan has no columns for occurrence binding".to_string()
            })?;
            let lineages = builder
                .source_fields_by_column_id
                .get(column_id)
                .ok_or_else(|| {
                    "MV CREATE persistence scan is missing occurrence lineage".to_string()
                })?;
            let mut occurrences = lineages
                .iter()
                .map(SqlMvPersistenceSourceFieldReference::occurrence_id)
                .collect::<Vec<_>>();
            occurrences.sort_unstable();
            occurrences.dedup();
            if occurrences.len() != 1 {
                return Err(
                    "MV CREATE persistence base scan resolved to multiple occurrences".to_string(),
                );
            }
            Ok(occurrences)
        }
        Relation::Subquery { query, .. } => query_relation_occurrence_ids(query, builder),
        Relation::Join(join) => {
            let mut ids = relation_occurrence_ids(&join.left, builder)?;
            ids.extend(relation_occurrence_ids(&join.right, builder)?);
            Ok(ids)
        }
        Relation::SystemTable { .. }
        | Relation::IcebergMetadataScan(_)
        | Relation::IcebergDeltaScan(_)
        | Relation::GenerateSeries(_)
        | Relation::Unnest(_)
        | Relation::CTEConsume { .. } => {
            Err("MV CREATE persistence facts require base-table scans and joins".to_string())
        }
    }
}

fn register_query(
    query: &ResolvedQuery,
    builder: &mut ProjectionBuilder,
) -> Result<Vec<BTreeSet<SqlMvPersistenceSourceFieldReference>>, String> {
    let lineages = match &query.body {
        QueryBody::Select(select) => {
            let relation = select
                .from
                .as_ref()
                .ok_or_else(|| "MV CREATE persistence facts require a FROM relation".to_string())?;
            register_relation(relation, builder)?;
            select
                .projection
                .iter()
                .map(|item| expression_source_field_set(&item.expr, builder))
                .collect::<Result<Vec<_>, String>>()?
        }
        QueryBody::SetOperation(set_op) => {
            if set_op.kind != SetOpKind::Union || !set_op.all {
                return Err(
                    "MV CREATE persistence facts require UNION ALL set operations".to_string(),
                );
            }
            let left = register_query(&set_op.left, builder)?;
            let right = register_query(&set_op.right, builder)?;
            merge_output_lineages(left, right)?
        }
        QueryBody::Values(_) => {
            return Err("MV CREATE persistence facts do not support VALUES".to_string());
        }
    };
    bind_output_lineages(&query.output_columns, &lineages, builder)?;
    Ok(lineages)
}

fn register_relation(relation: &Relation, builder: &mut ProjectionBuilder) -> Result<(), String> {
    match relation {
        Relation::Scan(scan) => {
            let occurrence_id = SqlMvRelationOccurrenceId::new(u32_from_usize(
                "MV relation occurrence id",
                builder.occurrences.len(),
            )?);
            let ScanSource::Sql(source) = &scan.table.source;
            let fields = scan
                .table
                .columns
                .iter()
                .chain(scan.table.iceberg_row_lineage_metadata_columns.iter())
                .collect::<Vec<_>>();
            if fields.len() != scan.column_ids.len() {
                return Err(format!(
                    "MV CREATE persistence scan field count mismatch for {}.{}.{}: fields={}, column_ids={}",
                    source.table.catalog,
                    source.table.namespace,
                    source.table.table,
                    fields.len(),
                    scan.column_ids.len()
                ));
            }
            for (field_ordinal, (column_id, field)) in
                scan.column_ids.iter().zip(fields).enumerate()
            {
                let reference = SqlMvPersistenceSourceFieldReference {
                    occurrence_id,
                    field_ordinal: u32_from_usize("MV source field ordinal", field_ordinal)?,
                    field_name: field.name.clone(),
                };
                bind_column_lineage(*column_id, BTreeSet::from([reference]), builder)?;
            }
            builder
                .occurrences
                .push(SqlMvPersistenceRelationOccurrenceFacts {
                    occurrence_id,
                    catalog: source.table.catalog.clone(),
                    namespace: source.table.namespace.clone(),
                    relation: source.table.table.clone(),
                    qualifier: scan
                        .alias
                        .clone()
                        .unwrap_or_else(|| source.table.table.clone()),
                    referenced_fields: Vec::new(),
                });
            Ok(())
        }
        Relation::Join(join) => {
            register_relation(&join.left, builder)?;
            register_relation(&join.right, builder)
        }
        Relation::Subquery {
            query,
            output_columns,
            ..
        } => {
            let lineages = register_query(query, builder)?;
            bind_output_lineages(output_columns, &lineages, builder)
        }
        Relation::SystemTable { .. }
        | Relation::IcebergMetadataScan(_)
        | Relation::IcebergDeltaScan(_)
        | Relation::GenerateSeries(_)
        | Relation::Unnest(_)
        | Relation::CTEConsume { .. } => {
            Err("MV CREATE persistence facts require base-table scans and joins".to_string())
        }
    }
}

fn merge_output_lineages(
    left: Vec<BTreeSet<SqlMvPersistenceSourceFieldReference>>,
    right: Vec<BTreeSet<SqlMvPersistenceSourceFieldReference>>,
) -> Result<Vec<BTreeSet<SqlMvPersistenceSourceFieldReference>>, String> {
    if left.len() != right.len() {
        return Err("MV CREATE persistence UNION output lineage arity mismatch".to_string());
    }
    Ok(left
        .into_iter()
        .zip(right)
        .map(|(mut left, right)| {
            left.extend(right);
            left
        })
        .collect())
}

fn bind_output_lineages(
    outputs: &[crate::analysis::AnalyzedOutputField],
    lineages: &[BTreeSet<SqlMvPersistenceSourceFieldReference>],
    builder: &mut ProjectionBuilder,
) -> Result<(), String> {
    if outputs.len() != lineages.len() {
        return Err(format!(
            "MV CREATE persistence analyzed output lineage mismatch: outputs={}, lineages={}",
            outputs.len(),
            lineages.len()
        ));
    }
    for (output, lineage) in outputs.iter().zip(lineages) {
        bind_column_lineage(output.column_id, lineage.clone(), builder)?;
    }
    Ok(())
}

fn bind_column_lineage(
    column_id: ColumnId,
    lineage: BTreeSet<SqlMvPersistenceSourceFieldReference>,
    builder: &mut ProjectionBuilder,
) -> Result<(), String> {
    match builder.source_fields_by_column_id.get(&column_id) {
        Some(existing) if existing != &lineage => Err(format!(
            "MV CREATE persistence analysis reused column identity {column_id} with different source lineage"
        )),
        Some(_) => Ok(()),
        None => {
            builder
                .source_fields_by_column_id
                .insert(column_id, lineage);
            Ok(())
        }
    }
}

fn project_leaf_expressions(
    query: &ResolvedQuery,
    builder: &mut ProjectionBuilder,
) -> Result<Vec<SqlMvPersistenceExpressionFacts>, String> {
    let QueryBody::Select(select) = &query.body else {
        return Err("MV CREATE persistence leaf must be SELECT".to_string());
    };
    let mut expressions = Vec::with_capacity(select.projection.len());
    for item in &select.projection {
        expressions.push(expression_facts(&item.expr, builder)?);
    }
    Ok(expressions)
}

fn collect_query_semantics(
    query: &ResolvedQuery,
    branch_occurrences: &[Vec<SqlMvRelationOccurrenceId>],
    builder: &mut ProjectionBuilder,
) -> Result<(), String> {
    match &query.body {
        QueryBody::Select(select) => {
            let query_occurrences = query_relation_occurrence_ids(query, builder)?;
            let branch_ordinal =
                branch_ordinal_for_occurrences(&query_occurrences, branch_occurrences)?;
            for (output_index, item) in select.projection.iter().enumerate() {
                collect_expression_references(&item.expr, builder)?;
                collect_aggregates(
                    &item.expr,
                    branch_ordinal,
                    Some(u32_from_usize("MV aggregate output ordinal", output_index)?),
                    builder,
                )?;
            }
            for expression in select
                .filter
                .iter()
                .chain(select.group_by.iter())
                .chain(select.having.iter())
            {
                collect_expression_references(expression, builder)?;
                collect_aggregates(expression, branch_ordinal, None, builder)?;
            }
            if let Some(relation) = &select.from {
                collect_relation_semantics(relation, branch_occurrences, builder)?;
                collect_relation_expression_references(relation, branch_ordinal, builder)?;
            }
        }
        QueryBody::SetOperation(set_op) => {
            collect_query_semantics(&set_op.left, branch_occurrences, builder)?;
            collect_query_semantics(&set_op.right, branch_occurrences, builder)?;
        }
        QueryBody::Values(_) => {
            return Err("MV CREATE persistence facts do not support VALUES".to_string());
        }
    }
    for item in &query.order_by {
        let query_occurrences = query_relation_occurrence_ids(query, builder)?;
        let branch_ordinal =
            branch_ordinal_for_occurrences(&query_occurrences, branch_occurrences)?;
        collect_sort_item_references(item, branch_ordinal, builder)?;
    }
    Ok(())
}

fn collect_relation_semantics(
    relation: &Relation,
    branch_occurrences: &[Vec<SqlMvRelationOccurrenceId>],
    builder: &mut ProjectionBuilder,
) -> Result<(), String> {
    match relation {
        Relation::Subquery { query, .. } => {
            collect_query_semantics(query, branch_occurrences, builder)
        }
        Relation::Join(join) => {
            collect_relation_semantics(&join.left, branch_occurrences, builder)?;
            collect_relation_semantics(&join.right, branch_occurrences, builder)
        }
        Relation::SystemTable { .. }
        | Relation::Scan(_)
        | Relation::IcebergMetadataScan(_)
        | Relation::IcebergDeltaScan(_)
        | Relation::GenerateSeries(_)
        | Relation::Unnest(_)
        | Relation::CTEConsume { .. } => Ok(()),
    }
}

fn branch_ordinal_for_occurrences(
    query_occurrences: &[SqlMvRelationOccurrenceId],
    branch_occurrences: &[Vec<SqlMvRelationOccurrenceId>],
) -> Result<Option<u32>, String> {
    let matches = branch_occurrences
        .iter()
        .enumerate()
        .filter(|(_, branch)| {
            query_occurrences
                .iter()
                .all(|occurrence| branch.contains(occurrence))
        })
        .map(|(index, _)| index)
        .collect::<Vec<_>>();
    match matches.as_slice() {
        [] => Ok(None),
        [index] => Ok(Some(u32_from_usize("UNION branch ordinal", *index)?)),
        _ => Err("MV CREATE persistence query belongs to multiple UNION branches".to_string()),
    }
}

fn collect_relation_expression_references(
    relation: &Relation,
    branch_ordinal: Option<u32>,
    builder: &mut ProjectionBuilder,
) -> Result<(), String> {
    if let Relation::Join(join) = relation {
        collect_relation_expression_references(&join.left, branch_ordinal, builder)?;
        collect_relation_expression_references(&join.right, branch_ordinal, builder)?;
        if let Some(condition) = &join.condition {
            collect_expression_references(condition, builder)?;
            collect_aggregates(condition, branch_ordinal, None, builder)?;
        }
    }
    Ok(())
}

fn collect_sort_item_references(
    item: &SortItem,
    branch_ordinal: Option<u32>,
    builder: &mut ProjectionBuilder,
) -> Result<(), String> {
    collect_expression_references(&item.expr, builder)?;
    collect_aggregates(&item.expr, branch_ordinal, None, builder)
}

fn expression_facts(
    expression: &TypedExpr,
    builder: &mut ProjectionBuilder,
) -> Result<SqlMvPersistenceExpressionFacts, String> {
    let expression = unwrap_nested(expression);
    let (kind, function_identity) = match &expression.kind {
        ExprKind::ColumnRef { .. } => (SqlMvPersistenceExpressionKind::Field, None),
        ExprKind::Literal(_) | ExprKind::Constant(_) => {
            (SqlMvPersistenceExpressionKind::Literal, None)
        }
        ExprKind::Cast { .. } => (SqlMvPersistenceExpressionKind::Cast, None),
        ExprKind::FunctionCall { name, .. }
        | ExprKind::AggregateCall { name, .. }
        | ExprKind::WindowCall { name, .. } => (
            SqlMvPersistenceExpressionKind::Function,
            Some(name.to_ascii_lowercase()),
        ),
        ExprKind::BinaryOp { op, .. } => (
            SqlMvPersistenceExpressionKind::Mixed,
            Some(binary_operator_identity(*op).to_string()),
        ),
        ExprKind::UnaryOp { op, .. } => (
            SqlMvPersistenceExpressionKind::Mixed,
            Some(unary_operator_identity(*op).to_string()),
        ),
        ExprKind::LambdaFunction { .. } | ExprKind::Lambda { .. } => (
            SqlMvPersistenceExpressionKind::Mixed,
            Some("expression/lambda".to_string()),
        ),
        ExprKind::IsNull { negated, .. } => (
            SqlMvPersistenceExpressionKind::Mixed,
            Some(
                if *negated {
                    "predicate/is-not-null"
                } else {
                    "predicate/is-null"
                }
                .to_string(),
            ),
        ),
        ExprKind::InList { negated, .. } => (
            SqlMvPersistenceExpressionKind::Mixed,
            Some(
                if *negated {
                    "predicate/not-in-list"
                } else {
                    "predicate/in-list"
                }
                .to_string(),
            ),
        ),
        ExprKind::Between { negated, .. } => (
            SqlMvPersistenceExpressionKind::Mixed,
            Some(
                if *negated {
                    "predicate/not-between"
                } else {
                    "predicate/between"
                }
                .to_string(),
            ),
        ),
        ExprKind::Like { negated, .. } => (
            SqlMvPersistenceExpressionKind::Mixed,
            Some(
                if *negated {
                    "predicate/not-like"
                } else {
                    "predicate/like"
                }
                .to_string(),
            ),
        ),
        ExprKind::Case { .. } => (
            SqlMvPersistenceExpressionKind::Mixed,
            Some("expression/case".to_string()),
        ),
        ExprKind::IsTruthValue { value, negated, .. } => (
            SqlMvPersistenceExpressionKind::Mixed,
            Some(
                match (*value, *negated) {
                    (true, false) => "predicate/is-true",
                    (true, true) => "predicate/is-not-true",
                    (false, false) => "predicate/is-false",
                    (false, true) => "predicate/is-not-false",
                }
                .to_string(),
            ),
        ),
        ExprKind::LambdaParamRef { .. } => (
            SqlMvPersistenceExpressionKind::Mixed,
            Some("expression/lambda-parameter".to_string()),
        ),
        ExprKind::SubqueryPlaceholder { .. } => {
            return Err(
                "MV CREATE persistence facts cannot contain subquery placeholders".to_string(),
            );
        }
        ExprKind::Nested(_) => unreachable!("nested expressions were unwrapped"),
    };
    let source_fields = expression_source_fields(expression, builder)?;
    Ok(SqlMvPersistenceExpressionFacts {
        kind,
        function_identity,
        source_fields,
    })
}

fn expression_source_fields(
    expression: &TypedExpr,
    builder: &mut ProjectionBuilder,
) -> Result<Vec<SqlMvPersistenceSourceFieldReference>, String> {
    let references = expression_source_field_set(expression, builder)?;
    builder.referenced_fields.extend(references.iter().cloned());
    Ok(references.into_iter().collect())
}

fn expression_source_field_set(
    expression: &TypedExpr,
    builder: &ProjectionBuilder,
) -> Result<BTreeSet<SqlMvPersistenceSourceFieldReference>, String> {
    let mut references = BTreeSet::new();
    walk_expression(expression, &mut |node| {
        if let ExprKind::ColumnRef {
            column_id, column, ..
        } = &node.kind
        {
            let bound = builder
                .source_fields_by_column_id
                .get(column_id)
                .ok_or_else(|| {
                    format!(
                        "MV CREATE persistence column `{column}` has no base occurrence lineage"
                    )
                })?;
            references.extend(bound.iter().cloned());
        }
        Ok(())
    })?;
    Ok(references)
}

fn collect_expression_references(
    expression: &TypedExpr,
    builder: &mut ProjectionBuilder,
) -> Result<(), String> {
    expression_source_fields(expression, builder).map(|_| ())
}

fn collect_aggregates(
    expression: &TypedExpr,
    branch_ordinal: Option<u32>,
    output_ordinal: Option<u32>,
    builder: &mut ProjectionBuilder,
) -> Result<(), String> {
    let mut calls = Vec::new();
    walk_expression(expression, &mut |node| {
        if let ExprKind::AggregateCall { name, .. } = &node.kind {
            calls.push((name.to_ascii_lowercase(), node));
        }
        Ok(())
    })?;
    for (function_identity, call) in calls {
        let aggregate_ordinal = u32_from_usize("MV aggregate ordinal", builder.aggregates.len())?;
        let source_fields = expression_source_fields(call, builder)?;
        builder.aggregates.push(SqlMvPersistenceAggregateFacts {
            aggregate_ordinal,
            branch_ordinal,
            output_ordinal,
            function_identity,
            source_fields,
        });
    }
    Ok(())
}

fn walk_expression<'a>(
    expression: &'a TypedExpr,
    visitor: &mut impl FnMut(&'a TypedExpr) -> Result<(), String>,
) -> Result<(), String> {
    visitor(expression)?;
    match &expression.kind {
        ExprKind::BinaryOp { left, right, .. } => {
            walk_expression(left, visitor)?;
            walk_expression(right, visitor)
        }
        ExprKind::UnaryOp { expr, .. }
        | ExprKind::Cast { expr, .. }
        | ExprKind::IsNull { expr, .. }
        | ExprKind::IsTruthValue { expr, .. }
        | ExprKind::Nested(expr)
        | ExprKind::Lambda { body: expr, .. }
        | ExprKind::LambdaFunction { body: expr, .. } => walk_expression(expr, visitor),
        ExprKind::FunctionCall { args, .. } => walk_expressions(args, visitor),
        ExprKind::AggregateCall { args, order_by, .. } => {
            walk_expressions(args, visitor)?;
            walk_sort_items(order_by, visitor)
        }
        ExprKind::InList { expr, list, .. } => {
            walk_expression(expr, visitor)?;
            walk_expressions(list, visitor)
        }
        ExprKind::Between {
            expr, low, high, ..
        } => {
            walk_expression(expr, visitor)?;
            walk_expression(low, visitor)?;
            walk_expression(high, visitor)
        }
        ExprKind::Like { expr, pattern, .. } => {
            walk_expression(expr, visitor)?;
            walk_expression(pattern, visitor)
        }
        ExprKind::Case {
            operand,
            when_then,
            else_expr,
        } => {
            if let Some(operand) = operand {
                walk_expression(operand, visitor)?;
            }
            for (when, then) in when_then {
                walk_expression(when, visitor)?;
                walk_expression(then, visitor)?;
            }
            if let Some(else_expr) = else_expr {
                walk_expression(else_expr, visitor)?;
            }
            Ok(())
        }
        ExprKind::WindowCall {
            args,
            function_order_by,
            partition_by,
            order_by,
            ..
        } => {
            walk_expressions(args, visitor)?;
            walk_sort_items(function_order_by, visitor)?;
            walk_expressions(partition_by, visitor)?;
            walk_sort_items(order_by, visitor)
        }
        ExprKind::ColumnRef { .. }
        | ExprKind::LambdaParamRef { .. }
        | ExprKind::Literal(_)
        | ExprKind::Constant(_)
        | ExprKind::SubqueryPlaceholder { .. } => Ok(()),
    }
}

fn walk_expressions<'a>(
    expressions: &'a [TypedExpr],
    visitor: &mut impl FnMut(&'a TypedExpr) -> Result<(), String>,
) -> Result<(), String> {
    for expression in expressions {
        walk_expression(expression, visitor)?;
    }
    Ok(())
}

fn walk_sort_items<'a>(
    items: &'a [SortItem],
    visitor: &mut impl FnMut(&'a TypedExpr) -> Result<(), String>,
) -> Result<(), String> {
    for item in items {
        walk_expression(&item.expr, visitor)?;
    }
    Ok(())
}

fn unwrap_nested(mut expression: &TypedExpr) -> &TypedExpr {
    while let ExprKind::Nested(inner) = &expression.kind {
        expression = inner;
    }
    expression
}

fn output_columns(query: &ResolvedQuery) -> Vec<(String, DataType, bool)> {
    if query.output_columns.is_empty() {
        match &query.body {
            QueryBody::Select(select) => select
                .projection
                .iter()
                .map(|item| {
                    (
                        item.output_name.clone(),
                        item.expr.value_type.data_type().clone(),
                        item.expr.value_type.nullable(),
                    )
                })
                .collect(),
            QueryBody::SetOperation(_) | QueryBody::Values(_) => Vec::new(),
        }
    } else {
        query
            .output_columns
            .iter()
            .map(|column| {
                (
                    column.name.clone(),
                    column.value_type.data_type().clone(),
                    column.value_type.nullable(),
                )
            })
            .collect()
    }
}

fn merge_branch_expression_facts(
    expressions: Vec<SqlMvPersistenceExpressionFacts>,
) -> SqlMvPersistenceExpressionFacts {
    let first = expressions
        .first()
        .expect("analyzed MV output has at least one branch");
    let same_shape = expressions.iter().all(|expression| {
        expression.kind == first.kind && expression.function_identity == first.function_identity
    });
    let first_kind = first.kind;
    let first_function_identity = first.function_identity.clone();
    let mut source_fields = BTreeSet::new();
    for expression in expressions {
        source_fields.extend(expression.source_fields);
    }
    SqlMvPersistenceExpressionFacts {
        kind: if same_shape {
            first_kind
        } else {
            SqlMvPersistenceExpressionKind::Mixed
        },
        function_identity: if same_shape {
            first_function_identity
        } else {
            Some("set-operation/union-all".to_string())
        },
        source_fields: source_fields.into_iter().collect(),
    }
}

fn binary_operator_identity(operator: BinOp) -> &'static str {
    match operator {
        BinOp::Add => "operator/add",
        BinOp::Sub => "operator/subtract",
        BinOp::Mul => "operator/multiply",
        BinOp::Div => "operator/divide",
        BinOp::Mod => "operator/modulo",
        BinOp::Eq => "operator/equal",
        BinOp::Ne => "operator/not-equal",
        BinOp::Lt => "operator/less-than",
        BinOp::Le => "operator/less-than-or-equal",
        BinOp::Gt => "operator/greater-than",
        BinOp::Ge => "operator/greater-than-or-equal",
        BinOp::EqForNull => "operator/null-safe-equal",
        BinOp::And => "operator/and",
        BinOp::Or => "operator/or",
    }
}

fn unary_operator_identity(operator: UnOp) -> &'static str {
    match operator {
        UnOp::Not => "operator/not",
        UnOp::Negate => "operator/negate",
        UnOp::BitwiseNot => "operator/bitwise-not",
    }
}

fn u32_from_usize(label: &str, value: usize) -> Result<u32, String> {
    u32::try_from(value).map_err(|_| format!("{label} exceeds u32"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{PlannerTableProvider, ResolvedAnalyzerTable};
    use crate::planner::table::{
        ScanSource, SqlScanKind, SqlScanSource, SqlTableIdentity, SqlTableVersionSelector, TableDef,
    };
    use crate::planning::mv::SqlResolvedMvRefreshInput;
    use novarocks_parser::ast;
    use novarocks_types::schema::ColumnDef;

    struct TestCatalog;

    impl PlannerTableProvider for TestCatalog {
        fn resolve_table_for_analysis(
            &self,
            catalog: Option<&str>,
            database: &str,
            table: &str,
        ) -> Result<ResolvedAnalyzerTable, String> {
            let planner = TableDef {
                name: table.to_string(),
                columns: vec![
                    column("id", DataType::Int64, false),
                    column("region", DataType::Utf8, true),
                    column("amount", DataType::Int64, true),
                ],
                iceberg_row_lineage_metadata_columns: Vec::new(),
                source: ScanSource::Sql(SqlScanSource::new(
                    crate::compiler::mv_rewrite::test_target_binding(),
                    SqlTableIdentity {
                        catalog: catalog.unwrap_or("ice").to_string(),
                        namespace: database.to_string(),
                        table: table.to_string(),
                    },
                    SqlScanKind::Data {
                        version: SqlTableVersionSelector::Current,
                    },
                )),
            };
            Ok(ResolvedAnalyzerTable::from_planner(
                catalog, database, planner,
            ))
        }
    }

    fn column(name: &str, data_type: DataType, nullable: bool) -> ColumnDef {
        ColumnDef {
            name: name.to_string(),
            data_type,
            nullable,
            write_default: None,
            logical_type: None,
        }
    }

    fn input(sql: &str) -> SqlResolvedMvRefreshInput {
        let statements = novarocks_parser::parse(sql).expect("parse query");
        let [ast::Statement::Query(query)] = statements.as_slice() else {
            panic!("expected query");
        };
        let (resolved, _, _) =
            crate::analyzer::analyze(query, &TestCatalog, "sales").expect("analyze query");
        SqlResolvedMvRefreshInput::from_analysis(resolved).expect("named MV output declaration")
    }

    fn facts(sql: &str) -> SqlMvCreatePersistenceFacts {
        input(sql)
            .create_persistence_facts()
            .expect("persistence facts")
    }

    fn refs(expression: &SqlMvPersistenceExpressionFacts) -> Vec<(u32, u32, &str)> {
        expression
            .source_fields()
            .iter()
            .map(|field| {
                (
                    field.occurrence_id().get(),
                    field.field_ordinal(),
                    field.field_name(),
                )
            })
            .collect()
    }

    #[test]
    fn projection_preserves_output_order_shapes_and_filter_fields() {
        let facts = facts(
            "SELECT id AS order_id, amount + 1 AS adjusted FROM orders WHERE region IS NOT NULL",
        );

        assert_eq!(facts.relation_occurrences().len(), 1);
        assert_eq!(facts.outputs()[0].name(), "order_id");
        assert_eq!(
            facts.outputs()[0].expression().kind(),
            SqlMvPersistenceExpressionKind::Field
        );
        assert_eq!(refs(facts.outputs()[0].expression()), vec![(0, 0, "id")]);
        assert_eq!(facts.outputs()[1].name(), "adjusted");
        assert_eq!(
            facts.outputs()[1].expression().kind(),
            SqlMvPersistenceExpressionKind::Mixed
        );
        assert_eq!(
            facts.outputs()[1].expression().function_identity(),
            Some("operator/add")
        );
        assert_eq!(
            refs(facts.outputs()[1].expression()),
            vec![(0, 2, "amount")]
        );
        assert_eq!(
            facts.relation_occurrences()[0]
                .referenced_fields()
                .iter()
                .map(SqlMvPersistenceSourceFieldFacts::name)
                .collect::<Vec<_>>(),
            vec!["id", "region", "amount"]
        );
    }

    #[test]
    fn join_preserves_both_occurrences_and_qualifies_output_and_predicate_fields() {
        let facts = facts("SELECT l.region, r.amount FROM orders l JOIN orders r ON l.id = r.id");

        assert_eq!(facts.relation_occurrences().len(), 2);
        assert_eq!(facts.relation_occurrences()[0].occurrence_id().get(), 0);
        assert_eq!(facts.relation_occurrences()[0].qualifier(), "l");
        assert_eq!(facts.relation_occurrences()[1].occurrence_id().get(), 1);
        assert_eq!(facts.relation_occurrences()[1].qualifier(), "r");
        assert_eq!(
            refs(facts.outputs()[0].expression()),
            vec![(0, 1, "region")]
        );
        assert_eq!(
            refs(facts.outputs()[1].expression()),
            vec![(1, 2, "amount")]
        );
        assert_eq!(
            facts.relation_occurrences()[0]
                .referenced_fields()
                .iter()
                .map(SqlMvPersistenceSourceFieldFacts::name)
                .collect::<Vec<_>>(),
            vec!["id", "region"]
        );
        assert_eq!(
            facts.relation_occurrences()[1]
                .referenced_fields()
                .iter()
                .map(SqlMvPersistenceSourceFieldFacts::name)
                .collect::<Vec<_>>(),
            vec!["id", "amount"]
        );
    }

    #[test]
    fn aggregate_calls_keep_output_order_function_and_source_occurrence() {
        let facts = facts(
            "SELECT region, sum(amount) AS total, count(*) AS rows FROM orders GROUP BY region",
        );

        assert_eq!(facts.aggregates().len(), 2);
        assert_eq!(facts.aggregates()[0].aggregate_ordinal(), 0);
        assert_eq!(facts.aggregates()[0].output_ordinal(), Some(1));
        assert_eq!(facts.aggregates()[0].function_identity(), "sum");
        assert_eq!(
            facts.aggregates()[0]
                .source_fields()
                .iter()
                .map(|field| (field.occurrence_id().get(), field.field_name()))
                .collect::<Vec<_>>(),
            vec![(0, "amount")]
        );
        assert_eq!(facts.aggregates()[1].output_ordinal(), Some(2));
        assert_eq!(facts.aggregates()[1].function_identity(), "count");
        assert!(facts.aggregates()[1].source_fields().is_empty());
        assert_eq!(
            facts.outputs()[1].expression().function_identity(),
            Some("sum")
        );
    }

    #[test]
    fn union_all_preserves_leaf_order_membership_and_combines_output_references() {
        let facts = facts(
            "SELECT id, amount FROM east UNION ALL SELECT id, amount FROM west UNION ALL SELECT id, amount FROM central",
        );

        assert_eq!(facts.relation_occurrences().len(), 3);
        assert_eq!(
            facts
                .relation_occurrences()
                .iter()
                .map(SqlMvPersistenceRelationOccurrenceFacts::relation)
                .collect::<Vec<_>>(),
            vec!["east", "west", "central"]
        );
        assert_eq!(facts.union_branches().len(), 3);
        assert_eq!(
            facts.union_branches()[0].relation_occurrence_ids(),
            &[SqlMvRelationOccurrenceId::new(0)]
        );
        assert_eq!(
            facts.union_branches()[1].relation_occurrence_ids(),
            &[SqlMvRelationOccurrenceId::new(1)]
        );
        assert_eq!(
            facts.union_branches()[2].relation_occurrence_ids(),
            &[SqlMvRelationOccurrenceId::new(2)]
        );
        assert_eq!(facts.union_branches()[2].output_ordinals(), &[0, 1]);
        assert_eq!(
            refs(facts.outputs()[0].expression()),
            vec![(0, 0, "id"), (1, 0, "id"), (2, 0, "id")]
        );
    }

    #[test]
    fn union_aggregate_calls_retain_their_ordered_branch_membership() {
        let facts = facts(
            "SELECT region, sum(amount) AS total FROM east GROUP BY region UNION ALL SELECT region, sum(amount) AS total FROM west GROUP BY region",
        );

        assert_eq!(facts.aggregates().len(), 2);
        assert_eq!(facts.aggregates()[0].aggregate_ordinal(), 0);
        assert_eq!(facts.aggregates()[0].branch_ordinal(), Some(0));
        assert_eq!(facts.aggregates()[0].output_ordinal(), Some(1));
        assert_eq!(facts.aggregates()[1].aggregate_ordinal(), 1);
        assert_eq!(facts.aggregates()[1].branch_ordinal(), Some(1));
        assert_eq!(facts.aggregates()[1].output_ordinal(), Some(1));
        assert_eq!(
            facts.aggregates()[0].source_fields()[0]
                .occurrence_id()
                .get(),
            0
        );
        assert_eq!(
            facts.aggregates()[1].source_fields()[0]
                .occurrence_id()
                .get(),
            1
        );
    }

    #[test]
    fn aggregate_over_union_fan_in_resolves_derived_columns_to_leaf_occurrences() {
        let facts = facts(
            "SELECT k, sum(v) AS total FROM (SELECT id AS k, amount AS v FROM east UNION ALL SELECT id AS k, amount AS v FROM west) u GROUP BY k",
        );

        assert_eq!(facts.relation_occurrences().len(), 2);
        // A UNION under an aggregate is an input to one aggregate, not a
        // branch. The aggregate merges both leaves into one group per key, so
        // there is no per-branch row to tell apart and nothing for a branch
        // discriminator to hold. Both leaves are still separate occurrences,
        // which is what the lineage below is resolved against.
        assert!(facts.union_branches().is_empty());
        assert_eq!(
            refs(facts.outputs()[0].expression()),
            vec![(0, 0, "id"), (1, 0, "id")]
        );
        assert_eq!(
            refs(facts.outputs()[1].expression()),
            vec![(0, 2, "amount"), (1, 2, "amount")]
        );
        assert_eq!(facts.aggregates().len(), 1);
        assert_eq!(facts.aggregates()[0].branch_ordinal(), None);
        assert_eq!(facts.aggregates()[0].output_ordinal(), Some(1));
        assert_eq!(
            facts.aggregates()[0]
                .source_fields()
                .iter()
                .map(|field| (field.occurrence_id().get(), field.field_name()))
                .collect::<Vec<_>>(),
            vec![(0, "amount"), (1, "amount")]
        );
    }

    #[test]
    fn nested_union_fan_in_keeps_left_to_right_leaf_and_lineage_order() {
        let facts = facts(
            "SELECT k, sum(v) AS total FROM (SELECT id AS k, amount AS v FROM east UNION ALL SELECT id AS k, amount AS v FROM west UNION ALL SELECT id AS k, amount AS v FROM central) u GROUP BY k",
        );

        assert_eq!(
            facts
                .relation_occurrences()
                .iter()
                .map(SqlMvPersistenceRelationOccurrenceFacts::relation)
                .collect::<Vec<_>>(),
            vec!["east", "west", "central"]
        );
        // Three leaves feeding one aggregate are three occurrences and no
        // branches; their left-to-right order is carried by the occurrences
        // and by the output lineage below.
        assert!(facts.union_branches().is_empty());
        assert_eq!(
            refs(facts.outputs()[1].expression()),
            vec![(0, 2, "amount"), (1, 2, "amount"), (2, 2, "amount")]
        );
    }

    /// The refresh contract names each base relation by the occurrence id the
    /// CREATE documents mint, and it can only do that because both are the
    /// same walk over the same query. Neither side derives its order from the
    /// other, so nothing but this test would notice if one of them changed.
    ///
    /// A fact recorded against occurrence 1 at CREATE and read at occurrence 1
    /// during refresh has to be the same relation, or the refresh reads one
    /// table's pin against another table's rows.
    #[test]
    fn persistence_and_refresh_agree_on_relation_occurrences() {
        for sql in [
            "SELECT id, amount FROM orders",
            "SELECT l.region, r.amount FROM orders l JOIN east r ON l.id = r.id",
            "SELECT region, sum(amount) AS total FROM orders GROUP BY region",
            "SELECT id, amount FROM east UNION ALL SELECT id, amount FROM west UNION ALL SELECT \
             id, amount FROM central",
            "SELECT k, sum(v) AS total FROM (SELECT id AS k, amount AS v FROM east UNION ALL \
             SELECT id AS k, amount AS v FROM west) u GROUP BY k",
            "SELECT region, sum(amount) AS total FROM orders GROUP BY region UNION ALL SELECT \
             region, sum(amount) AS total FROM west GROUP BY region",
        ] {
            let input = input(sql);
            let documents = input.create_persistence_facts().expect("persistence facts");
            let refresh = input
                .refresh_contract(&crate::compiler::SqlCompileControl::unbounded())
                .expect("refresh contract");

            let documented = documents
                .relation_occurrences()
                .iter()
                .map(|occurrence| {
                    (
                        occurrence.occurrence_id(),
                        format!(
                            "{}.{}.{}",
                            occurrence.catalog(),
                            occurrence.namespace(),
                            occurrence.relation()
                        ),
                    )
                })
                .collect::<Vec<_>>();
            let planned = refresh
                .base_refs
                .iter()
                .map(|base| (base.occurrence_id, base.table.fqn()))
                .collect::<Vec<_>>();

            assert_eq!(documented, planned, "{sql}");
        }
    }

    #[test]
    fn repeated_unaliased_union_occurrences_are_never_coalesced_by_fqn() {
        let facts = facts("SELECT id FROM orders UNION ALL SELECT id FROM orders");

        assert_eq!(facts.relation_occurrences().len(), 2);
        let first = &facts.relation_occurrences()[0];
        let second = &facts.relation_occurrences()[1];
        assert_eq!(first.catalog(), second.catalog());
        assert_eq!(first.namespace(), second.namespace());
        assert_eq!(first.relation(), second.relation());
        assert_eq!(first.qualifier(), "orders");
        assert_eq!(second.qualifier(), "orders");
        assert_ne!(first.occurrence_id().get(), second.occurrence_id().get());
        assert_eq!(
            refs(facts.outputs()[0].expression()),
            vec![(0, 0, "id"), (1, 0, "id")]
        );
    }
}
