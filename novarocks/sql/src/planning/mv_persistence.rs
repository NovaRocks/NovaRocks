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
use novarocks_functions::{
    AggregateBindingSelection, FunctionResultType, PersistentAggregateContract,
};
use novarocks_type_contract::{
    AggregateStateArgumentContract, CanonicalFactWriter, FunctionArgumentType, FunctionValueType,
    ValueTypeRef,
};

use crate::analysis::{
    ExprKind, LiteralValue, QueryBody, Relation, ResolvedQuery, SortItem, TypedExpr,
};
use crate::analyze_error::AnalyzeError;
use crate::binding::SqlTableBindingId;
use crate::column_id::ColumnId;
use crate::compiler::SqlMvRelationOccurrenceId;
use crate::planner::table::{ScanSource, SqlMvReadSelection};
use crate::planning::catalog::SqlMvViewDependency;
use crate::planning::mv::validate_mv_union_all;

/// Complete SQL-owned input for CREATE-time MV persistence mapping.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SqlMvCreatePersistenceFacts {
    relation_occurrences: Vec<SqlMvPersistenceRelationOccurrenceFacts>,
    outputs: Vec<SqlMvPersistenceOutputFacts>,
    aggregates: Vec<SqlMvPersistenceAggregateFacts>,
    union_branches: Vec<SqlMvPersistenceUnionBranchFacts>,
    view_dependencies: Vec<SqlMvViewDependency>,
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

    pub fn view_dependencies(&self) -> &[SqlMvViewDependency] {
        &self.view_dependencies
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
    binding: SqlTableBindingId,
    read_selection: SqlMvReadSelection,
    field_paths: Vec<SqlMvFieldPath>,
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

    /// The original statement-local binding used by this analyzed scan.
    pub fn binding(&self) -> SqlTableBindingId {
        self.binding
    }

    pub fn read_selection(&self) -> SqlMvReadSelection {
        self.read_selection
    }

    /// Stable SQL occurrence identity. The provider authors the object identity.
    pub fn canonical_bytes(&self) -> Vec<u8> {
        let mut writer = CanonicalFactWriter::new("novarocks.sql.mv-occurrence", 1);
        writer.u32(self.occurrence_id.get());
        writer.u32(match self.read_selection {
            SqlMvReadSelection::Current => 1,
            SqlMvReadSelection::Snapshot => 2,
            SqlMvReadSelection::Branch => 3,
            SqlMvReadSelection::Tag => 4,
        });
        writer.finish()
    }

    pub fn field_paths(&self) -> &[SqlMvFieldPath] {
        &self.field_paths
    }

    /// Top-level referenced fields in source-schema order.
    pub fn referenced_fields(&self) -> &[SqlMvPersistenceSourceFieldFacts] {
        &self.referenced_fields
    }
}

/// A coordinate in the exact schema used to analyze a field access.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct SqlMvFieldCoordinate {
    ordinal: u32,
    name: String,
}

impl SqlMvFieldCoordinate {
    pub fn ordinal(&self) -> u32 {
        self.ordinal
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}

/// A provider projection request in one analyzed scan's exact schema.
/// Only the provider's field IDs and type facts carry durable identity.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct SqlMvFieldPath {
    fields: Vec<SqlMvFieldCoordinate>,
    whole_subtree: bool,
}

impl SqlMvFieldPath {
    pub fn fields(&self) -> &[SqlMvFieldCoordinate] {
        &self.fields
    }

    pub fn whole_subtree(&self) -> bool {
        self.whole_subtree
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
    value_type: FunctionValueType,
    source_fields: Vec<SqlMvPersistenceSourceFieldReference>,
    direct_source: Option<SqlMvPersistenceSourceFieldReference>,
    canonical_bytes: Vec<u8>,
}

impl SqlMvPersistenceOutputFacts {
    pub fn output_ordinal(&self) -> u32 {
        self.output_ordinal
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn data_type(&self) -> &DataType {
        self.value_type.data_type()
    }

    pub fn nullable(&self) -> bool {
        self.value_type.nullable()
    }

    pub fn value_type(&self) -> &ValueTypeRef {
        self.value_type.value_type()
    }

    pub fn canonical_bytes(&self) -> &[u8] {
        &self.canonical_bytes
    }

    pub fn direct_source(&self) -> Option<&SqlMvPersistenceSourceFieldReference> {
        self.direct_source.as_ref()
    }

    pub fn source_fields(&self) -> &[SqlMvPersistenceSourceFieldReference] {
        &self.source_fields
    }
}

/// One analyzed aggregate call in branch/output traversal order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SqlMvPersistenceAggregateFacts {
    aggregate_ordinal: u32,
    branch_ordinal: Option<u32>,
    output_ordinal: Option<u32>,
    function_identity: String,
    semantic_contract: PersistentAggregateContract,
    signature: novarocks_type_contract::FunctionArgumentTypes,
    logical_argument_count: usize,
    result_type: FunctionValueType,
    selection: AggregateBindingSelection,
    canonical_bytes: Vec<u8>,
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

    pub fn semantic_contract(&self) -> &PersistentAggregateContract {
        &self.semantic_contract
    }

    pub fn function_id(&self) -> &novarocks_type_contract::FunctionId {
        &self.semantic_contract.function
    }

    pub fn overload_id(&self) -> &novarocks_type_contract::FunctionOverloadId {
        &self.semantic_contract.overload
    }

    pub fn argument_types(&self) -> &[FunctionArgumentType] {
        &self.signature[..self.logical_argument_count]
    }

    pub fn result_type(&self) -> &FunctionValueType {
        &self.result_type
    }

    pub fn selection(&self) -> &AggregateBindingSelection {
        &self.selection
    }

    /// Exact selected binding, independent of the call's position and source.
    pub fn canonical_bytes(&self) -> &[u8] {
        &self.canonical_bytes
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

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct FieldUse {
    occurrence_id: SqlMvRelationOccurrenceId,
    path: SqlMvFieldPath,
}

type FieldUses = BTreeSet<FieldUse>;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct ColumnLineage {
    fields: FieldUses,
    passthrough: bool,
}

struct ProjectionBuilder {
    occurrences: Vec<SqlMvPersistenceRelationOccurrenceFacts>,
    source_fields_by_column_id: BTreeMap<ColumnId, ColumnLineage>,
    referenced_fields: FieldUses,
    aggregates: Vec<SqlMvPersistenceAggregateFacts>,
    // The keys are only compared within this borrowed tree and never dereferenced.
    scan_occurrences: BTreeMap<*const crate::analysis::ScanRelation, SqlMvRelationOccurrenceId>,
}

pub(super) fn project_create_persistence_facts(
    query: &ResolvedQuery,
    names: &[String],
    view_dependencies: &[SqlMvViewDependency],
) -> Result<SqlMvCreatePersistenceFacts, AnalyzeError> {
    let mut output_leaves = Vec::new();
    let is_root_union = collect_union_leaves(query, &mut output_leaves)?;
    let mut builder = ProjectionBuilder {
        occurrences: Vec::new(),
        source_fields_by_column_id: BTreeMap::new(),
        referenced_fields: BTreeSet::new(),
        aggregates: Vec::new(),
        scan_occurrences: BTreeMap::new(),
    };
    let output_lineages = register_query(query, &mut builder)?;

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
            .collect::<Result<Vec<_>, AnalyzeError>>()?
    } else {
        Vec::new()
    };

    collect_query_semantics(query, &branch_occurrences, true, &mut builder)?;
    let output_columns = query
        .output_columns
        .iter()
        .filter(|column| !column.is_internal);
    let visible_count = output_columns.clone().count();
    if names.len() != visible_count || output_lineages.len() != visible_count {
        return Err(AnalyzeError::internal(
            "MV persistence output declaration width differs from analyzed output",
        ));
    }
    let outputs = output_columns
        .zip(output_lineages)
        .zip(names)
        .enumerate()
        .map(|(output_index, ((field, lineage), name))| {
            builder
                .referenced_fields
                .extend(lineage.fields.iter().cloned());
            let mut writer = CanonicalFactWriter::new("novarocks.sql.mv-output-type", 1);
            writer.value_type(field.value_type.value_type());
            let source_fields = root_source_fields(&lineage.fields);
            let direct_source = match source_fields.as_slice() {
                [field]
                    if lineage.passthrough
                        && lineage
                            .fields
                            .iter()
                            .all(|used| used.path.fields.len() == 1) =>
                {
                    Some(field.clone())
                }
                _ => None,
            };
            Ok(SqlMvPersistenceOutputFacts {
                output_ordinal: u32_from_usize("MV output ordinal", output_index)?,
                name: name.clone(),
                value_type: field.value_type.clone(),
                source_fields,
                direct_source,
                canonical_bytes: writer.finish(),
            })
        })
        .collect::<Result<Vec<_>, AnalyzeError>>()?;

    for reference in &builder.referenced_fields {
        let occurrence = builder
            .occurrences
            .get_mut(reference.occurrence_id.get() as usize)
            .ok_or_else(|| {
                AnalyzeError::internal("MV persistence field references unknown occurrence")
            })?;
        occurrence.field_paths.push(reference.path.clone());
    }
    for occurrence in &mut builder.occurrences {
        normalize_paths(&mut occurrence.field_paths);
        let mut roots = BTreeSet::new();
        for path in &occurrence.field_paths {
            roots.insert(&path.fields[0]);
        }
        occurrence.referenced_fields = roots
            .into_iter()
            .map(|field| SqlMvPersistenceSourceFieldFacts {
                field_ordinal: field.ordinal,
                name: field.name.clone(),
            })
            .collect();
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
                        .collect::<Result<Vec<_>, AnalyzeError>>()?,
                })
            })
            .collect::<Result<Vec<_>, AnalyzeError>>()?
    } else if is_root_union {
        return Err(AnalyzeError::internal(
            "MV CREATE persistence UNION ALL has no branches",
        ));
    } else {
        Vec::new()
    };

    Ok(SqlMvCreatePersistenceFacts {
        relation_occurrences: builder.occurrences,
        outputs,
        aggregates: builder.aggregates,
        union_branches,
        view_dependencies: view_dependencies.to_vec(),
    })
}

/// Returns whether the root is a UNION ALL while always collecting its leaves.
fn collect_union_leaves<'a>(
    query: &'a ResolvedQuery,
    leaves: &mut Vec<&'a ResolvedQuery>,
) -> Result<bool, AnalyzeError> {
    match &query.body {
        QueryBody::SetOperation(set_op) => {
            validate_mv_union_all(set_op)
                .map_err(AnalyzeError::unsupported_expression_without_span)?;
            collect_union_leaves(&set_op.left, leaves)?;
            collect_union_leaves(&set_op.right, leaves)?;
            Ok(true)
        }
        QueryBody::Select(_) => {
            leaves.push(query);
            Ok(false)
        }
        QueryBody::Values(_) => Err(AnalyzeError::unsupported_expression_without_span(
            "MV persistence facts do not support VALUES",
        )),
    }
}

fn query_relation_occurrence_ids(
    query: &ResolvedQuery,
    builder: &ProjectionBuilder,
) -> Result<Vec<SqlMvRelationOccurrenceId>, AnalyzeError> {
    match &query.body {
        QueryBody::Select(select) => {
            let mut ids = match &select.from {
                Some(relation) => relation_occurrence_ids(relation, builder)?,
                None => Vec::new(),
            };
            for inner in select
                .apply_specs
                .iter()
                .map(|spec| &spec.inner)
                .chain(select.predicate_apply_specs.iter().map(|spec| &spec.inner))
            {
                ids.extend(query_relation_occurrence_ids(inner, builder)?);
            }
            Ok(ids)
        }
        QueryBody::SetOperation(set_op) => {
            let mut ids = query_relation_occurrence_ids(&set_op.left, builder)?;
            ids.extend(query_relation_occurrence_ids(&set_op.right, builder)?);
            Ok(ids)
        }
        QueryBody::Values(_) => Err(AnalyzeError::unsupported_expression_without_span(
            "MV persistence facts do not support VALUES",
        )),
    }
}

fn relation_occurrence_ids(
    relation: &Relation,
    builder: &ProjectionBuilder,
) -> Result<Vec<SqlMvRelationOccurrenceId>, AnalyzeError> {
    match relation {
        Relation::Scan(scan) => {
            let occurrence = builder
                .scan_occurrences
                .get(&std::ptr::from_ref(scan))
                .copied()
                .ok_or_else(|| AnalyzeError::internal("MV persistence scan has no occurrence"))?;
            Ok(vec![occurrence])
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
        | Relation::CTEConsume { .. } => Err(AnalyzeError::unsupported_expression_without_span(
            "MV persistence facts require base-table scans and joins",
        )),
    }
}

fn register_query(
    query: &ResolvedQuery,
    builder: &mut ProjectionBuilder,
) -> Result<Vec<ColumnLineage>, AnalyzeError> {
    let lineages = match &query.body {
        QueryBody::Select(select) => {
            if let Some(relation) = &select.from {
                register_relation(relation, builder)?;
            }
            register_applies(select, builder)?;
            register_grouping_masks(select, builder)?;
            select
                .projection
                .iter()
                .map(|item| expression_lineage(&item.expr, builder))
                .collect::<Result<Vec<_>, AnalyzeError>>()?
        }
        QueryBody::SetOperation(set_op) => {
            validate_mv_union_all(set_op)
                .map_err(AnalyzeError::unsupported_expression_without_span)?;
            let left = register_query(&set_op.left, builder)?;
            let right = register_query(&set_op.right, builder)?;
            merge_output_lineages(left, right)?
        }
        QueryBody::Values(_) => {
            return Err(AnalyzeError::unsupported_expression_without_span(
                "MV persistence facts do not support VALUES",
            ));
        }
    };
    bind_output_lineages(&query.output_columns, &lineages, builder)?;
    Ok(lineages)
}

fn register_grouping_masks(
    select: &crate::analysis::ResolvedSelect,
    builder: &mut ProjectionBuilder,
) -> Result<(), AnalyzeError> {
    let Some(repeat) = &select.repeat else {
        return Ok(());
    };
    // Analysis appends the mask references after the original grouping keys.
    // Repeat key ColumnIds are only filled by logical planning, after this
    // projection. Use the analyzer's shared key tokens to join to its typed keys.
    let (keys, masks) = select
        .group_by
        .split_at_checked(repeat.all_rollup_columns.len())
        .ok_or_else(|| AnalyzeError::internal("MV grouping keys do not match Repeat analysis"))?;
    if masks.len() != repeat.grouping_fn_ids.len() || masks.len() != repeat.grouping_fn_args.len() {
        return Err(AnalyzeError::internal(
            "MV grouping masks do not match Repeat analysis",
        ));
    }
    let key_indices = repeat
        .all_rollup_columns
        .iter()
        .zip(keys)
        .collect::<BTreeMap<_, _>>();
    for ((name, id), (argument_name, arguments)) in
        repeat.grouping_fn_ids.iter().zip(&repeat.grouping_fn_args)
    {
        if name != argument_name {
            return Err(AnalyzeError::internal(
                "MV grouping mask arguments do not match their analyzed output",
            ));
        }
        let mut fields = FieldUses::new();
        for argument in arguments {
            let key = key_indices.get(argument).ok_or_else(|| {
                AnalyzeError::internal("MV grouping mask argument is not an analyzed grouping key")
            })?;
            fields.extend(expression_source_field_set(key, builder)?);
        }
        bind_column_lineage(
            *id,
            ColumnLineage {
                fields,
                passthrough: false,
            },
            builder,
        )?;
    }
    Ok(())
}

fn register_applies(
    select: &crate::analysis::ResolvedSelect,
    builder: &mut ProjectionBuilder,
) -> Result<(), AnalyzeError> {
    for spec in &select.apply_specs {
        let lineages = register_query(&spec.inner, builder)?;
        let mut fields = lineages
            .into_iter()
            .flat_map(|lineage| lineage.fields)
            .collect::<FieldUses>();
        if let Some(probe) = &spec.mark_probe {
            let lineage = expression_lineage(probe, builder)?;
            fields.extend(lineage.fields.iter().cloned());
            if let Some(mark) = &spec.relation_mark {
                bind_column_lineage(mark.probe_column.column_id, lineage, builder)?;
            }
        }
        if let Some(mark) = &spec.relation_mark {
            for item in &mark.demand_projections {
                let lineage = expression_lineage(&item.expr, builder)?;
                bind_column_lineage(item.output_column_id, lineage, builder)?;
            }
            fields.extend(expression_source_field_set(&mark.predicate, builder)?);
        }
        bind_column_lineage(
            spec.output_column.column_id,
            ColumnLineage {
                fields,
                passthrough: false,
            },
            builder,
        )?;
    }
    for spec in &select.predicate_apply_specs {
        let mut fields = register_query(&spec.inner, builder)?
            .into_iter()
            .flat_map(|lineage| lineage.fields)
            .collect::<FieldUses>();
        if let Some(probe) = &spec.in_lhs {
            fields.extend(expression_source_field_set(probe, builder)?);
        }
        builder.referenced_fields.extend(fields.iter().cloned());
        bind_column_lineage(
            spec.output_column.column_id,
            ColumnLineage {
                fields,
                passthrough: false,
            },
            builder,
        )?;
    }
    Ok(())
}

fn register_relation(
    relation: &Relation,
    builder: &mut ProjectionBuilder,
) -> Result<(), AnalyzeError> {
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
                return Err(AnalyzeError::internal(format!(
                    "MV persistence scan field count mismatch for {}.{}.{}: fields={}, column_ids={}",
                    source.table.catalog,
                    source.table.namespace,
                    source.table.table,
                    fields.len(),
                    scan.column_ids.len()
                )));
            }
            for (field_ordinal, (column_id, field)) in
                scan.column_ids.iter().zip(fields).enumerate()
            {
                let reference = FieldUse {
                    occurrence_id,
                    path: SqlMvFieldPath {
                        fields: vec![SqlMvFieldCoordinate {
                            ordinal: u32_from_usize("MV source field ordinal", field_ordinal)?,
                            name: field.name.clone(),
                        }],
                        whole_subtree: is_container(&field.data_type),
                    },
                };
                bind_column_lineage(
                    *column_id,
                    ColumnLineage {
                        fields: BTreeSet::from([reference]),
                        passthrough: true,
                    },
                    builder,
                )?;
            }
            builder
                .scan_occurrences
                .insert(std::ptr::from_ref(scan), occurrence_id);
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
                    binding: source.binding,
                    read_selection: source.mv_read_selection(),
                    field_paths: Vec::new(),
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
        | Relation::CTEConsume { .. } => Err(AnalyzeError::unsupported_expression_without_span(
            "MV persistence facts require base-table scans and joins",
        )),
    }
}

fn merge_output_lineages(
    left: Vec<ColumnLineage>,
    right: Vec<ColumnLineage>,
) -> Result<Vec<ColumnLineage>, AnalyzeError> {
    if left.len() != right.len() {
        return Err(AnalyzeError::internal(
            "MV persistence UNION output lineage arity mismatch",
        ));
    }
    Ok(left
        .into_iter()
        .zip(right)
        .map(|(mut left, right)| {
            left.fields.extend(right.fields);
            left.passthrough &= right.passthrough;
            left
        })
        .collect())
}

fn bind_output_lineages(
    outputs: &[crate::analysis::AnalyzedOutputField],
    lineages: &[ColumnLineage],
    builder: &mut ProjectionBuilder,
) -> Result<(), AnalyzeError> {
    let visible = outputs.iter().filter(|output| !output.is_internal);
    if visible.clone().count() != lineages.len() {
        return Err(AnalyzeError::internal(
            "MV persistence analyzed output lineage arity mismatch",
        ));
    }
    for (output, lineage) in visible.zip(lineages) {
        bind_column_lineage(output.column_id, lineage.clone(), builder)?;
    }
    Ok(())
}

fn bind_column_lineage(
    column_id: ColumnId,
    lineage: ColumnLineage,
    builder: &mut ProjectionBuilder,
) -> Result<(), AnalyzeError> {
    match builder.source_fields_by_column_id.get(&column_id) {
        Some(existing) if existing != &lineage => Err(AnalyzeError::internal(format!(
            "MV persistence analysis reused column identity {column_id} with different source lineage"
        ))),
        Some(_) => Ok(()),
        None => {
            builder
                .source_fields_by_column_id
                .insert(column_id, lineage);
            Ok(())
        }
    }
}

fn column_lineage(
    column_id: ColumnId,
    builder: &ProjectionBuilder,
) -> Result<&ColumnLineage, AnalyzeError> {
    builder
        .source_fields_by_column_id
        .get(&column_id)
        .ok_or_else(|| {
            AnalyzeError::internal(format!(
                "MV persistence column {column_id} has no base occurrence lineage"
            ))
        })
}

fn collect_query_semantics(
    query: &ResolvedQuery,
    branch_occurrences: &[Vec<SqlMvRelationOccurrenceId>],
    target_projection: bool,
    builder: &mut ProjectionBuilder,
) -> Result<(), AnalyzeError> {
    match &query.body {
        QueryBody::Select(select) => {
            let query_occurrences = query_relation_occurrence_ids(query, builder)?;
            let branch_ordinal =
                branch_ordinal_for_occurrences(&query_occurrences, branch_occurrences)?;
            for (output_index, item) in select.projection.iter().enumerate() {
                if select.distinct {
                    collect_expression_references(&item.expr, builder)?;
                }
                let output_ordinal = target_projection
                    .then(|| u32_from_usize("MV aggregate output ordinal", output_index))
                    .transpose()?;
                collect_aggregates(&item.expr, branch_ordinal, output_ordinal, builder)?;
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
            for inner in select
                .apply_specs
                .iter()
                .map(|spec| &spec.inner)
                .chain(select.predicate_apply_specs.iter().map(|spec| &spec.inner))
            {
                collect_query_semantics(inner, branch_occurrences, false, builder)?;
            }
            if let Some(relation) = &select.from {
                collect_relation_semantics(relation, branch_occurrences, builder)?;
                collect_relation_expression_references(relation, branch_ordinal, builder)?;
            }
        }
        QueryBody::SetOperation(set_op) => {
            collect_query_semantics(&set_op.left, branch_occurrences, target_projection, builder)?;
            collect_query_semantics(
                &set_op.right,
                branch_occurrences,
                target_projection,
                builder,
            )?;
        }
        QueryBody::Values(_) => {
            return Err(AnalyzeError::unsupported_expression_without_span(
                "MV persistence facts do not support VALUES",
            ));
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
) -> Result<(), AnalyzeError> {
    match relation {
        Relation::Subquery { query, .. } => {
            collect_query_semantics(query, branch_occurrences, false, builder)
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
) -> Result<Option<u32>, AnalyzeError> {
    if query_occurrences.is_empty() {
        return Ok(None);
    }
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
        _ => Err(AnalyzeError::internal(
            "MV persistence query belongs to multiple UNION branches",
        )),
    }
}

fn collect_relation_expression_references(
    relation: &Relation,
    branch_ordinal: Option<u32>,
    builder: &mut ProjectionBuilder,
) -> Result<(), AnalyzeError> {
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
) -> Result<(), AnalyzeError> {
    collect_expression_references(&item.expr, builder)?;
    collect_aggregates(&item.expr, branch_ordinal, None, builder)
}

/// A projection can carry a source path without consuming the surrounding
/// struct. A computation keeps the paths it reads but cannot propagate a
/// later field coordinate back through its result.
fn expression_lineage(
    expression: &TypedExpr,
    builder: &ProjectionBuilder,
) -> Result<ColumnLineage, AnalyzeError> {
    if let Some(lineage) = path_lineage(expression, builder)? {
        return Ok(lineage);
    }
    Ok(ColumnLineage {
        fields: expression_source_field_set(expression, builder)?,
        passthrough: false,
    })
}

fn path_lineage(
    expression: &TypedExpr,
    builder: &ProjectionBuilder,
) -> Result<Option<ColumnLineage>, AnalyzeError> {
    let expression = unwrap_nested(expression);
    match &expression.kind {
        ExprKind::ColumnRef { column_id, .. } => {
            Ok(Some(column_lineage(*column_id, builder)?.clone()))
        }
        ExprKind::FunctionCall { args, binding, .. }
            if binding.function_id.as_str() == "builtin.scalar/__row_field/v1" =>
        {
            let [base, ordinal] = args.as_slice() else {
                return Err(AnalyzeError::internal(
                    "bound ROW field access has invalid arity",
                ));
            };
            let ExprKind::Literal(LiteralValue::Int(ordinal)) = &ordinal.kind else {
                return Err(AnalyzeError::internal(
                    "bound ROW field access has no positional identity",
                ));
            };
            let ordinal = usize::try_from(*ordinal)
                .map_err(|_| AnalyzeError::internal("bound ROW field ordinal is out of range"))?;
            let Some(mut lineage) = path_lineage(base, builder)? else {
                return Ok(None);
            };
            if !lineage.passthrough {
                return Ok(Some(lineage));
            }
            let DataType::Struct(fields) = base.value_type.data_type() else {
                return Err(AnalyzeError::internal(
                    "bound ROW field access has no ROW layout",
                ));
            };
            let field = fields.get(ordinal).ok_or_else(|| {
                AnalyzeError::internal("bound ROW field is absent from the original layout")
            })?;
            let coordinate = SqlMvFieldCoordinate {
                ordinal: u32_from_usize("MV nested source field ordinal", ordinal)?,
                name: field.name().clone(),
            };
            lineage.fields = lineage
                .fields
                .into_iter()
                .map(|mut use_| {
                    use_.path.fields.push(coordinate.clone());
                    use_.path.whole_subtree = is_container(field.data_type());
                    use_
                })
                .collect();
            Ok(Some(lineage))
        }
        _ => Ok(None),
    }
}

fn is_container(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Struct(_)
            | DataType::List(_)
            | DataType::LargeList(_)
            | DataType::FixedSizeList(_, _)
            | DataType::ListView(_)
            | DataType::LargeListView(_)
            | DataType::Map(_, _)
    )
}

fn expression_source_field_set(
    expression: &TypedExpr,
    builder: &ProjectionBuilder,
) -> Result<FieldUses, AnalyzeError> {
    let mut references = FieldUses::new();
    walk_expression(expression, &mut |node| match path_lineage(node, builder)? {
        Some(lineage) => {
            references.extend(lineage.fields);
            Ok(false)
        }
        None if matches!(&node.kind, ExprKind::SubqueryPlaceholder { .. }) => Err(
            AnalyzeError::internal("MV persistence has an unresolved subquery placeholder"),
        ),
        None => Ok(true),
    })?;
    Ok(references)
}

fn collect_expression_references(
    expression: &TypedExpr,
    builder: &mut ProjectionBuilder,
) -> Result<(), AnalyzeError> {
    let references = expression_source_field_set(expression, builder)?;
    builder.referenced_fields.extend(references);
    Ok(())
}

fn root_source_fields(references: &FieldUses) -> Vec<SqlMvPersistenceSourceFieldReference> {
    references
        .iter()
        .map(|reference| {
            let root = &reference.path.fields[0];
            SqlMvPersistenceSourceFieldReference {
                occurrence_id: reference.occurrence_id,
                field_ordinal: root.ordinal,
                field_name: root.name.clone(),
            }
        })
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn normalize_paths(paths: &mut Vec<SqlMvFieldPath>) {
    paths.sort_unstable();
    let mut normalized: Vec<SqlMvFieldPath> = Vec::with_capacity(paths.len());
    for path in paths.drain(..) {
        // In lexicographic path order all descendants of a consumed subtree are
        // contiguous, so the most recent retained path is sufficient.
        if normalized.last().is_some_and(|previous| {
            previous == &path
                || (previous.whole_subtree && path.fields.starts_with(&previous.fields))
        }) {
            continue;
        }
        normalized.push(path);
    }
    *paths = normalized;
}

fn collect_aggregates(
    expression: &TypedExpr,
    branch_ordinal: Option<u32>,
    output_ordinal: Option<u32>,
    builder: &mut ProjectionBuilder,
) -> Result<(), AnalyzeError> {
    walk_expression(expression, &mut |node| {
        let ExprKind::AggregateCall { name, resolved, .. } = &node.kind else {
            return Ok(true);
        };
        let binding = resolved.resolved();
        let semantic_contract =
            PersistentAggregateContract::from_binding(binding).ok_or_else(|| {
                AnalyzeError::internal("MV aggregate lacks its selected state contract")
            })?;
        let Some(selection) = binding.selected.aggregate.as_ref() else {
            return Err(AnalyzeError::internal(
                "MV aggregate has no selected intermediate state",
            ));
        };
        let FunctionResultType::Scalar(result_type) = &binding.selected.result_type else {
            return Err(AnalyzeError::internal(
                "MV aggregate has no selected scalar result",
            ));
        };
        let logical_arguments = binding
            .selected
            .argument_types
            .get(..binding.logical_argument_count)
            .ok_or_else(|| {
                AnalyzeError::internal("MV aggregate logical argument count is invalid")
            })?;
        let mut writer = CanonicalFactWriter::new("novarocks.sql.mv-aggregate-binding", 1);
        writer.text(binding.function_id.as_str());
        writer.text(binding.selected.overload.as_str());
        writer.sequence(logical_arguments.len());
        for argument in logical_arguments {
            match argument {
                FunctionArgumentType::Value(value) => {
                    writer.u32(1);
                    writer.value_type(value.value_type());
                }
                FunctionArgumentType::CandidateGroup { .. } => {
                    return Err(AnalyzeError::internal(
                        "MV aggregate selected a grouped IN argument",
                    ));
                }
                FunctionArgumentType::Lambda {
                    parameter_types,
                    result_type,
                } => {
                    writer.u32(2);
                    writer.sequence(parameter_types.len());
                    for parameter in parameter_types.iter() {
                        writer.value_type(parameter.value_type());
                    }
                    writer.value_type(result_type.value_type());
                }
            }
        }
        writer.value_type(result_type.value_type());
        writer.u32(match selection.state_argument_contract {
            AggregateStateArgumentContract::ExactSignature => 1,
            AggregateStateArgumentContract::ValueRootNullabilityIndependent => 2,
        });
        writer.value_type(selection.intermediate_type.value_type());
        writer.text(selection.state_format.as_str());
        let aggregate_ordinal = u32_from_usize("MV aggregate ordinal", builder.aggregates.len())?;
        let source_fields = root_source_fields(&expression_source_field_set(node, builder)?);
        builder.aggregates.push(SqlMvPersistenceAggregateFacts {
            aggregate_ordinal,
            branch_ordinal,
            output_ordinal,
            function_identity: name.to_ascii_lowercase(),
            semantic_contract,
            signature: binding.selected.argument_types.clone(),
            logical_argument_count: binding.logical_argument_count,
            result_type: result_type.clone(),
            selection: selection.clone(),
            canonical_bytes: writer.finish(),
            source_fields,
        });
        Ok(true)
    })
}

fn walk_expression<'a, E>(
    expression: &'a TypedExpr,
    visitor: &mut impl FnMut(&'a TypedExpr) -> Result<bool, E>,
) -> Result<(), E> {
    if !visitor(expression)? {
        return Ok(());
    }
    match &expression.kind {
        ExprKind::BooleanConnective { left, right, .. }
        | ExprKind::Let {
            initializer: left,
            body: right,
            ..
        } => {
            walk_expression(left, visitor)?;
            walk_expression(right, visitor)
        }
        ExprKind::Cast { expr, .. }
        | ExprKind::IsNull { expr, .. }
        | ExprKind::Nested(expr)
        | ExprKind::Lambda { body: expr, .. }
        | ExprKind::LambdaFunction { body: expr, .. } => walk_expression(expr, visitor),
        ExprKind::FunctionCall { args, .. } => walk_expressions(args, visitor),
        ExprKind::AggregateCall {
            args,
            order_by,
            filter,
            ..
        } => {
            if let Some(filter) = filter {
                walk_expression(filter, visitor)?;
            }
            walk_expressions(args, visitor)?;
            walk_sort_items(order_by, visitor)
        }
        ExprKind::Case {
            operand,
            when_then,
            else_expr,
            ..
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
        | ExprKind::LocalValue(_)
        | ExprKind::LambdaParamRef { .. }
        | ExprKind::Literal(_)
        | ExprKind::Constant(_)
        | ExprKind::SubqueryPlaceholder { .. } => Ok(()),
    }
}

fn walk_expressions<'a, E>(
    expressions: &'a [TypedExpr],
    visitor: &mut impl FnMut(&'a TypedExpr) -> Result<bool, E>,
) -> Result<(), E> {
    for expression in expressions {
        walk_expression(expression, visitor)?;
    }
    Ok(())
}

fn walk_sort_items<'a, E>(
    items: &'a [SortItem],
    visitor: &mut impl FnMut(&'a TypedExpr) -> Result<bool, E>,
) -> Result<(), E> {
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

fn u32_from_usize(label: &str, value: usize) -> Result<u32, AnalyzeError> {
    u32::try_from(value).map_err(|_| AnalyzeError::internal(format!("{label} exceeds u32")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{PlannerTableProvider, ResolvedAnalyzerTable};
    use crate::planner::table::{
        ScanSource, SqlScanKind, SqlScanSource, SqlTableIdentity, SqlTableVersionSelector, TableDef,
    };
    use crate::planning::mv::SqlResolvedMvRefreshInput;
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
                    column(
                        "attrs",
                        DataType::Struct(
                            vec![
                                arrow::datatypes::Field::new("id", DataType::Int64, false),
                                arrow::datatypes::Field::new("name", DataType::Utf8, true),
                                arrow::datatypes::Field::new(
                                    "tags",
                                    DataType::List(std::sync::Arc::new(
                                        arrow::datatypes::Field::new("item", DataType::Int64, true),
                                    )),
                                    true,
                                ),
                            ]
                            .into(),
                        ),
                        true,
                    ),
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
        let query = &novarocks_parser::parser::parse_query(sql).expect("parse query");
        let (resolved, _, _) =
            crate::analyzer::analyze(query, &TestCatalog, "sales").expect("analyze query");
        SqlResolvedMvRefreshInput::checked_for_test(resolved).expect("named MV output declaration")
    }

    fn facts(sql: &str) -> SqlMvCreatePersistenceFacts {
        input(sql)
            .create_persistence_facts()
            .expect("persistence facts")
    }

    fn refs(output: &SqlMvPersistenceOutputFacts) -> Vec<(u32, u32, &str)> {
        output
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
    fn projection_preserves_output_order_type_and_filter_fields() {
        let facts = facts(
            "SELECT id AS order_id, amount + 1 AS adjusted FROM orders WHERE region IS NOT NULL",
        );
        assert_eq!(facts.relation_occurrences().len(), 1);
        assert_eq!(facts.outputs()[0].name(), "order_id");
        assert_eq!(facts.outputs()[0].output_ordinal(), 0);
        assert_eq!(facts.outputs()[0].data_type(), &DataType::Int64);
        let mut expected = CanonicalFactWriter::new("novarocks.sql.mv-output-type", 1);
        expected.value_type(FunctionValueType::new(DataType::Int64, false).value_type());
        assert_eq!(facts.outputs()[0].canonical_bytes(), expected.finish());
        assert_eq!(refs(&facts.outputs()[0]), vec![(0, 0, "id")]);
        assert_eq!(facts.outputs()[1].name(), "adjusted");
        assert_eq!(refs(&facts.outputs()[1]), vec![(0, 2, "amount")]);
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
        assert_eq!(refs(&facts.outputs()[0]), vec![(0, 1, "region")]);
        assert_eq!(refs(&facts.outputs()[1]), vec![(1, 2, "amount")]);
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
        assert_eq!(facts.aggregates()[0].argument_types().len(), 1);
        assert_eq!(
            facts.aggregates()[0].function_id().as_str(),
            "builtin.aggregate/sum/v1"
        );
        assert!(!facts.aggregates()[0].canonical_bytes().is_empty());
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
            refs(&facts.outputs()[0]),
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
        assert_eq!(refs(&facts.outputs()[0]), vec![(0, 0, "id"), (1, 0, "id")]);
        assert_eq!(
            refs(&facts.outputs()[1]),
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
            refs(&facts.outputs()[1]),
            vec![(0, 2, "amount"), (1, 2, "amount"), (2, 2, "amount")]
        );
    }

    fn paths(occurrence: &SqlMvPersistenceRelationOccurrenceFacts) -> Vec<(Vec<&str>, bool)> {
        occurrence
            .field_paths()
            .iter()
            .map(|path| {
                (
                    path.fields()
                        .iter()
                        .map(SqlMvFieldCoordinate::name)
                        .collect(),
                    path.whole_subtree(),
                )
            })
            .collect()
    }

    #[test]
    fn struct_paths_keep_the_original_layout_across_passthroughs() {
        let direct = facts("SELECT attrs.id AS a FROM orders");
        let positional = facts("SELECT attrs[1] AS a FROM orders");
        assert_eq!(
            positional.relation_occurrences()[0].canonical_bytes(),
            direct.relation_occurrences()[0].canonical_bytes()
        );
        let aliased =
            facts("SELECT payload.id AS a FROM (SELECT attrs AS payload, amount FROM orders) q");
        let occurrence = &aliased.relation_occurrences()[0];
        assert_eq!(paths(occurrence), vec![(vec!["attrs", "id"], false)]);
        let fields = occurrence.field_paths()[0].fields();
        assert_eq!((fields[0].ordinal(), fields[1].ordinal()), (3, 0));
        assert_eq!(occurrence.read_selection(), SqlMvReadSelection::Current);
        assert_eq!(
            occurrence.binding(),
            crate::compiler::mv_rewrite::test_target_binding()
        );
        assert_eq!(
            occurrence.canonical_bytes(),
            direct.relation_occurrences()[0].canonical_bytes()
        );
        assert_eq!(
            aliased.outputs()[0].canonical_bytes(),
            direct.outputs()[0].canonical_bytes()
        );
    }

    #[test]
    fn collections_and_whole_structs_cover_their_complete_consumed_subtree() {
        let indexed = facts("SELECT attrs.tags[id] AS item FROM orders");
        assert_eq!(
            paths(&indexed.relation_occurrences()[0]),
            vec![(vec!["id"], false), (vec!["attrs", "tags"], true)]
        );
        let whole_struct = facts("SELECT attrs AS payload, attrs.id AS subfield FROM orders");
        assert_eq!(
            paths(&whole_struct.relation_occurrences()[0]),
            vec![(vec!["attrs"], true)]
        );
    }

    #[test]
    fn grouping_sets_and_scalar_subqueries_contribute_source_paths() {
        let grouped = facts(
            "SELECT region, sum(amount) AS total FROM orders GROUP BY GROUPING SETS ((region, id), (region))",
        );
        assert_eq!(
            paths(&grouped.relation_occurrences()[0]),
            vec![
                (vec!["id"], false),
                (vec!["region"], false),
                (vec!["amount"], false)
            ]
        );
        let scalar = facts("SELECT id, (SELECT max(amount) FROM east) AS maximum FROM orders");
        assert_eq!(scalar.relation_occurrences().len(), 2);
        assert_eq!(
            paths(&scalar.relation_occurrences()[0]),
            vec![(vec!["id"], false)]
        );
        assert_eq!(
            paths(&scalar.relation_occurrences()[1]),
            vec![(vec!["amount"], false)]
        );
        assert_eq!(scalar.aggregates()[0].output_ordinal(), None);
    }

    #[test]
    fn aggregate_binding_excludes_filter_and_retains_distinct_call_addresses() {
        let calls = facts(
            "SELECT sum(id) AS a, sum(amount) FILTER (WHERE region IS NOT NULL) AS b FROM orders WHERE amount IS NOT NULL",
        );
        assert_eq!(calls.aggregates()[0].output_ordinal(), Some(0));
        assert_eq!(calls.aggregates()[1].output_ordinal(), Some(1));
        assert_eq!(
            calls.aggregates()[0].overload_id(),
            calls.aggregates()[1].overload_id()
        );
        assert_eq!(calls.aggregates()[0].argument_types().len(), 1);
        assert_eq!(calls.aggregates()[1].argument_types().len(), 1);
        assert_eq!(
            calls.aggregates()[0].selection().state_format,
            calls.aggregates()[1].selection().state_format
        );
        let without_filter = facts("SELECT sum(amount) AS a FROM orders");
        assert_eq!(
            calls.aggregates()[1].canonical_bytes(),
            without_filter.aggregates()[0].canonical_bytes()
        );
        assert!(paths(&calls.relation_occurrences()[0]).contains(&(vec!["region"], false)));
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
        assert_eq!(refs(&facts.outputs()[0]), vec![(0, 0, "id"), (1, 0, "id")]);
    }
}
