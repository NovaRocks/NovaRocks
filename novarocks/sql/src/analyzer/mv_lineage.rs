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

//! IVM-A11 MV lineage builder.
//!
//! Given a ResolvedQuery for a single-base projection/filter MV plus the
//! base table's current Iceberg schema, produce the field-id-based
//! lineage that A11's contract persists.

use crate::analysis::{
    BinOp, ExprKind, JoinKind, JoinRelation, QueryBody, Relation, ResolvedQuery, ResolvedSelect,
    TypedExpr,
};
use crate::analyze_error::AnalyzeError;
use std::collections::{BTreeMap, BTreeSet};

fn lineage_internal(message: impl Into<String>) -> AnalyzeError {
    AnalyzeError::internal(message)
}

/// Immutable SQL facts for one resolved base-table schema.
///
/// Application materialization projects provider metadata into this value
/// before lineage analysis. The SQL analyzer deliberately neither retains nor
/// reaches back to a connector schema object.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SqlMvLineageSchema {
    pub(crate) fields: Vec<SqlMvLineageField>,
}

/// A field identity and type fact used by SQL MV lineage planning.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SqlMvLineageField {
    pub(crate) field_id: i32,
    pub(crate) name_at_create: String,
    pub(crate) type_signature: String,
    pub(crate) required: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SqlMvExpressionKind {
    Column,
    Cast,
    Func,
    Literal,
    Mixed,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SqlMvQualifiedFieldLineage {
    pub(crate) table_fqn: String,
    pub(crate) qualifier_at_create: String,
    pub(crate) field_id: i32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SqlMvExpressionLineage {
    pub(crate) kind: SqlMvExpressionKind,
    pub(crate) referenced_base_field_ids: Vec<i32>,
    pub(crate) referenced_base_fields: Vec<SqlMvQualifiedFieldLineage>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SqlMvOutputColumnLineage {
    pub(crate) expression: SqlMvExpressionLineage,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SqlMvFilterLineage {
    pub(crate) referenced_base_field_ids: Vec<i32>,
    pub(crate) referenced_base_fields: Vec<SqlMvQualifiedFieldLineage>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SqlMvJoinContractKind {
    InnerEquiJoin,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SqlMvJoinPredicateLineage {
    pub(crate) left: SqlMvQualifiedFieldLineage,
    pub(crate) right: SqlMvQualifiedFieldLineage,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SqlMvJoinContract {
    pub(crate) kind: SqlMvJoinContractKind,
    pub(crate) predicates: Vec<SqlMvJoinPredicateLineage>,
}

pub(crate) struct SqlMvLineageResult {
    pub(crate) base_fields: Vec<SqlMvLineageField>,
    pub(crate) output_columns: Vec<SqlMvOutputColumnLineage>,
    pub(crate) filter: Option<SqlMvFilterLineage>,
}

pub(crate) struct SqlMvJoinLineageResult {
    pub(crate) base_fields_by_table: BTreeMap<String, Vec<SqlMvLineageField>>,
    pub(crate) output_columns: Vec<SqlMvOutputColumnLineage>,
    pub(crate) filter: Option<SqlMvFilterLineage>,
    pub(crate) join: SqlMvJoinContract,
}

/// Build A11 lineage for a single-base projection/filter MV. Caller
/// must have already classified the shape as ProjectionFilter; this
/// function defensively asserts that the resolved query is a single
/// SELECT over a single base scan and returns an error otherwise.
pub(crate) fn build_projection_filter_lineage(
    resolved: &ResolvedQuery,
    base_schema: &SqlMvLineageSchema,
) -> Result<SqlMvLineageResult, AnalyzeError> {
    let select = match &resolved.body {
        QueryBody::Select(s) => s,
        _ => {
            return Err(lineage_internal(
                "A11 lineage builder requires a SELECT query",
            ));
        }
    };
    single_scan_or_err(select)?;

    let mut output_columns = Vec::with_capacity(select.projection.len());
    let mut referenced: std::collections::BTreeMap<i32, SqlMvLineageField> =
        std::collections::BTreeMap::new();

    for item in &select.projection {
        let mut col_refs: Vec<(Option<String>, String)> = Vec::new();
        let mut kind_hint = ExpressionKindHint::default();
        collect_column_refs(&item.expr, &mut col_refs, &mut kind_hint);

        let mut field_ids = Vec::with_capacity(col_refs.len());
        for (_qualifier, name) in &col_refs {
            let field = resolve_field(base_schema, name)?;
            field_ids.push(field.field_id);
            referenced
                .entry(field.field_id)
                .or_insert_with(|| SqlMvLineageField {
                    field_id: field.field_id,
                    name_at_create: field.name_at_create.clone(),
                    type_signature: field.type_signature.clone(),
                    required: field.required,
                });
        }
        field_ids.sort_unstable();
        field_ids.dedup();

        output_columns.push(SqlMvOutputColumnLineage {
            expression: SqlMvExpressionLineage {
                kind: kind_hint.into_kind(),
                referenced_base_field_ids: field_ids,
                referenced_base_fields: vec![],
            },
        });
    }

    let filter = if let Some(filter_expr) = &select.filter {
        let mut col_refs: Vec<(Option<String>, String)> = Vec::new();
        let mut kind_hint = ExpressionKindHint::default();
        collect_column_refs(filter_expr, &mut col_refs, &mut kind_hint);

        let mut field_ids = Vec::with_capacity(col_refs.len());
        for (_qualifier, name) in &col_refs {
            let field = resolve_field(base_schema, name)?;
            field_ids.push(field.field_id);
            referenced
                .entry(field.field_id)
                .or_insert_with(|| SqlMvLineageField {
                    field_id: field.field_id,
                    name_at_create: field.name_at_create.clone(),
                    type_signature: field.type_signature.clone(),
                    required: field.required,
                });
        }
        field_ids.sort_unstable();
        field_ids.dedup();
        Some(SqlMvFilterLineage {
            referenced_base_field_ids: field_ids,
            referenced_base_fields: vec![],
        })
    } else {
        None
    };

    let base_fields = referenced.into_values().collect();
    Ok(SqlMvLineageResult {
        base_fields,
        output_columns,
        filter,
    })
}

pub(crate) fn build_join_projection_filter_lineage(
    resolved: &ResolvedQuery,
    base_schemas: &[(&str, &str, &SqlMvLineageSchema)],
) -> Result<SqlMvJoinLineageResult, AnalyzeError> {
    let select = match &resolved.body {
        QueryBody::Select(s) => s,
        _ => {
            return Err(lineage_internal(
                "join lineage builder requires a SELECT query",
            ));
        }
    };
    let join = match select.from.as_ref() {
        Some(Relation::Join(join)) => join,
        Some(_) => {
            return Err(lineage_internal(
                "join lineage builder requires a join relation",
            ));
        }
        None => {
            return Err(lineage_internal(
                "join lineage builder requires a FROM clause",
            ));
        }
    };

    let mut collector = QualifiedLineageCollector::new(base_schemas);
    let output_columns = select
        .projection
        .iter()
        .map(|item| collector.output_lineage(&item.expr))
        .collect::<Result<Vec<_>, _>>()?;
    let filter = select
        .filter
        .as_ref()
        .map(|expr| collector.filter_lineage(expr))
        .transpose()?;
    let join_contract = collector.join_contract(join)?;

    Ok(SqlMvJoinLineageResult {
        base_fields_by_table: collector.into_base_fields_by_table(),
        output_columns,
        filter,
        join: join_contract,
    })
}

struct QualifiedLineageCollector<'a> {
    schemas: BTreeMap<String, (&'a str, &'a SqlMvLineageSchema)>,
    base_fields_by_table: BTreeMap<String, BTreeMap<i32, SqlMvLineageField>>,
}

impl<'a> QualifiedLineageCollector<'a> {
    fn new(base_schemas: &[(&'a str, &'a str, &'a SqlMvLineageSchema)]) -> Self {
        let mut schemas = BTreeMap::new();
        for (table_fqn, alias, schema) in base_schemas {
            schemas.insert(alias.to_ascii_lowercase(), (*table_fqn, *schema));
        }
        Self {
            schemas,
            base_fields_by_table: BTreeMap::new(),
        }
    }

    fn output_lineage(
        &mut self,
        expr: &TypedExpr,
    ) -> Result<SqlMvOutputColumnLineage, AnalyzeError> {
        let mut refs = Vec::new();
        let mut kind_hint = ExpressionKindHint::default();
        self.collect_qualified_refs(expr, &mut refs, &mut kind_hint)?;
        Ok(SqlMvOutputColumnLineage {
            expression: SqlMvExpressionLineage {
                kind: kind_hint.into_kind(),
                referenced_base_field_ids: Vec::new(),
                referenced_base_fields: refs,
            },
        })
    }

    fn filter_lineage(&mut self, expr: &TypedExpr) -> Result<SqlMvFilterLineage, AnalyzeError> {
        let mut refs = Vec::new();
        let mut kind_hint = ExpressionKindHint::default();
        self.collect_qualified_refs(expr, &mut refs, &mut kind_hint)?;
        Ok(SqlMvFilterLineage {
            referenced_base_field_ids: Vec::new(),
            referenced_base_fields: refs,
        })
    }

    fn collect_qualified_refs(
        &mut self,
        expr: &TypedExpr,
        out: &mut Vec<SqlMvQualifiedFieldLineage>,
        kind: &mut ExpressionKindHint,
    ) -> Result<(), AnalyzeError> {
        match &expr.kind {
            ExprKind::ColumnRef {
                qualifier, column, ..
            } => {
                kind.saw_column();
                let qualifier = qualifier.as_ref().ok_or_else(|| {
                    lineage_internal(format!("join MV column `{column}` must be qualified"))
                })?;
                out.push(self.resolve_field(qualifier, column)?);
            }
            ExprKind::Literal(_) => kind.saw_literal(),
            ExprKind::Cast { .. } => {
                kind.saw_cast();
                for child in typed_expr_children(expr) {
                    self.collect_qualified_refs(child, out, kind)?;
                }
            }
            ExprKind::Nested(_) => {
                for child in typed_expr_children(expr) {
                    self.collect_qualified_refs(child, out, kind)?;
                }
            }
            _ => {
                kind.saw_func();
                for child in typed_expr_children(expr) {
                    self.collect_qualified_refs(child, out, kind)?;
                }
            }
        }
        out.sort_by(|a, b| {
            (a.table_fqn.as_str(), a.field_id).cmp(&(b.table_fqn.as_str(), b.field_id))
        });
        out.dedup_by(|a, b| a.table_fqn == b.table_fqn && a.field_id == b.field_id);
        Ok(())
    }

    fn resolve_field(
        &mut self,
        qualifier: &str,
        column: &str,
    ) -> Result<SqlMvQualifiedFieldLineage, AnalyzeError> {
        let key = qualifier.to_ascii_lowercase();
        let (table_fqn, schema) = self.schemas.get(&key).ok_or_else(|| {
            lineage_internal(format!(
                "join MV qualifier `{qualifier}` does not match a base table alias"
            ))
        })?;
        let field = resolve_field(schema, column)?;
        self.base_fields_by_table
            .entry((*table_fqn).to_string())
            .or_default()
            .entry(field.field_id)
            .or_insert_with(|| SqlMvLineageField {
                field_id: field.field_id,
                name_at_create: field.name_at_create.clone(),
                type_signature: field.type_signature.clone(),
                required: field.required,
            });
        Ok(SqlMvQualifiedFieldLineage {
            table_fqn: (*table_fqn).to_string(),
            qualifier_at_create: qualifier.to_string(),
            field_id: field.field_id,
        })
    }

    fn join_contract(&mut self, join: &JoinRelation) -> Result<SqlMvJoinContract, AnalyzeError> {
        if join.join_type != JoinKind::Inner {
            return Err(lineage_internal(
                "incremental join MV supports only inner equi-join lineage",
            ));
        }
        let condition = join
            .condition
            .as_ref()
            .ok_or_else(|| lineage_internal("join MV requires ON condition"))?;
        let sides = join_side_qualifiers(join)?;
        let mut predicates = Vec::new();
        self.collect_join_predicates(condition, &sides, &mut predicates)?;
        if predicates.is_empty() {
            return Err(lineage_internal(
                "incremental join MV requires at least one join predicate",
            ));
        }
        Ok(SqlMvJoinContract {
            kind: SqlMvJoinContractKind::InnerEquiJoin,
            predicates,
        })
    }

    fn collect_join_predicates(
        &mut self,
        expr: &TypedExpr,
        sides: &JoinSideQualifiers,
        out: &mut Vec<SqlMvJoinPredicateLineage>,
    ) -> Result<(), AnalyzeError> {
        match &unwrap_nested_expr(expr).kind {
            ExprKind::BinaryOp {
                left,
                op: BinOp::And,
                right,
                ..
            } => {
                self.collect_join_predicates(left, sides, out)?;
                self.collect_join_predicates(right, sides, out)
            }
            ExprKind::BinaryOp {
                left,
                op: BinOp::Eq,
                right,
                ..
            } => {
                let left_ref = self.single_qualified_column(left)?;
                let right_ref = self.single_qualified_column(right)?;
                out.push(normalize_join_predicate(left_ref, right_ref, sides)?);
                Ok(())
            }
            _ => Err(lineage_internal(
                "incremental join MV supports only AND-combined equi-join predicates",
            )),
        }
    }

    fn single_qualified_column(
        &mut self,
        expr: &TypedExpr,
    ) -> Result<SqlMvQualifiedFieldLineage, AnalyzeError> {
        let ExprKind::ColumnRef {
            qualifier, column, ..
        } = &unwrap_nested_expr(expr).kind
        else {
            return Err(lineage_internal(
                "incremental join MV join key must be a qualified column reference",
            ));
        };
        let qualifier = qualifier.as_ref().ok_or_else(|| {
            lineage_internal("incremental join MV join key must be <alias>.<column>")
        })?;
        self.resolve_field(qualifier, column)
    }

    fn into_base_fields_by_table(self) -> BTreeMap<String, Vec<SqlMvLineageField>> {
        self.base_fields_by_table
            .into_iter()
            .map(|(table, fields)| (table, fields.into_values().collect()))
            .collect()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum JoinSide {
    Left,
    Right,
}

struct JoinSideQualifiers {
    left: BTreeSet<String>,
    right: BTreeSet<String>,
}

fn join_side_qualifiers(join: &JoinRelation) -> Result<JoinSideQualifiers, AnalyzeError> {
    Ok(JoinSideQualifiers {
        left: relation_qualifiers(&join.left, "left")?,
        right: relation_qualifiers(&join.right, "right")?,
    })
}

fn relation_qualifiers(
    relation: &Relation,
    side_name: &str,
) -> Result<BTreeSet<String>, AnalyzeError> {
    match relation {
        Relation::Scan(scan) => {
            let qualifier = scan.alias.as_deref().unwrap_or(&scan.table.name);
            Ok(one_qualifier(qualifier))
        }
        Relation::Subquery { alias, .. } => Ok(one_qualifier(alias)),
        _ => Err(lineage_internal(format!(
            "join MV lineage requires a single scan or subquery on the {side_name} side"
        ))),
    }
}

fn one_qualifier(qualifier: &str) -> BTreeSet<String> {
    BTreeSet::from([qualifier.to_ascii_lowercase()])
}

fn normalize_join_predicate(
    left_ref: SqlMvQualifiedFieldLineage,
    right_ref: SqlMvQualifiedFieldLineage,
    sides: &JoinSideQualifiers,
) -> Result<SqlMvJoinPredicateLineage, AnalyzeError> {
    let left_side = join_side_for_qualifier(&left_ref.qualifier_at_create, sides)?;
    let right_side = join_side_for_qualifier(&right_ref.qualifier_at_create, sides)?;
    match (left_side, right_side) {
        (JoinSide::Left, JoinSide::Right) => Ok(SqlMvJoinPredicateLineage {
            left: left_ref,
            right: right_ref,
        }),
        (JoinSide::Right, JoinSide::Left) => Ok(SqlMvJoinPredicateLineage {
            left: right_ref,
            right: left_ref,
        }),
        _ => Err(lineage_internal(
            "incremental join MV join predicate must reference one column from each join side",
        )),
    }
}

fn join_side_for_qualifier(
    qualifier: &str,
    sides: &JoinSideQualifiers,
) -> Result<JoinSide, AnalyzeError> {
    let key = qualifier.to_ascii_lowercase();
    let on_left = sides.left.contains(&key);
    let on_right = sides.right.contains(&key);
    match (on_left, on_right) {
        (true, false) => Ok(JoinSide::Left),
        (false, true) => Ok(JoinSide::Right),
        (true, true) => Err(lineage_internal(format!(
            "join MV qualifier `{qualifier}` is ambiguous across join sides"
        ))),
        (false, false) => Err(lineage_internal(format!(
            "join MV qualifier `{qualifier}` does not match either join side"
        ))),
    }
}

fn unwrap_nested_expr(mut expr: &TypedExpr) -> &TypedExpr {
    while let ExprKind::Nested(inner) = &expr.kind {
        expr = inner.as_ref();
    }
    expr
}

fn typed_expr_children(expr: &TypedExpr) -> Vec<&TypedExpr> {
    match &expr.kind {
        ExprKind::BinaryOp { left, right, .. } => vec![left.as_ref(), right.as_ref()],
        ExprKind::UnaryOp { expr, .. }
        | ExprKind::Cast { expr, .. }
        | ExprKind::IsNull { expr, .. }
        | ExprKind::IsTruthValue { expr, .. }
        | ExprKind::Nested(expr)
        | ExprKind::Lambda { body: expr, .. } => vec![expr.as_ref()],
        ExprKind::FunctionCall { args, .. }
        | ExprKind::AggregateCall { args, .. }
        | ExprKind::WindowCall { args, .. } => args.iter().collect(),
        ExprKind::LambdaFunction { body, .. } => vec![body.as_ref()],
        ExprKind::InList { expr, list, .. } => {
            let mut out = Vec::with_capacity(1 + list.len());
            out.push(expr.as_ref());
            out.extend(list.iter());
            out
        }
        ExprKind::Between {
            expr, low, high, ..
        } => vec![expr.as_ref(), low.as_ref(), high.as_ref()],
        ExprKind::Like { expr, pattern, .. } => vec![expr.as_ref(), pattern.as_ref()],
        ExprKind::Case {
            operand,
            when_then,
            else_expr,
        } => {
            let mut out = Vec::new();
            if let Some(operand) = operand {
                out.push(operand.as_ref());
            }
            for (when, then) in when_then {
                out.push(when);
                out.push(then);
            }
            if let Some(else_expr) = else_expr {
                out.push(else_expr.as_ref());
            }
            out
        }
        ExprKind::ColumnRef { .. }
        | ExprKind::LambdaParamRef { .. }
        | ExprKind::Literal(_)
        | ExprKind::SubqueryPlaceholder { .. } => Vec::new(),
    }
}

fn single_scan_or_err(select: &ResolvedSelect) -> Result<(), AnalyzeError> {
    match select.from.as_ref() {
        Some(Relation::Scan(_)) => Ok(()),
        Some(_) => Err(lineage_internal(
            "A11 lineage builder requires a single-base SCAN, not a join or subquery",
        )),
        None => Err(lineage_internal(
            "A11 lineage builder requires a FROM clause",
        )),
    }
}

fn resolve_field<'a>(
    schema: &'a SqlMvLineageSchema,
    column_name: &str,
) -> Result<&'a SqlMvLineageField, AnalyzeError> {
    schema
        .fields
        .iter()
        .find(|field| field.name_at_create.eq_ignore_ascii_case(column_name))
        .ok_or_else(|| {
            lineage_internal(format!(
                "base iceberg schema does not contain column {column_name}; cannot build A11 lineage"
            ))
        })
}

/// Walks a TypedExpr, collecting every ColumnRef as (qualifier, name).
/// Also updates a coarse ExpressionKindHint.
fn collect_column_refs(
    expr: &TypedExpr,
    out: &mut Vec<(Option<String>, String)>,
    kind: &mut ExpressionKindHint,
) {
    match &expr.kind {
        ExprKind::ColumnRef {
            qualifier, column, ..
        } => {
            out.push((qualifier.clone(), column.clone()));
            kind.saw_column();
        }
        ExprKind::Literal(_) => {
            kind.saw_literal();
        }
        ExprKind::Cast { expr, .. } => {
            // A CAST over a plain column or literal (e.g. CAST(amount AS DOUBLE))
            // is a common projection; mark as cast so the kind can be classified
            // as ExpressionKind::Cast when no other operations are present.
            kind.saw_cast();
            collect_column_refs(expr, out, kind);
        }
        ExprKind::BinaryOp { left, right, .. } => {
            kind.saw_func();
            collect_column_refs(left, out, kind);
            collect_column_refs(right, out, kind);
        }
        ExprKind::UnaryOp { expr, .. } => {
            kind.saw_func();
            collect_column_refs(expr, out, kind);
        }
        ExprKind::FunctionCall { args, .. } | ExprKind::AggregateCall { args, .. } => {
            kind.saw_func();
            for a in args {
                collect_column_refs(a, out, kind);
            }
        }
        ExprKind::IsNull { expr, .. } => {
            kind.saw_func();
            collect_column_refs(expr, out, kind);
        }
        ExprKind::InList { expr, list, .. } => {
            kind.saw_func();
            collect_column_refs(expr, out, kind);
            for e in list {
                collect_column_refs(e, out, kind);
            }
        }
        ExprKind::Between {
            expr, low, high, ..
        } => {
            kind.saw_func();
            collect_column_refs(expr, out, kind);
            collect_column_refs(low, out, kind);
            collect_column_refs(high, out, kind);
        }
        ExprKind::Like { expr, pattern, .. } => {
            kind.saw_func();
            collect_column_refs(expr, out, kind);
            collect_column_refs(pattern, out, kind);
        }
        ExprKind::Case {
            operand,
            when_then,
            else_expr,
        } => {
            kind.saw_func();
            if let Some(op) = operand {
                collect_column_refs(op, out, kind);
            }
            for (w, t) in when_then {
                collect_column_refs(w, out, kind);
                collect_column_refs(t, out, kind);
            }
            if let Some(e) = else_expr {
                collect_column_refs(e, out, kind);
            }
        }
        ExprKind::IsTruthValue { expr, .. } => {
            kind.saw_func();
            collect_column_refs(expr, out, kind);
        }
        ExprKind::Nested(inner) => {
            collect_column_refs(inner, out, kind);
        }
        // Lambda, window, subquery placeholder, lambda param — not expected
        // in A11 phase 1 projection/filter MVs. A9 shape classification
        // rejects them before reaching here.
        _ => {
            kind.saw_func();
        }
    }
}

#[derive(Default)]
struct ExpressionKindHint {
    saw_column: bool,
    saw_literal: bool,
    saw_func: bool,
    saw_cast: bool,
}

impl ExpressionKindHint {
    fn saw_column(&mut self) {
        self.saw_column = true;
    }
    fn saw_literal(&mut self) {
        self.saw_literal = true;
    }
    fn saw_func(&mut self) {
        self.saw_func = true;
    }
    fn saw_cast(&mut self) {
        self.saw_cast = true;
    }
    fn into_kind(self) -> SqlMvExpressionKind {
        match (
            self.saw_column,
            self.saw_literal,
            self.saw_func,
            self.saw_cast,
        ) {
            (true, false, false, false) => SqlMvExpressionKind::Column,
            (false, true, false, false) => SqlMvExpressionKind::Literal,
            (false, false, true, false) => SqlMvExpressionKind::Func,
            // Cast over a column or literal with no other operations → Cast
            (_, _, false, true) => SqlMvExpressionKind::Cast,
            // All other combinations → Mixed
            _ => SqlMvExpressionKind::Mixed,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::binding::{SqlTableBindingId, SqlTableBindingScopeId};
    use crate::catalog::PlannerTableProvider;
    use crate::planner::table::{
        ScanSource, SqlScanKind, SqlScanSource, SqlTableIdentity, SqlTableVersionSelector, TableDef,
    };
    use novarocks_types::schema::ColumnDef;
    use std::num::{NonZeroU32, NonZeroU64};

    fn sql_schema(fields: &[(i32, &str, &str, bool)]) -> SqlMvLineageSchema {
        SqlMvLineageSchema {
            fields: fields
                .iter()
                .map(
                    |(field_id, name_at_create, type_signature, required)| SqlMvLineageField {
                        field_id: *field_id,
                        name_at_create: (*name_at_create).to_string(),
                        type_signature: (*type_signature).to_string(),
                        required: *required,
                    },
                )
                .collect(),
        }
    }

    fn base_schema() -> SqlMvLineageSchema {
        sql_schema(&[
            (1, "id", "long", true),
            (2, "region", "string", true),
            (3, "amount", "double", false),
        ])
    }

    fn sql_scan_source(table: &str) -> ScanSource {
        ScanSource::Sql(SqlScanSource::new(
            SqlTableBindingId::new(
                SqlTableBindingScopeId::new(NonZeroU64::new(1).expect("non-zero scope")),
                NonZeroU32::new(1).expect("non-zero ordinal"),
            ),
            SqlTableIdentity {
                catalog: "test".to_string(),
                namespace: "ns".to_string(),
                table: table.to_string(),
            },
            SqlScanKind::Data {
                version: SqlTableVersionSelector::Current,
            },
        ))
    }

    struct JoinLineageFixture {
        left_schema: SqlMvLineageSchema,
        right_schema: SqlMvLineageSchema,
    }

    struct SingleLineageFixture {
        schema: SqlMvLineageSchema,
    }

    impl SingleLineageFixture {
        fn new() -> Self {
            Self {
                schema: base_schema(),
            }
        }

        fn analyze(&self, sql: &str) -> ResolvedQuery {
            let mut statements = novarocks_parser::parse(sql).expect("parse");
            let Some(novarocks_parser::ast::Statement::Query(query)) = statements.pop() else {
                panic!("expected query");
            };
            assert!(statements.is_empty(), "expected exactly one statement");
            let (resolved, _registry, _factory) =
                crate::analyzer::analyze(&query, &SingleLineageCatalog, "default")
                    .expect("analyze");
            resolved
        }
    }

    struct SingleLineageCatalog;

    impl SingleLineageCatalog {
        fn get_table(&self, _database: &str, table: &str) -> Result<TableDef, String> {
            match table {
                "fact" => Ok(TableDef {
                    name: table.to_string(),
                    columns: vec![
                        ColumnDef {
                            name: "id".to_string(),
                            data_type: arrow::datatypes::DataType::Int64,
                            nullable: false,
                            write_default: None,
                            logical_type: None,
                        },
                        ColumnDef {
                            name: "region".to_string(),
                            data_type: arrow::datatypes::DataType::Utf8,
                            nullable: false,
                            write_default: None,
                            logical_type: None,
                        },
                        ColumnDef {
                            name: "amount".to_string(),
                            data_type: arrow::datatypes::DataType::Float64,
                            nullable: true,
                            write_default: None,
                            logical_type: None,
                        },
                    ],
                    iceberg_row_lineage_metadata_columns: vec![],
                    source: sql_scan_source(table),
                }),
                _ => Err(format!("table not found: {table}")),
            }
        }
    }

    impl PlannerTableProvider for SingleLineageCatalog {
        fn resolve_table_for_analysis(
            &self,
            catalog: Option<&str>,
            database: &str,
            table: &str,
        ) -> Result<crate::catalog::ResolvedAnalyzerTable, String> {
            let planner = self.get_table(database, table)?;
            Ok(crate::catalog::ResolvedAnalyzerTable::from_planner(
                catalog, database, planner,
            ))
        }
    }

    impl JoinLineageFixture {
        fn new() -> Self {
            Self {
                left_schema: sql_schema(&[
                    (10, "id", "long", true),
                    (11, "payload", "string", false),
                ]),
                right_schema: sql_schema(&[
                    (20, "id", "long", true),
                    (21, "payload", "string", false),
                    (22, "amount", "double", false),
                ]),
            }
        }

        fn analyze(&self, sql: &str) -> ResolvedQuery {
            let mut statements = novarocks_parser::parse(sql).expect("parse");
            let Some(novarocks_parser::ast::Statement::Query(query)) = statements.pop() else {
                panic!("expected query");
            };
            assert!(statements.is_empty(), "expected exactly one statement");
            let (resolved, _registry, _factory) =
                crate::analyzer::analyze(&query, &JoinLineageCatalog, "default").expect("analyze");
            resolved
        }
    }

    struct JoinLineageCatalog;

    impl JoinLineageCatalog {
        fn get_table(&self, _database: &str, table: &str) -> Result<TableDef, String> {
            match table {
                "left_tbl" | "right_tbl" => Ok(TableDef {
                    name: table.to_string(),
                    columns: vec![
                        ColumnDef {
                            name: "id".to_string(),
                            data_type: arrow::datatypes::DataType::Int64,
                            nullable: false,
                            write_default: None,
                            logical_type: None,
                        },
                        ColumnDef {
                            name: "payload".to_string(),
                            data_type: arrow::datatypes::DataType::Utf8,
                            nullable: true,
                            write_default: None,
                            logical_type: None,
                        },
                        ColumnDef {
                            name: "amount".to_string(),
                            data_type: arrow::datatypes::DataType::Float64,
                            nullable: true,
                            write_default: None,
                            logical_type: None,
                        },
                    ],
                    iceberg_row_lineage_metadata_columns: vec![],
                    source: sql_scan_source(table),
                }),
                _ => Err(format!("table not found: {table}")),
            }
        }
    }

    impl PlannerTableProvider for JoinLineageCatalog {
        fn resolve_table_for_analysis(
            &self,
            catalog: Option<&str>,
            database: &str,
            table: &str,
        ) -> Result<crate::catalog::ResolvedAnalyzerTable, String> {
            let planner = self.get_table(database, table)?;
            Ok(crate::catalog::ResolvedAnalyzerTable::from_planner(
                catalog, database, planner,
            ))
        }
    }

    #[test]
    fn sqlx2_mv_lineage_uses_sql_owned_schema_facts() {
        let fixture = SingleLineageFixture::new();
        let resolved = fixture.analyze(
            "select region, sum(amount) as total, count(amount) as non_null_amounts, count(*) as rows \
             from ns.fact group by region",
        );

        let result = build_projection_filter_lineage(&resolved, &fixture.schema).expect("lineage");

        assert_eq!(result.output_columns.len(), 4);
        assert_eq!(
            result.output_columns[0]
                .expression
                .referenced_base_field_ids,
            vec![2]
        );
        assert_eq!(
            result.output_columns[1]
                .expression
                .referenced_base_field_ids,
            vec![3]
        );
        assert_eq!(
            result.output_columns[2]
                .expression
                .referenced_base_field_ids,
            vec![3]
        );
        assert!(
            result.output_columns[3]
                .expression
                .referenced_base_field_ids
                .is_empty()
        );
        let base_field_ids = result
            .base_fields
            .iter()
            .map(|field| field.field_id)
            .collect::<Vec<_>>();
        assert_eq!(base_field_ids, vec![2, 3]);
    }

    mod join_lineage {
        use super::*;

        fn lineage_for_on(on_expr: &str) -> Result<SqlMvJoinLineageResult, AnalyzeError> {
            let sql = format!(
                "select l.id as left_id, r.id as right_id \
                 from ns.left_tbl l join ns.right_tbl r on {on_expr} \
                 where l.id > 0"
            );
            let fixture = JoinLineageFixture::new();
            let resolved = fixture.analyze(&sql);
            build_join_projection_filter_lineage(
                &resolved,
                &[
                    ("ice.ns.left_tbl", "l", &fixture.left_schema),
                    ("ice.ns.right_tbl", "r", &fixture.right_schema),
                ],
            )
        }

        #[test]
        fn distinguishes_same_named_columns_by_alias() {
            let result = lineage_for_on("l.id = r.id").expect("join lineage");
            assert_eq!(result.output_columns.len(), 2);
            assert_eq!(
                result.output_columns[0].expression.referenced_base_fields[0].table_fqn,
                "ice.ns.left_tbl"
            );
            assert_eq!(
                result.output_columns[1].expression.referenced_base_fields[0].table_fqn,
                "ice.ns.right_tbl"
            );
            assert_eq!(result.join.predicates.len(), 1);
            assert_eq!(result.join.predicates[0].left.table_fqn, "ice.ns.left_tbl");
            assert_eq!(result.join.predicates[0].left.qualifier_at_create, "l");
            assert_eq!(result.join.predicates[0].left.field_id, 10);
            assert_eq!(
                result.join.predicates[0].right.table_fqn,
                "ice.ns.right_tbl"
            );
            assert_eq!(result.join.predicates[0].right.qualifier_at_create, "r");
            assert_eq!(result.join.predicates[0].right.field_id, 20);
            assert_eq!(
                result.filter.as_ref().unwrap().referenced_base_fields[0].table_fqn,
                "ice.ns.left_tbl"
            );
            assert_eq!(result.base_fields_by_table["ice.ns.left_tbl"].len(), 1);
            assert_eq!(result.base_fields_by_table["ice.ns.right_tbl"].len(), 1);
        }

        #[test]
        fn accepts_parenthesized_join_predicate() {
            let result = lineage_for_on("(l.id = r.id)").expect("join lineage");
            assert_eq!(result.join.predicates.len(), 1);
            assert_eq!(result.join.predicates[0].left.table_fqn, "ice.ns.left_tbl");
            assert_eq!(
                result.join.predicates[0].right.table_fqn,
                "ice.ns.right_tbl"
            );
        }

        #[test]
        fn accepts_parenthesized_join_key_operands() {
            let result = lineage_for_on("(l.id) = (r.id)").expect("join lineage");
            assert_eq!(result.join.predicates.len(), 1);
            assert_eq!(result.join.predicates[0].left.qualifier_at_create, "l");
            assert_eq!(result.join.predicates[0].right.qualifier_at_create, "r");
        }

        #[test]
        fn normalizes_reversed_join_predicate_to_left_right_order() {
            let result = lineage_for_on("r.id = l.id").expect("join lineage");
            assert_eq!(result.join.predicates.len(), 1);
            assert_eq!(result.join.predicates[0].left.table_fqn, "ice.ns.left_tbl");
            assert_eq!(result.join.predicates[0].left.qualifier_at_create, "l");
            assert_eq!(
                result.join.predicates[0].right.table_fqn,
                "ice.ns.right_tbl"
            );
            assert_eq!(result.join.predicates[0].right.qualifier_at_create, "r");
        }

        #[test]
        fn rejects_same_side_join_predicate() {
            let err = match lineage_for_on("l.id = l.id") {
                Ok(_) => panic!("same side predicate should be rejected"),
                Err(err) => err,
            };
            assert!(
                err.message().contains("one column from each join side"),
                "unexpected error: {err}"
            );
            assert_eq!(err.span(), None, "lineage has no parser span to report");
        }

        #[test]
        fn aggregate_output_records_qualified_input_columns() {
            let sql = "select l.id, sum(r.amount) as total \
                       from ns.left_tbl l join ns.right_tbl r on l.id = r.id \
                       group by l.id";
            let fixture = JoinLineageFixture::new();
            let resolved = fixture.analyze(sql);
            let result = build_join_projection_filter_lineage(
                &resolved,
                &[
                    ("ice.ns.left_tbl", "l", &fixture.left_schema),
                    ("ice.ns.right_tbl", "r", &fixture.right_schema),
                ],
            )
            .expect("join lineage");

            assert_eq!(
                result.output_columns[0].expression.referenced_base_fields,
                vec![SqlMvQualifiedFieldLineage {
                    table_fqn: "ice.ns.left_tbl".to_string(),
                    qualifier_at_create: "l".to_string(),
                    field_id: 10,
                }]
            );
            assert_eq!(
                result.output_columns[1].expression.referenced_base_fields,
                vec![SqlMvQualifiedFieldLineage {
                    table_fqn: "ice.ns.right_tbl".to_string(),
                    qualifier_at_create: "r".to_string(),
                    field_id: 22,
                }]
            );
        }
    }

    #[test]
    fn expression_kind_hint_cast_over_column_is_cast() {
        let mut h = ExpressionKindHint::default();
        h.saw_cast();
        h.saw_column();
        assert_eq!(h.into_kind(), SqlMvExpressionKind::Cast);
    }

    #[test]
    fn expression_kind_hint_cast_over_literal_is_cast() {
        let mut h = ExpressionKindHint::default();
        h.saw_cast();
        h.saw_literal();
        assert_eq!(h.into_kind(), SqlMvExpressionKind::Cast);
    }

    #[test]
    fn expression_kind_hint_cast_plus_func_is_mixed() {
        let mut h = ExpressionKindHint::default();
        h.saw_cast();
        h.saw_func();
        h.saw_column();
        assert_eq!(h.into_kind(), SqlMvExpressionKind::Mixed);
    }

    #[test]
    fn expression_kind_hint_pure_column_is_column() {
        let mut h = ExpressionKindHint::default();
        h.saw_column();
        assert_eq!(h.into_kind(), SqlMvExpressionKind::Column);
    }

    #[test]
    fn expression_kind_hint_pure_literal_is_literal() {
        let mut h = ExpressionKindHint::default();
        h.saw_literal();
        assert_eq!(h.into_kind(), SqlMvExpressionKind::Literal);
    }

    #[test]
    fn resolve_field_finds_column_case_insensitive() {
        let s = base_schema();
        let f = resolve_field(&s, "REGION").expect("find region");
        assert_eq!(f.field_id, 2);
        assert_eq!(f.type_signature, "string");
    }

    #[test]
    fn resolve_field_errors_on_missing_column() {
        let s = base_schema();
        let err = resolve_field(&s, "nope").unwrap_err();
        assert!(err.message().contains("nope"), "{err}");
        assert_eq!(err.span(), None, "lineage has no parser span to report");
    }
}
