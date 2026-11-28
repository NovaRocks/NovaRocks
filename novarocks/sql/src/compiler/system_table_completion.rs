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

//! Collect and install metadata snapshots on their original optimized leaves.
use super::SqlCompileError;
use super::completion::{CompileNeedId, SystemTableSnapshotFact, SystemTableSnapshotNeed};
use super::completion_predicate::{
    ProviderPredicateColumn, lower_atom_with_literal, lower_literal,
};
use crate::analysis::scalar_effects::ScalarEffectQuery;
use crate::analysis::{BinOp, ExprKind, TypedExpr};
use crate::column_id::ColumnId;
use crate::common::binary_operator::typed_comparison;
use crate::planner::physical::{PhysicalPlanKind, PhysicalPlanNode};
use crate::planner::system_relation::PhysicalSystemTableState;
use crate::system_table::{
    SystemTableKeyConstraint, SystemTableKeyValues, SystemTableOccurrenceId,
};
use novarocks_functions::native_like::{self, LikeError, LikeObservation};
use novarocks_spi::connector::read_stack::{
    ConnectorValue, ConnectorValueType, MAX_CONNECTOR_VALUE_BYTES,
};
use novarocks_type_contract::{
    CharacterKind, CompileCheckpoints, CompilePhase, FunctionValueType, PureCompileControl,
    ValueKind, trino_can_coerce_scalar,
};
use std::collections::BTreeMap;

#[derive(Clone)]
struct Predicate {
    expression: TypedExpr,
    // Original predicate IDs -> IDs exposed by the current child.
    mapping: Option<BTreeMap<ColumnId, ProjectInput>>,
}

#[derive(Clone)]
enum ProjectInput {
    Column(ColumnId),
    Varchar {
        value: ConnectorValue,
        value_type: FunctionValueType,
    },
}

fn unnest(mut expression: &TypedExpr) -> &TypedExpr {
    while let ExprKind::Nested(inner) = &expression.kind {
        expression = inner;
    }
    expression
}
fn is_varchar(value_type: &FunctionValueType) -> bool {
    match value_type.value_type().kind() {
        ValueKind::String => true,
        ValueKind::BoundedCharacter(character) => character.kind() == CharacterKind::Varchar,
        _ => false,
    }
}

fn varchar_widening_source(expression: &TypedExpr) -> Option<&TypedExpr> {
    let mut input = unnest(expression);
    if !is_varchar(&input.value_type) {
        return None;
    }
    while let ExprKind::Cast { expr, target, .. } = &input.kind {
        if target != input.value_type.data_type()
            || input.value_type.nullable() != expr.value_type.nullable()
            || !is_varchar(&expr.value_type)
            || !trino_can_coerce_scalar(
                expr.value_type.value_type().kind(),
                input.value_type.value_type().kind(),
            )
        {
            return None;
        }
        input = unnest(expr);
    }
    Some(input)
}

fn lower_varchar_literal(expression: &TypedExpr) -> Option<ConnectorValue> {
    let source = varchar_widening_source(expression)?;
    if let ExprKind::Constant(constant) = &source.kind
        && !source.value_type.nullable()
        && constant.value_type() == &source.value_type
    {
        let text = constant.try_utf8().ok().flatten()?;
        return (text.len() <= MAX_CONNECTOR_VALUE_BYTES)
            .then(|| ConnectorValue::Varchar(std::sync::Arc::from(text)));
    }
    lower_literal(expression, ConnectorValueType::Varchar)
}

fn like_operands(expression: &TypedExpr) -> Option<(&TypedExpr, &TypedExpr, Option<&TypedExpr>)> {
    match &expression.kind {
        ExprKind::Like {
            expr,
            pattern,
            negated: false,
        } => Some((expr, pattern, None)),
        ExprKind::FunctionCall { binding, args, .. }
            if binding.function_id.as_str() == native_like::ESCAPE_FUNCTION_ID =>
        {
            let [input, pattern, escape] = &args[..] else {
                return None;
            };
            Some((input, pattern, Some(escape)))
        }
        _ => None,
    }
}

fn exact_like_key(
    input: &TypedExpr,
    pattern: &TypedExpr,
    escape: Option<&TypedExpr>,
    resolve_column: impl Fn(&TypedExpr) -> Option<ProviderPredicateColumn>,
    resolve_literal: impl Fn(&TypedExpr, ConnectorValueType) -> Option<ConnectorValue>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Option<(usize, String)>, SqlCompileError> {
    let Some(column) = resolve_column(input) else {
        return Ok(None);
    };
    let Some(ConnectorValue::Varchar(pattern)) =
        resolve_literal(pattern, ConnectorValueType::Varchar)
    else {
        return Ok(None);
    };
    let escape = match escape {
        Some(escape) => match resolve_literal(escape, ConnectorValueType::Varchar) {
            Some(ConnectorValue::Varchar(value)) => Some(value),
            _ => return Ok(None),
        },
        None => None,
    };
    Ok(exact_like_literal(&pattern, escape.as_deref(), work)?
        .map(|literal| (column.ordinal as usize, literal)))
}

fn exact_like_literal(
    pattern: &str,
    escape: Option<&str>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Option<String>, SqlCompileError> {
    match native_like::exact_literal_observed(pattern, escape, &mut |event| match event {
        LikeObservation::Step => work.step(),
        LikeObservation::OpaqueBoundary => work.flush(),
    }) {
        Ok(literal) => Ok(literal.map(|literal| literal.into_owned())),
        Err(LikeError::Host(error)) => Err(error.into()),
        Err(LikeError::Pattern(_) | LikeError::Operation(_)) => Ok(None),
    }
}

fn total_literal_like(
    expression: &TypedExpr,
    effects: &ScalarEffectQuery<'_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<bool, SqlCompileError> {
    let Some((input, pattern, escape)) = like_operands(expression) else {
        return Ok(false);
    };
    let Some(ConnectorValue::Varchar(pattern)) = lower_varchar_literal(pattern) else {
        return Ok(false);
    };
    let escape = match escape.map(lower_varchar_literal) {
        Some(Some(ConnectorValue::Varchar(value))) => Some(value),
        Some(_) => return Ok(false),
        None => None,
    };
    // The pattern owner discharges the call's possible row error for this
    // exact literal. Its input still has its own original demand.
    Ok(effects.typed(input)?.can_skip_for_earlier_filter()
        && exact_like_literal(&pattern, escape.as_deref(), work)?.is_some())
}

/// Retain only terms that may reject snapshot rows before the original Filter.
#[recursive::recursive]
fn collect_filter_prefix(
    expression: &TypedExpr,
    predicates: &mut Vec<Predicate>,
    effects: &ScalarEffectQuery<'_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<bool, SqlCompileError> {
    work.step()?;
    let expression = unnest(expression);
    if let ExprKind::BinaryOp {
        left,
        op: BinOp::And,
        right,
        ..
    } = &expression.kind
    {
        return Ok(collect_filter_prefix(left, predicates, effects, work)?
            && collect_filter_prefix(right, predicates, effects, work)?);
    }
    let fact = effects.typed(expression)?;
    if !(fact.can_skip_for_earlier_filter()
        || fact.permits_filter_row_elimination
            && fact.expression.permits_boolean_reordering()
            && total_literal_like(expression, effects, work)?)
    {
        return Ok(false);
    }
    predicates.push(Predicate {
        expression: expression.clone(),
        mapping: None,
    });
    Ok(true)
}

pub(super) fn collect(
    mut plan: PhysicalPlanNode,
    next_need: &mut u32,
    materialize: bool,
    effects: &ScalarEffectQuery<'_>,
) -> Result<(PhysicalPlanNode, Box<[SystemTableSnapshotNeed]>), SqlCompileError> {
    fn walk(
        plan: &mut PhysicalPlanNode,
        mut predicates: Vec<Predicate>,
        next_need: &mut u32,
        materialize: bool,
        needs: &mut Vec<SystemTableSnapshotNeed>,
        effects: &ScalarEffectQuery<'_>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), SqlCompileError> {
        work.step()?;
        match &mut plan.kind {
            PhysicalPlanKind::Filter(filter) => {
                let mut local = Vec::new();
                if !collect_filter_prefix(&filter.predicate, &mut local, effects, work)? {
                    // This child filter executes before every inherited condition.
                    predicates.clear();
                }
                predicates.extend(local);
            }
            PhysicalPlanKind::Project(project) => {
                let mapping = project
                    .items
                    .iter()
                    .map(|item| {
                        let expression = unnest(&item.expr);
                        let input = match &expression.kind {
                            ExprKind::ColumnRef { column_id, .. } => {
                                ProjectInput::Column(*column_id)
                            }
                            _ => ProjectInput::Varchar {
                                value: lower_varchar_literal(expression)?,
                                value_type: expression.value_type.clone(),
                            },
                        };
                        Some((item.output_column_id, input))
                    })
                    .collect::<Option<BTreeMap<_, _>>>();
                if let Some(mapping) = mapping {
                    for predicate in &mut predicates {
                        predicate.mapping = Some(match &predicate.mapping {
                            None => mapping.clone(),
                            Some(previous) => previous
                                .iter()
                                .filter_map(|(original, current)| {
                                    let next = match current {
                                        ProjectInput::Column(column) => mapping.get(column)?,
                                        ProjectInput::Varchar { .. } => current,
                                    };
                                    Some((*original, next.clone()))
                                })
                                .collect(),
                        });
                    }
                } else {
                    predicates.clear();
                }
            }
            PhysicalPlanKind::SystemTable(table) => {
                if !plan.children.is_empty()
                    || table.relation.columns.len() != table.relation.required_columns.len()
                {
                    return Err("system table leaf has inconsistent output geometry".into());
                }
                let mut constraint = SystemTableKeyConstraint::default();
                for predicate in predicates {
                    work.step()?;
                    let expression = unnest(&predicate.expression);
                    let resolve_column = |expression: &TypedExpr| {
                        let expression = varchar_widening_source(expression)?;
                        let ExprKind::ColumnRef { column_id, .. } = &expression.kind else {
                            return None;
                        };
                        let id = match &predicate.mapping {
                            None => *column_id,
                            Some(mapping) => match mapping.get(column_id)? {
                                ProjectInput::Column(id) => *id,
                                ProjectInput::Varchar { .. } => return None,
                            },
                        };
                        let (column, ordinal) = table
                            .relation
                            .columns
                            .iter()
                            .zip(&table.relation.required_columns)
                            .find(|(column, _)| column.column_id == id)?;
                        if !table.relation.declaration.key_columns.contains(ordinal)
                            || column.value_type != expression.value_type
                            || !is_varchar(&column.value_type)
                        {
                            return None;
                        }
                        Some(ProviderPredicateColumn {
                            ordinal: u32::try_from(*ordinal).ok()?,
                            value_type: ConnectorValueType::Varchar,
                        })
                    };
                    let resolve_literal = |expression: &TypedExpr, expected| {
                        if expected != ConnectorValueType::Varchar {
                            return None;
                        }
                        let source = varchar_widening_source(expression)?;
                        if let ExprKind::ColumnRef { column_id, .. } = &source.kind
                            && let Some(ProjectInput::Varchar { value, value_type }) =
                                predicate.mapping.as_ref()?.get(column_id)
                            && value_type == &source.value_type
                        {
                            return Some(value.clone());
                        }
                        lower_varchar_literal(expression)
                    };
                    if let Some((input, pattern, escape)) = like_operands(expression) {
                        if let Some((ordinal, literal)) = exact_like_key(
                            input,
                            pattern,
                            escape,
                            resolve_column,
                            resolve_literal,
                            work,
                        )? {
                            constraint.set_values(
                                ordinal,
                                SystemTableKeyValues::Values([literal].into()),
                            );
                        }
                        continue;
                    }
                    if !matches!(typed_comparison(expression), Some((BinOp::Eq, _)))
                        && !matches!(&expression.kind, ExprKind::InList { negated: false, .. })
                    {
                        continue;
                    }
                    let Some((ordinal, domain)) = lower_atom_with_literal(
                        &predicate.expression,
                        resolve_column,
                        resolve_literal,
                    ) else {
                        continue;
                    };
                    if domain.null_allowed() {
                        continue;
                    }
                    let Some(values) = domain.values().discrete_values() else {
                        continue;
                    };
                    let values = values
                        .into_iter()
                        .map(|value| match value {
                            ConnectorValue::Varchar(value) => Some(value.to_string()),
                            _ => None,
                        })
                        .collect::<Option<std::collections::BTreeSet<_>>>();
                    if let Some(values) = values {
                        constraint
                            .set_values(ordinal as usize, SystemTableKeyValues::Values(values));
                    }
                }
                table.key_constraint = constraint;
                if materialize {
                    if !matches!(table.state, PhysicalSystemTableState::Planning)
                        || table.occurrence.is_some()
                    {
                        return Err("system table snapshot occurrence was already assigned".into());
                    }
                    let id = CompileNeedId::new(*next_need);
                    *next_need = next_need.checked_add(1).ok_or_else(|| {
                        SqlCompileError::Compilation("system table need identity overflow".into())
                    })?;
                    let ordinal = u32::try_from(needs.len())
                        .ok()
                        .filter(|&ordinal| ordinal < u32::MAX)
                        .ok_or_else(|| {
                            SqlCompileError::Compilation(
                                "system table occurrence identity overflow".into(),
                            )
                        })?;
                    let occurrence = SystemTableOccurrenceId::new(ordinal);
                    table.occurrence = Some(occurrence);
                    needs.push(SystemTableSnapshotNeed::new(
                        id,
                        occurrence,
                        table.relation.statement_relation,
                        table.relation.declaration.clone(),
                        table.relation.required_columns.clone().into(),
                        table.key_constraint.clone(),
                    ));
                }
                return Ok(());
            }
            _ => predicates.clear(),
        }
        for child in &mut plan.children {
            walk(
                child,
                predicates.clone(),
                next_need,
                materialize,
                needs,
                effects,
                work,
            )?;
        }
        Ok(())
    }
    let mut work = CompileCheckpoints::try_new(effects.control, CompilePhase::ProviderValidation)?;
    // Ordinary queries should not analyze Filter effects for snapshot offers.
    let mut pending = vec![&plan];
    loop {
        let Some(node) = pending.pop() else {
            work.finish()?;
            return Ok((plan, Box::default()));
        };
        work.step()?;
        if matches!(node.kind, PhysicalPlanKind::SystemTable(_)) {
            break;
        }
        pending.extend(node.children.iter());
    }
    let mut needs = Vec::new();
    walk(
        &mut plan,
        Vec::new(),
        next_need,
        materialize,
        &mut needs,
        effects,
        &mut work,
    )?;
    work.finish()?;
    Ok((plan, needs.into_boxed_slice()))
}

pub(crate) fn install(
    plan: &mut PhysicalPlanNode,
    expected: &[SystemTableSnapshotNeed],
    facts: Box<[SystemTableSnapshotFact]>,
    control: &dyn PureCompileControl,
) -> Result<(), SqlCompileError> {
    let expected = expected
        .iter()
        .map(|need| (need.occurrence(), need))
        .collect::<BTreeMap<_, _>>();
    let mut snapshots = BTreeMap::new();
    for fact in facts {
        let (request, batches, storage) = fact.into_parts();
        if expected.get(&request.occurrence()).copied() != Some(&request)
            || snapshots
                .insert(request.occurrence(), (request, batches, storage))
                .is_some()
        {
            return Err("system table snapshot differs from exact occurrence coverage".into());
        }
    }
    if expected.len() != snapshots.len() {
        return Err("system table snapshot coverage is incomplete".into());
    }
    fn walk(
        plan: &mut PhysicalPlanNode,
        snapshots: &mut BTreeMap<
            SystemTableOccurrenceId,
            (
                SystemTableSnapshotNeed,
                std::sync::Arc<[arrow::record_batch::RecordBatch]>,
                novarocks_type_contract::ValueTypeStorageLifetime,
            ),
        >,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), SqlCompileError> {
        work.step()?;
        for child in &mut plan.children {
            walk(child, snapshots, work)?;
        }
        if let PhysicalPlanKind::SystemTable(table) = &mut plan.kind {
            let occurrence = table.occurrence.ok_or_else(|| {
                SqlCompileError::Compilation(
                    "executable system table has no snapshot occurrence".into(),
                )
            })?;
            let (request, batches, storage) = snapshots.remove(&occurrence).ok_or_else(|| {
                SqlCompileError::Compilation(
                    "system table snapshot occurrence was not answered exactly once".into(),
                )
            })?;
            if !matches!(table.state, PhysicalSystemTableState::Planning)
                || request.statement_relation() != table.relation.statement_relation
                || request.declaration() != &table.relation.declaration
                || request.required_columns() != table.relation.required_columns
                || request.key_constraint() != &table.key_constraint
            {
                return Err("system table snapshot no longer matches its original leaf".into());
            }
            table.state = PhysicalSystemTableState::Finalized { batches, storage };
        }
        Ok(())
    }
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::ProviderValidation)?;
    walk(plan, &mut snapshots, &mut work)?;
    if !snapshots.is_empty() {
        return Err("system table snapshot was not consumed by a physical leaf".into());
    }
    work.finish()?;
    Ok(())
}
