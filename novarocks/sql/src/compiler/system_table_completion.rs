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
use super::completion_predicate::{ProviderPredicateColumn, lower_atom_with_column};
use crate::analysis::{BinOp, ExprKind, TypedExpr};
use crate::column_id::ColumnId;
use crate::planner::physical::{PhysicalPlanKind, PhysicalPlanNode};
use crate::planner::system_relation::PhysicalSystemTableState;
use crate::system_table::{
    SystemTableKeyConstraint, SystemTableKeyValues, SystemTableOccurrenceId,
};
use novarocks_spi::connector::read_stack::{ConnectorValue, ConnectorValueType};
use novarocks_type_contract::{CompileCheckpoints, CompilePhase, PureCompileControl};
use std::collections::BTreeMap;

#[derive(Clone)]
struct Predicate {
    expression: TypedExpr,
    // Original predicate IDs -> IDs exposed by the current child.
    mapping: Option<BTreeMap<ColumnId, ColumnId>>,
}
fn unnest(mut expression: &TypedExpr) -> &TypedExpr {
    while let ExprKind::Nested(inner) = &expression.kind {
        expression = inner;
    }
    expression
}
fn add_conjuncts(expression: &TypedExpr, predicates: &mut Vec<Predicate>) {
    let expression = unnest(expression);
    if let ExprKind::BinaryOp {
        left,
        op: BinOp::And,
        right,
        ..
    } = &expression.kind
    {
        add_conjuncts(left, predicates);
        add_conjuncts(right, predicates);
    } else {
        predicates.push(Predicate {
            expression: expression.clone(),
            mapping: None,
        });
    }
}

pub(super) fn collect(
    mut plan: PhysicalPlanNode,
    next_need: &mut u32,
    materialize: bool,
    control: &dyn PureCompileControl,
) -> Result<(PhysicalPlanNode, Box<[SystemTableSnapshotNeed]>), SqlCompileError> {
    fn walk(
        plan: &mut PhysicalPlanNode,
        mut predicates: Vec<Predicate>,
        next_need: &mut u32,
        next_occurrence: &mut u32,
        materialize: bool,
        needs: &mut Vec<SystemTableSnapshotNeed>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), SqlCompileError> {
        work.step()?;
        match &mut plan.kind {
            PhysicalPlanKind::Filter(filter) => add_conjuncts(&filter.predicate, &mut predicates),
            PhysicalPlanKind::Project(project) => {
                let mapping = project
                    .items
                    .iter()
                    .map(|item| {
                        let ExprKind::ColumnRef { column_id, .. } = &unnest(&item.expr).kind else {
                            return None;
                        };
                        Some((item.output_column_id, *column_id))
                    })
                    .collect::<Option<BTreeMap<_, _>>>();
                if let Some(mapping) = mapping {
                    for predicate in &mut predicates {
                        predicate.mapping = Some(match &predicate.mapping {
                            None => mapping.clone(),
                            Some(previous) => previous
                                .iter()
                                .filter_map(|(original, current)| {
                                    mapping.get(current).map(|next| (*original, *next))
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
                    if !matches!(
                        &unnest(&predicate.expression).kind,
                        ExprKind::BinaryOp { op: BinOp::Eq, .. }
                            | ExprKind::InList { negated: false, .. }
                    ) {
                        continue;
                    }
                    let atom = lower_atom_with_column(&predicate.expression, |expression| {
                        let ExprKind::ColumnRef { column_id, .. } = &unnest(expression).kind else {
                            return None;
                        };
                        let id = match &predicate.mapping {
                            None => *column_id,
                            Some(mapping) => *mapping.get(column_id)?,
                        };
                        let (column, ordinal) = table
                            .relation
                            .columns
                            .iter()
                            .zip(&table.relation.required_columns)
                            .find(|(column, _)| column.column_id == id)?;
                        if !table.relation.declaration.key_columns.contains(ordinal)
                            || column.value_type != expression.value_type
                            || column.value_type.data_type() != &arrow::datatypes::DataType::Utf8
                        {
                            return None;
                        }
                        Some(ProviderPredicateColumn {
                            ordinal: u32::try_from(*ordinal).ok()?,
                            value_type: ConnectorValueType::Varchar,
                        })
                    });
                    let Some((ordinal, domain)) = atom else {
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
                    let occurrence = SystemTableOccurrenceId::new(*next_occurrence);
                    *next_occurrence = next_occurrence.checked_add(1).ok_or_else(|| {
                        SqlCompileError::Compilation(
                            "system table occurrence identity overflow".into(),
                        )
                    })?;
                    table.occurrence = Some(occurrence);
                    needs.push(SystemTableSnapshotNeed::new(
                        id,
                        occurrence,
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
                next_occurrence,
                materialize,
                needs,
                work,
            )?;
        }
        Ok(())
    }
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::ProviderValidation)?;
    let mut needs = Vec::new();
    walk(
        &mut plan,
        Vec::new(),
        next_need,
        &mut 0,
        materialize,
        &mut needs,
        &mut work,
    )?;
    work.finish()?;
    Ok((plan, needs.into_boxed_slice()))
}

pub(super) fn install(
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
