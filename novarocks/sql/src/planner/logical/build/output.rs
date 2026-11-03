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

use crate::analysis::*;
use crate::planner::logical::*;
use crate::planner::payload::*;

pub(crate) fn plan_output_columns(plan: &LogicalPlanNode) -> Result<Vec<OutputColumn>, String> {
    match &plan.kind {
        LogicalPlanKind::Scan(node) => Ok(node.columns.clone()),
        LogicalPlanKind::Filter(_) => plan_output_columns(plan.unary_input()),
        LogicalPlanKind::Project(node) => {
            let input_columns = plan_output_columns(plan.unary_input())?;
            Ok(node
                .items
                .iter()
                .map(|item| OutputColumn {
                    column_id: item.output_column_id,
                    name: item.output_name.clone(),
                    value_type: item.expr.value_type.clone(),

                    is_internal: project_item_refs_internal_column(item, &input_columns),
                })
                .collect())
        }
        LogicalPlanKind::Aggregate(node) => Ok(node.output_columns.clone()),
        LogicalPlanKind::Join(node) => {
            let left = plan_output_columns(plan.left())?;
            let right = plan_output_columns(plan.right())?;
            Ok(join_output_columns(node.join_type, left, right))
        }
        LogicalPlanKind::Sort(_) => plan_output_columns(plan.unary_input()),
        LogicalPlanKind::Limit(_) => plan_output_columns(plan.unary_input()),
        LogicalPlanKind::Union(node) => Ok(node.output_columns.clone()),
        LogicalPlanKind::Intersect(node) => Ok(node.output_columns.clone()),
        LogicalPlanKind::Except(node) => Ok(node.output_columns.clone()),
        LogicalPlanKind::Values(node) => Ok(node.columns.clone()),
        LogicalPlanKind::GenerateSeries(node) => Ok(vec![OutputColumn {
            column_id: node.output_column_id,
            name: node.column_name.clone(),
            value_type: novarocks_type_contract::FunctionValueType::new(
                arrow::datatypes::DataType::Int64,
                false,
            ),

            is_internal: false,
        }]),
        LogicalPlanKind::TableFunction(node) => {
            let mut columns = plan_output_columns(plan.unary_input())?;
            columns.extend(node.output_columns.clone());
            Ok(columns)
        }
        LogicalPlanKind::Window(node) => Ok(node.output_columns.clone()),
        LogicalPlanKind::Repeat(node) => {
            let mut columns = plan_output_columns(plan.unary_input())?;
            columns.extend(
                node.grouping_fn_ids
                    .iter()
                    .map(|(name, column_id)| OutputColumn {
                        column_id: *column_id,
                        name: name.clone(),
                        value_type: novarocks_type_contract::FunctionValueType::new(
                            arrow::datatypes::DataType::Int64,
                            false,
                        ),

                        is_internal: true,
                    }),
            );
            Ok(columns)
        }
        LogicalPlanKind::CTEAnchor(_) => plan_output_columns(plan.child(1)),
        LogicalPlanKind::CTEProduce(node) => Ok(node.output_columns.clone()),
        LogicalPlanKind::CTEConsume(node) => Ok(node.output_columns.clone()),
        LogicalPlanKind::Apply(node) => {
            let mut columns = plan_output_columns(plan.left())?;
            columns.push(node.output_column.clone());
            Ok(columns)
        }
        LogicalPlanKind::AssertOneRow(_) => plan_output_columns(plan.unary_input()),
        LogicalPlanKind::ImvDelta(_) | LogicalPlanKind::ImvVersion(_) => {
            Err("imv marker leaked into non-IMV planner output adaptation".to_string())
        }
    }
}

fn project_item_refs_internal_column(item: &ProjectItem, input_columns: &[OutputColumn]) -> bool {
    expr_refs_internal_column(&item.expr, input_columns)
}

fn expr_refs_internal_column(expr: &TypedExpr, input_columns: &[OutputColumn]) -> bool {
    match &expr.kind {
        ExprKind::ColumnRef {
            column_id, column, ..
        } => input_columns.iter().any(|input| {
            input.is_internal
                && (input.column_id == *column_id || input.name.eq_ignore_ascii_case(column))
        }),
        ExprKind::Cast { expr, .. } => expr_refs_internal_column(expr, input_columns),
        _ => false,
    }
}

pub(super) fn adapt_plan_output(
    input: LogicalPlanNode,
    target_output_columns: &[OutputColumn],
) -> Result<LogicalPlanNode, String> {
    adapt_plan_output_with_qualifier(input, target_output_columns, None)
}

pub(super) fn adapt_plan_output_with_qualifier(
    input: LogicalPlanNode,
    target_output_columns: &[OutputColumn],
    output_qualifier: Option<&str>,
) -> Result<LogicalPlanNode, String> {
    let source_output_columns = plan_output_columns(&input)?;
    if source_output_columns.len() != target_output_columns.len() {
        return Err(format!(
            "output column count mismatch while adapting subquery/CTE output: child has {}, target has {}",
            source_output_columns.len(),
            target_output_columns.len()
        ));
    }

    if source_output_columns
        .iter()
        .zip(target_output_columns.iter())
        .all(|(source, target)| output_column_metadata_equal(source, target))
        && output_qualifier.is_none()
    {
        return Ok(input);
    }

    let mut items = Vec::with_capacity(target_output_columns.len());
    for (source, target) in source_output_columns
        .iter()
        .zip(target_output_columns.iter())
    {
        if source.value_type.data_type() != target.value_type.data_type() {
            return Err(format!(
                "output type mismatch while adapting subquery/CTE column '{}': child={:?}, target={:?}",
                target.name,
                source.value_type.data_type(),
                target.value_type.data_type()
            ));
        }
        if source.value_type.nullable() && !target.value_type.nullable() {
            return Err(format!(
                "output nullability mismatch while adapting subquery/CTE column '{}': child={}, target={}",
                target.name,
                source.value_type.nullable(),
                target.value_type.nullable()
            ));
        }
        items.push(ProjectItem {
            expr: TypedExpr {
                kind: ExprKind::ColumnRef {
                    column_id: source.column_id,
                    qualifier: None,
                    column: source.name.clone(),
                },
                value_type: (source.value_type.clone())
                    .with_nullable(target.value_type.nullable())
                    .expect("valid value root nullability transform"),
            },
            output_name: target.name.clone(),
            output_column_id: target.column_id,
        });
    }

    Ok(LogicalPlanNode::new(
        LogicalPlanKind::Project(PlanProjectNode {
            items,
            output_qualifier: output_qualifier.map(str::to_string),
        }),
        vec![input],
        None,
    ))
}

fn output_column_metadata_equal(left: &OutputColumn, right: &OutputColumn) -> bool {
    left.column_id == right.column_id
        && left.name == right.name
        && left.value_type.data_type() == right.value_type.data_type()
        && left.value_type.nullable() == right.value_type.nullable()
        && left.is_internal == right.is_internal
}

fn join_output_columns(
    join_type: JoinKind,
    left: Vec<OutputColumn>,
    right: Vec<OutputColumn>,
) -> Vec<OutputColumn> {
    match join_type {
        JoinKind::LeftMark { .. } => {
            let mut output = left;
            output.push(join_type.mark_output().expect("mark output"));
            output
        }
        JoinKind::LeftSemi | JoinKind::LeftAnti | JoinKind::NullAwareLeftAnti => left,
        JoinKind::RightSemi | JoinKind::RightAnti => right,
        JoinKind::LeftOuter => {
            let mut out = left;
            out.extend(make_nullable(right));
            out
        }
        JoinKind::RightOuter => {
            let mut out = make_nullable(left);
            out.extend(right);
            out
        }
        JoinKind::FullOuter => {
            let mut out = make_nullable(left);
            out.extend(make_nullable(right));
            out
        }
        JoinKind::Inner | JoinKind::Cross => {
            let mut out = left;
            out.extend(right);
            out
        }
    }
}

fn make_nullable(mut columns: Vec<OutputColumn>) -> Vec<OutputColumn> {
    for column in &mut columns {
        column.value_type = column
            .value_type
            .with_nullable(true)
            .expect("validated root NULL contract");
    }
    columns
}
