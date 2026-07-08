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

use std::collections::HashSet;

use crate::sql::analysis::{ExprKind, SortItem, TypedExpr};
use crate::sql::column_id::ColumnId;
use crate::sql::optimizer::operator::{
    ChangeEventExpandOp, GenerateSeriesOp, Operator, PhysicalDistributionOp,
    PhysicalHashAggregateOp, PhysicalHashJoinOp, PhysicalNestLoopJoinOp, ProjectOp, RepeatOp,
    TableFunctionOp, WindowOp,
};
use crate::sql::optimizer::physical_tree::OptimizerPhysicalNode;
use crate::sql::optimizer::property::DistributionSpec;
use crate::sql::optimizer::scalar::ScalarArena;
use crate::sql::planner::optimizer_bridge::scalar::{
    materialize, materialize_aggregate_calls, materialize_exprs, materialize_project_items,
    materialize_sort_keys, materialize_window_exprs,
};

pub(crate) fn verify_optimizer_id_binding(plan: &OptimizerPhysicalNode) -> Result<(), String> {
    let scalars = plan
        .execution_props
        .scalar_arena
        .as_deref()
        .ok_or_else(|| {
            "OptimizerPhysicalNode missing scalar arena for optimizer id binding verification"
                .to_string()
        })?;
    verify_node(plan, scalars).map(|_| ())
}

fn verify_node(
    node: &OptimizerPhysicalNode,
    scalars: &ScalarArena,
) -> Result<HashSet<ColumnId>, String> {
    let child_outputs = node
        .children
        .iter()
        .map(|child| verify_node(child, scalars))
        .collect::<Result<Vec<_>, _>>()?;

    let derived = match &node.op {
        Operator::PhysicalScan(op) => Ok(output_ids(op.columns.iter().map(|c| c.column_id))),
        Operator::PhysicalValues(op) => {
            let empty = HashSet::new();
            for row in &op.rows {
                for expr in row {
                    let expr = materialize(scalars, *expr);
                    verify_expr(&expr, &empty, "PhysicalValues row")?;
                }
            }
            Ok(output_ids(op.columns.iter().map(|c| c.column_id)))
        }
        Operator::PhysicalGenerateSeries(op) => verify_generate_series(op),
        Operator::PhysicalCTEConsume(op) => {
            op.validate_mapping()?;
            for producer_id in &op.producer_column_ids {
                verify_output_id(*producer_id, "PhysicalCTEConsume producer mapping")?;
            }
            Ok(output_ids(op.output_columns.iter().map(|c| c.column_id)))
        }

        Operator::PhysicalFilter(op) => {
            let input = only_child(&child_outputs, "PhysicalFilter")?;
            let predicate = materialize(scalars, op.predicate);
            verify_expr(&predicate, input, "PhysicalFilter predicate")?;
            Ok(input.clone())
        }
        Operator::PhysicalSort(op) => {
            let input = only_child(&child_outputs, "PhysicalSort")?;
            let items = materialize_sort_keys(scalars, &op.items);
            verify_sort_items(&items, input, "PhysicalSort item")?;
            let analytic_partition_exprs = materialize_exprs(scalars, &op.analytic_partition_exprs);
            for expr in &analytic_partition_exprs {
                verify_expr(expr, input, "PhysicalSort analytic partition")?;
            }
            Ok(input.clone())
        }
        Operator::PhysicalTopN(op) => {
            let input = only_child(&child_outputs, "PhysicalTopN")?;
            let items = materialize_sort_keys(scalars, &op.items);
            verify_sort_items(&items, input, "PhysicalTopN item")?;
            Ok(input.clone())
        }
        Operator::PhysicalLimit(_)
        | Operator::PhysicalDistribution(_)
        | Operator::PhysicalAssertOneRow(_) => {
            let input = only_child(&child_outputs, "pass-through physical node")?;
            if let Operator::PhysicalDistribution(op) = &node.op {
                verify_distribution(op, input)?;
            }
            Ok(input.clone())
        }

        Operator::PhysicalProject(op) => verify_project(op, &child_outputs, scalars),
        Operator::PhysicalHashAggregate(op) => verify_hash_aggregate(op, &child_outputs, scalars),
        Operator::PhysicalWindow(op) => verify_window(op, &child_outputs, scalars),
        Operator::PhysicalHashJoin(op) => verify_hash_join(op, &child_outputs, scalars),
        Operator::PhysicalNestLoopJoin(op) => verify_nested_loop_join(op, &child_outputs, scalars),
        Operator::PhysicalTableFunction(op) => verify_table_function(op, &child_outputs, scalars),
        Operator::PhysicalRepeat(op) => verify_repeat(op, &child_outputs),
        Operator::PhysicalChangeEventExpand(op) => {
            verify_change_event_expand(op, &child_outputs, scalars)
        }

        Operator::PhysicalUnion(op) => {
            Ok(output_ids(op.output_columns.iter().map(|c| c.column_id)))
        }
        Operator::PhysicalIntersect(op) => {
            Ok(output_ids(op.output_columns.iter().map(|c| c.column_id)))
        }
        Operator::PhysicalExcept(op) => {
            Ok(output_ids(op.output_columns.iter().map(|c| c.column_id)))
        }
        Operator::PhysicalCTEProduce(op) => {
            let input = only_child(&child_outputs, "PhysicalCTEProduce")?;
            for column in &op.output_columns {
                verify_output_id(column.column_id, "PhysicalCTEProduce output")?;
            }
            if !op.output_columns.is_empty() {
                let child = node
                    .children
                    .first()
                    .ok_or_else(|| "PhysicalCTEProduce expected one child".to_string())?;
                if child.output_columns.len() != op.output_columns.len() {
                    return Err(format!(
                        "PhysicalCTEProduce output arity mismatch: child has {}, declared has {}",
                        child.output_columns.len(),
                        op.output_columns.len()
                    ));
                }
                for (idx, (child_col, declared_col)) in child
                    .output_columns
                    .iter()
                    .zip(op.output_columns.iter())
                    .enumerate()
                {
                    if child_col.column_id != declared_col.column_id {
                        return Err(format!(
                            "PhysicalCTEProduce output ColumnId mismatch at index {}: child={}, declared={}",
                            idx, child_col.column_id, declared_col.column_id
                        ));
                    }
                }
            }
            Ok(if op.output_columns.is_empty() {
                input.clone()
            } else {
                output_ids(op.output_columns.iter().map(|c| c.column_id))
            })
        }
        Operator::PhysicalCTEAnchor(_) => {
            if child_outputs.len() != 2 {
                return Err(format!(
                    "PhysicalCTEAnchor expected 2 children, got {}",
                    child_outputs.len()
                ));
            }
            Ok(child_outputs[1].clone())
        }
        other => Err(format!(
            "non-physical operator reached optimizer id binding verifier: {:?}",
            other
        )),
    }?;
    let declared = if uses_declared_node_outputs(&node.op) {
        declared_node_output_ids(node)?
    } else {
        HashSet::new()
    };
    if declared.is_empty() {
        Ok(derived)
    } else {
        Ok(declared)
    }
}

fn verify_project(
    op: &ProjectOp,
    child_outputs: &[HashSet<ColumnId>],
    scalars: &ScalarArena,
) -> Result<HashSet<ColumnId>, String> {
    let input = only_child(child_outputs, "PhysicalProject")?;
    let items = materialize_project_items(scalars, &op.items);
    for item in &items {
        verify_expr(&item.expr, input, "PhysicalProject item")?;
        verify_output_id(item.output_column_id, "PhysicalProject output")?;
    }
    Ok(output_ids(items.iter().map(|item| item.output_column_id)))
}

fn verify_hash_aggregate(
    op: &PhysicalHashAggregateOp,
    child_outputs: &[HashSet<ColumnId>],
    scalars: &ScalarArena,
) -> Result<HashSet<ColumnId>, String> {
    let input = only_child(child_outputs, "PhysicalHashAggregate")?;
    let group_by = materialize_exprs(scalars, &op.group_by);
    for expr in &group_by {
        verify_expr(expr, input, "PhysicalHashAggregate group-by")?;
    }
    op.output_layout
        .validate_aggregate_calls(&op.aggregates, op.is_merge.len())
        .map_err(|err| format!("PhysicalHashAggregate layout contract: {err}"))?;
    op.output_layout
        .validate_visible_outputs(&op.output_columns)
        .map_err(|err| format!("PhysicalHashAggregate visible outputs contract: {err}"))?;
    let aggregates = materialize_aggregate_calls(scalars, &op.aggregates, &op.output_layout);
    for (idx, aggregate) in aggregates.iter().enumerate() {
        if !op.is_merge.get(idx).copied().unwrap_or(false) {
            for arg in &aggregate.args {
                verify_expr(arg, input, "PhysicalHashAggregate aggregate arg")?;
            }
            verify_sort_items(
                &aggregate.order_by,
                input,
                "PhysicalHashAggregate aggregate order-by",
            )?;
        }
        verify_output_id(
            aggregate.output_column_id,
            "PhysicalHashAggregate aggregate output",
        )?;
    }
    let mut out = HashSet::new();
    for column in &op.output_columns {
        verify_output_id(column.column_id, "PhysicalHashAggregate visible output")?;
        out.insert(column.column_id);
    }
    for column in &op.output_layout.group_key_columns {
        verify_output_id(
            column.column_id,
            "PhysicalHashAggregate group layout output",
        )?;
        out.insert(column.column_id);
    }
    for aggregate in &aggregates {
        verify_output_id(
            aggregate.output_column_id,
            "PhysicalHashAggregate aggregate output",
        )?;
        out.insert(aggregate.output_column_id);
    }
    Ok(out)
}

fn verify_window(
    op: &WindowOp,
    child_outputs: &[HashSet<ColumnId>],
    scalars: &ScalarArena,
) -> Result<HashSet<ColumnId>, String> {
    let input = only_child(child_outputs, "PhysicalWindow")?;
    let window_exprs = materialize_window_exprs(scalars, &op.window_exprs, &op.output_columns);
    for window in &window_exprs {
        for arg in &window.args {
            verify_expr(arg, input, "PhysicalWindow arg")?;
        }
        for expr in &window.partition_by {
            verify_expr(expr, input, "PhysicalWindow partition")?;
        }
        verify_sort_items(&window.order_by, input, "PhysicalWindow order-by")?;
        verify_output_id(window.output_column_id, "PhysicalWindow output")?;
    }
    Ok(output_ids(op.output_columns.iter().map(|c| c.column_id)))
}

fn verify_hash_join(
    op: &PhysicalHashJoinOp,
    child_outputs: &[HashSet<ColumnId>],
    scalars: &ScalarArena,
) -> Result<HashSet<ColumnId>, String> {
    let (left, right) = two_children(child_outputs, "PhysicalHashJoin")?;
    for eq in &op.eq_conditions {
        let left_key = materialize(scalars, eq.left);
        let right_key = materialize(scalars, eq.right);
        verify_expr(&left_key, left, "PhysicalHashJoin left key")?;
        verify_expr(&right_key, right, "PhysicalHashJoin right key")?;
    }
    if let Some(condition) = &op.other_condition {
        let condition = materialize(scalars, *condition);
        verify_expr(
            &condition,
            &union_ids(left, right),
            "PhysicalHashJoin other condition",
        )?;
    }
    Ok(union_ids(left, right))
}

fn verify_nested_loop_join(
    op: &PhysicalNestLoopJoinOp,
    child_outputs: &[HashSet<ColumnId>],
    scalars: &ScalarArena,
) -> Result<HashSet<ColumnId>, String> {
    let (left, right) = two_children(child_outputs, "PhysicalNestLoopJoin")?;
    if let Some(condition) = &op.condition {
        let condition = materialize(scalars, *condition);
        verify_expr(
            &condition,
            &union_ids(left, right),
            "PhysicalNestLoopJoin condition",
        )?;
    }
    Ok(union_ids(left, right))
}

fn verify_table_function(
    op: &TableFunctionOp,
    child_outputs: &[HashSet<ColumnId>],
    scalars: &ScalarArena,
) -> Result<HashSet<ColumnId>, String> {
    let input = only_child(child_outputs, "PhysicalTableFunction")?;
    let args = materialize_exprs(scalars, &op.args);
    for arg in &args {
        verify_expr(arg, input, "PhysicalTableFunction arg")?;
    }
    let mut out = input.clone();
    out.extend(op.output_columns.iter().map(|c| c.column_id));
    verify_ids(&out, "PhysicalTableFunction output")?;
    Ok(out)
}

fn verify_repeat(
    op: &RepeatOp,
    child_outputs: &[HashSet<ColumnId>],
) -> Result<HashSet<ColumnId>, String> {
    let input = only_child(child_outputs, "PhysicalRepeat")?;
    for column_id in &op.all_rollup_column_ids {
        verify_input_id(*column_id, input, "PhysicalRepeat rollup key")?;
    }
    for column_id in op.repeat_column_ref_ids.iter().flatten() {
        verify_input_id(*column_id, input, "PhysicalRepeat non-null key")?;
    }
    if op.grouping_fn_arg_ids.len() != op.grouping_fn_args.len() {
        return Err(format!(
            "PhysicalRepeat grouping function id metadata mismatch: args={}, ids={}",
            op.grouping_fn_args.len(),
            op.grouping_fn_arg_ids.len()
        ));
    }
    for column_id in op.grouping_fn_arg_ids.iter().flatten() {
        verify_input_id(*column_id, input, "PhysicalRepeat grouping arg")?;
    }
    let mut out = input.clone();
    for (_, column_id) in &op.grouping_fn_ids {
        verify_output_id(*column_id, "PhysicalRepeat grouping output")?;
        out.insert(*column_id);
    }
    Ok(out)
}

fn verify_change_event_expand(
    op: &ChangeEventExpandOp,
    child_outputs: &[HashSet<ColumnId>],
    scalars: &ScalarArena,
) -> Result<HashSet<ColumnId>, String> {
    let input = only_child(child_outputs, "PhysicalChangeEventExpand")?;
    let outputs = output_ids(op.output_columns.iter().map(|c| c.column_id));
    verify_declared_output_id(
        op.change_op_column_id,
        &outputs,
        "PhysicalChangeEventExpand change-op column",
    )?;
    if let Some(column_id) = op.data_route_column_id {
        verify_declared_output_id(
            column_id,
            &outputs,
            "PhysicalChangeEventExpand data-route column",
        )?;
    }
    for event in &op.events {
        let _route_key = event.branch_kind.route_key();
        if let Some(predicate) = event.predicate {
            let predicate = materialize(scalars, predicate);
            verify_expr(&predicate, input, "PhysicalChangeEventExpand predicate")?;
        }
        for assignment in &event.assignments {
            verify_declared_output_id(
                assignment.output_column_id,
                &outputs,
                "PhysicalChangeEventExpand assignment output",
            )?;
            if let Some(expr) = assignment.expr {
                let expr = materialize(scalars, expr);
                verify_expr(&expr, input, "PhysicalChangeEventExpand assignment")?;
            }
        }
    }
    Ok(outputs)
}

fn verify_generate_series(op: &GenerateSeriesOp) -> Result<HashSet<ColumnId>, String> {
    verify_output_id(op.output_column_id, "PhysicalGenerateSeries output")?;
    Ok(output_ids(std::iter::once(op.output_column_id)))
}

fn verify_distribution(
    op: &PhysicalDistributionOp,
    input: &HashSet<ColumnId>,
) -> Result<(), String> {
    if let DistributionSpec::HashPartitioned { cols, .. } = &op.spec {
        for column_id in cols {
            verify_input_id(*column_id, input, "PhysicalDistribution hash column")?;
        }
    }
    Ok(())
}

fn verify_expr(expr: &TypedExpr, input: &HashSet<ColumnId>, context: &str) -> Result<(), String> {
    match &expr.kind {
        ExprKind::ColumnRef {
            column_id, column, ..
        } => {
            if *column_id == ColumnId::UNSET {
                return Err(format!("{context}: UNSET ColumnRef `{column}`"));
            }
            verify_input_id(*column_id, input, &format!("{context} `{column}`"))
        }
        ExprKind::LambdaParamRef { .. } | ExprKind::Literal(_) => Ok(()),
        ExprKind::BinaryOp { left, right, .. } => {
            verify_expr(left, input, context)?;
            verify_expr(right, input, context)
        }
        ExprKind::UnaryOp { expr, .. }
        | ExprKind::Cast { expr, .. }
        | ExprKind::IsNull { expr, .. }
        | ExprKind::IsTruthValue { expr, .. }
        | ExprKind::Nested(expr) => verify_expr(expr, input, context),
        ExprKind::FunctionCall { args, .. } | ExprKind::AggregateCall { args, .. } => {
            for arg in args {
                verify_expr(arg, input, context)?;
            }
            if let ExprKind::AggregateCall { order_by, .. } = &expr.kind {
                verify_sort_items(order_by, input, context)?;
            }
            Ok(())
        }
        ExprKind::LambdaFunction { body, .. } | ExprKind::Lambda { body, .. } => {
            verify_expr(body, input, context)
        }
        ExprKind::InList { expr, list, .. } => {
            verify_expr(expr, input, context)?;
            for item in list {
                verify_expr(item, input, context)?;
            }
            Ok(())
        }
        ExprKind::Between {
            expr, low, high, ..
        } => {
            verify_expr(expr, input, context)?;
            verify_expr(low, input, context)?;
            verify_expr(high, input, context)
        }
        ExprKind::Like { expr, pattern, .. } => {
            verify_expr(expr, input, context)?;
            verify_expr(pattern, input, context)
        }
        ExprKind::Case {
            operand,
            when_then,
            else_expr,
        } => {
            if let Some(operand) = operand {
                verify_expr(operand, input, context)?;
            }
            for (when, then) in when_then {
                verify_expr(when, input, context)?;
                verify_expr(then, input, context)?;
            }
            if let Some(else_expr) = else_expr {
                verify_expr(else_expr, input, context)?;
            }
            Ok(())
        }
        ExprKind::WindowCall {
            args,
            partition_by,
            order_by,
            ..
        } => {
            for arg in args {
                verify_expr(arg, input, context)?;
            }
            for expr in partition_by {
                verify_expr(expr, input, context)?;
            }
            verify_sort_items(order_by, input, context)
        }
        ExprKind::SubqueryPlaceholder { .. } => Err(format!(
            "{context}: subquery placeholder reached executable physical plan"
        )),
    }
}

fn verify_sort_items(
    items: &[SortItem],
    input: &HashSet<ColumnId>,
    context: &str,
) -> Result<(), String> {
    for item in items {
        verify_expr(&item.expr, input, context)?;
    }
    Ok(())
}

fn only_child<'a>(
    child_outputs: &'a [HashSet<ColumnId>],
    context: &str,
) -> Result<&'a HashSet<ColumnId>, String> {
    if child_outputs.len() != 1 {
        return Err(format!(
            "{context} expected 1 child, got {}",
            child_outputs.len()
        ));
    }
    Ok(&child_outputs[0])
}

fn two_children<'a>(
    child_outputs: &'a [HashSet<ColumnId>],
    context: &str,
) -> Result<(&'a HashSet<ColumnId>, &'a HashSet<ColumnId>), String> {
    if child_outputs.len() != 2 {
        return Err(format!(
            "{context} expected 2 children, got {}",
            child_outputs.len()
        ));
    }
    Ok((&child_outputs[0], &child_outputs[1]))
}

fn output_ids(ids: impl IntoIterator<Item = ColumnId>) -> HashSet<ColumnId> {
    ids.into_iter()
        .filter(|id| *id != ColumnId::UNSET)
        .collect()
}

fn declared_node_output_ids(node: &OptimizerPhysicalNode) -> Result<HashSet<ColumnId>, String> {
    let mut out = HashSet::new();
    for column in &node.output_columns {
        verify_output_id(
            column.column_id,
            &format!(
                "OptimizerPhysicalNode output `{}` on {:?}",
                column.name, node.op
            ),
        )?;
        out.insert(column.column_id);
    }
    Ok(out)
}

fn uses_declared_node_outputs(op: &Operator) -> bool {
    !matches!(
        op,
        Operator::PhysicalFilter(_)
            | Operator::PhysicalProject(_)
            | Operator::PhysicalHashJoin(_)
            | Operator::PhysicalNestLoopJoin(_)
            | Operator::PhysicalSort(_)
            | Operator::PhysicalTopN(_)
            | Operator::PhysicalLimit(_)
            | Operator::PhysicalDistribution(_)
            | Operator::PhysicalAssertOneRow(_)
            | Operator::PhysicalHashAggregate(_)
            | Operator::PhysicalRepeat(_)
    )
}

fn verify_ids(ids: &HashSet<ColumnId>, context: &str) -> Result<(), String> {
    for column_id in ids {
        verify_output_id(*column_id, context)?;
    }
    Ok(())
}

fn verify_output_id(column_id: ColumnId, context: &str) -> Result<(), String> {
    if column_id == ColumnId::UNSET {
        return Err(format!("{context}: output ColumnId::UNSET"));
    }
    Ok(())
}

fn verify_input_id(
    column_id: ColumnId,
    input: &HashSet<ColumnId>,
    context: &str,
) -> Result<(), String> {
    verify_output_id(column_id, context)?;
    if !input.contains(&column_id) {
        let mut available = input.iter().map(|id| id.0).collect::<Vec<_>>();
        available.sort_unstable();
        return Err(format!(
            "{context}: ColumnId({}) is not produced by child scope; available={:?}",
            column_id.0, available
        ));
    }
    Ok(())
}

fn verify_declared_output_id(
    column_id: ColumnId,
    outputs: &HashSet<ColumnId>,
    context: &str,
) -> Result<(), String> {
    verify_output_id(column_id, context)?;
    if !outputs.contains(&column_id) {
        let mut available = outputs.iter().map(|id| id.0).collect::<Vec<_>>();
        available.sort_unstable();
        return Err(format!(
            "{context}: ColumnId({}) is not declared by operator output columns; available={:?}",
            column_id.0, available
        ));
    }
    Ok(())
}

fn union_ids(left: &HashSet<ColumnId>, right: &HashSet<ColumnId>) -> HashSet<ColumnId> {
    let mut out = left.clone();
    out.extend(right.iter().copied());
    out
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::DataType;

    use super::*;
    use crate::sql::analysis::{ExprKind, OutputColumn, ProjectItem, TypedExpr};
    use crate::sql::optimizer::operator::{
        AggMode, AggregateOutputLayout, CTEConsumeOp, PhysicalHashAggregateOp, ProjectOp, RepeatOp,
        ValuesOp,
    };
    use crate::sql::optimizer::physical_tree::{PlanExecutionProps, attach_scalar_arena};
    use crate::sql::optimizer::scalar::ScalarArena;
    use crate::sql::optimizer::statistics::Statistics;
    use crate::sql::planner::optimizer_bridge::scalar::{
        intern_aggregate_calls, intern_exprs, intern_project_items,
    };
    use crate::sql::planner::plan::AggregateCall;

    fn int_col(column_id: ColumnId, name: &str) -> OutputColumn {
        OutputColumn {
            column_id,
            name: name.to_string(),
            data_type: DataType::Int32,
            nullable: false,
            is_internal: false,
        }
    }

    fn column_ref(column_id: ColumnId, name: &str) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id,
                qualifier: None,
                column: name.to_string(),
            },
            data_type: DataType::Int32,
            nullable: false,
        }
    }

    fn values_node(columns: Vec<OutputColumn>) -> OptimizerPhysicalNode {
        OptimizerPhysicalNode {
            op: Operator::PhysicalValues(ValuesOp {
                rows: vec![],
                columns: columns.clone(),
            }),
            children: vec![],
            stats: Statistics::default(),
            explain_stats: crate::sql::optimizer::physical_tree::OptimizerExplainStats::default(),
            output_columns: columns,
            execution_props: PlanExecutionProps::default(),
        }
    }

    fn project_over(
        child: OptimizerPhysicalNode,
        expr: TypedExpr,
        output_id: ColumnId,
    ) -> OptimizerPhysicalNode {
        let mut scalars = child
            .execution_props
            .scalar_arena
            .as_deref()
            .cloned()
            .unwrap_or_else(ScalarArena::new);
        let items = vec![ProjectItem {
            expr,
            output_name: "p".to_string(),
            output_column_id: output_id,
        }];
        let mut plan = OptimizerPhysicalNode {
            op: Operator::PhysicalProject(ProjectOp {
                items: intern_project_items(&mut scalars, &items),
                output_qualifier: None,
            }),
            children: vec![child],
            stats: Statistics::default(),
            explain_stats: crate::sql::optimizer::physical_tree::OptimizerExplainStats::default(),
            output_columns: vec![int_col(output_id, "p")],
            execution_props: PlanExecutionProps::default(),
        };
        attach_scalar_arena(&mut plan, Arc::new(scalars));
        plan
    }

    fn hash_aggregate_over(
        child: OptimizerPhysicalNode,
        aggregate_output_id: ColumnId,
        declared_visible_id: ColumnId,
    ) -> OptimizerPhysicalNode {
        let mut scalars = child
            .execution_props
            .scalar_arena
            .as_deref()
            .cloned()
            .unwrap_or_else(ScalarArena::new);
        let aggregate_calls = vec![AggregateCall {
            name: "sum".to_string(),
            args: vec![column_ref(ColumnId::new_for_test(1), "a")],
            distinct: false,
            result_type: DataType::Int32,
            order_by: vec![],
            output_column_id: aggregate_output_id,
        }];
        let mut plan = OptimizerPhysicalNode {
            op: Operator::PhysicalHashAggregate(PhysicalHashAggregateOp {
                mode: AggMode::Single,
                group_by: vec![],
                aggregates: intern_aggregate_calls(&mut scalars, &aggregate_calls),
                output_layout: AggregateOutputLayout::new(
                    vec![],
                    vec![int_col(aggregate_output_id, "sum(a)")],
                ),
                output_columns: vec![int_col(aggregate_output_id, "sum(a)")],
                is_merge: vec![false],
            }),
            children: vec![child],
            stats: Statistics::default(),
            explain_stats: crate::sql::optimizer::physical_tree::OptimizerExplainStats::default(),
            output_columns: vec![int_col(declared_visible_id, "sum(a) + 1")],
            execution_props: PlanExecutionProps::default(),
        };
        attach_scalar_arena(&mut plan, Arc::new(scalars));
        plan
    }

    fn repeat_over(
        child: OptimizerPhysicalNode,
        grouping_output_id: ColumnId,
    ) -> OptimizerPhysicalNode {
        let rollup_id = ColumnId::new_for_test(1);
        OptimizerPhysicalNode {
            op: Operator::PhysicalRepeat(RepeatOp {
                repeat_column_ref_list: vec![vec!["a".to_string()], vec![]],
                repeat_column_ref_ids: vec![vec![rollup_id], vec![]],
                grouping_ids: vec![0, 1],
                all_rollup_columns: vec!["a".to_string()],
                all_rollup_column_ids: vec![rollup_id],
                grouping_key_aliases: vec![],
                grouping_fn_args: vec![("__grouping_fn_0".to_string(), vec!["a".to_string()])],
                grouping_fn_arg_ids: vec![vec![rollup_id]],
                grouping_fn_ids: vec![("__grouping_fn_0".to_string(), grouping_output_id)],
            }),
            children: vec![child],
            stats: Statistics::default(),
            explain_stats: crate::sql::optimizer::physical_tree::OptimizerExplainStats::default(),
            output_columns: vec![int_col(rollup_id, "a")],
            execution_props: PlanExecutionProps::default(),
        }
    }

    #[test]
    fn p3_verify_optimizer_id_binding_rejects_unset_columnref() {
        let err = verify_expr(
            &column_ref(ColumnId::UNSET, "a"),
            &std::collections::HashSet::new(),
            "test expr",
        )
        .expect_err("UNSET ColumnRef must fail");
        assert!(err.contains("UNSET ColumnRef"), "unexpected err={err}");
    }

    #[test]
    fn p3_verify_optimizer_id_binding_rejects_missing_input_binding() {
        let input_id = ColumnId::new_for_test(1);
        let missing_id = ColumnId::new_for_test(99);
        let output_id = ColumnId::new_for_test(2);
        let plan = project_over(
            values_node(vec![int_col(input_id, "a")]),
            column_ref(missing_id, "a"),
            output_id,
        );

        let err = verify_optimizer_id_binding(&plan).expect_err("missing ColumnId must fail");
        assert!(
            err.contains("not produced by child scope"),
            "unexpected err={err}"
        );
    }

    #[test]
    fn p3_verify_optimizer_id_binding_rejects_logical_operator_with_planner_bridge_label() {
        let mut plan = values_node(vec![]);
        plan.op = Operator::LogicalValues(ValuesOp {
            rows: vec![],
            columns: vec![],
        });
        attach_scalar_arena(&mut plan, Arc::new(ScalarArena::new()));

        let err = verify_optimizer_id_binding(&plan).expect_err("logical op must fail");
        assert!(
            err.contains("optimizer id binding verifier"),
            "unexpected err={err}"
        );
        let stale_label = ["codegen", "id", "verifier"].join(" ");
        assert!(!err.contains(&stale_label), "unexpected err={err}");
    }

    #[test]
    fn p3_hash_aggregate_outputs_aggregate_call_ids_not_declared_visible_ids() {
        let input_id = ColumnId::new_for_test(1);
        let aggregate_output_id = ColumnId::new_for_test(4);
        let visible_output_id = ColumnId::new_for_test(5);
        let project_output_id = ColumnId::new_for_test(6);
        let aggregate = hash_aggregate_over(
            values_node(vec![int_col(input_id, "a")]),
            aggregate_output_id,
            visible_output_id,
        );
        let plan = project_over(
            aggregate,
            column_ref(aggregate_output_id, "sum(a)"),
            project_output_id,
        );

        verify_optimizer_id_binding(&plan).expect("project should bind aggregate call output id");
    }

    #[test]
    fn p3_verify_optimizer_id_binding_accepts_hidden_group_key_layout_output() {
        let input_id = ColumnId::new_for_test(1);
        let group_output_id = ColumnId::new_for_test(4);
        let aggregate_output_id = ColumnId::new_for_test(5);
        let project_output_id = ColumnId::new_for_test(6);
        let child = values_node(vec![int_col(input_id, "a")]);
        let mut scalars = ScalarArena::new();
        let aggregate_calls = vec![AggregateCall {
            name: "sum".to_string(),
            args: vec![column_ref(input_id, "a")],
            distinct: false,
            result_type: DataType::Int32,
            order_by: vec![],
            output_column_id: aggregate_output_id,
        }];
        let mut aggregate = OptimizerPhysicalNode {
            op: Operator::PhysicalHashAggregate(PhysicalHashAggregateOp {
                mode: AggMode::Single,
                group_by: intern_exprs(&mut scalars, &[column_ref(input_id, "a")]),
                aggregates: intern_aggregate_calls(&mut scalars, &aggregate_calls),
                output_layout: AggregateOutputLayout::new(
                    vec![int_col(group_output_id, "a")],
                    vec![int_col(aggregate_output_id, "sum(a)")],
                ),
                output_columns: vec![int_col(aggregate_output_id, "sum(a)")],
                is_merge: vec![false],
            }),
            children: vec![child],
            stats: Statistics::default(),
            explain_stats: crate::sql::optimizer::physical_tree::OptimizerExplainStats::default(),
            output_columns: vec![int_col(aggregate_output_id, "sum(a)")],
            execution_props: PlanExecutionProps::default(),
        };
        attach_scalar_arena(&mut aggregate, Arc::new(scalars));
        let plan = project_over(
            aggregate,
            column_ref(group_output_id, "a"),
            project_output_id,
        );

        verify_optimizer_id_binding(&plan)
            .expect("project should bind hidden group layout output id");
    }

    #[test]
    fn p3_repeat_outputs_grouping_function_ids_not_declared_passthrough_ids() {
        let input_id = ColumnId::new_for_test(1);
        let grouping_output_id = ColumnId::new_for_test(4);
        let project_output_id = ColumnId::new_for_test(5);
        let repeat = repeat_over(
            values_node(vec![int_col(input_id, "a")]),
            grouping_output_id,
        );
        let plan = project_over(
            repeat,
            column_ref(grouping_output_id, "__grouping_fn_0"),
            project_output_id,
        );

        verify_optimizer_id_binding(&plan).expect("project should bind Repeat grouping output id");
    }

    #[test]
    fn p3_distribution_over_repeat_preserves_grouping_ids() {
        let input_id = ColumnId::new_for_test(1);
        let grouping_output_id = ColumnId::new_for_test(4);
        let repeat = repeat_over(
            values_node(vec![int_col(input_id, "a")]),
            grouping_output_id,
        );
        let distribution = OptimizerPhysicalNode {
            op: Operator::PhysicalDistribution(PhysicalDistributionOp {
                spec: DistributionSpec::Gather,
            }),
            children: vec![repeat],
            stats: Statistics::default(),
            explain_stats: crate::sql::optimizer::physical_tree::OptimizerExplainStats::default(),
            output_columns: vec![int_col(input_id, "a")],
            execution_props: PlanExecutionProps::default(),
        };
        let mut scalars = ScalarArena::new();
        let aggregate_output_columns = vec![
            int_col(input_id, "a"),
            int_col(grouping_output_id, "__grouping_fn_0"),
        ];
        let mut aggregate = OptimizerPhysicalNode {
            op: Operator::PhysicalHashAggregate(PhysicalHashAggregateOp {
                mode: AggMode::Single,
                group_by: intern_exprs(
                    &mut scalars,
                    &[
                        column_ref(input_id, "a"),
                        column_ref(grouping_output_id, "__grouping_fn_0"),
                    ],
                ),
                aggregates: vec![],
                output_layout: AggregateOutputLayout::new(aggregate_output_columns.clone(), vec![]),
                output_columns: aggregate_output_columns,
                is_merge: vec![],
            }),
            children: vec![distribution],
            stats: Statistics::default(),
            explain_stats: crate::sql::optimizer::physical_tree::OptimizerExplainStats::default(),
            output_columns: vec![
                int_col(input_id, "a"),
                int_col(grouping_output_id, "__grouping_fn_0"),
            ],
            execution_props: PlanExecutionProps::default(),
        };
        attach_scalar_arena(&mut aggregate, Arc::new(scalars));

        verify_optimizer_id_binding(&aggregate)
            .expect("distribution must preserve Repeat grouping output id for aggregate grouping");
    }

    #[test]
    fn p3_cte_consume_rejects_mapping_arity_mismatch() {
        let consumer_a = ColumnId::new_for_test(1);
        let consumer_b = ColumnId::new_for_test(2);
        let producer_a = ColumnId::new_for_test(11);
        let output_columns = vec![int_col(consumer_a, "a"), int_col(consumer_b, "b")];
        let mut plan = OptimizerPhysicalNode {
            op: Operator::PhysicalCTEConsume(CTEConsumeOp {
                cte_id: 9,
                alias: "cte9".to_string(),
                output_columns: output_columns.clone(),
                producer_column_ids: vec![producer_a],
            }),
            children: vec![],
            stats: Statistics::default(),
            explain_stats: crate::sql::optimizer::physical_tree::OptimizerExplainStats::default(),
            output_columns,
            execution_props: PlanExecutionProps::default(),
        };
        attach_scalar_arena(&mut plan, Arc::new(ScalarArena::new()));

        let err =
            verify_optimizer_id_binding(&plan).expect_err("CTEConsume arity mismatch must fail");
        assert!(
            err.contains("CTEConsume output/producers arity mismatch for cte_id=9"),
            "unexpected err={err}"
        );
    }
}
