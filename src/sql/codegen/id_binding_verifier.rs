use std::collections::HashSet;

use crate::sql::analysis::{ExprKind, SortItem, TypedExpr};
use crate::sql::column_id::ColumnId;
use crate::sql::optimizer::operator::{
    AggregateStateMergeOp, DecodeOp, GenerateSeriesOp, Operator, PhysicalDistributionOp,
    PhysicalHashAggregateOp, PhysicalHashJoinOp, PhysicalNestLoopJoinOp, ProjectOp, RepeatOp,
    TableFunctionOp, WindowOp,
};
use crate::sql::optimizer::physical_plan::PhysicalPlanNode;
use crate::sql::optimizer::property::DistributionSpec;
use crate::sql::optimizer::scalar::ScalarArena;
use crate::sql::planner::optimizer_bridge::scalar::materialize;
use crate::sql::planner::optimizer_bridge::scalar::{
    materialize_aggregate_calls, materialize_exprs, materialize_project_items,
    materialize_sort_keys, materialize_window_exprs,
};

pub(crate) fn verify_id_binding(plan: &PhysicalPlanNode) -> Result<(), String> {
    let scalars = plan
        .execution_props
        .scalar_arena
        .as_deref()
        .ok_or_else(|| {
            "PhysicalPlanNode missing scalar arena for codegen id verification".to_string()
        })?;
    verify_node(plan, scalars).map(|_| ())
}

fn verify_node(
    node: &PhysicalPlanNode,
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
        Operator::PhysicalDecode(op) => verify_decode(op, &child_outputs),

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
        Operator::PhysicalAggregateStateMerge(op) => {
            verify_aggregate_state_merge(op, &child_outputs)
        }

        other => Err(format!(
            "non-physical operator reached codegen id verifier: {:?}",
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
    let aggregates = materialize_aggregate_calls(
        scalars,
        &op.aggregates,
        op.group_by.len(),
        &op.output_columns,
    );
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
    for column in op.output_columns.iter().take(op.group_by.len()) {
        verify_output_id(column.column_id, "PhysicalHashAggregate group output")?;
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

fn verify_decode(
    op: &DecodeOp,
    child_outputs: &[HashSet<ColumnId>],
) -> Result<HashSet<ColumnId>, String> {
    let input = only_child(child_outputs, "PhysicalDecode")?;
    let output_ids = output_ids(op.output_columns.iter().map(|c| c.column_id));
    for mapping in &op.mappings {
        verify_input_id(mapping.source_column_id, input, "PhysicalDecode source")?;
        verify_output_id(mapping.output_column_id, "PhysicalDecode output")?;
        if !output_ids.contains(&mapping.output_column_id) {
            return Err(format!(
                "PhysicalDecode mapping output ColumnId({}) is not in Decode output columns",
                mapping.output_column_id.0
            ));
        }
    }
    Ok(output_ids)
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

fn verify_aggregate_state_merge(
    op: &AggregateStateMergeOp,
    child_outputs: &[HashSet<ColumnId>],
) -> Result<HashSet<ColumnId>, String> {
    if child_outputs.len() != 2 {
        return Err(format!(
            "PhysicalAggregateStateMerge expected 2 children, got {}",
            child_outputs.len()
        ));
    }
    // IMV AggregateStateMerge still has internal machine columns addressed by
    // names in its dedicated operator metadata. It does not compile regular
    // ColumnRef expressions here; keep this as the narrow P3 exception.
    Ok(output_ids(op.output_columns.iter().map(|c| c.column_id)))
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

fn declared_node_output_ids(node: &PhysicalPlanNode) -> Result<HashSet<ColumnId>, String> {
    let mut out = HashSet::new();
    for column in &node.output_columns {
        verify_output_id(
            column.column_id,
            &format!("PhysicalPlanNode output `{}` on {:?}", column.name, node.op),
        )?;
        out.insert(column.column_id);
    }
    Ok(out)
}

fn uses_declared_node_outputs(op: &Operator) -> bool {
    !matches!(
        op,
        Operator::PhysicalFilter(_)
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
        AggMode, PhysicalHashAggregateOp, ProjectOp, RepeatOp, ValuesOp,
    };
    use crate::sql::optimizer::physical_plan::{PlanExecutionProps, attach_scalar_arena};
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

    fn values_node(columns: Vec<OutputColumn>) -> PhysicalPlanNode {
        PhysicalPlanNode {
            op: Operator::PhysicalValues(ValuesOp {
                rows: vec![],
                columns: columns.clone(),
            }),
            children: vec![],
            stats: Statistics::default(),
            output_columns: columns,
            execution_props: PlanExecutionProps::default(),
            build_runtime_filters: vec![],
            probe_runtime_filters: vec![],
        }
    }

    fn project_over(
        child: PhysicalPlanNode,
        expr: TypedExpr,
        output_id: ColumnId,
    ) -> PhysicalPlanNode {
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
        let mut plan = PhysicalPlanNode {
            op: Operator::PhysicalProject(ProjectOp {
                items: intern_project_items(&mut scalars, &items),
                output_qualifier: None,
            }),
            children: vec![child],
            stats: Statistics::default(),
            output_columns: vec![int_col(output_id, "p")],
            execution_props: PlanExecutionProps::default(),
            build_runtime_filters: vec![],
            probe_runtime_filters: vec![],
        };
        attach_scalar_arena(&mut plan, Arc::new(scalars));
        plan
    }

    fn hash_aggregate_over(
        child: PhysicalPlanNode,
        aggregate_output_id: ColumnId,
        declared_visible_id: ColumnId,
    ) -> PhysicalPlanNode {
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
        let mut plan = PhysicalPlanNode {
            op: Operator::PhysicalHashAggregate(PhysicalHashAggregateOp {
                mode: AggMode::Single,
                group_by: vec![],
                aggregates: intern_aggregate_calls(&mut scalars, &aggregate_calls),
                output_columns: vec![int_col(aggregate_output_id, "sum(a)")],
                is_merge: vec![false],
            }),
            children: vec![child],
            stats: Statistics::default(),
            output_columns: vec![int_col(declared_visible_id, "sum(a) + 1")],
            execution_props: PlanExecutionProps::default(),
            build_runtime_filters: vec![],
            probe_runtime_filters: vec![],
        };
        attach_scalar_arena(&mut plan, Arc::new(scalars));
        plan
    }

    fn repeat_over(child: PhysicalPlanNode, grouping_output_id: ColumnId) -> PhysicalPlanNode {
        let rollup_id = ColumnId::new_for_test(1);
        PhysicalPlanNode {
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
            output_columns: vec![int_col(rollup_id, "a")],
            execution_props: PlanExecutionProps::default(),
            build_runtime_filters: vec![],
            probe_runtime_filters: vec![],
        }
    }

    #[test]
    fn p3_verify_id_binding_rejects_unset_columnref() {
        let err = verify_expr(
            &column_ref(ColumnId::UNSET, "a"),
            &std::collections::HashSet::new(),
            "test expr",
        )
        .expect_err("UNSET ColumnRef must fail");
        assert!(err.contains("UNSET ColumnRef"), "unexpected err={err}");
    }

    #[test]
    fn p3_verify_id_binding_rejects_missing_input_binding() {
        let input_id = ColumnId::new_for_test(1);
        let missing_id = ColumnId::new_for_test(99);
        let output_id = ColumnId::new_for_test(2);
        let plan = project_over(
            values_node(vec![int_col(input_id, "a")]),
            column_ref(missing_id, "a"),
            output_id,
        );

        let err = verify_id_binding(&plan).expect_err("missing ColumnId must fail");
        assert!(
            err.contains("not produced by child scope"),
            "unexpected err={err}"
        );
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

        verify_id_binding(&plan).expect("project should bind aggregate call output id");
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

        verify_id_binding(&plan).expect("project should bind Repeat grouping output id");
    }

    #[test]
    fn p3_distribution_over_repeat_preserves_grouping_ids() {
        let input_id = ColumnId::new_for_test(1);
        let grouping_output_id = ColumnId::new_for_test(4);
        let repeat = repeat_over(
            values_node(vec![int_col(input_id, "a")]),
            grouping_output_id,
        );
        let distribution = PhysicalPlanNode {
            op: Operator::PhysicalDistribution(PhysicalDistributionOp {
                spec: DistributionSpec::Gather,
            }),
            children: vec![repeat],
            stats: Statistics::default(),
            output_columns: vec![int_col(input_id, "a")],
            execution_props: PlanExecutionProps::default(),
            build_runtime_filters: vec![],
            probe_runtime_filters: vec![],
        };
        let mut scalars = ScalarArena::new();
        let mut aggregate = PhysicalPlanNode {
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
                output_columns: vec![
                    int_col(input_id, "a"),
                    int_col(grouping_output_id, "__grouping_fn_0"),
                ],
                is_merge: vec![],
            }),
            children: vec![distribution],
            stats: Statistics::default(),
            output_columns: vec![
                int_col(input_id, "a"),
                int_col(grouping_output_id, "__grouping_fn_0"),
            ],
            execution_props: PlanExecutionProps::default(),
            build_runtime_filters: vec![],
            probe_runtime_filters: vec![],
        };
        attach_scalar_arena(&mut aggregate, Arc::new(scalars));

        verify_id_binding(&aggregate)
            .expect("distribution must preserve Repeat grouping output id for aggregate grouping");
    }
}
