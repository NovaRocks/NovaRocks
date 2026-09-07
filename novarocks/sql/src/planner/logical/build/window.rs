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

use arrow::datatypes::DataType;

use crate::analysis::*;
use crate::column_id::{ColumnId, ColumnRefFactory};
use crate::planner::logical::*;
use crate::planner::optimizer_bridge::property::ordering_spec_from_sort_items;
use crate::planner::ordering::OrderingSpec;
use crate::planner::payload::*;

use super::output::plan_output_columns;

/// Check if an expression contains any WindowCall.
/// Build Window + Project nodes if the projection contains window functions,
/// otherwise just a Project node.
pub(super) fn build_window_and_project(
    input: LogicalPlanNode,
    project_items: Vec<ProjectItem>,
    factory: &mut ColumnRefFactory,
) -> Result<LogicalPlanNode, String> {
    let project_items = dedup_project_item_output_ids(project_items, factory);
    let has_window = project_items.iter().any(|item| has_window_call(&item.expr));
    if has_window {
        let mut output_columns = plan_output_columns(&input)?;
        let (window_exprs, rewritten_items) =
            extract_window_calls(&project_items, &mut output_columns, factory);
        // The analytic operator requires input sorted by (partition_by, order_by).
        // Insert a Sort node before the Window node using the first window
        // function's sort keys.  When window functions have different
        // partition/order signatures, `build_distributed_plan` splits them into
        // separate Sort + Analytic nodes in planner/distributed/build/lowering.rs.
        let first_win = &window_exprs[0];
        let mut sort_items = Vec::new();
        for p in &first_win.partition_by {
            sort_items.push(SortItem {
                expr: p.clone(),
                asc: true,
                nulls_first: true,
            });
        }
        for ob in &first_win.order_by {
            sort_items.push(ob.clone());
        }
        // Tag the Sort with the window's partition columns so the optimizer
        // can require Hash(partition_by) distribution from the child instead
        // of forcing Gather — letting the sort run locally per analytic
        // partition. This mirrors StarRocks's
        // `TSortNode.analytic_partition_exprs` mechanism.
        let analytic_partition_by = first_win.partition_by.clone();
        let input_already_ordered =
            logical_plan_satisfies_window_ordering(&input, &sort_items, &analytic_partition_by);
        let sorted_input = if sort_items.is_empty() || input_already_ordered {
            input
        } else {
            LogicalPlanNode::new(
                LogicalPlanKind::Sort(PlanSortNode {
                    items: sort_items,
                    analytic_partition_by,
                    output_columns: vec![],
                    offset: None,
                    partition_limit: None,
                    topn_type: None,
                }),
                vec![input],
                None,
            )
        };

        let windowed = LogicalPlanNode::new(
            LogicalPlanKind::Window(PlanWindowNode {
                window_exprs,
                output_columns,
            }),
            vec![sorted_input],
            None,
        );
        Ok(LogicalPlanNode::new(
            LogicalPlanKind::Project(PlanProjectNode {
                items: rewritten_items,
                output_qualifier: None,
            }),
            vec![windowed],
            None,
        ))
    } else if !project_items.is_empty() {
        Ok(LogicalPlanNode::new(
            LogicalPlanKind::Project(PlanProjectNode {
                items: project_items,
                output_qualifier: None,
            }),
            vec![input],
            None,
        ))
    } else {
        Ok(input)
    }
}

fn dedup_project_item_output_ids(
    mut project_items: Vec<ProjectItem>,
    factory: &mut ColumnRefFactory,
) -> Vec<ProjectItem> {
    let mut seen = std::collections::HashSet::new();
    for item in &mut project_items {
        if item.output_column_id != ColumnId::UNSET && seen.insert(item.output_column_id) {
            continue;
        }
        item.output_column_id = factory.create(
            None,
            item.output_name.clone(),
            item.expr.data_type.clone(),
            item.expr.nullable,
        );
        seen.insert(item.output_column_id);
    }
    project_items
}

fn logical_plan_satisfies_window_ordering(
    input: &LogicalPlanNode,
    required_items: &[SortItem],
    partition_by: &[TypedExpr],
) -> bool {
    match &input.kind {
        LogicalPlanKind::Project(project) if project_preserves_column_identity(project) => {
            logical_plan_satisfies_window_ordering(
                input.unary_input(),
                required_items,
                partition_by,
            )
        }
        LogicalPlanKind::Sort(sort) => {
            logical_sort_satisfies_window_ordering(sort, required_items, partition_by)
        }
        _ => false,
    }
}

fn project_preserves_column_identity(project: &PlanProjectNode) -> bool {
    project.items.iter().all(|item| {
        matches!(
            &item.expr.kind,
            ExprKind::ColumnRef { column_id, .. } if item.output_column_id == *column_id
        )
    })
}

fn logical_sort_satisfies_window_ordering(
    sort: &PlanSortNode,
    required_items: &[SortItem],
    partition_by: &[TypedExpr],
) -> bool {
    let required = ordering_spec_from_sort_items(required_items);
    let provided = ordering_spec_from_sort_items(&sort.items);
    if matches!(required, OrderingSpec::Any) || !provided.satisfies(&required) {
        return false;
    }
    // A regular ORDER BY Sort gathers globally. That is enough only for
    // non-partitioned windows; partitioned windows need the analytic-partition
    // tag unless the child Sort already has an equivalent tag.
    partition_by.is_empty()
        || ordering_spec_from_sort_items(
            &sort
                .analytic_partition_by
                .iter()
                .map(|expr| SortItem {
                    expr: expr.clone(),
                    asc: true,
                    nulls_first: true,
                })
                .collect::<Vec<_>>(),
        )
        .satisfies(&ordering_spec_from_sort_items(
            &partition_by
                .iter()
                .map(|expr| SortItem {
                    expr: expr.clone(),
                    asc: true,
                    nulls_first: true,
                })
                .collect::<Vec<_>>(),
        ))
}

fn has_window_call(expr: &TypedExpr) -> bool {
    match &expr.kind {
        ExprKind::WindowCall { .. } => true,
        ExprKind::BinaryOp { left, right, .. } => has_window_call(left) || has_window_call(right),
        ExprKind::UnaryOp { expr, .. } => has_window_call(expr),
        ExprKind::FunctionCall { args, .. } | ExprKind::AggregateCall { args, .. } => {
            args.iter().any(has_window_call)
        }
        ExprKind::Cast { expr, .. } => has_window_call(expr),
        ExprKind::IsNull { expr, .. } | ExprKind::IsTruthValue { expr, .. } => {
            has_window_call(expr)
        }
        ExprKind::InList { expr, list, .. } => {
            has_window_call(expr) || list.iter().any(has_window_call)
        }
        ExprKind::Between {
            expr, low, high, ..
        } => has_window_call(expr) || has_window_call(low) || has_window_call(high),
        ExprKind::Like { expr, pattern, .. } => has_window_call(expr) || has_window_call(pattern),
        ExprKind::Case {
            operand,
            when_then,
            else_expr,
        } => {
            operand.as_deref().is_some_and(has_window_call)
                || when_then
                    .iter()
                    .any(|(when, then)| has_window_call(when) || has_window_call(then))
                || else_expr.as_deref().is_some_and(has_window_call)
        }
        ExprKind::Nested(inner) => has_window_call(inner),
        _ => false,
    }
}

/// Converse of a window-frame boundary: swap PRECEDING ↔ FOLLOWING (including
/// unbounded variants) and leave CURRENT_ROW alone. Matches StarRocks FE
/// `AnalyticWindowBoundary.BoundaryType.converse()`.
fn converse_window_bound(bound: &WindowBound) -> WindowBound {
    match bound {
        WindowBound::UnboundedPreceding => WindowBound::UnboundedFollowing,
        WindowBound::UnboundedFollowing => WindowBound::UnboundedPreceding,
        WindowBound::Preceding(n) => WindowBound::Following(*n),
        WindowBound::Following(n) => WindowBound::Preceding(*n),
        WindowBound::CurrentRow => WindowBound::CurrentRow,
    }
}

/// Reverse a window frame in place: new_start = converse(old_end),
/// new_end = converse(old_start). Mirrors StarRocks FE
/// `AnalyticWindow.reverse()`.
fn reverse_window_frame(frame: &WindowFrame) -> WindowFrame {
    WindowFrame {
        frame_type: frame.frame_type,
        start: converse_window_bound(&frame.end),
        end: converse_window_bound(&frame.start),
    }
}

/// Normalize a window frame so the BE only sees frames whose start is
/// UNBOUNDED PRECEDING. When the original frame ends at UNBOUNDED FOLLOWING
/// and does not start at UNBOUNDED PRECEDING, we reverse the ORDER BY
/// direction and converse the frame bounds. For FIRST_VALUE / LAST_VALUE we
/// also swap the function name because reversing the iteration flips which
/// row is "first" vs "last".
///
/// Mirrors StarRocks FE `WindowTransformer.visit(AnalyticExpr)`.
fn normalize_window_frame_for_be(
    name: &str,
    order_by: Vec<SortItem>,
    window_frame: Option<WindowFrame>,
) -> (String, Vec<SortItem>, Option<WindowFrame>) {
    let Some(frame) = window_frame else {
        return (name.to_string(), order_by, None);
    };

    let needs_reverse = matches!(frame.end, WindowBound::UnboundedFollowing)
        && !matches!(frame.start, WindowBound::UnboundedPreceding);
    if !needs_reverse {
        return (name.to_string(), order_by, Some(frame));
    }

    let reversed_order_by = order_by
        .into_iter()
        .map(|item| SortItem {
            expr: item.expr,
            asc: !item.asc,
            nulls_first: !item.nulls_first,
        })
        .collect();
    let reversed_frame = reverse_window_frame(&frame);

    let reversed_name = match name.to_ascii_lowercase().as_str() {
        "first_value" => "last_value".to_string(),
        "last_value" => "first_value".to_string(),
        _ => name.to_string(),
    };

    (reversed_name, reversed_order_by, Some(reversed_frame))
}

/// Extract window function calls from the projection items.
/// Returns (window_exprs, rewritten_projection_items).
/// Each window call is replaced with a ColumnRef to its output name.
/// Window calls may be nested inside expressions (e.g., `sum(x) * 100 / sum(sum(x)) OVER (...)`).
fn extract_window_calls(
    items: &[ProjectItem],
    output_columns: &mut Vec<OutputColumn>,
    factory: &mut ColumnRefFactory,
) -> (Vec<WindowExpr>, Vec<ProjectItem>) {
    let mut window_exprs = Vec::new();
    let mut rewritten = Vec::new();

    for item in items {
        if has_window_call(&item.expr) {
            let mut counter = 0usize;
            let mut output_ids = WindowOutputIdAllocator {
                factory,
                output_columns,
                visible_output_column_id: item.output_column_id,
                reuse_visible_output_id: is_exact_window_call(&item.expr),
                visible_output_id_used: false,
            };
            let new_expr = rewrite_window_calls(
                &item.expr,
                &item.output_name,
                &mut output_ids,
                &mut window_exprs,
                &mut counter,
            );
            rewritten.push(ProjectItem {
                expr: new_expr,
                output_name: item.output_name.clone(),
                output_column_id: item.output_column_id,
            });
        } else {
            rewritten.push(item.clone());
        }
    }

    (window_exprs, rewritten)
}

struct WindowOutputIdAllocator<'a> {
    factory: &'a mut ColumnRefFactory,
    output_columns: &'a mut Vec<OutputColumn>,
    visible_output_column_id: ColumnId,
    reuse_visible_output_id: bool,
    visible_output_id_used: bool,
}

impl WindowOutputIdAllocator<'_> {
    fn allocate(&mut self, output_name: &str, data_type: DataType, nullable: bool) -> ColumnId {
        let reuse_visible_output_id = self.reuse_visible_output_id
            && !self.visible_output_id_used
            && self.visible_output_column_id != ColumnId::UNSET;
        let column_id = if reuse_visible_output_id {
            self.visible_output_id_used = true;
            self.visible_output_column_id
        } else {
            self.factory
                .create(None, output_name.to_string(), data_type.clone(), nullable)
        };
        self.output_columns.push(OutputColumn {
            column_id,
            name: output_name.to_string(),
            data_type,
            nullable,
            is_internal: !reuse_visible_output_id,
        });
        column_id
    }
}

fn is_exact_window_call(expr: &TypedExpr) -> bool {
    match &expr.kind {
        ExprKind::WindowCall { .. } => true,
        ExprKind::Nested(inner) => is_exact_window_call(inner),
        _ => false,
    }
}

/// Recursively rewrite an expression tree, replacing each WindowCall node
/// with a ColumnRef that points to the window function's output column.
fn rewrite_window_calls(
    expr: &TypedExpr,
    base_name: &str,
    output_ids: &mut WindowOutputIdAllocator<'_>,
    window_exprs: &mut Vec<WindowExpr>,
    counter: &mut usize,
) -> TypedExpr {
    match &expr.kind {
        ExprKind::WindowCall {
            name,
            args,
            distinct,
            function_order_by,
            aggregate_binding,
            partition_by,
            order_by,
            window_frame,
            ignore_nulls,
        } => {
            let win_output_name = if *counter == 0 {
                base_name.to_string()
            } else {
                format!("{}__win{}", base_name, counter)
            };
            *counter += 1;

            // Normalize frames that end at UNBOUNDED FOLLOWING by reversing the
            // ORDER BY direction and converse-ing the frame bounds, so the BE
            // only sees frames whose start is UNBOUNDED PRECEDING. This mirrors
            // StarRocks FE `WindowTransformer.visit(AnalyticExpr)` which reverses
            // such frames before lowering; the BE analytor relies on this
            // invariant (it `DCHECK`s !window_start for RANGE and assumes
            // cumulative processing).
            //
            // For FIRST_VALUE / LAST_VALUE the reversal also swaps the function
            // because reversing the iteration direction inverts which row is
            // "first" vs "last".
            let (rewritten_name, rewritten_order_by, rewritten_frame) =
                normalize_window_frame_for_be(name, order_by.clone(), window_frame.clone());
            let output_column_id =
                output_ids.allocate(&win_output_name, expr.data_type.clone(), expr.nullable);

            window_exprs.push(WindowExpr {
                name: rewritten_name,
                args: args.clone(),
                distinct: *distinct,
                function_order_by: function_order_by.clone(),
                aggregate_binding: aggregate_binding.clone(),
                partition_by: partition_by.clone(),
                order_by: rewritten_order_by,
                window_frame: rewritten_frame,
                result_type: expr.data_type.clone(),
                output_name: win_output_name.clone(),
                output_column_id,
                ignore_nulls: *ignore_nulls,
            });
            TypedExpr {
                kind: ExprKind::ColumnRef {
                    column_id: output_column_id,
                    qualifier: None,
                    column: win_output_name,
                },
                data_type: expr.data_type.clone(),
                nullable: expr.nullable,
            }
        }
        ExprKind::BinaryOp { left, right, op } => TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(rewrite_window_calls(
                    left,
                    base_name,
                    output_ids,
                    window_exprs,
                    counter,
                )),
                op: *op,
                right: Box::new(rewrite_window_calls(
                    right,
                    base_name,
                    output_ids,
                    window_exprs,
                    counter,
                )),
            },
            data_type: expr.data_type.clone(),
            nullable: expr.nullable,
        },
        ExprKind::UnaryOp { op, expr: inner } => TypedExpr {
            kind: ExprKind::UnaryOp {
                op: *op,
                expr: Box::new(rewrite_window_calls(
                    inner,
                    base_name,
                    output_ids,
                    window_exprs,
                    counter,
                )),
            },
            data_type: expr.data_type.clone(),
            nullable: expr.nullable,
        },
        ExprKind::FunctionCall {
            name,
            args,
            distinct,
            volatility,
        } => TypedExpr {
            kind: ExprKind::FunctionCall {
                name: name.clone(),
                args: args
                    .iter()
                    .map(|arg| {
                        rewrite_window_calls(arg, base_name, output_ids, window_exprs, counter)
                    })
                    .collect(),
                distinct: *distinct,
                volatility: *volatility,
            },
            data_type: expr.data_type.clone(),
            nullable: expr.nullable,
        },
        ExprKind::AggregateCall {
            name,
            args,
            distinct,
            order_by,
            resolved,
        } => TypedExpr {
            kind: ExprKind::AggregateCall {
                name: name.clone(),
                args: args
                    .iter()
                    .map(|arg| {
                        rewrite_window_calls(arg, base_name, output_ids, window_exprs, counter)
                    })
                    .collect(),
                distinct: *distinct,
                order_by: order_by
                    .iter()
                    .map(|item| SortItem {
                        expr: rewrite_window_calls(
                            &item.expr,
                            base_name,
                            output_ids,
                            window_exprs,
                            counter,
                        ),
                        asc: item.asc,
                        nulls_first: item.nulls_first,
                    })
                    .collect(),
                resolved: resolved.clone(),
            },
            data_type: expr.data_type.clone(),
            nullable: expr.nullable,
        },
        ExprKind::Cast {
            expr: inner,
            target,
        } => TypedExpr {
            kind: ExprKind::Cast {
                expr: Box::new(rewrite_window_calls(
                    inner,
                    base_name,
                    output_ids,
                    window_exprs,
                    counter,
                )),
                target: target.clone(),
            },
            data_type: expr.data_type.clone(),
            nullable: expr.nullable,
        },
        ExprKind::IsNull {
            expr: inner,
            negated,
        } => TypedExpr {
            kind: ExprKind::IsNull {
                expr: Box::new(rewrite_window_calls(
                    inner,
                    base_name,
                    output_ids,
                    window_exprs,
                    counter,
                )),
                negated: *negated,
            },
            data_type: expr.data_type.clone(),
            nullable: expr.nullable,
        },
        ExprKind::InList {
            expr: inner,
            list,
            negated,
        } => TypedExpr {
            kind: ExprKind::InList {
                expr: Box::new(rewrite_window_calls(
                    inner,
                    base_name,
                    output_ids,
                    window_exprs,
                    counter,
                )),
                list: list
                    .iter()
                    .map(|item| {
                        rewrite_window_calls(item, base_name, output_ids, window_exprs, counter)
                    })
                    .collect(),
                negated: *negated,
            },
            data_type: expr.data_type.clone(),
            nullable: expr.nullable,
        },
        ExprKind::Between {
            expr: inner,
            low,
            high,
            negated,
        } => TypedExpr {
            kind: ExprKind::Between {
                expr: Box::new(rewrite_window_calls(
                    inner,
                    base_name,
                    output_ids,
                    window_exprs,
                    counter,
                )),
                low: Box::new(rewrite_window_calls(
                    low,
                    base_name,
                    output_ids,
                    window_exprs,
                    counter,
                )),
                high: Box::new(rewrite_window_calls(
                    high,
                    base_name,
                    output_ids,
                    window_exprs,
                    counter,
                )),
                negated: *negated,
            },
            data_type: expr.data_type.clone(),
            nullable: expr.nullable,
        },
        ExprKind::Like {
            expr: inner,
            pattern,
            negated,
        } => TypedExpr {
            kind: ExprKind::Like {
                expr: Box::new(rewrite_window_calls(
                    inner,
                    base_name,
                    output_ids,
                    window_exprs,
                    counter,
                )),
                pattern: Box::new(rewrite_window_calls(
                    pattern,
                    base_name,
                    output_ids,
                    window_exprs,
                    counter,
                )),
                negated: *negated,
            },
            data_type: expr.data_type.clone(),
            nullable: expr.nullable,
        },
        ExprKind::Case {
            operand,
            when_then,
            else_expr,
        } => TypedExpr {
            kind: ExprKind::Case {
                operand: operand.as_ref().map(|inner| {
                    Box::new(rewrite_window_calls(
                        inner,
                        base_name,
                        output_ids,
                        window_exprs,
                        counter,
                    ))
                }),
                when_then: when_then
                    .iter()
                    .map(|(when, then)| {
                        (
                            rewrite_window_calls(
                                when,
                                base_name,
                                output_ids,
                                window_exprs,
                                counter,
                            ),
                            rewrite_window_calls(
                                then,
                                base_name,
                                output_ids,
                                window_exprs,
                                counter,
                            ),
                        )
                    })
                    .collect(),
                else_expr: else_expr.as_ref().map(|inner| {
                    Box::new(rewrite_window_calls(
                        inner,
                        base_name,
                        output_ids,
                        window_exprs,
                        counter,
                    ))
                }),
            },
            data_type: expr.data_type.clone(),
            nullable: expr.nullable,
        },
        ExprKind::IsTruthValue {
            expr: inner,
            value,
            negated,
        } => TypedExpr {
            kind: ExprKind::IsTruthValue {
                expr: Box::new(rewrite_window_calls(
                    inner,
                    base_name,
                    output_ids,
                    window_exprs,
                    counter,
                )),
                value: *value,
                negated: *negated,
            },
            data_type: expr.data_type.clone(),
            nullable: expr.nullable,
        },
        ExprKind::Nested(inner) => TypedExpr {
            kind: ExprKind::Nested(Box::new(rewrite_window_calls(
                inner,
                base_name,
                output_ids,
                window_exprs,
                counter,
            ))),
            data_type: expr.data_type.clone(),
            nullable: expr.nullable,
        },
        // For any other node types, return as-is (no window calls inside)
        _ => expr.clone(),
    }
}
