use crate::plan_nodes;
use crate::sql::plan::*;

use crate::sql::physical::expr_compiler::ExprCompiler;
use crate::sql::physical::nodes;
use crate::sql::physical::resolve::{ColumnBinding, ExprScope};

use super::EmitResult;

impl<'a> super::ThriftEmitter<'a> {
    pub(super) fn emit_window(&mut self, node: WindowNode) -> Result<EmitResult, String> {
        use crate::sql::ir::{WindowBound, WindowFrameType};

        // Group window expressions by (partition_by, order_by) signature.
        // Different signatures need separate Sort + Analytic nodes.
        let groups = group_win_exprs_by_sig(&node.window_exprs);
        if groups.len() > 1 {
            return self.emit_window_multi_group(node, groups);
        }

        let child = self.emit_node(*node.input)?;
        let analytic_node_id = self.alloc_node();

        // Allocate intermediate and output tuples
        let intermediate_tuple_id = self.alloc_tuple();
        let output_tuple_id = self.alloc_tuple();

        // --- Compile partition_by and order_by from the first window expr ---
        // (All window exprs in a single WindowNode share the same partition/order.)
        let first_win = node.window_exprs.first().ok_or("empty window_exprs")?;

        let mut partition_exprs = Vec::new();
        for expr in &first_win.partition_by {
            let mut compiler = ExprCompiler::new(&child.scope);
            partition_exprs.push(compiler.compile_typed(expr)?);
        }

        let mut order_by_exprs = Vec::new();
        for item in &first_win.order_by {
            let mut compiler = ExprCompiler::new(&child.scope);
            let texpr = compiler.compile_typed(&item.expr)?;
            // Wrap in sort info by using the raw expression
            order_by_exprs.push(texpr);
        }

        // --- Compile analytic functions ---
        let mut analytic_functions = Vec::new();
        for win_expr in &node.window_exprs {
            let mut compiler = ExprCompiler::new(&child.scope);
            let agg_call = crate::sql::plan::AggregateCall {
                name: win_expr.name.clone(),
                args: win_expr.args.clone(),
                distinct: win_expr.distinct,
                result_type: win_expr.result_type.clone(),
                order_by: vec![],
            };
            let texpr = compiler.compile_aggregate_call_typed(&agg_call)?;
            analytic_functions.push(texpr);
        }

        // --- Register intermediate slots (one per window function) ---
        for (idx, win_expr) in node.window_exprs.iter().enumerate() {
            let slot_id = self.alloc_slot();
            self.desc_builder.add_slot(
                slot_id,
                intermediate_tuple_id,
                &format!("__win_intermediate_{idx}"),
                &win_expr.result_type,
                true,
                idx as i32,
            );
        }
        self.desc_builder.add_tuple(intermediate_tuple_id);

        // --- Register output slots (one per window function) ---
        let mut output_scope = ExprScope::new();
        // First, inherit all child scope columns
        for (name, binding) in child.scope.iter_columns() {
            output_scope.add_column(None, name.clone(), binding.clone());
        }
        // Then add window function output columns
        for (idx, win_expr) in node.window_exprs.iter().enumerate() {
            let slot_id = self.alloc_slot();
            self.desc_builder.add_slot(
                slot_id,
                output_tuple_id,
                &win_expr.output_name,
                &win_expr.result_type,
                true,
                idx as i32,
            );
            output_scope.add_column(
                None,
                win_expr.output_name.clone(),
                ColumnBinding {
                    tuple_id: output_tuple_id,
                    slot_id,
                    data_type: win_expr.result_type.clone(),
                    nullable: true,
                },
            );
        }
        self.desc_builder.add_tuple(output_tuple_id);

        // --- Window frame ---
        let window = first_win.window_frame.as_ref().map(|frame| {
            let window_type = match frame.frame_type {
                WindowFrameType::Rows => plan_nodes::TAnalyticWindowType::ROWS,
                WindowFrameType::Range => plan_nodes::TAnalyticWindowType::RANGE,
            };
            let window_start = match &frame.start {
                WindowBound::UnboundedPreceding => None,
                WindowBound::CurrentRow => Some(plan_nodes::TAnalyticWindowBoundary {
                    type_: plan_nodes::TAnalyticWindowBoundaryType::CURRENT_ROW,
                    range_offset_predicate: None,
                    rows_offset_value: None,
                }),
                WindowBound::Preceding(n) => Some(plan_nodes::TAnalyticWindowBoundary {
                    type_: plan_nodes::TAnalyticWindowBoundaryType::PRECEDING,
                    range_offset_predicate: None,
                    rows_offset_value: Some(*n),
                }),
                WindowBound::Following(n) => Some(plan_nodes::TAnalyticWindowBoundary {
                    type_: plan_nodes::TAnalyticWindowBoundaryType::FOLLOWING,
                    range_offset_predicate: None,
                    rows_offset_value: Some(*n),
                }),
                WindowBound::UnboundedFollowing => None,
            };
            let window_end = match &frame.end {
                WindowBound::UnboundedFollowing => None,
                WindowBound::CurrentRow => Some(plan_nodes::TAnalyticWindowBoundary {
                    type_: plan_nodes::TAnalyticWindowBoundaryType::CURRENT_ROW,
                    range_offset_predicate: None,
                    rows_offset_value: None,
                }),
                WindowBound::Following(n) => Some(plan_nodes::TAnalyticWindowBoundary {
                    type_: plan_nodes::TAnalyticWindowBoundaryType::FOLLOWING,
                    range_offset_predicate: None,
                    rows_offset_value: Some(*n),
                }),
                WindowBound::Preceding(n) => Some(plan_nodes::TAnalyticWindowBoundary {
                    type_: plan_nodes::TAnalyticWindowBoundaryType::PRECEDING,
                    range_offset_predicate: None,
                    rows_offset_value: Some(*n),
                }),
                WindowBound::UnboundedPreceding => None,
            };
            plan_nodes::TAnalyticWindow {
                type_: window_type,
                window_start,
                window_end,
            }
        });

        // --- Build TAnalyticNode ---
        let analytic_tnode = plan_nodes::TAnalyticNode {
            partition_exprs,
            order_by_exprs,
            analytic_functions,
            window,
            intermediate_tuple_id,
            output_tuple_id,
            buffered_tuple_id: None,
            partition_by_eq: None,
            order_by_eq: None,
            sql_partition_keys: None,
            sql_aggregate_functions: None,
            has_outer_join_child: None,
            use_hash_based_partition: None,
            is_skewed: None,
        };

        let mut plan_node = nodes::default_plan_node();
        plan_node.node_id = analytic_node_id;
        plan_node.node_type = plan_nodes::TPlanNodeType::ANALYTIC_EVAL_NODE;
        plan_node.num_children = 1;
        plan_node.limit = -1;
        // The output includes both child tuples and the analytic output tuple
        let mut row_tuples = child.tuple_ids.clone();
        row_tuples.push(output_tuple_id);
        plan_node.row_tuples = row_tuples;
        plan_node.nullable_tuples = vec![];
        plan_node.analytic_node = Some(analytic_tnode);

        // Pre-order: analytic node first, then child
        let mut plan_nodes = vec![plan_node];
        plan_nodes.extend(child.plan_nodes);

        Ok(EmitResult {
            plan_nodes,
            scope: output_scope,
            tuple_ids: child.tuple_ids,
        })
    }

    /// Handle window functions with multiple different partition/order signatures.
    /// Each group gets its own Sort + Analytic node, chained sequentially.
    fn emit_window_multi_group(
        &mut self,
        node: WindowNode,
        groups: Vec<Vec<usize>>,
    ) -> Result<EmitResult, String> {
        use crate::sql::physical::nodes as phys_nodes;

        let mut current_result = self.emit_node(*node.input)?;

        for group_indices in &groups {
            let group_exprs: Vec<_> = group_indices
                .iter()
                .map(|&i| node.window_exprs[i].clone())
                .collect();
            let sub_node = WindowNode {
                input: Box::new(crate::sql::plan::LogicalPlan::Values(
                    crate::sql::plan::ValuesNode {
                        rows: vec![],
                        columns: vec![],
                    },
                )), // placeholder, not used
                window_exprs: group_exprs,
                output_columns: node.output_columns.clone(),
            };

            // Build sort node for this group's partition+order
            let first_win = &sub_node.window_exprs[0];
            let mut sort_ordering = Vec::new();
            let mut sort_is_asc = Vec::new();
            let mut sort_nulls_first = Vec::new();
            for expr in &first_win.partition_by {
                let mut compiler = ExprCompiler::new(&current_result.scope);
                sort_ordering.push(compiler.compile_typed(expr)?);
                sort_is_asc.push(true);
                sort_nulls_first.push(true);
            }
            for item in &first_win.order_by {
                let mut compiler = ExprCompiler::new(&current_result.scope);
                sort_ordering.push(compiler.compile_typed(&item.expr)?);
                sort_is_asc.push(item.asc);
                sort_nulls_first.push(item.nulls_first);
            }

            if !sort_ordering.is_empty() {
                let sort_node_id = self.alloc_node();
                let sort_plan = phys_nodes::build_sort_node_raw(
                    sort_node_id,
                    current_result.tuple_ids.clone(),
                    sort_ordering,
                    sort_is_asc,
                    sort_nulls_first,
                    -1,
                    None,
                );
                let mut pnodes = vec![sort_plan];
                pnodes.extend(current_result.plan_nodes);
                current_result.plan_nodes = pnodes;
            }

            // Now build the analytic node for this group
            let analytic_node_id = self.alloc_node();
            let intermediate_tuple_id = self.alloc_tuple();
            let output_tuple_id = self.alloc_tuple();

            let mut partition_exprs = Vec::new();
            for expr in &first_win.partition_by {
                let mut compiler = ExprCompiler::new(&current_result.scope);
                partition_exprs.push(compiler.compile_typed(expr)?);
            }
            let mut order_by_exprs = Vec::new();
            for item in &first_win.order_by {
                let mut compiler = ExprCompiler::new(&current_result.scope);
                order_by_exprs.push(compiler.compile_typed(&item.expr)?);
            }

            let mut analytic_functions = Vec::new();
            for win_expr in &sub_node.window_exprs {
                let mut compiler = ExprCompiler::new(&current_result.scope);
                let agg_call = crate::sql::plan::AggregateCall {
                    name: win_expr.name.clone(),
                    args: win_expr.args.clone(),
                    distinct: win_expr.distinct,
                    result_type: win_expr.result_type.clone(),
                    order_by: vec![],
                };
                analytic_functions.push(compiler.compile_aggregate_call_typed(&agg_call)?);
            }

            for (idx, win_expr) in sub_node.window_exprs.iter().enumerate() {
                let slot_id = self.alloc_slot();
                self.desc_builder.add_slot(
                    slot_id,
                    intermediate_tuple_id,
                    &format!("__win_intermediate_{idx}"),
                    &win_expr.result_type,
                    true,
                    idx as i32,
                );
            }
            self.desc_builder.add_tuple(intermediate_tuple_id);

            let mut output_scope = ExprScope::new();
            for (name, binding) in current_result.scope.iter_columns() {
                output_scope.add_column(None, name.clone(), binding.clone());
            }
            for (idx, win_expr) in sub_node.window_exprs.iter().enumerate() {
                let slot_id = self.alloc_slot();
                self.desc_builder.add_slot(
                    slot_id,
                    output_tuple_id,
                    &win_expr.output_name,
                    &win_expr.result_type,
                    true,
                    idx as i32,
                );
                output_scope.add_column(
                    None,
                    win_expr.output_name.clone(),
                    ColumnBinding {
                        tuple_id: output_tuple_id,
                        slot_id,
                        data_type: win_expr.result_type.clone(),
                        nullable: true,
                    },
                );
            }
            self.desc_builder.add_tuple(output_tuple_id);

            let analytic_tnode = plan_nodes::TAnalyticNode {
                partition_exprs,
                order_by_exprs,
                analytic_functions,
                window: None,
                intermediate_tuple_id,
                output_tuple_id,
                buffered_tuple_id: None,
                partition_by_eq: None,
                order_by_eq: None,
                sql_partition_keys: None,
                sql_aggregate_functions: None,
                has_outer_join_child: None,
                use_hash_based_partition: None,
                is_skewed: None,
            };

            let mut plan_node = phys_nodes::default_plan_node();
            plan_node.node_id = analytic_node_id;
            plan_node.node_type = plan_nodes::TPlanNodeType::ANALYTIC_EVAL_NODE;
            plan_node.num_children = 1;
            plan_node.limit = -1;
            let mut row_tuples = current_result.tuple_ids.clone();
            row_tuples.push(output_tuple_id);
            plan_node.row_tuples = row_tuples;
            plan_node.nullable_tuples = vec![];
            plan_node.analytic_node = Some(analytic_tnode);

            let mut pnodes = vec![plan_node];
            pnodes.extend(current_result.plan_nodes);
            current_result = EmitResult {
                plan_nodes: pnodes,
                scope: output_scope,
                tuple_ids: current_result.tuple_ids,
            };
        }

        Ok(current_result)
    }
}

/// Group window expressions by their (partition_by, order_by) signature.
pub fn group_win_exprs_by_sig(exprs: &[crate::sql::plan::WindowExpr]) -> Vec<Vec<usize>> {
    use crate::sql::ir::ExprKind;
    let sig = |e: &crate::sql::plan::WindowExpr| -> String {
        format!(
            "{:?}|{:?}",
            e.partition_by
                .iter()
                .map(|p| format!("{:?}", p.kind))
                .collect::<Vec<_>>(),
            e.order_by
                .iter()
                .map(|o| format!("{:?}:{}", o.expr.kind, o.asc))
                .collect::<Vec<_>>(),
        )
    };
    let mut groups: Vec<(String, Vec<usize>)> = Vec::new();
    for (i, e) in exprs.iter().enumerate() {
        let s = sig(e);
        if let Some(g) = groups.iter_mut().find(|(gs, _)| *gs == s) {
            g.1.push(i);
        } else {
            groups.push((s, vec![i]));
        }
    }
    groups.into_iter().map(|(_, indices)| indices).collect()
}
