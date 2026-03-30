use crate::plan_nodes;
use crate::sql::plan::*;

use crate::sql::physical::expr_compiler;
use crate::sql::physical::nodes;
use crate::sql::physical::resolve::{ColumnBinding, ExprScope};
use crate::sql::physical::type_infer;

use super::EmitResult;

impl<'a> super::ThriftEmitter<'a> {
    pub(super) fn emit_union(&mut self, node: UnionNode) -> Result<EmitResult, String> {
        let union_result = self.emit_set_op_common(
            &node.inputs,
            plan_nodes::TPlanNodeType::UNION_NODE,
            |plan_node, tnode| {
                plan_node.union_node = Some(tnode);
            },
        )?;

        if node.all {
            // UNION ALL — no dedup needed
            Ok(union_result)
        } else {
            // UNION DISTINCT — add an AGG node that groups by all output columns
            // (no aggregate functions, just GROUP BY = DISTINCT)
            self.emit_distinct_on_top(union_result)
        }
    }

    pub(super) fn emit_intersect(&mut self, node: IntersectNode) -> Result<EmitResult, String> {
        self.emit_set_op_common(
            &node.inputs,
            plan_nodes::TPlanNodeType::INTERSECT_NODE,
            |plan_node, tnode| {
                plan_node.intersect_node = Some(plan_nodes::TIntersectNode {
                    tuple_id: tnode.tuple_id,
                    result_expr_lists: tnode.result_expr_lists,
                    const_expr_lists: tnode.const_expr_lists,
                    first_materialized_child_idx: tnode.first_materialized_child_idx,
                    has_outer_join_child: None,
                    local_partition_by_exprs: None,
                });
            },
        )
    }

    pub(super) fn emit_except(&mut self, node: ExceptNode) -> Result<EmitResult, String> {
        self.emit_set_op_common(
            &node.inputs,
            plan_nodes::TPlanNodeType::EXCEPT_NODE,
            |plan_node, tnode| {
                plan_node.except_node = Some(plan_nodes::TExceptNode {
                    tuple_id: tnode.tuple_id,
                    result_expr_lists: tnode.result_expr_lists,
                    const_expr_lists: tnode.const_expr_lists,
                    first_materialized_child_idx: tnode.first_materialized_child_idx,
                    local_partition_by_exprs: None,
                });
            },
        )
    }

    /// Add a DISTINCT (GROUP BY all columns, no agg functions) on top of a child result.
    pub(super) fn emit_distinct_on_top(&mut self, child: EmitResult) -> Result<EmitResult, String> {
        let agg_tuple_id = self.alloc_tuple();
        let agg_node_id = self.alloc_node();

        let mut agg_scope = ExprScope::new();
        let mut grouping_exprs = Vec::new();

        let child_cols: Vec<(String, ColumnBinding)> = child
            .scope
            .iter_columns()
            .map(|(n, b)| (n.clone(), b.clone()))
            .collect();

        for (idx, (name, binding)) in child_cols.iter().enumerate() {
            // Build a slot-ref TExpr pointing to the child's output
            let type_desc = type_infer::arrow_type_to_type_desc(&binding.data_type)?;
            let texpr =
                expr_compiler::build_slot_ref_texpr(binding.slot_id, binding.tuple_id, type_desc);
            grouping_exprs.push(texpr);

            let slot_id = self.alloc_slot();
            self.desc_builder.add_slot(
                slot_id,
                agg_tuple_id,
                name,
                &binding.data_type,
                binding.nullable,
                idx as i32,
            );
            agg_scope.add_column(
                None,
                name.clone(),
                ColumnBinding {
                    tuple_id: agg_tuple_id,
                    slot_id,
                    data_type: binding.data_type.clone(),
                    nullable: binding.nullable,
                },
            );
        }

        self.desc_builder.add_tuple(agg_tuple_id);
        let agg_plan_node = nodes::build_aggregation_node(
            agg_node_id,
            agg_tuple_id,
            agg_tuple_id,
            grouping_exprs,
            vec![], // no aggregate functions — pure DISTINCT
        );

        let mut plan_nodes = vec![agg_plan_node];
        plan_nodes.extend(child.plan_nodes);

        Ok(EmitResult {
            plan_nodes,
            scope: agg_scope,
            tuple_ids: vec![agg_tuple_id],
        })
    }

    /// Common helper for UNION/INTERSECT/EXCEPT set operations.
    pub(super) fn emit_set_op_common(
        &mut self,
        inputs: &[LogicalPlan],
        node_type: plan_nodes::TPlanNodeType,
        apply_payload: impl FnOnce(&mut plan_nodes::TPlanNode, plan_nodes::TUnionNode),
    ) -> Result<EmitResult, String> {
        if inputs.is_empty() {
            return Err("set operation node has no inputs".into());
        }

        let mut child_results = Vec::with_capacity(inputs.len());
        for input in inputs {
            child_results.push(self.emit_node(input.clone())?);
        }

        let output_tuple_id = self.alloc_tuple();
        let set_op_node_id = self.alloc_node();

        let mut output_scope = ExprScope::new();
        let first_child_cols: Vec<(String, ColumnBinding)> = child_results[0]
            .scope
            .iter_columns()
            .map(|(name, binding)| (name.clone(), binding.clone()))
            .collect();

        for (idx, (name, child_binding)) in first_child_cols.iter().enumerate() {
            let slot_id = self.alloc_slot();
            self.desc_builder.add_slot(
                slot_id,
                output_tuple_id,
                name,
                &child_binding.data_type,
                child_binding.nullable,
                idx as i32,
            );
            output_scope.add_column(
                None,
                name.clone(),
                ColumnBinding {
                    tuple_id: output_tuple_id,
                    slot_id,
                    data_type: child_binding.data_type.clone(),
                    nullable: child_binding.nullable,
                },
            );
        }
        self.desc_builder.add_tuple(output_tuple_id);

        let mut result_expr_lists = Vec::with_capacity(child_results.len());
        for child_result in &child_results {
            let mut expr_list = Vec::new();
            for (col_idx, (_, child_binding)) in child_result.scope.iter_columns().enumerate() {
                let output_type = first_child_cols.get(col_idx).map(|(_, b)| &b.data_type);
                // If the child column is Null but the output column is a real type
                // (e.g., ROLLUP NULL vs Utf8), insert a CAST so the executor sees
                // the correct type instead of DataType::Null.
                let needs_cast =
                    matches!(child_binding.data_type, arrow::datatypes::DataType::Null)
                        && output_type
                            .is_some_and(|t| !matches!(t, arrow::datatypes::DataType::Null));
                if needs_cast {
                    let target_type = output_type.unwrap();
                    let target_desc = type_infer::arrow_type_to_type_desc(target_type)?;
                    let child_desc = type_infer::arrow_type_to_type_desc(&child_binding.data_type)?;
                    let slot_ref = expr_compiler::build_slot_ref_texpr(
                        child_binding.slot_id,
                        child_binding.tuple_id,
                        child_desc,
                    );
                    expr_list.push(expr_compiler::build_cast_texpr(slot_ref, target_desc));
                } else {
                    let type_desc = type_infer::arrow_type_to_type_desc(&child_binding.data_type)?;
                    expr_list.push(expr_compiler::build_slot_ref_texpr(
                        child_binding.slot_id,
                        child_binding.tuple_id,
                        type_desc,
                    ));
                }
            }
            result_expr_lists.push(expr_list);
        }

        let tnode = plan_nodes::TUnionNode {
            tuple_id: output_tuple_id,
            result_expr_lists,
            const_expr_lists: vec![],
            first_materialized_child_idx: 0,
            pass_through_slot_maps: None,
            local_exchanger_type: None,
            local_partition_by_exprs: None,
        };

        let mut plan_node = nodes::default_plan_node();
        plan_node.node_id = set_op_node_id;
        plan_node.node_type = node_type;
        plan_node.row_tuples = vec![output_tuple_id];
        plan_node.nullable_tuples = vec![];

        apply_payload(&mut plan_node, tnode);

        // num_children is the number of direct child branches, not total plan nodes
        plan_node.num_children = child_results.len() as i32;
        let mut plan_nodes_out = vec![plan_node];
        for child_result in child_results {
            plan_nodes_out.extend(child_result.plan_nodes);
        }

        Ok(EmitResult {
            plan_nodes: plan_nodes_out,
            scope: output_scope,
            tuple_ids: vec![output_tuple_id],
        })
    }
}
