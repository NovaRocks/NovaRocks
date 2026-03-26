use crate::exprs;
use crate::plan_nodes;
use crate::sql::plan::*;

use crate::sql::physical::expr_compiler::ExprCompiler;
use crate::sql::physical::nodes;

use super::EmitResult;

impl<'a> super::ThriftEmitter<'a> {
    pub(super) fn emit_sort(&mut self, node: SortNode) -> Result<EmitResult, String> {
        let child = self.emit_node(*node.input)?;

        let sort_node_id = self.alloc_node();
        // The sort node references the output tuple of its child (PROJECT tuple)
        let sort_tuple_id = *child.tuple_ids.last().unwrap();

        let mut ordering_exprs = Vec::new();
        let mut is_asc = Vec::new();
        let mut nulls_first_list = Vec::new();

        for item in &node.items {
            let mut compiler = ExprCompiler::new(&child.scope);
            let texpr = compiler.compile_typed(&item.expr)?;
            ordering_exprs.push(texpr);
            is_asc.push(item.asc);
            nulls_first_list.push(item.nulls_first);
        }

        let sort_info = plan_nodes::TSortInfo::new(
            ordering_exprs,
            is_asc,
            nulls_first_list,
            None::<Vec<exprs::TExpr>>,
        );

        let mut sort_plan_node = nodes::default_plan_node();
        sort_plan_node.node_id = sort_node_id;
        sort_plan_node.node_type = plan_nodes::TPlanNodeType::SORT_NODE;
        sort_plan_node.num_children = 1;
        sort_plan_node.limit = -1;
        sort_plan_node.row_tuples = vec![sort_tuple_id];
        sort_plan_node.nullable_tuples = vec![];
        sort_plan_node.compact_data = true;
        sort_plan_node.sort_node = Some(plan_nodes::TSortNode {
            sort_info,
            use_top_n: false,
            offset: None,
            ordering_exprs: None,
            is_asc_order: None,
            is_default_limit: None,
            nulls_first: None,
            sort_tuple_slot_exprs: None,
            has_outer_join_child: None,
            sql_sort_keys: None,
            analytic_partition_exprs: None,
            partition_exprs: None,
            partition_limit: None,
            topn_type: None,
            build_runtime_filters: None,
            max_buffered_rows: None,
            max_buffered_bytes: None,
            late_materialization: None,
            enable_parallel_merge: None,
            analytic_partition_skewed: None,
            pre_agg_exprs: None,
            pre_agg_output_slot_id: None,
            pre_agg_insert_local_shuffle: None,
            parallel_merge_late_materialize_mode: None,
            per_pipeline: None,
        });

        // Pre-order: sort first, then child
        let mut plan_nodes = vec![sort_plan_node];
        plan_nodes.extend(child.plan_nodes);

        Ok(EmitResult {
            plan_nodes,
            scope: child.scope,
            tuple_ids: child.tuple_ids,
        })
    }

    pub(super) fn emit_limit(&mut self, node: LimitNode) -> Result<EmitResult, String> {
        let mut child = self.emit_node(*node.input)?;

        // Apply limit/offset to the top-most sort node if present,
        // or to the top-most node otherwise.
        if let Some(top) = child.plan_nodes.first_mut() {
            if top.node_type == plan_nodes::TPlanNodeType::SORT_NODE {
                // Set limit on sort node
                if let Some(limit) = node.limit {
                    top.limit = limit;
                    if let Some(ref mut sn) = top.sort_node {
                        sn.use_top_n = true;
                        sn.offset = node.offset;
                    }
                }
            } else {
                // Set limit on the top node directly
                if let Some(limit) = node.limit {
                    top.limit = limit;
                }
            }
        }

        Ok(child)
    }
}
