use crate::sql::plan::*;

use crate::sql::physical::expr_compiler::ExprCompiler;
use crate::sql::physical::nodes;
use crate::sql::physical::resolve::{ColumnBinding, ExprScope};

use super::helpers::{agg_call_display_name, typed_expr_display_name};
use super::EmitResult;

impl<'a> super::ThriftEmitter<'a> {
    pub(super) fn emit_aggregate(&mut self, node: AggregateNode) -> Result<EmitResult, String> {
        let child = self.emit_node(*node.input)?;

        let agg_tuple_id = self.alloc_tuple();
        let agg_node_id = self.alloc_node();

        let mut agg_scope = ExprScope::new();
        let mut grouping_exprs = Vec::new();

        // Compile GROUP BY expressions
        for (idx, gb_expr) in node.group_by.iter().enumerate() {
            let mut compiler = ExprCompiler::new(&child.scope);
            let texpr = compiler.compile_typed(gb_expr)?;
            let data_type = gb_expr.data_type.clone();
            let nullable = gb_expr.nullable;
            let name = typed_expr_display_name(gb_expr);
            let slot_id = self.alloc_slot();
            self.desc_builder.add_slot(
                slot_id,
                agg_tuple_id,
                &name,
                &data_type,
                nullable,
                idx as i32,
            );
            agg_scope.add_column(
                None,
                name,
                ColumnBinding {
                    tuple_id: agg_tuple_id,
                    slot_id,
                    data_type,
                    nullable,
                },
            );
            grouping_exprs.push(texpr);
        }

        // Compile aggregate function expressions
        let agg_start_col = node.group_by.len();
        let mut aggregate_functions = Vec::new();

        for (idx, agg_call) in node.aggregates.iter().enumerate() {
            let mut compiler = ExprCompiler::new(&child.scope);
            let texpr =
                compiler.compile_aggregate_call_typed(agg_call)?;
            let data_type = agg_call.result_type.clone();
            let nullable = true;
            let name = agg_call_display_name(agg_call);
            let slot_id = self.alloc_slot();
            let col_pos = (agg_start_col + idx) as i32;
            self.desc_builder.add_slot(
                slot_id,
                agg_tuple_id,
                &name,
                &data_type,
                nullable,
                col_pos,
            );
            agg_scope.add_column(
                None,
                name,
                ColumnBinding {
                    tuple_id: agg_tuple_id,
                    slot_id,
                    data_type,
                    nullable,
                },
            );
            aggregate_functions.push(texpr);
        }

        self.desc_builder.add_tuple(agg_tuple_id);
        let agg_plan_node = nodes::build_aggregation_node(
            agg_node_id,
            agg_tuple_id,
            agg_tuple_id,
            grouping_exprs,
            aggregate_functions,
        );

        // Pre-order: agg first, then child nodes
        let mut plan_nodes = vec![agg_plan_node];
        plan_nodes.extend(child.plan_nodes);

        Ok(EmitResult {
            plan_nodes,
            scope: agg_scope,
            tuple_ids: vec![agg_tuple_id],
        })
    }
}
