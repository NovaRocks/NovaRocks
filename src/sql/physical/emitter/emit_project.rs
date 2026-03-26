use std::collections::BTreeMap;

use crate::sql::ir::ExprKind;
use crate::sql::plan::*;

use crate::sql::physical::expr_compiler::ExprCompiler;
use crate::sql::physical::nodes;
use crate::sql::physical::resolve::{ColumnBinding, ExprScope};
use crate::sql::physical::OutputColumn;

use super::EmitResult;

impl<'a> super::ThriftEmitter<'a> {
    pub(super) fn emit_project(&mut self, node: ProjectNode) -> Result<EmitResult, String> {
        let child = self.emit_node(*node.input)?;

        let project_tuple_id = self.alloc_tuple();
        let project_node_id = self.alloc_node();

        let mut output_columns = Vec::new();
        let mut slot_map = BTreeMap::new();
        let mut project_scope = ExprScope::new();
        let mut slot_ids = Vec::new();

        for item in &node.items {
            let mut compiler = ExprCompiler::new(&child.scope);
            let texpr = compiler.compile_typed(&item.expr)?;
            let data_type = item.expr.data_type.clone();
            let nullable = item.expr.nullable;
            let name = item.output_name.clone();
            let slot_id = self.alloc_slot();
            self.desc_builder.add_slot(
                slot_id,
                project_tuple_id,
                &name,
                &data_type,
                nullable,
                output_columns.len() as i32,
            );
            slot_map.insert(slot_id, texpr);
            slot_ids.push(slot_id);
            output_columns.push(OutputColumn {
                name: name.clone(),
                data_type: data_type.clone(),
                nullable,
            });

            // Add to project scope for downstream use (ORDER BY, etc.)
            project_scope.add_column(
                None,
                name.clone(),
                ColumnBinding {
                    tuple_id: project_tuple_id,
                    slot_id,
                    data_type: data_type.clone(),
                    nullable,
                },
            );

            // Also register with qualifier if the expression is a column ref
            if let ExprKind::ColumnRef {
                qualifier: Some(ref q),
                ref column,
            } = item.expr.kind
            {
                project_scope.add_column(
                    Some(q.clone()),
                    column.clone(),
                    ColumnBinding {
                        tuple_id: project_tuple_id,
                        slot_id,
                        data_type,
                        nullable,
                    },
                );
            }
        }

        self.desc_builder.add_tuple(project_tuple_id);
        let project_plan_node =
            nodes::build_project_node(project_node_id, project_tuple_id, slot_map);

        // Pre-order: project first, then child nodes
        let mut plan_nodes = vec![project_plan_node];
        plan_nodes.extend(child.plan_nodes);

        Ok(EmitResult {
            plan_nodes,
            scope: project_scope,
            tuple_ids: vec![project_tuple_id],
        })
    }
}
