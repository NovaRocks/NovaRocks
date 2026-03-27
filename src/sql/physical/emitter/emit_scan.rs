use crate::sql::plan::*;

use crate::sql::physical::expr_compiler::ExprCompiler;
use crate::sql::physical::nodes;
use crate::sql::physical::resolve::{ColumnBinding, ExprScope, ResolvedTable};

use super::EmitResult;

impl<'a> super::ThriftEmitter<'a> {
    pub(super) fn emit_scan(&mut self, node: ScanNode) -> Result<EmitResult, String> {
        let scan_tuple_id = self.alloc_tuple();
        let scan_node_id = self.alloc_node();

        let mut scope = ExprScope::new();
        let qualifier = node.alias.as_deref().or(Some(&node.table.name));

        // Determine which columns to emit: use required_columns if the
        // optimizer has pruned, otherwise emit all columns.
        let required: Option<std::collections::HashSet<String>> = node
            .required_columns
            .as_ref()
            .map(|cols| cols.iter().map(|c| c.to_lowercase()).collect());

        for (idx, col) in node.table.columns.iter().enumerate() {
            if let Some(ref req) = required {
                if !req.contains(&col.name.to_lowercase()) {
                    continue;
                }
            }
            let slot_id = self.alloc_slot();
            self.desc_builder.add_slot(
                slot_id,
                scan_tuple_id,
                &col.name,
                &col.data_type,
                col.nullable,
                idx as i32,
            );
            let binding = ColumnBinding {
                tuple_id: scan_tuple_id,
                slot_id,
                data_type: col.data_type.clone(),
                nullable: col.nullable,
            };
            scope.add_column(
                qualifier.map(|s| s.to_string()),
                col.name.clone(),
                binding.clone(),
            );
            // When an alias is present, also register with the original table name
            if node
                .alias
                .as_deref()
                .is_some_and(|a| !a.eq_ignore_ascii_case(&node.table.name))
            {
                scope.add_column(Some(node.table.name.clone()), col.name.clone(), binding);
            }
        }
        self.desc_builder.add_tuple(scan_tuple_id);

        // Compile predicates pushed down by the optimizer
        let pushed_conjuncts = if node.predicates.is_empty() {
            vec![]
        } else {
            let mut conjuncts = Vec::new();
            for pred in &node.predicates {
                let mut compiler = ExprCompiler::new(&scope);
                conjuncts.push(compiler.compile_typed(pred)?);
            }
            conjuncts
        };

        let resolved = ResolvedTable {
            database: node.database.clone(),
            table: node.table.clone(),
            alias: node.alias.clone(),
        };

        let scan_plan_node =
            nodes::build_scan_node(scan_node_id, scan_tuple_id, &resolved, pushed_conjuncts);

        self.scan_tables.push((scan_node_id, resolved));

        Ok(EmitResult {
            plan_nodes: vec![scan_plan_node],
            scope,
            tuple_ids: vec![scan_tuple_id],
        })
    }

    pub(super) fn emit_filter(&mut self, node: FilterNode) -> Result<EmitResult, String> {
        let mut child = self.emit_node(*node.input)?;

        // Compile the predicate against the child scope
        let conjuncts = self.split_and_compile_conjuncts(&node.predicate, &child.scope)?;

        if !conjuncts.is_empty() {
            // Push conjuncts onto the first (scan) node if it has none yet
            if let Some(scan) = child.plan_nodes.first_mut() {
                if scan.conjuncts.is_none() {
                    scan.conjuncts = Some(conjuncts);
                } else {
                    // Append to existing conjuncts
                    scan.conjuncts.as_mut().unwrap().extend(conjuncts);
                }
            }
        }

        Ok(child)
    }
}
