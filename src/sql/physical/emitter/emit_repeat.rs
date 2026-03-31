use std::collections::BTreeSet;

use arrow::datatypes::DataType;

use crate::plan_nodes;
use crate::sql::plan::RepeatPlanNode;

use crate::sql::physical::nodes;
use crate::sql::physical::resolve::{ColumnBinding, ExprScope};

use super::EmitResult;

impl<'a> super::ThriftEmitter<'a> {
    pub(super) fn emit_repeat(&mut self, node: RepeatPlanNode) -> Result<EmitResult, String> {
        let child = self.emit_node(*node.input)?;

        let repeat_node_id = self.alloc_node();

        // The output_tuple_id for TRepeatNode contains ONLY virtual slots
        // (grouping_id, GROUPING() calls). Pass-through columns stay in
        // the child's tuples. The lowering code expects grouping_list.len()
        // == number of slots in output_tuple_id.
        let virtual_tuple_id = self.alloc_tuple();

        // Collect child columns for rollup slot mapping
        let child_cols: Vec<(String, ColumnBinding)> = child
            .scope
            .iter_columns()
            .map(|(n, b)| (n.clone(), b.clone()))
            .collect();

        // Start with the child's full scope (preserves qualified entries like cd1.col)
        let mut output_scope = child.scope;

        // Add virtual slots to the virtual-only tuple
        let num_virtual = 1 + node.grouping_fn_args.len();
        let mut virtual_slot_ids = Vec::with_capacity(num_virtual);

        let grouping_id_slot = self.alloc_slot();
        self.desc_builder.add_slot(
            grouping_id_slot,
            virtual_tuple_id,
            "__grouping_id",
            &DataType::Int64,
            false,
            0,
        );
        output_scope.add_column(
            None,
            "__grouping_id".to_string(),
            ColumnBinding {
                tuple_id: virtual_tuple_id,
                slot_id: grouping_id_slot,
                data_type: DataType::Int64,
                nullable: false,
            },
        );
        virtual_slot_ids.push(grouping_id_slot);

        for (fn_idx, (fn_name, _)) in node.grouping_fn_args.iter().enumerate() {
            let slot = self.alloc_slot();
            self.desc_builder.add_slot(
                slot,
                virtual_tuple_id,
                fn_name,
                &DataType::Int64,
                false,
                1 + fn_idx as i32,
            );
            output_scope.add_column(
                None,
                fn_name.clone(),
                ColumnBinding {
                    tuple_id: virtual_tuple_id,
                    slot_id: slot,
                    data_type: DataType::Int64,
                    nullable: false,
                },
            );
            virtual_slot_ids.push(slot);
        }

        self.desc_builder.add_tuple(virtual_tuple_id);

        // Build slot_id_set_list: for each repeat level, which rollup
        // column slots (from the CHILD tuples) are NON-null.
        // Map rollup column names to their slot IDs in the child scope.
        let all_rollup_slot_ids: BTreeSet<i32> = node
            .all_rollup_columns
            .iter()
            .filter_map(|col| {
                child_cols.iter().find_map(|(name, binding)| {
                    if name.to_lowercase() == col.to_lowercase() {
                        Some(binding.slot_id)
                    } else {
                        None
                    }
                })
            })
            .collect();

        let slot_id_set_list: Vec<BTreeSet<i32>> = node
            .repeat_column_ref_list
            .iter()
            .map(|non_null_cols| {
                non_null_cols
                    .iter()
                    .filter_map(|col| {
                        child_cols.iter().find_map(|(name, binding)| {
                            if name.to_lowercase() == col.to_lowercase() {
                                Some(binding.slot_id)
                            } else {
                                None
                            }
                        })
                    })
                    .collect()
            })
            .collect();

        // Build grouping_list: one row per virtual slot.
        // First row = grouping_id bitmaps, additional rows = GROUPING() values.
        let repeat_times = node.grouping_ids.len();
        let mut grouping_list: Vec<Vec<i64>> = Vec::with_capacity(num_virtual);

        grouping_list.push(node.grouping_ids.iter().map(|g| *g as i64).collect());

        for (_fn_name, fn_args) in &node.grouping_fn_args {
            let mut values = Vec::with_capacity(repeat_times);
            for non_null_cols in &node.repeat_column_ref_list {
                let mut bits: u64 = 0;
                for (bit_pos, arg_col) in fn_args.iter().enumerate() {
                    let is_null = !non_null_cols
                        .iter()
                        .any(|c| c.to_lowercase() == arg_col.to_lowercase());
                    if is_null {
                        bits |= 1 << bit_pos;
                    }
                }
                values.push(bits as i64);
            }
            grouping_list.push(values);
        }

        let repeat_id_list: Vec<i64> = node.grouping_ids.iter().map(|g| *g as i64).collect();

        // Build TPlanNode with TRepeatNode payload.
        // row_tuples = child tuples + virtual tuple (for layout resolution).
        let mut row_tuples = child.tuple_ids.clone();
        row_tuples.push(virtual_tuple_id);

        let mut plan_node = nodes::default_plan_node();
        plan_node.node_id = repeat_node_id;
        plan_node.node_type = plan_nodes::TPlanNodeType::REPEAT_NODE;
        plan_node.num_children = 1;
        plan_node.limit = -1;
        plan_node.row_tuples = row_tuples;
        plan_node.nullable_tuples = vec![];
        plan_node.compact_data = true;
        plan_node.repeat_node = Some(plan_nodes::TRepeatNode {
            output_tuple_id: virtual_tuple_id,
            slot_id_set_list,
            repeat_id_list,
            grouping_list,
            all_slot_ids: all_rollup_slot_ids,
        });

        // Pre-order: repeat node first, then child nodes
        let mut plan_nodes = vec![plan_node];
        plan_nodes.extend(child.plan_nodes);

        // Output tuple_ids = child tuple_ids + virtual tuple
        let mut output_tuple_ids = child.tuple_ids;
        output_tuple_ids.push(virtual_tuple_id);

        Ok(EmitResult {
            plan_nodes,
            scope: output_scope,
            tuple_ids: output_tuple_ids,
        })
    }
}
