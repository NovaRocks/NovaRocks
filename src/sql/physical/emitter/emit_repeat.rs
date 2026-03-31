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
        let output_tuple_id = self.alloc_tuple();

        // 1. Pass through all child columns to the output tuple.
        //    Rollup columns become nullable (they get NULLed per level).
        let child_cols: Vec<(String, ColumnBinding)> = child
            .scope
            .iter_columns()
            .map(|(n, b)| (n.clone(), b.clone()))
            .collect();

        let mut output_scope = ExprScope::new();
        let mut child_slot_ids: Vec<i32> = Vec::new();

        for (idx, (name, binding)) in child_cols.iter().enumerate() {
            let slot_id = self.alloc_slot();
            self.desc_builder.add_slot(
                slot_id,
                output_tuple_id,
                name,
                &binding.data_type,
                true, // all columns become nullable in repeat output
                idx as i32,
            );
            output_scope.add_column(
                None,
                name.clone(),
                ColumnBinding {
                    tuple_id: output_tuple_id,
                    slot_id,
                    data_type: binding.data_type.clone(),
                    nullable: true,
                },
            );
            child_slot_ids.push(slot_id);
        }

        // 2. Add virtual slots: __grouping_id and each GROUPING() function call.
        let num_virtual = 1 + node.grouping_fn_args.len();
        let mut virtual_slot_ids = Vec::with_capacity(num_virtual);

        let grouping_id_slot = self.alloc_slot();
        let grouping_id_col_pos = child_cols.len() as i32;
        self.desc_builder.add_slot(
            grouping_id_slot,
            output_tuple_id,
            "__grouping_id",
            &DataType::Int64,
            false,
            grouping_id_col_pos,
        );
        output_scope.add_column(
            None,
            "__grouping_id".to_string(),
            ColumnBinding {
                tuple_id: output_tuple_id,
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
                output_tuple_id,
                fn_name,
                &DataType::Int64,
                false,
                grouping_id_col_pos + 1 + fn_idx as i32,
            );
            output_scope.add_column(
                None,
                fn_name.clone(),
                ColumnBinding {
                    tuple_id: output_tuple_id,
                    slot_id: slot,
                    data_type: DataType::Int64,
                    nullable: false,
                },
            );
            virtual_slot_ids.push(slot);
        }

        self.desc_builder.add_tuple(output_tuple_id);

        // 3. Build slot_id_set_list: for each repeat level, which rollup
        //    column slots are NON-null. The lowering code computes the
        //    complement (null slots) from this.
        let all_rollup_slot_ids: BTreeSet<i32> = node
            .all_rollup_columns
            .iter()
            .filter_map(|col| {
                child_cols
                    .iter()
                    .zip(child_slot_ids.iter())
                    .find_map(|((name, _), sid)| {
                        if name.to_lowercase() == col.to_lowercase() {
                            Some(*sid)
                        } else {
                            None
                        }
                    })
            })
            .collect();

        let slot_id_set_list: Vec<BTreeSet<i32>> =
            node.repeat_column_ref_list
                .iter()
                .map(|non_null_cols| {
                    non_null_cols
                        .iter()
                        .filter_map(|col| {
                            child_cols.iter().zip(child_slot_ids.iter()).find_map(
                                |((name, _), sid)| {
                                    if name.to_lowercase() == col.to_lowercase() {
                                        Some(*sid)
                                    } else {
                                        None
                                    }
                                },
                            )
                        })
                        .collect()
                })
                .collect();

        // 4. Build grouping_list: first row = grouping_id values,
        //    additional rows = per-GROUPING() function values.
        let repeat_times = node.grouping_ids.len();
        let mut grouping_list: Vec<Vec<i64>> = Vec::with_capacity(num_virtual);

        // First row: grouping_id bitmap for each level
        grouping_list.push(node.grouping_ids.iter().map(|g| *g as i64).collect());

        // Additional rows: one per GROUPING() function call
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

        // 5. Build TPlanNode with TRepeatNode payload
        let mut plan_node = nodes::default_plan_node();
        plan_node.node_id = repeat_node_id;
        plan_node.node_type = plan_nodes::TPlanNodeType::REPEAT_NODE;
        plan_node.num_children = 1;
        plan_node.limit = -1;
        plan_node.row_tuples = vec![output_tuple_id];
        plan_node.nullable_tuples = vec![];
        plan_node.compact_data = true;
        plan_node.repeat_node = Some(plan_nodes::TRepeatNode {
            output_tuple_id,
            slot_id_set_list,
            repeat_id_list,
            grouping_list,
            all_slot_ids: all_rollup_slot_ids,
        });

        // Pre-order: repeat node first, then child nodes
        let mut plan_nodes = vec![plan_node];
        plan_nodes.extend(child.plan_nodes);

        Ok(EmitResult {
            plan_nodes,
            scope: output_scope,
            tuple_ids: vec![output_tuple_id],
        })
    }
}
