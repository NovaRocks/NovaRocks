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
use std::time::Duration;

use crate::common::ids::SlotId;
use crate::exec::expr::ExprArena;
use crate::exec::node::exchange_source::ExchangeSourceNode;
use crate::exec::node::limit::LimitNode;
use crate::exec::node::sort::{SortExpression, SortNode, SortTopNType};
use crate::exec::node::{ExecNode, ExecNodeKind};
use crate::runtime::query_context::{QueryId, query_context_manager};
use crate::novarocks_logging::warn;

use crate::common::config::exchange_wait_ms;
use crate::lower::expr::lower_t_expr;
use crate::lower::layout::Layout;
use crate::lower::node::{Lowered, local_rf_waiting_set};
use crate::runtime::exchange;
use crate::{internal_service, plan_nodes, types};

/// Lower an EXCHANGE_NODE plan node to a `Lowered` ExecNode.
///
/// This helper encapsulates both receiver and sender exchange lowering logic.
pub(crate) fn lower_exchange_node(
    children: Vec<Lowered>,
    node: &plan_nodes::TPlanNode,
    exec_params: Option<&internal_service::TPlanFragmentExecParams>,
    arena: &mut ExprArena,
    out_layout: &Layout,
    last_query_id: Option<&str>,
    fe_addr: Option<&types::TNetworkAddress>,
) -> Result<Lowered, String> {
    if children.is_empty() {
        let params = exec_params
            .ok_or_else(|| "EXCHANGE_NODE missing exec_params for exchange receiver".to_string())?;

        let expected = if let Some(v) = params.per_exch_num_senders.get(&node.node_id) {
            (*v).max(0) as usize
        } else {
            let query_id = QueryId {
                hi: params.query_id.hi,
                lo: params.query_id.lo,
            };
            let from_batch = query_context_manager()
                .exchange_sender_count(query_id, node.node_id)
                .unwrap_or(0);
            if from_batch == 0 {
                return Err(format!(
                    "EXCHANGE_NODE missing sender count for node_id {} (query_id={}:{}).",
                    node.node_id, query_id.hi, query_id.lo
                ));
            }
            warn!(
                target: "novarocks::exec",
                node_id = node.node_id,
                expected_senders = from_batch,
                "EXCHANGE_NODE missing per_exch_num_senders; using sender count from batch"
            );
            from_batch
        };
        if expected == 0 {
            return Err(format!(
                "EXCHANGE_NODE expected_senders must be > 0, node_id={}",
                node.node_id
            ));
        }

        let key = exchange::ExchangeKey {
            finst_id_hi: params.fragment_instance_id.hi,
            finst_id_lo: params.fragment_instance_id.lo,
            node_id: node.node_id,
        };
        let exchange_timeout_ms = exchange_wait_ms();

        let output_slots = out_layout
            .order
            .iter()
            .map(|(_, slot_id)| SlotId::try_from(*slot_id))
            .collect::<Result<Vec<_>, _>>()?;
        let mut out = ExecNode {
            kind: ExecNodeKind::ExchangeSource(
                ExchangeSourceNode::new(
                    key,
                    expected,
                    Duration::from_millis(exchange_timeout_ms),
                    output_slots,
                )
                .with_local_rf_waiting_set(local_rf_waiting_set(node)),
            ),
        };

        // Some plans (e.g. global ORDER BY) use a merging exchange without an explicit SORT_NODE.
        // Use exchange_node.sort_info (if present) to produce deterministic order.
        // For non-ordering exchange, keep LIMIT/OFFSET semantics via LimitNode.
        if let Some(exch) = node.exchange_node.as_ref() {
            let offset = match exch.offset.unwrap_or(0) {
                v if v < 0 => {
                    return Err(format!("EXCHANGE_NODE offset must be >= 0, got {v}"));
                }
                v => v as usize,
            };
            if let Some(info) = exch.sort_info.as_ref() {
                let order_by = build_sort_order_by(
                    info,
                    arena,
                    out_layout,
                    &format!("EXCHANGE_NODE node_id={}", node.node_id),
                    last_query_id,
                    fe_addr,
                )?;

                let limit = if node.limit >= 0 {
                    Some(node.limit as usize)
                } else {
                    None
                };

                out = ExecNode {
                    kind: ExecNodeKind::Sort(SortNode {
                        input: Box::new(out),
                        node_id: node.node_id,
                        use_top_n: false,
                        order_by,
                        limit,
                        offset,
                        topn_type: SortTopNType::RowNumber,
                        max_buffered_rows: None,
                        max_buffered_bytes: None,
                    }),
                };
            } else if node.limit >= 0 || offset > 0 {
                out = ExecNode {
                    kind: ExecNodeKind::Limit(LimitNode {
                        input: Box::new(out),
                        node_id: node.node_id,
                        limit: (node.limit >= 0).then_some(node.limit as usize),
                        offset,
                    }),
                };
            }
        }

        Ok(Lowered {
            node: out,
            layout: out_layout.clone(),
        })
    } else {
        // Sender Exchange (if it appears in plan tree? usually it's a sink)
        // Or maybe a pass-through?
        if children.len() != 1 {
            return Err(format!(
                "EXCHANGE_NODE expected 0 or 1 child, got {}",
                children.len()
            ));
        }
        Ok(children.into_iter().next().expect("child"))
    }
}

fn build_sort_order_by(
    info: &plan_nodes::TSortInfo,
    arena: &mut ExprArena,
    input_layout: &Layout,
    node_label: &str,
    last_query_id: Option<&str>,
    fe_addr: Option<&types::TNetworkAddress>,
) -> Result<Vec<SortExpression>, String> {
    let key_count = info.ordering_exprs.len();
    if info.is_asc_order.len() != key_count {
        return Err(format!(
            "{node_label} sort_info.is_asc_order length mismatch: ordering_exprs={} is_asc_order={}",
            key_count,
            info.is_asc_order.len()
        ));
    }
    if info.nulls_first.len() != key_count {
        return Err(format!(
            "{node_label} sort_info.nulls_first length mismatch: ordering_exprs={} nulls_first={}",
            key_count,
            info.nulls_first.len()
        ));
    }

    let mut order_by = Vec::with_capacity(key_count);
    for (i, expr) in info.ordering_exprs.iter().enumerate() {
        let expr_id = lower_t_expr(expr, arena, input_layout, last_query_id, fe_addr)?;
        order_by.push(SortExpression {
            expr: expr_id,
            asc: info.is_asc_order[i],
            nulls_first: info.nulls_first[i],
        });
    }
    Ok(order_by)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exprs::{TExpr, TExprNode, TExprNodeType, TSlotRef};
    use crate::types::{TTypeDesc, TTypeNode, TTypeNodeType};
    use std::collections::{BTreeMap, HashMap};

    fn dummy_type_desc() -> TTypeDesc {
        TTypeDesc {
            types: Some(vec![TTypeNode {
                type_: TTypeNodeType::SCALAR,
                scalar_type: None,
                struct_fields: None,
                is_named: None,
            }]),
        }
    }

    fn default_expr_node() -> TExprNode {
        TExprNode {
            node_type: TExprNodeType::INT_LITERAL,
            type_: dummy_type_desc(),
            opcode: None,
            num_children: 0,
            agg_expr: None,
            bool_literal: None,
            case_expr: None,
            date_literal: None,
            float_literal: None,
            int_literal: None,
            in_predicate: None,
            is_null_pred: None,
            like_pred: None,
            literal_pred: None,
            slot_ref: None,
            string_literal: None,
            tuple_is_null_pred: None,
            info_func: None,
            decimal_literal: None,
            output_scale: 0,
            fn_call_expr: None,
            large_int_literal: None,
            output_column: None,
            output_type: None,
            vector_opcode: None,
            fn_: None,
            vararg_start_idx: None,
            child_type: None,
            vslot_ref: None,
            used_subfield_names: None,
            binary_literal: None,
            copy_flag: None,
            check_is_out_of_bounds: None,
            use_vectorized: None,
            has_nullable_child: None,
            is_nullable: None,
            child_type_desc: None,
            is_monotonic: None,
            dict_query_expr: None,
            dictionary_get_expr: None,
            is_index_only_filter: None,
            is_nondeterministic: None,
        }
    }

    fn slot_ref_expr(tuple_id: i32, slot_id: i32) -> TExpr {
        TExpr {
            nodes: vec![TExprNode {
                node_type: TExprNodeType::SLOT_REF,
                type_: dummy_type_desc(),
                num_children: 0,
                slot_ref: Some(TSlotRef { slot_id, tuple_id }),
                ..default_expr_node()
            }],
        }
    }

    fn single_slot_layout(tuple_id: i32, slot_id: i32) -> Layout {
        let mut index = HashMap::new();
        index.insert((tuple_id, slot_id), 0);
        Layout {
            order: vec![(tuple_id, slot_id)],
            index,
        }
    }

    fn exchange_plan_node(sort_info: plan_nodes::TSortInfo) -> plan_nodes::TPlanNode {
        plan_nodes::TPlanNode {
            node_id: 11,
            node_type: plan_nodes::TPlanNodeType::EXCHANGE_NODE,
            num_children: 0,
            limit: -1,
            row_tuples: vec![],
            nullable_tuples: vec![],
            conjuncts: None,
            compact_data: true,
            common: None,
            hash_join_node: None,
            agg_node: None,
            sort_node: None,
            merge_node: None,
            exchange_node: Some(plan_nodes::TExchangeNode {
                input_row_tuples: vec![0],
                sort_info: Some(sort_info),
                offset: Some(0),
                partition_type: None,
                enable_parallel_merge: None,
                parallel_merge_late_materialize_mode: None,
            }),
            mysql_scan_node: None,
            olap_scan_node: None,
            file_scan_node: None,
            schema_scan_node: None,
            meta_scan_node: None,
            analytic_node: None,
            union_node: None,
            resource_profile: None,
            es_scan_node: None,
            repeat_node: None,
            assert_num_rows_node: None,
            intersect_node: None,
            except_node: None,
            merge_join_node: None,
            raw_values_node: None,
            use_vectorized: None,
            hdfs_scan_node: None,
            project_node: None,
            table_function_node: None,
            probe_runtime_filters: None,
            decode_node: None,
            local_rf_waiting_set: None,
            filter_null_value_columns: None,
            need_create_tuple_columns: None,
            jdbc_scan_node: None,
            connector_scan_node: None,
            cross_join_node: None,
            lake_scan_node: None,
            nestloop_join_node: None,
            starrocks_scan_node: None,
            stream_scan_node: None,
            stream_join_node: None,
            stream_agg_node: None,
            select_node: None,
            fetch_node: None,
            look_up_node: None,
        }
    }

    fn exchange_exec_params(node_id: i32) -> internal_service::TPlanFragmentExecParams {
        let mut per_exch_num_senders = BTreeMap::new();
        per_exch_num_senders.insert(node_id, 1);
        internal_service::TPlanFragmentExecParams::new(
            types::TUniqueId { hi: 1, lo: 2 },
            types::TUniqueId { hi: 3, lo: 4 },
            BTreeMap::new(),
            per_exch_num_senders,
            None::<Vec<crate::data_sinks::TPlanFragmentDestination>>,
            None::<i32>,
            None::<i32>,
            None::<bool>,
            None::<bool>,
            None::<crate::runtime_filter::TRuntimeFilterParams>,
            None::<i32>,
            None::<bool>,
            None::<BTreeMap<i32, BTreeMap<i32, Vec<internal_service::TScanRangeParams>>>>,
            None::<bool>,
            None::<i32>,
            None::<bool>,
            None::<Vec<internal_service::TExecDebugOption>>,
        )
    }

    #[test]
    fn lower_exchange_node_rejects_sort_flag_length_mismatch() {
        let out_layout = single_slot_layout(0, 1);
        let sort_info = plan_nodes::TSortInfo {
            ordering_exprs: vec![slot_ref_expr(0, 1)],
            is_asc_order: vec![true],
            nulls_first: vec![],
            sort_tuple_slot_exprs: None,
        };
        let node = exchange_plan_node(sort_info);
        let params = exchange_exec_params(node.node_id);
        let mut arena = ExprArena::default();

        let err = lower_exchange_node(
            vec![],
            &node,
            Some(&params),
            &mut arena,
            &out_layout,
            None,
            None,
        )
        .unwrap_err();

        assert!(
            err.contains("sort_info.nulls_first length mismatch"),
            "unexpected error: {err}"
        );
    }
}
