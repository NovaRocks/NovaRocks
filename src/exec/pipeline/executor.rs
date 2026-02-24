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
//! Top-level pipeline executor entrypoint.
//!
//! Responsibilities:
//! - Builds runtime pipeline context and executes one plan fragment to completion.
//! - Bridges fragment context, driver executor, and terminal sink orchestration.
//!
//! Key exported interfaces:
//! - Functions: `execute_plan_with_pipeline`.
//!
//! Current limitations:
//! - Implements only the execution semantics currently wired by novarocks plan lowering and pipeline builder.
//! - Unsupported states should be surfaced as explicit runtime errors instead of fallback behavior.

use std::sync::Arc;
use std::time::Duration;

use crate::common::app_config;
use crate::exec::node::ExecPlan;
use crate::runtime::query_context::query_context_manager;
use crate::runtime::runtime_state::RuntimeState;
use crate::novarocks_logging::info;

use super::builder::build_pipeline_graph_for_exec_plan_with_dop;
use super::dependency::DependencyManager;
use super::fragment_context::FragmentContext;
use super::global_driver_executor::{DriverTask, FragmentCompletion, global_driver_executor};
use super::operator_factory::OperatorFactory;
use super::pipeline::Pipeline;

use crate::runtime::profile::Profiler;

/// Execute one plan fragment through pipeline runtime and return the terminal sink outcome.
pub(crate) fn execute_plan_with_pipeline(
    plan: ExecPlan,
    debug: bool,
    time_slice: Duration,
    sink: Box<dyn OperatorFactory>,
    exchange_finst_id: Option<(i64, i64)>,
    profiler: Option<Profiler>,
    pipeline_dop: i32,
    runtime_state: std::sync::Arc<RuntimeState>,
    query_id: Option<crate::runtime::query_context::QueryId>,
    fe_addr: Option<crate::types::TNetworkAddress>,
    backend_num: Option<i32>,
) -> Result<(), String> {
    let dep_manager = DependencyManager::new();
    let runtime_filter_hub = match query_id {
        Some(qid) => {
            if let Some(hub) = query_context_manager().get_runtime_filter_hub(qid) {
                hub
            } else {
                let hub = Arc::new(crate::runtime::runtime_filter_hub::RuntimeFilterHub::new(
                    DependencyManager::new(),
                ));
                let _ = query_context_manager().set_runtime_filter_hub(qid, Arc::clone(&hub));
                hub
            }
        }
        None => Arc::new(crate::runtime::runtime_filter_hub::RuntimeFilterHub::new(
            DependencyManager::new(),
        )),
    };
    runtime_filter_hub.set_wait_timeouts(
        runtime_state.runtime_filter_scan_wait_timeout(),
        runtime_state.runtime_filter_wait_timeout(),
    );
    if let Some(qid) = query_id {
        if let Some(params) = runtime_state.runtime_filter_params().cloned() {
            let _ = query_context_manager().set_runtime_filter_params(qid, params);
        }
        let _ = query_context_manager().get_or_create_runtime_filter_worker(qid);
    }

    // Use the DOP from FE (already calculated by calc_pipeline_dop)
    let graph = build_pipeline_graph_for_exec_plan_with_dop(
        &plan,
        debug,
        dep_manager.clone(),
        exchange_finst_id,
        pipeline_dop,
        Arc::clone(&runtime_filter_hub),
    )?;

    let finst_id = runtime_state.fragment_instance_id();
    let ctx = Arc::new(FragmentContext::new(
        profiler,
        runtime_state,
        exchange_finst_id,
        query_id,
        fe_addr,
        backend_num,
    ));
    let mut sink = Some(sink);

    // Collect all drivers
    let mut all_drivers = Vec::new();
    for pipeline_plan in graph.pipelines {
        let mut factories = pipeline_plan.factories;
        if pipeline_plan.id == graph.root_id {
            if !pipeline_plan.needs_sink {
                return Err("root pipeline missing sink requirement".to_string());
            }
            let root_sink = sink
                .take()
                .ok_or_else(|| "root pipeline sink already attached".to_string())?;
            factories.push(root_sink);
        } else if pipeline_plan.needs_sink {
            return Err("non-root pipeline requires sink".to_string());
        }

        let pipeline = Pipeline::new(pipeline_plan.id, factories, pipeline_plan.dop);
        let drivers = pipeline.instantiate_drivers(&ctx)?;
        all_drivers.extend(drivers);
    }

    if sink.is_some() {
        return Err("root pipeline sink not attached".to_string());
    }

    // Fixed time slice: 10ms (similar to StarRocks)
    const TIME_SLICE_MS: u64 = 10;
    let time_slice_fixed = Duration::from_millis(TIME_SLICE_MS);

    // Get executor thread count from config
    let num_threads = app_config::config()
        .ok()
        .map(|c| c.runtime.actual_exec_threads())
        .unwrap_or_else(|| {
            std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(1)
        });

    // Use a shared global executor across fragments, following StarRocks' design.
    // When `num_threads <= 1`, keep the caller-provided time slice for backward compatibility.
    let effective_time_slice = if num_threads > 1 {
        info!(
            "Using global executor: threads={}, dop={}, time_slice={}ms",
            num_threads, pipeline_dop, TIME_SLICE_MS
        );
        time_slice_fixed
    } else {
        info!("Using global executor: threads=1, dop={}", pipeline_dop);
        time_slice
    };

    let completion = FragmentCompletion::new(all_drivers.len(), Arc::clone(&ctx));
    if query_id.is_some() {
        if let Some(finst_id) = finst_id {
            query_context_manager().register_fragment_completion(finst_id, Arc::clone(&completion));
        }
    }
    if let Some(query_id) = query_id {
        if query_context_manager().is_query_canceled(query_id) {
            completion.abort_from_query("query canceled".to_string());
        }
    }
    let mut tasks = Vec::with_capacity(all_drivers.len());
    for driver in all_drivers {
        let task = DriverTask::new(driver, Arc::clone(&completion), effective_time_slice);
        tasks.push(task);
    }
    global_driver_executor().submit(tasks);
    let res = completion.wait();
    if let Some(finst_id) = finst_id {
        query_context_manager().unregister_fragment_completion(finst_id);
    }
    res?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::Duration;

    use arrow::array::{Array, Int32Array, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    use crate::common::ids::SlotId;
    use crate::exec::chunk::{Chunk, field_with_slot_id};
    use crate::exec::expr::{ExprArena, ExprNode};
    use crate::exec::node::aggregate::{AggFunction, AggTypeSignature, AggregateNode};
    use crate::exec::node::analytic::{
        AnalyticNode, AnalyticOutputColumn, WindowBoundary, WindowFrame, WindowFunctionKind,
        WindowFunctionSpec, WindowType,
    };
    use crate::exec::node::join::{JoinDistributionMode, JoinNode, JoinType};
    use crate::exec::node::nljoin::{NestedLoopJoinNode, NestedLoopJoinType};
    use crate::exec::node::values::ValuesNode;
    use crate::exec::node::{ExecNode, ExecNodeKind, ExecPlan};
    use crate::exec::operators::{ResultSinkFactory, ResultSinkHandle};
    use crate::runtime::runtime_state::RuntimeState;

    use super::execute_plan_with_pipeline;

    #[test]
    fn group_by_sum_is_correct_with_dop_2() {
        let schema = Arc::new(Schema::new(vec![
            field_with_slot_id(Field::new("k", DataType::Int32, false), SlotId::new(1)),
            field_with_slot_id(Field::new("v", DataType::Int32, false), SlotId::new(2)),
        ]));
        let keys = Arc::new(Int32Array::from(vec![1, 1, 2, 3, 3, 3])) as arrow::array::ArrayRef;
        let vals = Arc::new(Int32Array::from(vec![10, 20, 5, 7, 8, 9])) as arrow::array::ArrayRef;
        let batch = RecordBatch::try_new(schema, vec![keys, vals]).expect("record batch");
        let chunk = Chunk::new(batch);

        let mut arena = ExprArena::default();
        let k = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Int32);
        let v = arena.push_typed(ExprNode::SlotId(SlotId::new(2)), DataType::Int32);

        let plan = ExecPlan {
            arena,
            root: ExecNode {
                kind: ExecNodeKind::Aggregate(AggregateNode {
                    input: Box::new(ExecNode {
                        kind: ExecNodeKind::Values(ValuesNode { chunk, node_id: 0 }),
                    }),
                    node_id: 0,
                    group_by: vec![k],
                    functions: vec![AggFunction {
                        name: "sum".to_string(),
                        inputs: vec![v],
                        input_is_intermediate: false,
                        types: Some(AggTypeSignature {
                            intermediate_type: None,
                            output_type: Some(DataType::Int64),
                            input_arg_type: None,
                        }),
                    }],
                    need_finalize: true,
                    input_is_intermediate: false,
                    output_slots: vec![SlotId::new(1), SlotId::new(2)],
                }),
            },
        };

        let handle = ResultSinkHandle::new();
        let runtime_state = Arc::new(RuntimeState::default());
        execute_plan_with_pipeline(
            plan,
            false,
            Duration::from_millis(10),
            Box::new(ResultSinkFactory::new(handle.clone())),
            None,
            None,
            1,
            runtime_state,
            None,
            None,
            None,
        )
        .expect("execute plan");

        let chunks = handle.take_chunks();
        let mut out: HashMap<i32, i64> = HashMap::new();
        for chunk in chunks {
            if chunk.is_empty() {
                continue;
            }
            assert_eq!(chunk.columns().len(), 2);
            let k_col = chunk.column_by_slot_id(SlotId::new(1)).expect("k column");
            let k_arr = k_col
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("k Int32");
            let v_col = chunk.column_by_slot_id(SlotId::new(2)).expect("sum column");
            if let Some(sum_arr) = v_col.as_any().downcast_ref::<Int64Array>() {
                for i in 0..chunk.len() {
                    out.insert(k_arr.value(i), sum_arr.value(i));
                }
            } else if let Some(sum_arr) = v_col.as_any().downcast_ref::<Int32Array>() {
                for i in 0..chunk.len() {
                    out.insert(k_arr.value(i), sum_arr.value(i) as i64);
                }
            } else {
                panic!("unexpected sum column type: {:?}", v_col.data_type());
            }
        }

        assert_eq!(out.get(&1).copied(), Some(30));
        assert_eq!(out.get(&2).copied(), Some(5));
        assert_eq!(out.get(&3).copied(), Some(24));
        assert_eq!(out.len(), 3);
    }

    #[test]
    fn nljoin_inner_with_conjunct_is_correct() {
        let left_schema = Arc::new(Schema::new(vec![field_with_slot_id(
            Field::new("a", DataType::Int32, false),
            SlotId::new(1),
        )]));
        let left_arr = Arc::new(Int32Array::from(vec![1, 3])) as arrow::array::ArrayRef;
        let left_batch =
            RecordBatch::try_new(Arc::clone(&left_schema), vec![left_arr]).expect("left batch");

        let right_schema = Arc::new(Schema::new(vec![field_with_slot_id(
            Field::new("b", DataType::Int32, false),
            SlotId::new(2),
        )]));
        let right_arr = Arc::new(Int32Array::from(vec![2, 4])) as arrow::array::ArrayRef;
        let right_batch =
            RecordBatch::try_new(Arc::clone(&right_schema), vec![right_arr]).expect("right batch");

        let join_scope_schema = Arc::new(Schema::new(vec![
            field_with_slot_id(Field::new("a", DataType::Int32, false), SlotId::new(1)),
            field_with_slot_id(Field::new("b", DataType::Int32, false), SlotId::new(2)),
        ]));

        let mut arena = ExprArena::default();
        let a = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Int32);
        let b = arena.push_typed(ExprNode::SlotId(SlotId::new(2)), DataType::Int32);
        let pred = arena.push_typed(ExprNode::Lt(a, b), DataType::Boolean);

        let plan = ExecPlan {
            arena,
            root: ExecNode {
                kind: ExecNodeKind::NestedLoopJoin(NestedLoopJoinNode {
                    left: Box::new(ExecNode {
                        kind: ExecNodeKind::Values(ValuesNode {
                            chunk: Chunk::new(left_batch),
                            node_id: 0,
                        }),
                    }),
                    right: Box::new(ExecNode {
                        kind: ExecNodeKind::Values(ValuesNode {
                            chunk: Chunk::new(right_batch),
                            node_id: 0,
                        }),
                    }),
                    node_id: 1,
                    join_type: NestedLoopJoinType::Inner,
                    join_conjunct: Some(pred),
                    left_schema,
                    right_schema,
                    join_scope_schema,
                }),
            },
        };

        let handle = ResultSinkHandle::new();
        let runtime_state = Arc::new(RuntimeState::default());
        execute_plan_with_pipeline(
            plan,
            false,
            Duration::from_millis(10),
            Box::new(ResultSinkFactory::new(handle.clone())),
            None,
            None,
            1,
            runtime_state,
            None,
            None,
            None,
        )
        .expect("execute plan");

        let chunks = handle.take_chunks();
        let mut pairs = Vec::new();
        for chunk in chunks {
            if chunk.is_empty() {
                continue;
            }
            let a_arr = chunk
                .columns()
                .get(0)
                .expect("a column")
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("a Int32");
            let b_arr = chunk
                .columns()
                .get(1)
                .expect("b column")
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("b Int32");
            for i in 0..chunk.len() {
                pairs.push((a_arr.value(i), b_arr.value(i)));
            }
        }

        assert_eq!(pairs, vec![(1, 2), (1, 4), (3, 4)]);
    }

    #[test]
    fn nljoin_left_outer_emits_null_extended_rows() {
        let left_schema = Arc::new(Schema::new(vec![field_with_slot_id(
            Field::new("a", DataType::Int32, false),
            SlotId::new(1),
        )]));
        let left_arr = Arc::new(Int32Array::from(vec![1, 3, 5])) as arrow::array::ArrayRef;
        let left_batch =
            RecordBatch::try_new(Arc::clone(&left_schema), vec![left_arr]).expect("left batch");

        let right_schema = Arc::new(Schema::new(vec![field_with_slot_id(
            Field::new("b", DataType::Int32, false),
            SlotId::new(2),
        )]));
        let right_arr = Arc::new(Int32Array::from(vec![2, 4])) as arrow::array::ArrayRef;
        let right_batch =
            RecordBatch::try_new(Arc::clone(&right_schema), vec![right_arr]).expect("right batch");

        let join_scope_schema = Arc::new(Schema::new(vec![
            field_with_slot_id(Field::new("a", DataType::Int32, false), SlotId::new(1)),
            field_with_slot_id(Field::new("b", DataType::Int32, true), SlotId::new(2)),
        ]));

        let mut arena = ExprArena::default();
        let a = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Int32);
        let b = arena.push_typed(ExprNode::SlotId(SlotId::new(2)), DataType::Int32);
        let pred = arena.push_typed(ExprNode::Lt(a, b), DataType::Boolean);

        let plan = ExecPlan {
            arena,
            root: ExecNode {
                kind: ExecNodeKind::NestedLoopJoin(NestedLoopJoinNode {
                    left: Box::new(ExecNode {
                        kind: ExecNodeKind::Values(ValuesNode {
                            chunk: Chunk::new(left_batch),
                            node_id: 0,
                        }),
                    }),
                    right: Box::new(ExecNode {
                        kind: ExecNodeKind::Values(ValuesNode {
                            chunk: Chunk::new(right_batch),
                            node_id: 0,
                        }),
                    }),
                    node_id: 1,
                    join_type: NestedLoopJoinType::LeftOuter,
                    join_conjunct: Some(pred),
                    left_schema,
                    right_schema,
                    join_scope_schema,
                }),
            },
        };

        let handle = ResultSinkHandle::new();
        let runtime_state = Arc::new(RuntimeState::default());
        execute_plan_with_pipeline(
            plan,
            false,
            Duration::from_millis(10),
            Box::new(ResultSinkFactory::new(handle.clone())),
            None,
            None,
            1,
            runtime_state,
            None,
            None,
            None,
        )
        .expect("execute plan");

        let chunks = handle.take_chunks();
        let mut rows = Vec::new();
        for chunk in chunks {
            if chunk.is_empty() {
                continue;
            }
            let a_arr = chunk
                .columns()
                .get(0)
                .expect("a column")
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("a Int32");
            let b_arr = chunk
                .columns()
                .get(1)
                .expect("b column")
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("b Int32");
            for i in 0..chunk.len() {
                let b = if b_arr.is_valid(i) {
                    Some(b_arr.value(i))
                } else {
                    None
                };
                rows.push((a_arr.value(i), b));
            }
        }
        rows.sort();
        assert_eq!(
            rows,
            vec![(1, Some(2)), (1, Some(4)), (3, Some(4)), (5, None)]
        );
    }

    #[test]
    fn nljoin_full_outer_with_empty_left_emits_unmatched_build() {
        let left_schema = Arc::new(Schema::new(vec![field_with_slot_id(
            Field::new("a", DataType::Int32, false),
            SlotId::new(1),
        )]));
        let left_batch = RecordBatch::new_empty(Arc::clone(&left_schema));

        let right_schema = Arc::new(Schema::new(vec![field_with_slot_id(
            Field::new("b", DataType::Int32, false),
            SlotId::new(2),
        )]));
        let right_arr = Arc::new(Int32Array::from(vec![2, 4])) as arrow::array::ArrayRef;
        let right_batch =
            RecordBatch::try_new(Arc::clone(&right_schema), vec![right_arr]).expect("right batch");

        let join_scope_schema = Arc::new(Schema::new(vec![
            field_with_slot_id(Field::new("a", DataType::Int32, true), SlotId::new(1)),
            field_with_slot_id(Field::new("b", DataType::Int32, false), SlotId::new(2)),
        ]));

        let mut arena = ExprArena::default();
        let a = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Int32);
        let b = arena.push_typed(ExprNode::SlotId(SlotId::new(2)), DataType::Int32);
        let pred = arena.push_typed(ExprNode::Lt(a, b), DataType::Boolean);

        let plan = ExecPlan {
            arena,
            root: ExecNode {
                kind: ExecNodeKind::NestedLoopJoin(NestedLoopJoinNode {
                    left: Box::new(ExecNode {
                        kind: ExecNodeKind::Values(ValuesNode {
                            chunk: Chunk::new(left_batch),
                            node_id: 0,
                        }),
                    }),
                    right: Box::new(ExecNode {
                        kind: ExecNodeKind::Values(ValuesNode {
                            chunk: Chunk::new(right_batch),
                            node_id: 0,
                        }),
                    }),
                    node_id: 1,
                    join_type: NestedLoopJoinType::FullOuter,
                    join_conjunct: Some(pred),
                    left_schema,
                    right_schema,
                    join_scope_schema,
                }),
            },
        };

        let handle = ResultSinkHandle::new();
        let runtime_state = Arc::new(RuntimeState::default());
        execute_plan_with_pipeline(
            plan,
            false,
            Duration::from_millis(10),
            Box::new(ResultSinkFactory::new(handle.clone())),
            None,
            None,
            1,
            runtime_state,
            None,
            None,
            None,
        )
        .expect("execute plan");

        let chunks = handle.take_chunks();
        let mut rows = Vec::new();
        for chunk in chunks {
            if chunk.is_empty() {
                continue;
            }
            let a_arr = chunk
                .columns()
                .get(0)
                .expect("a column")
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("a Int32");
            let b_arr = chunk
                .columns()
                .get(1)
                .expect("b column")
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("b Int32");
            for i in 0..chunk.len() {
                let a = if a_arr.is_valid(i) {
                    Some(a_arr.value(i))
                } else {
                    None
                };
                rows.push((a, b_arr.value(i)));
            }
        }
        rows.sort();
        assert_eq!(rows, vec![(None, 2), (None, 4)]);
    }

    #[test]
    fn hash_left_outer_residual_treats_false_as_no_match() {
        let left_schema = Arc::new(Schema::new(vec![
            field_with_slot_id(Field::new("k", DataType::Int32, false), SlotId::new(1)),
            field_with_slot_id(Field::new("v", DataType::Int32, false), SlotId::new(2)),
        ]));
        let left_k = Arc::new(Int32Array::from(vec![1, 1, 2])) as arrow::array::ArrayRef;
        let left_v = Arc::new(Int32Array::from(vec![10, 20, 30])) as arrow::array::ArrayRef;
        let left_batch =
            RecordBatch::try_new(Arc::clone(&left_schema), vec![left_k, left_v]).expect("left");

        let right_schema = Arc::new(Schema::new(vec![
            field_with_slot_id(Field::new("k", DataType::Int32, false), SlotId::new(3)),
            field_with_slot_id(Field::new("w", DataType::Int32, false), SlotId::new(4)),
        ]));
        let right_k = Arc::new(Int32Array::from(vec![1, 1, 3])) as arrow::array::ArrayRef;
        let right_w = Arc::new(Int32Array::from(vec![100, 5, 7])) as arrow::array::ArrayRef;
        let right_batch =
            RecordBatch::try_new(Arc::clone(&right_schema), vec![right_k, right_w]).expect("right");

        let join_scope_schema = Arc::new(Schema::new(vec![
            field_with_slot_id(Field::new("k", DataType::Int32, false), SlotId::new(1)),
            field_with_slot_id(Field::new("v", DataType::Int32, false), SlotId::new(2)),
            field_with_slot_id(Field::new("k", DataType::Int32, true), SlotId::new(3)),
            field_with_slot_id(Field::new("w", DataType::Int32, true), SlotId::new(4)),
        ]));

        let mut arena = ExprArena::default();
        let key_left = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Int32);
        let key_right = arena.push_typed(ExprNode::SlotId(SlotId::new(3)), DataType::Int32);
        let left_v = arena.push_typed(ExprNode::SlotId(SlotId::new(2)), DataType::Int32);
        let right_w = arena.push_typed(ExprNode::SlotId(SlotId::new(4)), DataType::Int32);
        let residual = arena.push_typed(ExprNode::Lt(left_v, right_w), DataType::Boolean);

        let plan = ExecPlan {
            arena,
            root: ExecNode {
                kind: ExecNodeKind::Join(JoinNode {
                    left: Box::new(ExecNode {
                        kind: ExecNodeKind::Values(ValuesNode {
                            chunk: Chunk::new(left_batch),
                            node_id: 0,
                        }),
                    }),
                    right: Box::new(ExecNode {
                        kind: ExecNodeKind::Values(ValuesNode {
                            chunk: Chunk::new(right_batch),
                            node_id: 0,
                        }),
                    }),
                    node_id: 1,
                    join_type: JoinType::LeftOuter,
                    distribution_mode: JoinDistributionMode::Partitioned,
                    left_schema: Arc::clone(&left_schema),
                    right_schema: Arc::clone(&right_schema),
                    join_scope_schema,
                    probe_keys: vec![key_left],
                    build_keys: vec![key_right],
                    eq_null_safe: vec![false],
                    residual_predicate: Some(residual),
                    runtime_filters: Vec::new(),
                }),
            },
        };

        let handle = ResultSinkHandle::new();
        let runtime_state = Arc::new(RuntimeState::default());
        execute_plan_with_pipeline(
            plan,
            false,
            Duration::from_millis(10),
            Box::new(ResultSinkFactory::new(handle.clone())),
            None,
            None,
            1,
            runtime_state,
            None,
            None,
            None,
        )
        .expect("execute plan");

        let chunks = handle.take_chunks();
        let mut rows = Vec::new();
        for chunk in chunks {
            if chunk.is_empty() {
                continue;
            }
            let k1 = chunk
                .columns()
                .get(0)
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            let v = chunk
                .columns()
                .get(1)
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            let k2 = chunk
                .columns()
                .get(2)
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            let w = chunk
                .columns()
                .get(3)
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();

            for i in 0..chunk.len() {
                let rk = if k2.is_valid(i) {
                    Some(k2.value(i))
                } else {
                    None
                };
                let rw = if w.is_valid(i) {
                    Some(w.value(i))
                } else {
                    None
                };
                rows.push((k1.value(i), v.value(i), rk, rw));
            }
        }
        rows.sort();
        assert_eq!(
            rows,
            vec![
                (1, 10, Some(1), Some(100)),
                (1, 20, Some(1), Some(100)),
                (2, 30, None, None)
            ]
        );
    }

    #[test]
    fn hash_right_outer_emits_unmatched_probe_rows() {
        let left_schema = Arc::new(Schema::new(vec![
            field_with_slot_id(Field::new("k", DataType::Int32, false), SlotId::new(1)),
            field_with_slot_id(Field::new("v", DataType::Int32, false), SlotId::new(2)),
        ]));
        let left_k = Arc::new(Int32Array::from(vec![1])) as arrow::array::ArrayRef;
        let left_v = Arc::new(Int32Array::from(vec![10])) as arrow::array::ArrayRef;
        let left_batch =
            RecordBatch::try_new(Arc::clone(&left_schema), vec![left_k, left_v]).expect("left");

        let right_schema = Arc::new(Schema::new(vec![
            field_with_slot_id(Field::new("k", DataType::Int32, false), SlotId::new(3)),
            field_with_slot_id(Field::new("w", DataType::Int32, false), SlotId::new(4)),
        ]));
        let right_k = Arc::new(Int32Array::from(vec![1, 2])) as arrow::array::ArrayRef;
        let right_w = Arc::new(Int32Array::from(vec![100, 200])) as arrow::array::ArrayRef;
        let right_batch =
            RecordBatch::try_new(Arc::clone(&right_schema), vec![right_k, right_w]).expect("right");

        let join_scope_schema = Arc::new(Schema::new(vec![
            field_with_slot_id(Field::new("k", DataType::Int32, true), SlotId::new(1)),
            field_with_slot_id(Field::new("v", DataType::Int32, true), SlotId::new(2)),
            field_with_slot_id(Field::new("k", DataType::Int32, false), SlotId::new(3)),
            field_with_slot_id(Field::new("w", DataType::Int32, false), SlotId::new(4)),
        ]));

        let mut arena = ExprArena::default();
        let key_left = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Int32);
        let key_right = arena.push_typed(ExprNode::SlotId(SlotId::new(3)), DataType::Int32);

        let plan = ExecPlan {
            arena,
            root: ExecNode {
                kind: ExecNodeKind::Join(JoinNode {
                    left: Box::new(ExecNode {
                        kind: ExecNodeKind::Values(ValuesNode {
                            chunk: Chunk::new(left_batch),
                            node_id: 0,
                        }),
                    }),
                    right: Box::new(ExecNode {
                        kind: ExecNodeKind::Values(ValuesNode {
                            chunk: Chunk::new(right_batch),
                            node_id: 0,
                        }),
                    }),
                    node_id: 1,
                    join_type: JoinType::RightOuter,
                    distribution_mode: JoinDistributionMode::Partitioned,
                    left_schema: Arc::clone(&left_schema),
                    right_schema: Arc::clone(&right_schema),
                    join_scope_schema,
                    probe_keys: vec![key_left],
                    build_keys: vec![key_right],
                    eq_null_safe: vec![false],
                    residual_predicate: None,
                    runtime_filters: Vec::new(),
                }),
            },
        };

        let handle = ResultSinkHandle::new();
        let runtime_state = Arc::new(RuntimeState::default());
        execute_plan_with_pipeline(
            plan,
            false,
            Duration::from_millis(10),
            Box::new(ResultSinkFactory::new(handle.clone())),
            None,
            None,
            1,
            runtime_state,
            None,
            None,
            None,
        )
        .expect("execute plan");

        let chunks = handle.take_chunks();
        let mut rows = Vec::new();
        for chunk in chunks {
            if chunk.is_empty() {
                continue;
            }
            let lk = chunk
                .columns()
                .get(0)
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            let lv = chunk
                .columns()
                .get(1)
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            let rk = chunk
                .columns()
                .get(2)
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            let rw = chunk
                .columns()
                .get(3)
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            for i in 0..chunk.len() {
                let left = if lk.is_valid(i) && lv.is_valid(i) {
                    Some((lk.value(i), lv.value(i)))
                } else {
                    None
                };
                rows.push((left, rk.value(i), rw.value(i)));
            }
        }
        rows.sort_by_key(|r| r.1);
        assert_eq!(rows, vec![(Some((1, 10)), 1, 100), (None, 2, 200)]);
    }

    #[test]
    fn hash_full_outer_with_empty_left_emits_unmatched_build() {
        let left_schema = Arc::new(Schema::new(vec![
            field_with_slot_id(Field::new("k", DataType::Int32, false), SlotId::new(1)),
            field_with_slot_id(Field::new("v", DataType::Int32, false), SlotId::new(2)),
        ]));
        let left_batch = RecordBatch::new_empty(Arc::clone(&left_schema));

        let right_schema = Arc::new(Schema::new(vec![
            field_with_slot_id(Field::new("k", DataType::Int32, false), SlotId::new(3)),
            field_with_slot_id(Field::new("w", DataType::Int32, false), SlotId::new(4)),
        ]));
        let right_k = Arc::new(Int32Array::from(vec![1])) as arrow::array::ArrayRef;
        let right_w = Arc::new(Int32Array::from(vec![100])) as arrow::array::ArrayRef;
        let right_batch =
            RecordBatch::try_new(Arc::clone(&right_schema), vec![right_k, right_w]).expect("right");

        let join_scope_schema = Arc::new(Schema::new(vec![
            field_with_slot_id(Field::new("k", DataType::Int32, true), SlotId::new(1)),
            field_with_slot_id(Field::new("v", DataType::Int32, true), SlotId::new(2)),
            field_with_slot_id(Field::new("k", DataType::Int32, false), SlotId::new(3)),
            field_with_slot_id(Field::new("w", DataType::Int32, false), SlotId::new(4)),
        ]));

        let mut arena = ExprArena::default();
        let key_left = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Int32);
        let key_right = arena.push_typed(ExprNode::SlotId(SlotId::new(3)), DataType::Int32);

        let plan = ExecPlan {
            arena,
            root: ExecNode {
                kind: ExecNodeKind::Join(JoinNode {
                    left: Box::new(ExecNode {
                        kind: ExecNodeKind::Values(ValuesNode {
                            chunk: Chunk::new(left_batch),
                            node_id: 0,
                        }),
                    }),
                    right: Box::new(ExecNode {
                        kind: ExecNodeKind::Values(ValuesNode {
                            chunk: Chunk::new(right_batch),
                            node_id: 0,
                        }),
                    }),
                    node_id: 1,
                    join_type: JoinType::FullOuter,
                    distribution_mode: JoinDistributionMode::Broadcast,
                    left_schema: Arc::clone(&left_schema),
                    right_schema: Arc::clone(&right_schema),
                    join_scope_schema,
                    probe_keys: vec![key_left],
                    build_keys: vec![key_right],
                    eq_null_safe: vec![false],
                    residual_predicate: None,
                    runtime_filters: Vec::new(),
                }),
            },
        };

        let handle = ResultSinkHandle::new();
        let runtime_state = Arc::new(RuntimeState::default());
        execute_plan_with_pipeline(
            plan,
            false,
            Duration::from_millis(10),
            Box::new(ResultSinkFactory::new(handle.clone())),
            None,
            None,
            1,
            runtime_state,
            None,
            None,
            None,
        )
        .expect("execute plan");

        let chunks = handle.take_chunks();
        let mut rows = Vec::new();
        for chunk in chunks {
            if chunk.is_empty() {
                continue;
            }
            let lk = chunk
                .columns()
                .get(0)
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            let lv = chunk
                .columns()
                .get(1)
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            let rk = chunk
                .columns()
                .get(2)
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            let rw = chunk
                .columns()
                .get(3)
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            for i in 0..chunk.len() {
                let left_is_null = !lk.is_valid(i) && !lv.is_valid(i);
                rows.push((left_is_null, rk.value(i), rw.value(i)));
            }
        }
        rows.sort_by_key(|r| r.1);
        assert_eq!(rows, vec![(true, 1, 100)]);
    }

    #[test]
    fn analytic_row_number_rank_sum_is_correct() {
        let schema = Arc::new(Schema::new(vec![
            field_with_slot_id(Field::new("k", DataType::Int32, false), SlotId::new(1)),
            field_with_slot_id(Field::new("o", DataType::Int32, false), SlotId::new(2)),
            field_with_slot_id(Field::new("v", DataType::Int32, false), SlotId::new(3)),
        ]));
        let k = Arc::new(Int32Array::from(vec![1, 1, 1, 2, 2])) as arrow::array::ArrayRef;
        let o = Arc::new(Int32Array::from(vec![1, 1, 2, 1, 2])) as arrow::array::ArrayRef;
        let v = Arc::new(Int32Array::from(vec![10, 20, 5, 7, 8])) as arrow::array::ArrayRef;
        let batch = RecordBatch::try_new(schema, vec![k, o, v]).expect("record batch");
        let chunk = Chunk::new(batch);

        let mut arena = ExprArena::default();
        let k_expr = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Int32);
        let o_expr = arena.push_typed(ExprNode::SlotId(SlotId::new(2)), DataType::Int32);
        let v_expr = arena.push_typed(ExprNode::SlotId(SlotId::new(3)), DataType::Int32);

        let plan = ExecPlan {
            arena,
            root: ExecNode {
                kind: ExecNodeKind::Analytic(AnalyticNode {
                    input: Box::new(ExecNode {
                        kind: ExecNodeKind::Values(ValuesNode { chunk, node_id: 0 }),
                    }),
                    node_id: 0,
                    partition_exprs: vec![k_expr],
                    order_by_exprs: vec![o_expr],
                    functions: vec![
                        WindowFunctionSpec {
                            kind: WindowFunctionKind::RowNumber,
                            args: vec![],
                            return_type: DataType::Int64,
                        },
                        WindowFunctionSpec {
                            kind: WindowFunctionKind::Rank,
                            args: vec![],
                            return_type: DataType::Int64,
                        },
                        WindowFunctionSpec {
                            kind: WindowFunctionKind::Sum,
                            args: vec![v_expr],
                            return_type: DataType::Int64,
                        },
                    ],
                    window: Some(WindowFrame {
                        window_type: WindowType::Rows,
                        start: None,
                        end: Some(WindowBoundary::CurrentRow),
                    }),
                    output_columns: vec![
                        AnalyticOutputColumn::InputSlotId(SlotId::new(1)),
                        AnalyticOutputColumn::InputSlotId(SlotId::new(2)),
                        AnalyticOutputColumn::InputSlotId(SlotId::new(3)),
                        AnalyticOutputColumn::Window(0),
                        AnalyticOutputColumn::Window(1),
                        AnalyticOutputColumn::Window(2),
                    ],
                    output_slots: vec![
                        SlotId::new(1),
                        SlotId::new(2),
                        SlotId::new(3),
                        SlotId::new(4),
                        SlotId::new(5),
                        SlotId::new(6),
                    ],
                }),
            },
        };

        let handle = ResultSinkHandle::new();
        let runtime_state = Arc::new(RuntimeState::default());
        execute_plan_with_pipeline(
            plan,
            false,
            Duration::from_millis(10),
            Box::new(ResultSinkFactory::new(handle.clone())),
            None,
            None,
            1,
            runtime_state,
            None,
            None,
            None,
        )
        .expect("execute plan");

        let mut out_rows: Vec<(i32, i32, i32, i64, i64, i64)> = Vec::new();
        for c in handle.take_chunks() {
            if c.is_empty() {
                continue;
            }
            let cols = c.columns();
            assert_eq!(cols.len(), 6);
            let k_arr = cols[0].as_any().downcast_ref::<Int32Array>().unwrap();
            let o_arr = cols[1].as_any().downcast_ref::<Int32Array>().unwrap();
            let v_arr = cols[2].as_any().downcast_ref::<Int32Array>().unwrap();
            let rn_arr = cols[3].as_any().downcast_ref::<Int64Array>().unwrap();
            let r_arr = cols[4].as_any().downcast_ref::<Int64Array>().unwrap();
            let sum_arr = cols[5].as_any().downcast_ref::<Int64Array>().unwrap();
            for i in 0..c.len() {
                out_rows.push((
                    k_arr.value(i),
                    o_arr.value(i),
                    v_arr.value(i),
                    rn_arr.value(i),
                    r_arr.value(i),
                    sum_arr.value(i),
                ));
            }
        }

        // Preserve input order within each partition.
        assert_eq!(
            out_rows,
            vec![
                (1, 1, 10, 1, 1, 10),
                (1, 1, 20, 2, 1, 30),
                (1, 2, 5, 3, 3, 35),
                (2, 1, 7, 1, 1, 7),
                (2, 2, 8, 2, 2, 15),
            ]
        );
    }

    #[test]
    fn mixed_merge_and_update_aggregates_work() {
        let schema = Arc::new(Schema::new(vec![
            field_with_slot_id(Field::new("c1", DataType::Int32, false), SlotId::new(1)),
            field_with_slot_id(
                Field::new("sum_state", DataType::Int64, false),
                SlotId::new(2),
            ),
        ]));
        let c1 = Arc::new(Int32Array::from(vec![1, 2])) as arrow::array::ArrayRef;
        let sum_state = Arc::new(Int64Array::from(vec![30_i64, 5_i64])) as arrow::array::ArrayRef;
        let batch = RecordBatch::try_new(schema, vec![c1, sum_state]).expect("record batch");
        let chunk = Chunk::new(batch);

        let mut arena = ExprArena::default();
        let c1_expr = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Int32);
        let sum_expr = arena.push_typed(ExprNode::SlotId(SlotId::new(2)), DataType::Int64);

        let plan = ExecPlan {
            arena,
            root: ExecNode {
                kind: ExecNodeKind::Aggregate(AggregateNode {
                    input: Box::new(ExecNode {
                        kind: ExecNodeKind::Values(ValuesNode { chunk, node_id: 0 }),
                    }),
                    node_id: 0,
                    group_by: vec![],
                    functions: vec![
                        AggFunction {
                            name: "count".to_string(),
                            inputs: vec![c1_expr],
                            input_is_intermediate: false,
                            types: Some(AggTypeSignature {
                                intermediate_type: None,
                                output_type: Some(DataType::Int64),
                                input_arg_type: None,
                            }),
                        },
                        AggFunction {
                            name: "sum".to_string(),
                            inputs: vec![sum_expr],
                            input_is_intermediate: true,
                            types: Some(AggTypeSignature {
                                intermediate_type: None,
                                output_type: Some(DataType::Int64),
                                input_arg_type: None,
                            }),
                        },
                    ],
                    need_finalize: true,
                    input_is_intermediate: false,
                    output_slots: vec![SlotId::new(3), SlotId::new(4)],
                }),
            },
        };

        let handle = ResultSinkHandle::new();
        let runtime_state = Arc::new(RuntimeState::default());
        execute_plan_with_pipeline(
            plan,
            false,
            Duration::from_millis(10),
            Box::new(ResultSinkFactory::new(handle.clone())),
            None,
            None,
            2,
            runtime_state,
            None,
            None,
            None,
        )
        .expect("execute plan");

        let mut out_count = None;
        let mut out_sum = None;
        for chunk in handle.take_chunks() {
            if chunk.is_empty() {
                continue;
            }
            let count_col = chunk
                .column_by_slot_id(SlotId::new(3))
                .expect("count column");
            let sum_col = chunk.column_by_slot_id(SlotId::new(4)).expect("sum column");
            let count_arr = count_col
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("count Int64");
            let sum_arr = sum_col
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("sum Int64");
            out_count = Some(count_arr.value(0));
            out_sum = Some(sum_arr.value(0));
        }

        assert_eq!(out_count, Some(2));
        assert_eq!(out_sum, Some(35));
    }
}
