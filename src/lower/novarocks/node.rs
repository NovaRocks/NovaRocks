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

//! Proto node lowering placeholder.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Array, ArrayRef};
use arrow::compute::concat;
use arrow::datatypes::{DataType, Field, Fields, Schema};
use arrow::record_batch::{RecordBatch, RecordBatchOptions};

use super::expr::lower_proto_expr;
use super::layout::{
    Layout, chunk_schema_from_output_columns, layout_from_output_columns,
    slot_schemas_from_output_columns,
};
use crate::common::config::exchange_wait_ms;
use crate::common::ids::SlotId;
use crate::exec::chunk::{Chunk, ChunkSchema, ChunkSchemaRef, ChunkSlotSchema};
use crate::exec::expr::{ExprArena, ExprNode, cast_array_to_target};
use crate::exec::node::aggregate::{AggFunction, AggOrderSpec, AggTypeSignature, AggregateNode};
use crate::exec::node::analytic::{
    AnalyticNode, AnalyticOutputColumn, WindowBoundary, WindowFrame, WindowFunctionKind,
    WindowFunctionSpec, WindowType,
};
use crate::exec::node::assert::{AssertNumRowsMode, AssertNumRowsNode, Assertion};
use crate::exec::node::change_event_expand::{
    ChangeEventExpandNode, ChangeEventRuntimeOutputExpr, ChangeEventRuntimeSpec,
};
use crate::exec::node::exchange_source::ExchangeSourceNode;
use crate::exec::node::filter::FilterNode;
use crate::exec::node::join::{JoinDistributionMode, JoinNode, JoinRuntimeFilterSpec, JoinType};
use crate::exec::node::limit::LimitNode;
use crate::exec::node::nljoin::{NestedLoopJoinNode, NestedLoopJoinType};
use crate::exec::node::project::ProjectNode;
use crate::exec::node::repeat::RepeatNode;
use crate::exec::node::set_op::{SetOpKind, SetOpNode};
use crate::exec::node::sort::{SortExpression, SortNode, SortTopNType};
use crate::exec::node::table_function::{TableFunctionNode, TableFunctionOutputSlot};
use crate::exec::node::union_all::UnionAllNode;
use crate::exec::node::values::ValuesNode;
use crate::exec::node::{ExecNode, ExecNodeKind};
use crate::proto::{common, expr, novarocks, plan};
use crate::runtime::exchange::ExchangeKey;
use crate::runtime::query_options::QueryOptions;
use crate::sql::codegen::expr_compiler::infer_agg_function_types;
use crate::sql::common::ChangeStreamBranchKind;
use crate::types::wider_type;

#[derive(Clone, Debug)]
pub(crate) struct LoweredNode {
    pub node: ExecNode,
    pub layout: Layout,
    pub output_schema: ChunkSchemaRef,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct NodeLoweringContext {
    exchange_sender_counts: HashMap<ExchangeKey, usize>,
    scan_ranges: HashMap<i32, Vec<novarocks::ScanRangeParams>>,
    query_options: Option<QueryOptions>,
    connectors: Option<Arc<crate::connector::ConnectorRegistry>>,
    fragment_instance_hi: i64,
    fragment_instance_lo: i64,
}

impl NodeLoweringContext {
    #[allow(dead_code)]
    pub(crate) fn with_fragment_instance_id(mut self, hi: i64, lo: i64) -> Self {
        self.fragment_instance_hi = hi;
        self.fragment_instance_lo = lo;
        self
    }

    #[allow(dead_code)]
    pub(crate) fn with_exchange_sender_count(mut self, key: ExchangeKey, count: usize) -> Self {
        self.exchange_sender_counts.insert(key, count);
        self
    }

    #[allow(dead_code)]
    pub(crate) fn with_scan_ranges(
        mut self,
        node_id: i32,
        ranges: Vec<novarocks::ScanRangeParams>,
    ) -> Self {
        self.scan_ranges.insert(node_id, ranges);
        self
    }

    #[allow(dead_code)]
    pub(crate) fn with_query_options(mut self, query_options: Option<QueryOptions>) -> Self {
        self.query_options = query_options;
        self
    }

    #[allow(dead_code)]
    pub(crate) fn with_connector_registry(
        mut self,
        connectors: Arc<crate::connector::ConnectorRegistry>,
    ) -> Self {
        self.connectors = Some(connectors);
        self
    }

    pub(crate) fn scan_ranges(
        &self,
        node_id: i32,
    ) -> Result<&[novarocks::ScanRangeParams], String> {
        self.scan_ranges
            .get(&node_id)
            .map(Vec::as_slice)
            .ok_or_else(|| format!("native ScanNode node_id={node_id} missing scan ranges"))
    }

    pub(crate) fn query_options(&self) -> Option<&QueryOptions> {
        self.query_options.as_ref()
    }

    pub(crate) fn connectors(&self) -> Result<&crate::connector::ConnectorRegistry, String> {
        self.connectors.as_deref().ok_or_else(|| {
            "native ScanNode requires ConnectorRegistry in NodeLoweringContext".to_string()
        })
    }

    fn exchange_key(&self, node_id: i32) -> ExchangeKey {
        ExchangeKey {
            finst_id_hi: self.fragment_instance_hi,
            finst_id_lo: self.fragment_instance_lo,
            node_id,
        }
    }
}

#[allow(dead_code)]
pub(crate) fn lower_proto_node(
    node: &plan::DistributedNode,
    arena: &mut ExprArena,
    ctx: &NodeLoweringContext,
) -> Result<LoweredNode, String> {
    let children = node
        .children
        .iter()
        .map(|child| lower_proto_node(child, arena, ctx))
        .collect::<Result<Vec<_>, _>>()?;

    let payload = node
        .payload
        .as_ref()
        .ok_or_else(|| format!("DistributedNode node_id={} payload missing", node.node_id))?;
    let lowered = match payload {
        plan::distributed_node::Payload::Physical(physical) => {
            lower_physical_node(node, physical, children, arena, ctx)
        }
        plan::distributed_node::Payload::Exchange(exchange) => {
            lower_exchange_receiver(node, exchange, children, arena, ctx)
        }
    }?;
    apply_distributed_limit_if_needed(node, lowered)
}

fn apply_distributed_limit_if_needed(
    node: &plan::DistributedNode,
    mut lowered: LoweredNode,
) -> Result<LoweredNode, String> {
    let Some(limit) = parse_distributed_limit(node.limit, "DistributedNode.limit")? else {
        return Ok(lowered);
    };
    if matches!(
        lowered.node.kind,
        ExecNodeKind::Limit(_) | ExecNodeKind::Sort(_)
    ) {
        return Ok(lowered);
    }
    lowered.node = ExecNode {
        kind: ExecNodeKind::Limit(LimitNode {
            input: Box::new(lowered.node),
            node_id: node.node_id,
            limit: Some(limit),
            offset: 0,
        }),
    };
    Ok(lowered)
}

fn lower_physical_node(
    node: &plan::DistributedNode,
    physical: &plan::PlanNode,
    children: Vec<LoweredNode>,
    arena: &mut ExprArena,
    ctx: &NodeLoweringContext,
) -> Result<LoweredNode, String> {
    let kind = physical
        .kind
        .as_ref()
        .ok_or_else(|| format!("PlanNode node_id={} kind missing", node.node_id))?;
    match kind {
        plan::plan_node::Kind::Values(values) => {
            lower_values_node(node, physical, values, children, arena)
        }
        plan::plan_node::Kind::Project(project) => {
            lower_project_node(node, project, children, arena)
        }
        plan::plan_node::Kind::Filter(filter) => lower_filter_node(node, filter, children, arena),
        plan::plan_node::Kind::Limit(limit) => lower_limit_node(node, limit, children),
        plan::plan_node::Kind::Sort(sort) => lower_sort_node(node, physical, sort, children, arena),
        plan::plan_node::Kind::Topn(topn) => lower_topn_node(node, topn, children, arena),
        plan::plan_node::Kind::SetOp(set_op) => {
            lower_set_op_node(node, physical, set_op, children, arena)
        }
        plan::plan_node::Kind::AssertOneRow(assert) => {
            lower_assert_one_row_node(node, assert, children)
        }
        plan::plan_node::Kind::Scan(scan) => {
            super::scan::lower_scan_node(node, physical, scan, ctx, arena)
        }
        plan::plan_node::Kind::HashAggregate(aggregate) => {
            lower_hash_aggregate_node(node, physical, aggregate, children, arena)
        }
        plan::plan_node::Kind::HashJoin(join) => {
            lower_hash_join_node(node, physical, join, children, arena)
        }
        plan::plan_node::Kind::NestLoopJoin(join) => {
            lower_nest_loop_join_node(node, physical, join, children, arena)
        }
        plan::plan_node::Kind::Window(window) => {
            lower_window_node(node, physical, window, children, arena)
        }
        plan::plan_node::Kind::Repeat(repeat) => lower_repeat_node(node, repeat, children),
        plan::plan_node::Kind::GenerateSeries(generate_series) => {
            lower_generate_series_node(node, generate_series, children, arena)
        }
        plan::plan_node::Kind::TableFunction(table_function) => {
            lower_table_function_node(node, table_function, children, arena)
        }
        plan::plan_node::Kind::Decode(_) => unsupported("Decode"),
        plan::plan_node::Kind::ChangeEventExpand(expand) => {
            lower_change_event_expand_node(node, physical, expand, children, arena)
        }
        plan::plan_node::Kind::CteAnchor(_) => unsupported("CTEAnchor"),
        plan::plan_node::Kind::CteProduce(_) => unsupported("CTEProduce"),
        plan::plan_node::Kind::CteConsume(_) => unsupported("CTEConsume"),
        plan::plan_node::Kind::Redistribute(redistribute) => {
            lower_redistribute_node(physical, redistribute, children, arena)
        }
    }
}

fn unsupported<T>(kind: &str) -> Result<T, String> {
    Err(format!(
        "{kind} native proto node lowering is not implemented"
    ))
}

fn exec_node_kind_label(kind: &ExecNodeKind) -> &'static str {
    match kind {
        ExecNodeKind::Scan(_) => "Scan",
        ExecNodeKind::IcebergDeltaScan(_) => "IcebergDeltaScan",
        ExecNodeKind::Project(_) => "Project",
        ExecNodeKind::Filter(_) => "Filter",
        ExecNodeKind::Aggregate(_) => "Aggregate",
        ExecNodeKind::Join(_) => "Join",
        ExecNodeKind::NestedLoopJoin(_) => "NestedLoopJoin",
        ExecNodeKind::Sort(_) => "Sort",
        ExecNodeKind::Limit(_) => "Limit",
        ExecNodeKind::ExchangeSource(_) => "ExchangeSource",
        ExecNodeKind::UnionAll(_) => "UnionAll",
        ExecNodeKind::SetOp(_) => "SetOp",
        ExecNodeKind::Values(_) => "Values",
        ExecNodeKind::TableFunction(_) => "TableFunction",
        ExecNodeKind::Repeat(_) => "Repeat",
        ExecNodeKind::ChangeEventExpand(_) => "ChangeEventExpand",
        ExecNodeKind::AssertNumRows(_) => "AssertNumRows",
        ExecNodeKind::Analytic(_) => "Analytic",
        ExecNodeKind::Fetch(_) => "Fetch",
        ExecNodeKind::LookUp(_) => "LookUp",
    }
}

fn check_arity(kind: &str, expected: &str, actual: usize, ok: bool) -> Result<(), String> {
    if ok {
        Ok(())
    } else {
        Err(format!("{kind} expected {expected} children, got {actual}"))
    }
}

fn check_exact_arity(kind: &str, expected: usize, actual: usize) -> Result<(), String> {
    check_arity(kind, &expected.to_string(), actual, actual == expected)
}

fn check_min_arity(kind: &str, min: usize, actual: usize) -> Result<(), String> {
    check_arity(kind, &format!(">={min}"), actual, actual >= min)
}

fn lower_values_node(
    node: &plan::DistributedNode,
    physical: &plan::PlanNode,
    values: &plan::ValuesNode,
    children: Vec<LoweredNode>,
    arena: &mut ExprArena,
) -> Result<LoweredNode, String> {
    check_exact_arity("ValuesNode", 0, children.len())?;
    let columns = if values.columns.is_empty() {
        &physical.output_columns
    } else {
        &values.columns
    };
    let layout = layout_from_output_columns(columns)?;
    let output_schema = chunk_schema_from_output_columns(columns)?;
    let chunk = materialize_values_chunk(&values.rows, columns, output_schema.clone(), arena)?;
    Ok(LoweredNode {
        node: ExecNode {
            kind: ExecNodeKind::Values(ValuesNode {
                chunk,
                node_id: node.node_id,
            }),
        },
        layout,
        output_schema,
    })
}

fn materialize_values_chunk(
    rows: &[plan::ExprList],
    columns: &[common::OutputColumn],
    output_schema: ChunkSchemaRef,
    arena: &mut ExprArena,
) -> Result<Chunk, String> {
    if columns.is_empty() {
        return empty_chunk_with_row_count(rows.len().max(1));
    }
    if rows.is_empty() {
        let batch = RecordBatch::new_empty(output_schema.arrow_schema_ref());
        return Chunk::try_new_with_chunk_schema(batch, output_schema);
    }
    let column_count = columns.len();
    if output_schema.slots().len() != column_count {
        return Err(format!(
            "ValuesNode output schema width mismatch: columns={}, schema_slots={}",
            column_count,
            output_schema.slots().len()
        ));
    }
    let target_types = output_schema
        .slots()
        .iter()
        .map(|slot| slot.data_type().clone())
        .collect::<Vec<_>>();
    let mut arrays_by_column = vec![Vec::<ArrayRef>::with_capacity(rows.len()); column_count];
    let input_layout = Layout::default();
    let one_row = empty_chunk_with_row_count(1)?;

    for (row_idx, row) in rows.iter().enumerate() {
        if row.values.len() != column_count {
            return Err(format!(
                "ValuesNode row {row_idx} width mismatch: expected {column_count}, got {}",
                row.values.len()
            ));
        }
        for (col_idx, expr) in row.values.iter().enumerate() {
            let expr_id = lower_proto_expr(expr, arena, &input_layout)
                .map_err(|err| format!("ValuesNode row {row_idx} column {col_idx}: {err}"))?;
            let array = arena
                .eval(expr_id, &one_row)
                .map_err(|err| format!("ValuesNode row {row_idx} column {col_idx}: {err}"))?;
            if array.len() != 1 {
                return Err(format!(
                    "ValuesNode row {row_idx} column {col_idx} evaluated to {} rows, expected 1",
                    array.len()
                ));
            }
            let array = normalize_values_array(row_idx, col_idx, array, &target_types[col_idx])?;
            arrays_by_column[col_idx].push(array);
        }
    }

    let columns = arrays_by_column
        .into_iter()
        .enumerate()
        .map(|(col_idx, parts)| {
            let refs = parts
                .iter()
                .map(|part| part.as_ref() as &dyn Array)
                .collect::<Vec<_>>();
            concat(&refs).map_err(|err| format!("ValuesNode column {col_idx} concat failed: {err}"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    Chunk::try_new_with_columns(output_schema, columns)
}

fn normalize_values_array(
    row_idx: usize,
    col_idx: usize,
    array: ArrayRef,
    target_type: &DataType,
) -> Result<ArrayRef, String> {
    if array.data_type() == target_type || matches!(target_type, DataType::Null) {
        return Ok(array);
    }
    cast_array_to_target(&array, target_type).map_err(|err| {
        format!(
            "ValuesNode row {row_idx} column {col_idx} cast from {:?} to {:?} failed: {err}",
            array.data_type(),
            target_type
        )
    })
}

fn empty_chunk_with_row_count(row_count: usize) -> Result<Chunk, String> {
    let schema = Arc::new(Schema::empty());
    let options = RecordBatchOptions::new().with_row_count(Some(row_count));
    let batch = RecordBatch::try_new_with_options(schema, Vec::new(), &options)
        .map_err(|err| format!("build empty values input chunk failed: {err}"))?;
    Chunk::try_new_with_chunk_schema(batch, Arc::new(ChunkSchema::empty()))
}

fn lower_generate_series_node(
    node: &plan::DistributedNode,
    generate_series: &plan::GenerateSeriesNode,
    children: Vec<LoweredNode>,
    arena: &mut ExprArena,
) -> Result<LoweredNode, String> {
    check_exact_arity("GenerateSeriesNode", 0, children.len())?;
    if generate_series.step == 0 {
        return Err("GenerateSeriesNode step must not be zero".to_string());
    }

    let param_slots = generate_series_param_slots(generate_series.output_column_id)?;
    let param_columns = vec![
        bigint_output_column(param_slots[0].as_u32(), "generate_series_start", false),
        bigint_output_column(param_slots[1].as_u32(), "generate_series_end", false),
        bigint_output_column(param_slots[2].as_u32(), "generate_series_step", false),
    ];
    let input_schema = chunk_schema_from_output_columns(&param_columns)?;
    let rows = vec![plan::ExprList {
        values: vec![
            int64_literal_expr(generate_series.start),
            int64_literal_expr(generate_series.end),
            int64_literal_expr(generate_series.step),
        ],
    }];
    let input_chunk = materialize_values_chunk(&rows, &param_columns, input_schema, arena)?;

    let output_columns = vec![bigint_output_column(
        generate_series.output_column_id,
        if generate_series.column_name.is_empty() {
            "generate_series"
        } else {
            &generate_series.column_name
        },
        false,
    )];
    let layout = layout_from_output_columns(&output_columns)?;
    let output_schema = chunk_schema_from_output_columns(&output_columns)?;

    Ok(LoweredNode {
        node: ExecNode {
            kind: ExecNodeKind::TableFunction(TableFunctionNode {
                input: Box::new(ExecNode {
                    kind: ExecNodeKind::Values(ValuesNode {
                        chunk: input_chunk,
                        node_id: node.node_id,
                    }),
                }),
                node_id: node.node_id,
                function_name: "generate_series".to_string(),
                param_slots: param_slots.to_vec(),
                outer_slots: Vec::new(),
                fn_result_slots: vec![SlotId::new(generate_series.output_column_id)],
                fn_result_required: true,
                is_left_join: false,
                param_types: vec![DataType::Int64, DataType::Int64, DataType::Int64],
                ret_types: vec![DataType::Int64],
                output_chunk_schema: output_schema.clone(),
                output_slot_sources: vec![TableFunctionOutputSlot::Result { index: 0 }],
            }),
        },
        layout,
        output_schema,
    })
}

fn generate_series_param_slots(output_column_id: u32) -> Result<[SlotId; 3], String> {
    let mut slot = u32::MAX;
    let mut slots = Vec::with_capacity(3);
    while slots.len() < 3 {
        if slot != output_column_id {
            slots.push(SlotId::new(slot));
        }
        slot = slot
            .checked_sub(1)
            .ok_or_else(|| "GenerateSeriesNode could not allocate internal slots".to_string())?;
    }
    Ok([slots[0], slots[1], slots[2]])
}

fn bigint_output_column(column_id: u32, name: &str, nullable: bool) -> common::OutputColumn {
    common::OutputColumn {
        column_id,
        name: name.to_string(),
        r#type: Some(bigint_type_desc()),
        nullable,
        is_internal: false,
    }
}

fn bigint_type_desc() -> common::TypeDesc {
    common::TypeDesc {
        kind: Some(common::type_desc::Kind::Scalar(common::ScalarType {
            r#type: common::PrimitiveType::Bigint as i32,
            len: None,
            precision: None,
            scale: None,
            time_unit: None,
        })),
    }
}

fn int64_literal_expr(value: i64) -> expr::Expr {
    expr::Expr {
        r#type: Some(bigint_type_desc()),
        nullable: false,
        kind: Some(expr::expr::Kind::Literal(expr::LiteralExpr {
            value: Some(common::LiteralValue {
                value: Some(common::literal_value::Value::IntValue(value)),
            }),
        })),
    }
}

fn lower_table_function_node(
    node: &plan::DistributedNode,
    table_function: &plan::TableFunctionNode,
    mut children: Vec<LoweredNode>,
    arena: &mut ExprArena,
) -> Result<LoweredNode, String> {
    check_exact_arity("TableFunctionNode", 1, children.len())?;
    let child = children.pop().expect("child");
    validate_table_function_signature(table_function)?;

    let param_slots = table_function_param_slots(
        &child.layout,
        &table_function.output_columns,
        &table_function.args,
    )?;
    let (param_types, param_slot_schemas) =
        table_function_param_schemas(table_function, &param_slots)?;
    let result_slot_schemas = slot_schemas_from_output_columns(&table_function.output_columns)?;
    let ret_types = table_function_result_types(table_function)?;

    let mut project_exprs = Vec::with_capacity(child.layout.order().len() + param_slots.len());
    let mut project_slot_ids = Vec::with_capacity(project_exprs.capacity());
    let mut project_slot_schemas =
        Vec::with_capacity(child.output_schema.slots().len() + param_slot_schemas.len());
    for slot_schema in child.output_schema.slots() {
        let slot_id = slot_schema.slot_id();
        project_exprs
            .push(arena.push_typed(ExprNode::SlotId(slot_id), slot_schema.data_type().clone()));
        project_slot_ids.push(slot_id);
        project_slot_schemas.push(slot_schema.clone());
    }
    for ((idx, arg), slot_schema) in table_function
        .args
        .iter()
        .enumerate()
        .zip(param_slot_schemas.iter())
    {
        let expr = lower_proto_expr(arg, arena, &child.layout)
            .map_err(|err| format!("TableFunctionNode arg {idx}: {err}"))?;
        project_exprs.push(expr);
        project_slot_ids.push(slot_schema.slot_id());
        project_slot_schemas.push(slot_schema.clone());
    }
    let project_output_schema = Arc::new(ChunkSchema::try_new(project_slot_schemas)?);

    let mut output_slot_schemas =
        Vec::with_capacity(child.output_schema.slots().len() + result_slot_schemas.len());
    let mut output_slot_sources =
        Vec::with_capacity(child.output_schema.slots().len() + result_slot_schemas.len());
    let mut outer_slots = Vec::with_capacity(child.output_schema.slots().len());
    for slot_schema in child.output_schema.slots() {
        let slot_id = slot_schema.slot_id();
        outer_slots.push(slot_id);
        output_slot_schemas.push(slot_schema.clone());
        output_slot_sources.push(TableFunctionOutputSlot::Outer { slot: slot_id });
    }
    let mut fn_result_slots = Vec::with_capacity(result_slot_schemas.len());
    for (idx, slot_schema) in result_slot_schemas.iter().enumerate() {
        let slot_id = slot_schema.slot_id();
        fn_result_slots.push(slot_id);
        output_slot_schemas.push(slot_schema.clone());
        output_slot_sources.push(TableFunctionOutputSlot::Result { index: idx });
    }
    let output_schema = Arc::new(ChunkSchema::try_new(output_slot_schemas)?);
    let layout = Layout::for_slots(output_schema.slot_ids().iter().copied());

    Ok(LoweredNode {
        node: ExecNode {
            kind: ExecNodeKind::TableFunction(TableFunctionNode {
                input: Box::new(ExecNode {
                    kind: ExecNodeKind::Project(ProjectNode {
                        input: Box::new(child.node),
                        node_id: node.node_id,
                        is_subordinate: true,
                        exprs: project_exprs,
                        expr_slot_ids: project_slot_ids,
                        expr_slot_schemas: Some(project_output_schema.slots().to_vec()),
                        output_indices: None,
                        output_chunk_schema: project_output_schema,
                    }),
                }),
                node_id: node.node_id,
                function_name: table_function.function_name.clone(),
                param_slots,
                outer_slots,
                fn_result_slots,
                fn_result_required: true,
                is_left_join: table_function.is_left_join,
                param_types,
                ret_types,
                output_chunk_schema: output_schema.clone(),
                output_slot_sources,
            }),
        },
        layout,
        output_schema,
    })
}

fn validate_table_function_signature(
    table_function: &plan::TableFunctionNode,
) -> Result<(), String> {
    let function_name = table_function.function_name.to_ascii_lowercase();
    let param_types = table_function_arg_types(table_function)?;
    let ret_types = table_function_result_types(table_function)?;
    match function_name.as_str() {
        "unnest" => validate_unnest_table_function(&param_types, &ret_types),
        "unnest_bitmap" => {
            validate_table_function_arity("unnest_bitmap", &param_types, &ret_types, 1, 1)?;
            if !matches!(param_types.first(), Some(DataType::Binary)) {
                return Err(format!(
                    "table function unnest_bitmap param 0 expects Binary, got {:?}",
                    param_types.first()
                ));
            }
            if !matches!(ret_types.first(), Some(DataType::Int64)) {
                return Err(format!(
                    "table function unnest_bitmap return type expects Int64, got {:?}",
                    ret_types.first()
                ));
            }
            Ok(())
        }
        "subdivide_bitmap" => {
            validate_table_function_arity("subdivide_bitmap", &param_types, &ret_types, 2, 1)?;
            if !matches!(param_types.first(), Some(DataType::Binary)) {
                return Err(format!(
                    "table function subdivide_bitmap param 0 expects Binary, got {:?}",
                    param_types.first()
                ));
            }
            if !matches!(ret_types.first(), Some(DataType::Binary)) {
                return Err(format!(
                    "table function subdivide_bitmap return type expects Binary, got {:?}",
                    ret_types.first()
                ));
            }
            Ok(())
        }
        "generate_series" => {
            if !(param_types.len() == 2 || param_types.len() == 3) || ret_types.len() != 1 {
                return Err(format!(
                    "table function generate_series expects 2 or 3 args and 1 output, got args={} outputs={}",
                    param_types.len(),
                    ret_types.len()
                ));
            }
            if !ret_types.iter().all(is_table_function_integer_type) {
                return Err(format!(
                    "table function generate_series return type expects integer, got {:?}",
                    ret_types.first()
                ));
            }
            for (idx, param_type) in param_types.iter().enumerate() {
                if !is_table_function_integer_type(param_type) {
                    return Err(format!(
                        "table function generate_series param {idx} expects integer, got {param_type:?}"
                    ));
                }
            }
            Ok(())
        }
        _ => Err(format!(
            "unsupported native table function: {}",
            table_function.function_name
        )),
    }
}

fn validate_unnest_table_function(
    param_types: &[DataType],
    ret_types: &[DataType],
) -> Result<(), String> {
    if param_types.is_empty() {
        return Err("table function unnest requires at least one argument".to_string());
    }
    if param_types.len() != ret_types.len() {
        return Err(format!(
            "table function unnest output column count mismatch: args={} outputs={}",
            param_types.len(),
            ret_types.len()
        ));
    }
    for (idx, (param_type, ret_type)) in param_types.iter().zip(ret_types.iter()).enumerate() {
        let DataType::List(item_field) = param_type else {
            return Err(format!(
                "table function unnest param {idx} expects List, got {param_type:?}"
            ));
        };
        if item_field.data_type() != ret_type {
            return Err(format!(
                "table function unnest result type mismatch for param {idx}: item={:?} output={:?}",
                item_field.data_type(),
                ret_type
            ));
        }
    }
    Ok(())
}

fn validate_table_function_arity(
    name: &str,
    param_types: &[DataType],
    ret_types: &[DataType],
    expected_params: usize,
    expected_results: usize,
) -> Result<(), String> {
    if param_types.len() != expected_params || ret_types.len() != expected_results {
        return Err(format!(
            "table function {name} expects {expected_params} args and {expected_results} outputs, got args={} outputs={}",
            param_types.len(),
            ret_types.len()
        ));
    }
    Ok(())
}

fn is_table_function_integer_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
    )
}

fn table_function_arg_types(
    table_function: &plan::TableFunctionNode,
) -> Result<Vec<DataType>, String> {
    table_function
        .args
        .iter()
        .enumerate()
        .map(|(idx, arg)| {
            let type_desc = arg
                .r#type
                .as_ref()
                .ok_or_else(|| format!("TableFunctionNode arg {idx} type missing"))?;
            super::decode_type(type_desc)
                .map_err(|err| format!("TableFunctionNode arg {idx} type decode failed: {err}"))
        })
        .collect()
}

fn table_function_result_types(
    table_function: &plan::TableFunctionNode,
) -> Result<Vec<DataType>, String> {
    table_function
        .output_columns
        .iter()
        .enumerate()
        .map(|(idx, column)| {
            let type_desc = column.r#type.as_ref().ok_or_else(|| {
                format!(
                    "TableFunctionNode output column {} '{}' type missing",
                    idx, column.name
                )
            })?;
            super::decode_type(type_desc).map_err(|err| {
                format!(
                    "TableFunctionNode output column {} '{}' type decode failed: {err}",
                    idx, column.name
                )
            })
        })
        .collect()
}

fn table_function_param_schemas(
    table_function: &plan::TableFunctionNode,
    param_slots: &[SlotId],
) -> Result<(Vec<DataType>, Vec<ChunkSlotSchema>), String> {
    let mut param_types = Vec::with_capacity(table_function.args.len());
    let mut slot_schemas = Vec::with_capacity(table_function.args.len());
    for (idx, (arg, slot_id)) in table_function
        .args
        .iter()
        .zip(param_slots.iter())
        .enumerate()
    {
        let type_desc = arg
            .r#type
            .as_ref()
            .ok_or_else(|| format!("TableFunctionNode arg {idx} type missing"))?;
        let data_type = super::decode_type(type_desc)
            .map_err(|err| format!("TableFunctionNode arg {idx} type decode failed: {err}"))?;
        let field =
            super::decode_field_type(&format!("__tf_arg_{idx}"), arg.nullable, type_desc)
                .map_err(|err| format!("TableFunctionNode arg {idx} field decode failed: {err}"))?;
        slot_schemas.push(ChunkSchema::slot_schema_from_arrow_field(*slot_id, &field)?);
        param_types.push(data_type);
    }
    Ok((param_types, slot_schemas))
}

fn table_function_param_slots(
    input_layout: &Layout,
    output_columns: &[common::OutputColumn],
    args: &[expr::Expr],
) -> Result<Vec<SlotId>, String> {
    let mut used = input_layout
        .order()
        .iter()
        .map(|slot| slot.as_u32())
        .collect::<HashSet<_>>();
    used.extend(output_columns.iter().map(|column| column.column_id));
    let mut slot = u32::MAX;
    let mut slots = Vec::with_capacity(args.len());
    while slots.len() < args.len() {
        if used.insert(slot) {
            slots.push(SlotId::new(slot));
        }
        slot = slot
            .checked_sub(1)
            .ok_or_else(|| "TableFunctionNode could not allocate internal slots".to_string())?;
    }
    Ok(slots)
}

fn lower_project_node(
    node: &plan::DistributedNode,
    project: &plan::ProjectNode,
    mut children: Vec<LoweredNode>,
    arena: &mut ExprArena,
) -> Result<LoweredNode, String> {
    check_exact_arity("ProjectNode", 1, children.len())?;
    let child = children.pop().expect("child");
    let project_outputs = project_output_plan(project, &child.layout)?;
    let layout = layout_from_output_columns(&project_outputs.output_columns)?;
    let output_schema = chunk_schema_from_output_columns(&project_outputs.output_columns)?;
    let expr_slot_schemas = slot_schemas_from_output_columns(&project_outputs.computed_columns)?;

    let exprs = project_outputs
        .computed_item_indices
        .iter()
        .map(|idx| {
            let item = project
                .items
                .get(*idx)
                .ok_or_else(|| format!("ProjectNode item {idx} missing"))?;
            let expr = item
                .expr
                .as_ref()
                .ok_or_else(|| format!("ProjectNode item {} expr missing", idx))?;
            lower_proto_expr(expr, arena, &child.layout)
                .map_err(|err| format!("ProjectNode item {}: {err}", idx))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let expr_slot_ids = project_outputs
        .computed_columns
        .iter()
        .map(|column| SlotId::new(column.column_id))
        .collect();

    Ok(LoweredNode {
        node: ExecNode {
            kind: ExecNodeKind::Project(ProjectNode {
                input: Box::new(child.node),
                node_id: node.node_id,
                is_subordinate: false,
                exprs,
                expr_slot_ids,
                expr_slot_schemas: Some(expr_slot_schemas),
                output_indices: project_outputs.output_indices,
                output_chunk_schema: output_schema.clone(),
            }),
        },
        layout,
        output_schema,
    })
}

struct ProjectOutputPlan {
    computed_item_indices: Vec<usize>,
    computed_columns: Vec<common::OutputColumn>,
    output_columns: Vec<common::OutputColumn>,
    output_indices: Option<Vec<usize>>,
}

fn project_output_plan(
    project: &plan::ProjectNode,
    input_layout: &Layout,
) -> Result<ProjectOutputPlan, String> {
    let item_outputs = project
        .items
        .iter()
        .enumerate()
        .map(project_item_output)
        .collect::<Result<Vec<_>, _>>()?;
    let input_column_ids = input_layout
        .order()
        .iter()
        .map(|slot| slot.as_u32())
        .collect::<HashSet<_>>();
    let output_column_id_candidates = item_outputs
        .iter()
        .map(|item| item.output_column_id)
        .collect::<HashSet<_>>();
    let mut used_output_column_ids = HashSet::new();
    let mut used_compute_column_ids = input_column_ids.clone();
    let mut next_synthetic_column_id = output_column_id_candidates
        .iter()
        .chain(used_compute_column_ids.iter())
        .copied()
        .max()
        .unwrap_or(0)
        .saturating_add(1);
    let mut first_expr_index_by_column_id = HashMap::new();
    let mut computed_item_indices = Vec::new();
    let mut computed_columns = Vec::new();
    let mut output_columns = Vec::with_capacity(project.items.len());
    let mut output_indices = Vec::with_capacity(project.items.len());
    let mut needs_output_indices = false;

    for item in item_outputs {
        let preferred_compute_column_id = item.preferred_compute_column_id;
        let mut compute_column_id = if item.can_reuse_input_slot
            || !input_column_ids.contains(&preferred_compute_column_id)
        {
            preferred_compute_column_id
        } else {
            allocate_project_synthetic_column_id(
                &mut next_synthetic_column_id,
                &mut used_output_column_ids,
                &mut used_compute_column_ids,
            )?
        };
        if !item.can_reuse_input_slot && used_compute_column_ids.contains(&compute_column_id) {
            compute_column_id = allocate_project_synthetic_column_id(
                &mut next_synthetic_column_id,
                &mut used_output_column_ids,
                &mut used_compute_column_ids,
            )?;
        }

        let (computed_idx, is_duplicate_compute) = if item.can_reuse_input_slot
            && let Some(computed_idx) = first_expr_index_by_column_id.get(&compute_column_id)
        {
            (*computed_idx, true)
        } else {
            let computed_idx = computed_columns.len();
            first_expr_index_by_column_id.insert(compute_column_id, computed_idx);
            used_compute_column_ids.insert(compute_column_id);
            computed_item_indices.push(item.item_index);
            computed_columns.push(common::OutputColumn {
                column_id: compute_column_id,
                name: item.output_name.clone(),
                r#type: Some(item.r#type.clone()),
                nullable: item.nullable,
                is_internal: false,
            });
            (computed_idx, false)
        };

        let output_column_id = if used_output_column_ids.insert(item.output_column_id) {
            item.output_column_id
        } else {
            allocate_project_synthetic_column_id(
                &mut next_synthetic_column_id,
                &mut used_output_column_ids,
                &mut used_compute_column_ids,
            )?
        };
        output_columns.push(common::OutputColumn {
            column_id: output_column_id,
            name: item.output_name.clone(),
            r#type: Some(item.r#type),
            nullable: item.nullable,
            is_internal: false,
        });
        if is_duplicate_compute
            || computed_idx != output_indices.len()
            || compute_column_id != output_column_id
        {
            needs_output_indices = true;
        }
        output_indices.push(computed_idx);
    }

    Ok(ProjectOutputPlan {
        computed_item_indices,
        computed_columns,
        output_columns,
        output_indices: needs_output_indices.then_some(output_indices),
    })
}

fn allocate_project_synthetic_column_id(
    next_synthetic_column_id: &mut u32,
    used_output_column_ids: &mut HashSet<u32>,
    used_compute_column_ids: &mut HashSet<u32>,
) -> Result<u32, String> {
    while used_output_column_ids.contains(next_synthetic_column_id)
        || used_compute_column_ids.contains(next_synthetic_column_id)
    {
        *next_synthetic_column_id = next_synthetic_column_id
            .checked_add(1)
            .ok_or_else(|| "ProjectNode cannot allocate synthetic output column id".to_string())?;
    }
    let synthetic = *next_synthetic_column_id;
    used_output_column_ids.insert(synthetic);
    used_compute_column_ids.insert(synthetic);
    *next_synthetic_column_id = next_synthetic_column_id
        .checked_add(1)
        .ok_or_else(|| "ProjectNode cannot allocate synthetic output column id".to_string())?;
    Ok(synthetic)
}

struct ProjectItemOutput {
    item_index: usize,
    preferred_compute_column_id: u32,
    output_column_id: u32,
    can_reuse_input_slot: bool,
    output_name: String,
    r#type: common::TypeDesc,
    nullable: bool,
}

fn project_item_output(
    (idx, item): (usize, &plan::ProjectItem),
) -> Result<ProjectItemOutput, String> {
    let expr = item
        .expr
        .as_ref()
        .ok_or_else(|| format!("ProjectNode item {idx} expr missing"))?;
    let r#type = expr
        .r#type
        .clone()
        .ok_or_else(|| format!("ProjectNode item {idx} expr type missing"))?;
    let (preferred_compute_column_id, can_reuse_input_slot) = match expr.kind.as_ref() {
        Some(expr::expr::Kind::ColumnRef(column)) => (column.column_id, true),
        _ => (item.output_column_id, false),
    };
    Ok(ProjectItemOutput {
        item_index: idx,
        preferred_compute_column_id,
        output_column_id: item.output_column_id,
        can_reuse_input_slot,
        output_name: item.output_name.clone(),
        r#type,
        nullable: expr.nullable,
    })
}

fn lower_filter_node(
    node: &plan::DistributedNode,
    filter: &plan::FilterNode,
    mut children: Vec<LoweredNode>,
    arena: &mut ExprArena,
) -> Result<LoweredNode, String> {
    check_exact_arity("FilterNode", 1, children.len())?;
    let child = children.pop().expect("child");
    let predicate = filter
        .predicate
        .as_ref()
        .ok_or_else(|| "FilterNode predicate missing".to_string())?;
    let predicate = lower_proto_expr(predicate, arena, &child.layout)
        .map_err(|err| format!("FilterNode predicate: {err}"))?;
    Ok(LoweredNode {
        node: ExecNode {
            kind: ExecNodeKind::Filter(FilterNode {
                input: Box::new(child.node),
                node_id: node.node_id,
                predicate,
            }),
        },
        layout: child.layout,
        output_schema: child.output_schema,
    })
}

fn lower_limit_node(
    node: &plan::DistributedNode,
    limit_node: &plan::LimitNode,
    mut children: Vec<LoweredNode>,
) -> Result<LoweredNode, String> {
    check_exact_arity("LimitNode", 1, children.len())?;
    let child = children.pop().expect("child");
    let payload_limit = parse_optional_nonnegative_i64(limit_node.limit, "LimitNode.limit")?;
    let outer_limit = parse_distributed_limit(node.limit, "LimitNode DistributedNode.limit")?;
    let limit = merge_limits("LimitNode", payload_limit, outer_limit)?;
    let offset =
        parse_optional_nonnegative_i64(limit_node.offset, "LimitNode.offset")?.unwrap_or(0);
    Ok(LoweredNode {
        node: ExecNode {
            kind: ExecNodeKind::Limit(LimitNode {
                input: Box::new(child.node),
                node_id: node.node_id,
                limit,
                offset,
            }),
        },
        layout: child.layout,
        output_schema: child.output_schema,
    })
}

fn lower_sort_node(
    node: &plan::DistributedNode,
    physical: &plan::PlanNode,
    sort: &plan::SortNode,
    mut children: Vec<LoweredNode>,
    arena: &mut ExprArena,
) -> Result<LoweredNode, String> {
    check_exact_arity("SortNode", 1, children.len())?;
    let child = children.pop().expect("child");
    let output_columns = if sort.output_columns.is_empty() {
        &physical.output_columns
    } else {
        &sort.output_columns
    };
    let order_by = lower_sort_items("SortNode", &sort.items, arena, &child.layout)?;
    let limit = parse_distributed_limit(node.limit, "SortNode DistributedNode.limit")?;
    let offset = parse_optional_nonnegative_i64(sort.offset, "SortNode.offset")?.unwrap_or(0);
    let topn_type = parse_sort_topn_type(sort.topn_type)?;
    let partition_exprs = sort
        .analytic_partition_by
        .iter()
        .enumerate()
        .map(|(idx, expr)| {
            let expr = lower_proto_expr(expr, arena, &child.layout)
                .map_err(|err| format!("SortNode analytic_partition_by[{idx}]: {err}"))?;
            Ok(SortExpression {
                expr,
                asc: true,
                nulls_first: true,
            })
        })
        .collect::<Result<Vec<_>, String>>()?;
    let partition_limit = sort.partition_limit.map(|value| value as usize);
    let use_top_n = partition_limit.is_some();
    if use_top_n && topn_type != SortTopNType::RowNumber && offset != 0 {
        return Err(format!(
            "SortNode node_id={} topn_type {:?} requires offset=0, got {}",
            node.node_id, topn_type, offset
        ));
    }
    let sort_node = ExecNode {
        kind: ExecNodeKind::Sort(SortNode {
            input: Box::new(child.node),
            node_id: node.node_id,
            use_top_n,
            order_by,
            limit,
            offset,
            topn_type,
            max_buffered_rows: None,
            max_buffered_bytes: None,
            partition_exprs,
            partition_limit,
        }),
    };
    let sorted = LoweredNode {
        node: sort_node,
        layout: child.layout.clone(),
        output_schema: child.output_schema.clone(),
    };
    if output_columns.is_empty() {
        return Ok(sorted);
    }

    let layout = layout_from_output_columns(output_columns)?;
    let output_schema = chunk_schema_from_output_columns(output_columns)?;
    if layout.order() == child.layout.order() {
        return Ok(LoweredNode {
            node: sorted.node,
            layout,
            output_schema,
        });
    }

    build_slot_projection("SortNode", sorted, output_columns, node.node_id, arena)
}

fn build_slot_projection(
    label: &str,
    input: LoweredNode,
    output_columns: &[common::OutputColumn],
    node_id: i32,
    arena: &mut ExprArena,
) -> Result<LoweredNode, String> {
    let layout = layout_from_output_columns(output_columns)?;
    let output_schema = chunk_schema_from_output_columns(output_columns)?;
    let expr_slot_schemas = slot_schemas_from_output_columns(output_columns)?;
    let mut exprs = Vec::with_capacity(layout.order().len());
    for slot in layout.order().iter().copied() {
        if !input.layout.contains_slot(slot) {
            return Err(format!(
                "{label} output column id {} has no input slot",
                slot.as_u32()
            ));
        }
        exprs.push(arena.push(ExprNode::SlotId(slot)));
    }

    Ok(LoweredNode {
        node: ExecNode {
            kind: ExecNodeKind::Project(ProjectNode {
                input: Box::new(input.node),
                node_id,
                is_subordinate: true,
                exprs,
                expr_slot_ids: layout.order().to_vec(),
                expr_slot_schemas: Some(expr_slot_schemas),
                output_indices: None,
                output_chunk_schema: output_schema.clone(),
            }),
        },
        layout,
        output_schema,
    })
}

fn lower_topn_node(
    node: &plan::DistributedNode,
    topn: &plan::TopNNode,
    mut children: Vec<LoweredNode>,
    arena: &mut ExprArena,
) -> Result<LoweredNode, String> {
    check_exact_arity("TopNNode", 1, children.len())?;
    let child = children.pop().expect("child");
    let payload_limit = parse_optional_nonnegative_i64(topn.limit, "TopNNode.limit")?;
    let outer_limit = parse_distributed_limit(node.limit, "TopNNode DistributedNode.limit")?;
    let limit = merge_limits("TopNNode", payload_limit, outer_limit)?;
    if limit.is_none() {
        return Err("TopNNode requires a non-negative limit".to_string());
    }
    let offset = parse_optional_nonnegative_i64(topn.offset, "TopNNode.offset")?.unwrap_or(0);
    let phase = plan::TopNPhase::try_from(topn.phase)
        .map_err(|_| format!("TopNNode unknown phase {}", topn.phase))?;
    if phase == plan::TopNPhase::TopnPhaseUnspecified {
        return Err("TopNNode phase is unspecified".to_string());
    }
    if topn.is_split && phase == plan::TopNPhase::TopnPhaseFinal {
        return Err(
            "TopNNode final split must be represented as ExchangeReceiver TopNSplit".to_string(),
        );
    }
    let order_by = lower_sort_items("TopNNode", &topn.items, arena, &child.layout)?;
    Ok(LoweredNode {
        node: ExecNode {
            kind: ExecNodeKind::Sort(SortNode {
                input: Box::new(child.node),
                node_id: node.node_id,
                use_top_n: true,
                order_by,
                limit,
                offset,
                topn_type: SortTopNType::RowNumber,
                max_buffered_rows: None,
                max_buffered_bytes: None,
                partition_exprs: Vec::new(),
                partition_limit: None,
            }),
        },
        layout: child.layout,
        output_schema: child.output_schema,
    })
}

fn lower_window_node(
    node: &plan::DistributedNode,
    physical: &plan::PlanNode,
    window: &plan::WindowNode,
    mut children: Vec<LoweredNode>,
    arena: &mut ExprArena,
) -> Result<LoweredNode, String> {
    check_exact_arity("WindowNode", 1, children.len())?;
    let child = children.pop().expect("child");
    if window.window_exprs.is_empty() {
        return Err("WindowNode has no window expressions".to_string());
    }
    let output_columns = if !window.output_columns.is_empty() {
        window.output_columns.as_slice()
    } else {
        physical.output_columns.as_slice()
    };
    if output_columns.is_empty() {
        return Err("WindowNode output_columns missing".to_string());
    }
    let final_layout = layout_from_output_columns(output_columns)?;
    let final_output_schema = chunk_schema_from_output_columns(output_columns)?;

    let groups = group_window_exprs_by_spec(&window.window_exprs);
    if groups.is_empty() {
        return Err("WindowNode produced no window expression groups".to_string());
    }

    let mut current = child;
    let mut next_node_id = node.node_id;
    for (group_idx, group_indices) in groups.iter().enumerate() {
        let first_idx = group_indices
            .first()
            .copied()
            .ok_or_else(|| format!("WindowNode group {group_idx} is empty"))?;
        let first = &window.window_exprs[first_idx];
        let is_last = group_idx + 1 == groups.len();

        if group_idx > 0 && window_expr_has_sort_keys(first) {
            current = sort_window_group_input(next_node_id, group_idx, first, current, arena)?;
            next_node_id = next_node_id.checked_add(1).ok_or_else(|| {
                format!("WindowNode node_id {next_node_id} overflows after sort group {group_idx}")
            })?;
        }

        let (layout, output_schema) = if is_last {
            (final_layout.clone(), final_output_schema.clone())
        } else {
            intermediate_window_output(&current, group_indices, window, &final_output_schema)
                .map_err(|err| format!("WindowNode group {group_idx}: {err}"))?
        };

        let partition_exprs = first
            .partition_by
            .iter()
            .enumerate()
            .map(|(idx, expr)| {
                lower_proto_expr(expr, arena, &current.layout).map_err(|err| {
                    format!("WindowNode group {group_idx} partition_by[{idx}]: {err}")
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let order_by_exprs = first
            .order_by
            .iter()
            .enumerate()
            .map(|(idx, item)| {
                let expr = item.expr.as_ref().ok_or_else(|| {
                    format!("WindowNode group {group_idx} order_by[{idx}] expr missing")
                })?;
                lower_proto_expr(expr, arena, &current.layout)
                    .map_err(|err| format!("WindowNode group {group_idx} order_by[{idx}]: {err}"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let frame = first
            .window_frame
            .as_ref()
            .map(lower_window_frame)
            .transpose()?;
        validate_window_frame(&frame, order_by_exprs.is_empty())?;

        let mut functions = Vec::with_capacity(group_indices.len());
        for (local_idx, expr_idx) in group_indices.iter().copied().enumerate() {
            let expr = &window.window_exprs[expr_idx];
            functions.push(
                lower_window_function(expr, arena, &current.layout).map_err(|err| {
                    format!("WindowNode group {group_idx} function[{local_idx}]: {err}")
                })?,
            );
        }

        let mut function_by_slot = HashMap::with_capacity(group_indices.len());
        for (local_idx, expr_idx) in group_indices.iter().copied().enumerate() {
            let expr = &window.window_exprs[expr_idx];
            let slot = SlotId::new(expr.output_column_id);
            if function_by_slot.insert(slot, local_idx).is_some() {
                return Err(format!(
                    "WindowNode duplicate output_column_id {}",
                    expr.output_column_id
                ));
            }
        }
        let analytic_output_columns =
            window_analytic_output_columns(&layout, &current.layout, &function_by_slot, group_idx)?;
        let group_node_id = next_node_id;
        next_node_id = next_node_id.checked_add(1).ok_or_else(|| {
            format!("WindowNode node_id {next_node_id} overflows after analytic group {group_idx}")
        })?;

        current = LoweredNode {
            node: ExecNode {
                kind: ExecNodeKind::Analytic(AnalyticNode {
                    input: Box::new(current.node),
                    node_id: group_node_id,
                    partition_exprs,
                    order_by_exprs,
                    functions,
                    window: frame,
                    output_columns: analytic_output_columns,
                    output_chunk_schema: output_schema.clone(),
                }),
            },
            layout,
            output_schema,
        };
    }

    Ok(current)
}

fn window_expr_has_sort_keys(expr: &plan::WindowExpr) -> bool {
    !expr.partition_by.is_empty() || !expr.order_by.is_empty()
}

fn sort_window_group_input(
    node_id: i32,
    group_idx: usize,
    first: &plan::WindowExpr,
    input: LoweredNode,
    arena: &mut ExprArena,
) -> Result<LoweredNode, String> {
    let mut order_by = Vec::with_capacity(first.partition_by.len() + first.order_by.len());
    for (idx, expr) in first.partition_by.iter().enumerate() {
        let expr = lower_proto_expr(expr, arena, &input.layout).map_err(|err| {
            format!("WindowNode group {group_idx} sort partition_by[{idx}]: {err}")
        })?;
        order_by.push(SortExpression {
            expr,
            asc: true,
            nulls_first: true,
        });
    }
    order_by.extend(lower_sort_items(
        &format!("WindowNode group {group_idx} sort"),
        &first.order_by,
        arena,
        &input.layout,
    )?);

    Ok(LoweredNode {
        node: ExecNode {
            kind: ExecNodeKind::Sort(SortNode {
                input: Box::new(input.node),
                node_id,
                use_top_n: false,
                order_by,
                limit: None,
                offset: 0,
                topn_type: SortTopNType::RowNumber,
                max_buffered_rows: None,
                max_buffered_bytes: None,
                partition_exprs: Vec::new(),
                partition_limit: None,
            }),
        },
        layout: input.layout,
        output_schema: input.output_schema,
    })
}

fn group_window_exprs_by_spec(exprs: &[plan::WindowExpr]) -> Vec<Vec<usize>> {
    let mut groups: Vec<Vec<usize>> = Vec::new();
    for (idx, expr) in exprs.iter().enumerate() {
        if let Some(group) = groups
            .iter_mut()
            .find(|group| same_window_spec(&exprs[group[0]], expr))
        {
            group.push(idx);
        } else {
            groups.push(vec![idx]);
        }
    }
    groups
}

fn intermediate_window_output(
    current: &LoweredNode,
    group_indices: &[usize],
    window: &plan::WindowNode,
    final_output_schema: &ChunkSchema,
) -> Result<(Layout, ChunkSchemaRef), String> {
    let mut slot_ids = current.layout.order().to_vec();
    let mut slots = Vec::with_capacity(slot_ids.len() + group_indices.len());
    for slot_id in current.layout.order() {
        let slot = current.output_schema.slot(*slot_id).cloned().ok_or_else(|| {
            format!(
                "current output schema missing input slot {} for intermediate WindowNode output",
                slot_id
            )
        })?;
        slots.push(slot);
    }

    for expr_idx in group_indices {
        let expr = window
            .window_exprs
            .get(*expr_idx)
            .ok_or_else(|| format!("window expression index {expr_idx} is out of bounds"))?;
        let slot_id = SlotId::new(expr.output_column_id);
        if slot_ids.contains(&slot_id) {
            continue;
        }
        slot_ids.push(slot_id);
        slots.push(window_expr_slot_schema(expr, final_output_schema)?);
    }

    Ok((
        Layout::for_slots(slot_ids),
        Arc::new(ChunkSchema::try_new(slots)?),
    ))
}

fn window_expr_slot_schema(
    expr: &plan::WindowExpr,
    final_output_schema: &ChunkSchema,
) -> Result<ChunkSlotSchema, String> {
    let slot_id = SlotId::new(expr.output_column_id);
    if let Some(slot) = final_output_schema.slot(slot_id) {
        return Ok(slot.clone());
    }

    let type_desc = expr
        .result_type
        .as_ref()
        .ok_or_else(|| format!("window function {} result_type missing", expr.name))?;
    let data_type = super::decode_type(type_desc)?;
    let field = Field::new(&expr.output_name, data_type, true);
    ChunkSchema::slot_schema_from_arrow_field(slot_id, &field)
}

fn window_analytic_output_columns(
    layout: &Layout,
    input_layout: &Layout,
    function_by_slot: &HashMap<SlotId, usize>,
    group_idx: usize,
) -> Result<Vec<AnalyticOutputColumn>, String> {
    layout
        .order()
        .iter()
        .map(|slot| {
            if let Some(idx) = function_by_slot.get(slot) {
                Ok(AnalyticOutputColumn::Window(*idx))
            } else if input_layout.contains_slot(*slot) {
                Ok(AnalyticOutputColumn::InputSlotId(*slot))
            } else {
                Err(format!(
                    "WindowNode group {group_idx} output slot {} has no input slot or window result",
                    slot
                ))
            }
        })
        .collect()
}

fn same_window_spec(left: &plan::WindowExpr, right: &plan::WindowExpr) -> bool {
    left.partition_by == right.partition_by
        && left.order_by == right.order_by
        && left.window_frame == right.window_frame
}

fn lower_window_function(
    expr: &plan::WindowExpr,
    arena: &mut ExprArena,
    input_layout: &Layout,
) -> Result<WindowFunctionSpec, String> {
    let name = expr.name.to_ascii_lowercase();
    let kind = window_function_kind(&name, expr.distinct, expr.ignore_nulls)?;
    let return_type = expr
        .result_type
        .as_ref()
        .ok_or_else(|| format!("window function {} result_type missing", expr.name))
        .and_then(super::decode_type)?;
    let mut args = expr
        .args
        .iter()
        .enumerate()
        .map(|(idx, arg)| {
            lower_proto_expr(arg, arena, input_layout)
                .map_err(|err| format!("window function {} arg {idx}: {err}", expr.name))
        })
        .collect::<Result<Vec<_>, _>>()?;
    if matches!(
        kind,
        WindowFunctionKind::ArrayAgg { .. }
            | WindowFunctionKind::MaxBy
            | WindowFunctionKind::MaxByV2
            | WindowFunctionKind::MinBy
            | WindowFunctionKind::MinByV2
    ) {
        args = pack_window_function_inputs(args, arena)?;
    }
    validate_window_function_signature(&kind, &args, &return_type, arena)?;
    Ok(WindowFunctionSpec {
        kind,
        args,
        return_type,
    })
}

fn window_function_kind(
    name: &str,
    distinct: bool,
    ignore_nulls: bool,
) -> Result<WindowFunctionKind, String> {
    let base = name.split('|').next().unwrap_or(name);
    match base {
        "row_number" => Ok(WindowFunctionKind::RowNumber),
        "rank" => Ok(WindowFunctionKind::Rank),
        "dense_rank" => Ok(WindowFunctionKind::DenseRank),
        "cume_dist" => Ok(WindowFunctionKind::CumeDist),
        "percent_rank" => Ok(WindowFunctionKind::PercentRank),
        "ntile" => Ok(WindowFunctionKind::Ntile),
        "first_value" => Ok(WindowFunctionKind::FirstValue { ignore_nulls }),
        "first_value_rewrite" => Ok(WindowFunctionKind::FirstValueRewrite { ignore_nulls }),
        "last_value" => Ok(WindowFunctionKind::LastValue { ignore_nulls }),
        "lead" => Ok(WindowFunctionKind::Lead { ignore_nulls }),
        "lag" => Ok(WindowFunctionKind::Lag { ignore_nulls }),
        "session_number" => Ok(WindowFunctionKind::SessionNumber),
        "count" => Ok(WindowFunctionKind::Count),
        "sum" => Ok(WindowFunctionKind::Sum),
        "avg" => Ok(WindowFunctionKind::Avg),
        "min" => Ok(WindowFunctionKind::Min),
        "max" => Ok(WindowFunctionKind::Max),
        "bitmap_union" => Ok(WindowFunctionKind::BitmapUnion),
        "bitmap_union_count" => Ok(WindowFunctionKind::BitmapUnionCount),
        "max_by" => Ok(WindowFunctionKind::MaxBy),
        "max_by_v2" => Ok(WindowFunctionKind::MaxByV2),
        "min_by" => Ok(WindowFunctionKind::MinBy),
        "min_by_v2" => Ok(WindowFunctionKind::MinByV2),
        "var_samp" | "variance_samp" => Ok(WindowFunctionKind::VarianceSamp),
        "stddev_samp" => Ok(WindowFunctionKind::StddevSamp),
        "bool_or" | "boolor_agg" => Ok(WindowFunctionKind::BoolOr),
        "covar_pop" => Ok(WindowFunctionKind::CovarPop),
        "covar_samp" => Ok(WindowFunctionKind::CovarSamp),
        "corr" => Ok(WindowFunctionKind::Corr),
        "array_agg" | "array_agg_distinct" | "array_unique_agg" => {
            Ok(WindowFunctionKind::ArrayAgg {
                is_distinct: distinct || matches!(base, "array_agg_distinct" | "array_unique_agg"),
                is_asc_order: Vec::new(),
                nulls_first: Vec::new(),
            })
        }
        "approx_top_k" => Ok(WindowFunctionKind::ApproxTopK),
        other => Err(format!("unsupported window function: {other}")),
    }
}

fn lower_window_frame(frame: &expr::WindowFrame) -> Result<WindowFrame, String> {
    let window_type = match expr::WindowFrameType::try_from(frame.frame_type)
        .map_err(|_| format!("WindowNode unknown frame type {}", frame.frame_type))?
    {
        expr::WindowFrameType::Rows => WindowType::Rows,
        expr::WindowFrameType::Range => WindowType::Range,
        expr::WindowFrameType::Unspecified => {
            return Err("WindowNode frame type is unspecified".to_string());
        }
    };
    let start = match frame.start.as_ref() {
        Some(bound) => lower_window_bound(bound, true, &window_type)?,
        None => None,
    };
    let end = match frame.end.as_ref() {
        Some(bound) => lower_window_bound(bound, false, &window_type)?,
        None => None,
    };
    Ok(WindowFrame {
        start,
        end,
        window_type,
    })
}

fn lower_window_bound(
    bound: &expr::WindowBound,
    is_start: bool,
    window_type: &WindowType,
) -> Result<Option<WindowBoundary>, String> {
    use expr::window_bound::Bound;

    let label = if is_start { "start" } else { "end" };
    let bound = bound
        .bound
        .as_ref()
        .ok_or_else(|| format!("WindowNode {label} bound missing"))?;
    match bound {
        Bound::UnboundedPreceding(true) if is_start => Ok(None),
        Bound::UnboundedFollowing(true) if !is_start => Ok(None),
        Bound::CurrentRow(true) => Ok(Some(WindowBoundary::CurrentRow)),
        Bound::Preceding(value) => {
            if !matches!(window_type, WindowType::Rows) {
                return Err("RANGE window boundary PRECEDING not supported".to_string());
            }
            Ok(Some(WindowBoundary::Preceding(*value)))
        }
        Bound::Following(value) => {
            if !matches!(window_type, WindowType::Rows) {
                return Err("RANGE window boundary FOLLOWING not supported".to_string());
            }
            Ok(Some(WindowBoundary::Following(*value)))
        }
        Bound::UnboundedPreceding(false)
        | Bound::UnboundedFollowing(false)
        | Bound::CurrentRow(false) => Err(format!(
            "WindowNode {label} boolean bound marker must be true"
        )),
        Bound::UnboundedPreceding(true) => {
            Err(format!("WindowNode {label} cannot be UNBOUNDED PRECEDING"))
        }
        Bound::UnboundedFollowing(true) => {
            Err(format!("WindowNode {label} cannot be UNBOUNDED FOLLOWING"))
        }
    }
}

fn validate_window_frame(
    frame: &Option<WindowFrame>,
    order_by_is_empty: bool,
) -> Result<(), String> {
    let Some(frame) = frame.as_ref() else {
        return Ok(());
    };
    if matches!(frame.window_type, WindowType::Range) {
        if frame.start.is_some() {
            return Err("RANGE window must have UNBOUNDED PRECEDING start".to_string());
        }
        if let Some(end) = frame.end.as_ref()
            && !matches!(end, WindowBoundary::CurrentRow)
        {
            return Err("RANGE window end must be CURRENT ROW or UNBOUNDED FOLLOWING".to_string());
        }
        if order_by_is_empty {
            return Err("RANGE window requires non-empty order_by_exprs".to_string());
        }
    }
    Ok(())
}

fn pack_window_function_inputs(
    args: Vec<crate::exec::expr::ExprId>,
    arena: &mut ExprArena,
) -> Result<Vec<crate::exec::expr::ExprId>, String> {
    if args.len() <= 1 {
        return Ok(args);
    }
    let mut fields = Vec::with_capacity(args.len());
    for (idx, expr_id) in args.iter().enumerate() {
        let data_type = arena
            .data_type(*expr_id)
            .ok_or_else(|| "window function input type missing".to_string())?;
        if matches!(data_type, DataType::Null) {
            return Err("window function input type is null".to_string());
        }
        fields.push(Field::new(format!("f{idx}"), data_type.clone(), true));
    }
    let struct_type = DataType::Struct(Fields::from(fields));
    let struct_expr = arena.push_typed(ExprNode::StructExpr { fields: args }, struct_type);
    Ok(vec![struct_expr])
}

fn validate_window_function_signature(
    kind: &WindowFunctionKind,
    args: &[crate::exec::expr::ExprId],
    return_type: &DataType,
    arena: &ExprArena,
) -> Result<(), String> {
    match kind {
        WindowFunctionKind::RowNumber
        | WindowFunctionKind::Rank
        | WindowFunctionKind::DenseRank
        | WindowFunctionKind::Ntile
        | WindowFunctionKind::SessionNumber
        | WindowFunctionKind::Count => {
            if !matches!(return_type, DataType::Int64) {
                return Err(format!(
                    "window function expects Int64 return type, got {:?}",
                    return_type
                ));
            }
        }
        WindowFunctionKind::CumeDist
        | WindowFunctionKind::PercentRank
        | WindowFunctionKind::VarianceSamp
        | WindowFunctionKind::StddevSamp
        | WindowFunctionKind::CovarPop
        | WindowFunctionKind::CovarSamp
        | WindowFunctionKind::Corr => {
            if !matches!(return_type, DataType::Float64) {
                return Err(format!(
                    "window function expects Float64 return type, got {:?}",
                    return_type
                ));
            }
        }
        WindowFunctionKind::BoolOr => {
            if !matches!(return_type, DataType::Boolean) {
                return Err(format!(
                    "window function expects Boolean return type, got {:?}",
                    return_type
                ));
            }
        }
        _ => {}
    }

    match kind {
        WindowFunctionKind::RowNumber
        | WindowFunctionKind::Rank
        | WindowFunctionKind::DenseRank
        | WindowFunctionKind::CumeDist
        | WindowFunctionKind::PercentRank => {
            if !args.is_empty() {
                return Err("window function expects 0 arguments".to_string());
            }
        }
        WindowFunctionKind::Ntile => {
            if args.len() != 1 {
                return Err("ntile expects 1 argument".to_string());
            }
        }
        WindowFunctionKind::FirstValue { .. } | WindowFunctionKind::LastValue { .. } => {
            if args.len() != 1 {
                return Err("first_value/last_value expects 1 argument".to_string());
            }
            validate_window_arg_matches_return(args[0], return_type, arena)?;
        }
        WindowFunctionKind::FirstValueRewrite { .. } => {
            if !(1..=2).contains(&args.len()) {
                return Err("first_value_rewrite expects 1 or 2 arguments".to_string());
            }
            validate_window_arg_matches_return(args[0], return_type, arena)?;
        }
        WindowFunctionKind::Lead { .. } | WindowFunctionKind::Lag { .. } => {
            if !(1..=3).contains(&args.len()) {
                return Err("lead/lag expects 1 to 3 arguments".to_string());
            }
            validate_window_arg_matches_return(args[0], return_type, arena)?;
        }
        WindowFunctionKind::SessionNumber => {
            if args.len() != 2 {
                return Err("session_number expects 2 arguments".to_string());
            }
        }
        WindowFunctionKind::Count => {
            if args.len() > 1 {
                return Err("count expects 0 or 1 arguments".to_string());
            }
        }
        WindowFunctionKind::BitmapUnion | WindowFunctionKind::BitmapUnionCount => {
            if args.len() != 1 {
                return Err("bitmap_union/bitmap_union_count expects 1 argument".to_string());
            }
        }
        WindowFunctionKind::MaxBy
        | WindowFunctionKind::MaxByV2
        | WindowFunctionKind::MinBy
        | WindowFunctionKind::MinByV2 => {
            if args.len() != 1 {
                return Err(
                    "max_by/max_by_v2/min_by/min_by_v2 expects 1 packed struct argument"
                        .to_string(),
                );
            }
        }
        WindowFunctionKind::Sum
        | WindowFunctionKind::Avg
        | WindowFunctionKind::Min
        | WindowFunctionKind::Max
        | WindowFunctionKind::VarianceSamp
        | WindowFunctionKind::StddevSamp
        | WindowFunctionKind::BoolOr => {
            if args.len() != 1 {
                return Err("aggregate window function expects 1 argument".to_string());
            }
            if matches!(kind, WindowFunctionKind::Min | WindowFunctionKind::Max) {
                validate_window_arg_matches_return(args[0], return_type, arena)?;
            }
        }
        WindowFunctionKind::CovarPop | WindowFunctionKind::CovarSamp | WindowFunctionKind::Corr => {
            if args.len() != 2 {
                return Err("covar/corr window function expects 2 arguments".to_string());
            }
        }
        WindowFunctionKind::ApproxTopK => {
            if !(1..=3).contains(&args.len()) {
                return Err("approx_top_k window function expects 1 to 3 arguments".to_string());
            }
        }
        WindowFunctionKind::ArrayAgg { .. } => {
            if args.len() != 1 {
                return Err("array_agg window function expects 1 argument".to_string());
            }
            if !matches!(return_type, DataType::List(_)) {
                return Err(format!(
                    "array_agg window function expects LIST return type, got {:?}",
                    return_type
                ));
            }
        }
    }

    Ok(())
}

fn validate_window_arg_matches_return(
    arg: crate::exec::expr::ExprId,
    return_type: &DataType,
    arena: &ExprArena,
) -> Result<(), String> {
    let arg_type = arena
        .data_type(arg)
        .ok_or_else(|| "missing arg type in arena".to_string())?;
    if arg_type != return_type {
        return Err(format!(
            "window function return type mismatch: arg={:?} ret={:?}",
            arg_type, return_type
        ));
    }
    Ok(())
}

fn lower_sort_items(
    node_kind: &str,
    items: &[expr::SortItem],
    arena: &mut ExprArena,
    input_layout: &Layout,
) -> Result<Vec<SortExpression>, String> {
    items
        .iter()
        .enumerate()
        .map(|(idx, item)| {
            let expr = item
                .expr
                .as_ref()
                .ok_or_else(|| format!("{node_kind} sort item {idx} expr missing"))?;
            let expr = lower_proto_expr(expr, arena, input_layout)
                .map_err(|err| format!("{node_kind} sort item {idx}: {err}"))?;
            Ok(SortExpression {
                expr,
                asc: item.asc,
                nulls_first: item.nulls_first,
            })
        })
        .collect()
}

fn parse_sort_topn_type(value: Option<i32>) -> Result<SortTopNType, String> {
    let Some(value) = value else {
        return Ok(SortTopNType::RowNumber);
    };
    match plan::SortTopNType::try_from(value)
        .map_err(|_| format!("SortNode unknown topn_type {value}"))?
    {
        plan::SortTopNType::SortTopnTypeUnspecified | plan::SortTopNType::SortTopnTypeRowNumber => {
            Ok(SortTopNType::RowNumber)
        }
        plan::SortTopNType::SortTopnTypeRank => Ok(SortTopNType::Rank),
        plan::SortTopNType::SortTopnTypeDenseRank => Ok(SortTopNType::DenseRank),
    }
}

fn lower_exchange_receiver(
    node: &plan::DistributedNode,
    exchange: &plan::ExchangeReceiver,
    children: Vec<LoweredNode>,
    arena: &mut ExprArena,
    ctx: &NodeLoweringContext,
) -> Result<LoweredNode, String> {
    check_exact_arity("ExchangeReceiver", 0, children.len())?;
    let flavor = exchange
        .flavor
        .as_ref()
        .and_then(|flavor| flavor.kind.as_ref())
        .ok_or_else(|| "ExchangeReceiver flavor missing".to_string())?;
    match flavor {
        plan::exchange_flavor::Kind::Distribution(true) => {}
        plan::exchange_flavor::Kind::Distribution(false) => {
            return Err("ExchangeReceiver distribution flavor must be true".to_string());
        }
        plan::exchange_flavor::Kind::LimitOffset(_) => {}
        plan::exchange_flavor::Kind::TopnSplit(_) => {}
        plan::exchange_flavor::Kind::CteMulticast(_) => {}
    }

    let key = ctx.exchange_key(node.node_id);
    let expected_senders = ctx
        .exchange_sender_counts
        .get(&key)
        .copied()
        .ok_or_else(|| {
            format!(
                "ExchangeReceiver missing sender count for node_id {} (key={:?})",
                node.node_id, key
            )
        })?;
    if expected_senders == 0 {
        return Err(format!(
            "ExchangeReceiver sender count must be > 0 for node_id {}",
            node.node_id
        ));
    }
    let layout = layout_from_output_columns(&exchange.output_columns)?;
    let output_schema = chunk_schema_from_output_columns(&exchange.output_columns)?;
    let mut lowered = LoweredNode {
        node: ExecNode {
            kind: ExecNodeKind::ExchangeSource(ExchangeSourceNode::new(
                key,
                expected_senders,
                Duration::from_millis(exchange_wait_ms()),
                output_schema.clone(),
            )),
        },
        layout,
        output_schema,
    };

    match flavor {
        plan::exchange_flavor::Kind::LimitOffset(limit_offset) => {
            let limit = parse_optional_nonnegative_i64(
                limit_offset.limit,
                "ExchangeReceiver LimitOffset.limit",
            )?;
            let offset = parse_optional_nonnegative_i64(
                limit_offset.offset,
                "ExchangeReceiver LimitOffset.offset",
            )?
            .unwrap_or(0);
            if limit.is_some() || offset > 0 {
                lowered.node = ExecNode {
                    kind: ExecNodeKind::Limit(LimitNode {
                        input: Box::new(lowered.node),
                        node_id: node.node_id,
                        limit,
                        offset,
                    }),
                };
            }
        }
        plan::exchange_flavor::Kind::TopnSplit(topn) => {
            let order_by = lower_sort_items(
                "ExchangeReceiver TopNSplit",
                &topn.items,
                arena,
                &lowered.layout,
            )?;
            let limit =
                parse_optional_nonnegative_i64(topn.limit, "ExchangeReceiver TopNSplit.limit")?;
            let offset =
                parse_optional_nonnegative_i64(topn.offset, "ExchangeReceiver TopNSplit.offset")?
                    .unwrap_or(0);
            lowered.node = ExecNode {
                kind: ExecNodeKind::Sort(SortNode {
                    input: Box::new(lowered.node),
                    node_id: node.node_id,
                    use_top_n: false,
                    order_by,
                    limit,
                    offset,
                    topn_type: SortTopNType::RowNumber,
                    max_buffered_rows: None,
                    max_buffered_bytes: None,
                    partition_exprs: Vec::new(),
                    partition_limit: None,
                }),
            };
        }
        _ => {}
    }

    Ok(lowered)
}

fn lower_set_op_node(
    node: &plan::DistributedNode,
    physical: &plan::PlanNode,
    set_op: &plan::SetOpNode,
    children: Vec<LoweredNode>,
    arena: &mut ExprArena,
) -> Result<LoweredNode, String> {
    check_min_arity("SetOpNode", 2, children.len())?;
    let kind = plan::PlanSetOpKind::try_from(set_op.kind)
        .map_err(|_| format!("SetOpNode unknown kind {}", set_op.kind))?;
    let output_columns = if set_op.output_columns.is_empty() {
        &physical.output_columns
    } else {
        &set_op.output_columns
    };
    let layout = layout_from_output_columns(output_columns)?;
    let output_schema = chunk_schema_from_output_columns(output_columns)?;
    let inputs = normalize_set_op_inputs(
        node.node_id,
        children,
        &set_op.child_output_columns,
        output_columns,
        output_schema.clone(),
        arena,
    )?;
    match kind {
        plan::PlanSetOpKind::UnionAll => Ok(LoweredNode {
            node: ExecNode {
                kind: ExecNodeKind::UnionAll(UnionAllNode {
                    inputs,
                    node_id: node.node_id,
                }),
            },
            layout,
            output_schema,
        }),
        plan::PlanSetOpKind::Intersect => Ok(LoweredNode {
            node: ExecNode {
                kind: ExecNodeKind::SetOp(SetOpNode {
                    kind: SetOpKind::Intersect,
                    inputs,
                    node_id: node.node_id,
                    output_chunk_schema: output_schema.clone(),
                }),
            },
            layout,
            output_schema,
        }),
        plan::PlanSetOpKind::Except => Ok(LoweredNode {
            node: ExecNode {
                kind: ExecNodeKind::SetOp(SetOpNode {
                    kind: SetOpKind::Except,
                    inputs,
                    node_id: node.node_id,
                    output_chunk_schema: output_schema.clone(),
                }),
            },
            layout,
            output_schema,
        }),
        plan::PlanSetOpKind::UnionDistinct => unsupported("UnionDistinct"),
        plan::PlanSetOpKind::Unspecified => Err("SetOpNode kind is unspecified".to_string()),
    }
}

fn normalize_set_op_inputs(
    node_id: i32,
    children: Vec<LoweredNode>,
    child_output_columns: &[plan::OutputColumnList],
    output_columns: &[common::OutputColumn],
    output_schema: ChunkSchemaRef,
    arena: &mut ExprArena,
) -> Result<Vec<ExecNode>, String> {
    if child_output_columns.is_empty() {
        return normalize_set_op_inputs_by_position(
            node_id,
            children,
            output_columns,
            output_schema,
            arena,
        );
    }
    if child_output_columns.len() != children.len() {
        return Err(format!(
            "SetOpNode child_output_columns size mismatch: expected {}, got {}",
            children.len(),
            child_output_columns.len()
        ));
    }
    let output_slots = slot_ids_from_columns(output_columns)?;
    let output_slot_schemas = slot_schemas_from_output_columns(output_columns)?;
    children
        .into_iter()
        .zip(child_output_columns.iter())
        .enumerate()
        .map(|(idx, (child, child_columns))| {
            if child_columns.columns.len() != output_columns.len() {
                return Err(format!(
                    "SetOpNode child {idx} output width mismatch: expected {}, got {}",
                    output_columns.len(),
                    child_columns.columns.len()
                ));
            }
            let expected_child_layout = layout_from_output_columns(&child_columns.columns)?;
            if expected_child_layout.order() != child.layout.order() {
                return Err(format!(
                    "SetOpNode child {idx} output columns do not match child layout: columns={:?} layout={:?}",
                    expected_child_layout.order(),
                    child.layout.order()
                ));
            }
            let exprs = child_columns
                .columns
                .iter()
                .map(|col| {
                    let slot = SlotId::new(col.column_id);
                    let data_type = col
                        .r#type
                        .as_ref()
                        .ok_or_else(|| {
                            format!(
                                "SetOpNode child {idx} column {} type missing",
                                col.column_id
                            )
                        })
                        .and_then(super::decode_type)?;
                    Ok(arena.push_typed(ExprNode::SlotId(slot), data_type))
                })
                .collect::<Result<Vec<_>, String>>()?;
            Ok(ExecNode {
                kind: ExecNodeKind::Project(ProjectNode {
                    input: Box::new(child.node),
                    node_id,
                    is_subordinate: true,
                    exprs,
                    expr_slot_ids: output_slots.clone(),
                    expr_slot_schemas: Some(output_slot_schemas.clone()),
                    output_indices: None,
                    output_chunk_schema: output_schema.clone(),
                }),
            })
        })
        .collect()
}

fn normalize_set_op_inputs_by_position(
    node_id: i32,
    children: Vec<LoweredNode>,
    output_columns: &[common::OutputColumn],
    output_schema: ChunkSchemaRef,
    arena: &mut ExprArena,
) -> Result<Vec<ExecNode>, String> {
    let output_slots = slot_ids_from_columns(output_columns)?;
    let output_slot_schemas = slot_schemas_from_output_columns(output_columns)?;
    children
        .into_iter()
        .enumerate()
        .map(|(idx, child)| {
            if child.layout.order().len() != output_slots.len() {
                return Err(format!(
                    "SetOpNode child {idx} width mismatch without child_output_columns: expected {}, got {}",
                    output_slots.len(),
                    child.layout.order().len()
                ));
            }
            if child.layout.order() == output_slots.as_slice() {
                return Ok(child.node);
            }
            let exprs = child
                .layout
                .order()
                .iter()
                .copied()
                .map(|slot| {
                    let data_type = child
                        .output_schema
                        .slot(slot)
                        .ok_or_else(|| {
                            format!(
                                "SetOpNode child {idx} slot {} missing from child output schema",
                                slot
                            )
                        })?
                        .data_type()
                        .clone();
                    Ok(arena.push_typed(ExprNode::SlotId(slot), data_type))
                })
                .collect::<Result<Vec<_>, String>>()?;
            Ok(ExecNode {
                kind: ExecNodeKind::Project(ProjectNode {
                    input: Box::new(child.node),
                    node_id,
                    is_subordinate: true,
                    exprs,
                    expr_slot_ids: output_slots.clone(),
                    expr_slot_schemas: Some(output_slot_schemas.clone()),
                    output_indices: None,
                    output_chunk_schema: output_schema.clone(),
                }),
            })
        })
        .collect()
}

fn slot_ids_from_columns(cols: &[common::OutputColumn]) -> Result<Vec<SlotId>, String> {
    Ok(layout_from_output_columns(cols)?.order().to_vec())
}

fn lower_hash_aggregate_node(
    node: &plan::DistributedNode,
    physical: &plan::PlanNode,
    aggregate: &plan::HashAggregateNode,
    mut children: Vec<LoweredNode>,
    arena: &mut ExprArena,
) -> Result<LoweredNode, String> {
    check_exact_arity("HashAggregateNode", 1, children.len())?;
    let child = children.pop().expect("child");
    if aggregate.is_merge.len() != aggregate.aggregates.len() {
        return Err(format!(
            "HashAggregateNode is_merge length mismatch: is_merge={} aggregates={}",
            aggregate.is_merge.len(),
            aggregate.aggregates.len()
        ));
    }
    let mode = plan::AggMode::try_from(aggregate.mode)
        .map_err(|_| format!("HashAggregateNode unknown mode {}", aggregate.mode))?;
    if mode == plan::AggMode::Unspecified {
        return Err("HashAggregateNode mode is unspecified".to_string());
    }
    let output_layout = aggregate
        .output_layout
        .as_ref()
        .ok_or_else(|| "HashAggregateNode output_layout missing".to_string())?;
    let aggregate_output_columns = aggregate_output_columns_from_layout(
        output_layout.group_key_columns.as_slice(),
        output_layout.aggregate_columns.as_slice(),
    );
    let visible_output_columns = if !aggregate.output_columns.is_empty() {
        aggregate.output_columns.as_slice()
    } else if !physical.output_columns.is_empty() {
        physical.output_columns.as_slice()
    } else {
        aggregate_output_columns.as_slice()
    };
    let aggregate_layout = layout_from_output_columns(&aggregate_output_columns)?;
    let aggregate_output_schema = chunk_schema_from_output_columns(&aggregate_output_columns)?;
    if output_layout.aggregate_columns.len() != aggregate.aggregates.len() {
        return Err(format!(
            "HashAggregateNode output_layout aggregate column mismatch: columns={} aggregates={}",
            output_layout.aggregate_columns.len(),
            aggregate.aggregates.len()
        ));
    }
    if output_layout.group_key_columns.len() != aggregate.group_by.len() {
        return Err(format!(
            "HashAggregateNode output_layout group key mismatch: columns={} group_by={}",
            output_layout.group_key_columns.len(),
            aggregate.group_by.len()
        ));
    }

    let group_by = aggregate
        .group_by
        .iter()
        .enumerate()
        .map(|(idx, expr)| {
            lower_proto_expr(expr, arena, &child.layout).map_err(|err| {
                format!(
                    "HashAggregateNode group_by[{idx}]: {err}; child_kind={} child_slots={:?}",
                    exec_node_kind_label(&child.node.kind),
                    child.layout.order()
                )
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    for expr_id in &group_by {
        if let Some(dt) = arena.data_type(*expr_id)
            && matches!(dt, DataType::LargeBinary)
        {
            return Err("VARIANT is not supported in GROUP BY".to_string());
        }
    }

    let need_finalize = matches!(mode, plan::AggMode::Single | plan::AggMode::Global);
    let mut functions = Vec::with_capacity(aggregate.aggregates.len());
    for (idx, call) in aggregate.aggregates.iter().enumerate() {
        let is_merge = aggregate.is_merge[idx];
        let output_col = output_layout
            .aggregate_columns
            .get(idx)
            .ok_or_else(|| format!("HashAggregateNode aggregate column {idx} missing"))?;
        let result_type = call
            .result_type
            .as_ref()
            .ok_or_else(|| format!("HashAggregateNode aggregate {idx} result_type missing"))
            .and_then(super::decode_type)?;
        let function_name = aggregate_function_name(call)?;
        let signature_arg_types = aggregate_signature_arg_types(call)?;
        let (semantic_output_type, intermediate_type) =
            infer_agg_function_types(&function_name, &signature_arg_types, call.distinct).map_err(
                |err| format!("HashAggregateNode aggregate {idx} type inference: {err}"),
            )?;
        let signature_input_arg_type = signature_arg_types.first().cloned();
        let signature_output_type = if need_finalize {
            result_type
        } else {
            semantic_output_type
        };

        let raw_args = if is_merge {
            let slot = SlotId::new(output_col.column_id);
            let data_type = intermediate_type.clone().ok_or_else(|| {
                format!(
                    "HashAggregateNode merge aggregate {idx} requires a known intermediate type for {}",
                    function_name
                )
            })?;
            vec![arena.push_typed(ExprNode::SlotId(slot), data_type)]
        } else {
            lower_aggregate_update_inputs(call, idx, &child, arena)?
        };
        let inputs =
            select_aggregate_inputs(&call.name.to_ascii_lowercase(), is_merge, raw_args, arena)?;
        functions.push(AggFunction {
            name: function_name,
            inputs,
            input_is_intermediate: is_merge,
            types: Some(AggTypeSignature {
                intermediate_type,
                output_type: Some(signature_output_type),
                input_arg_type: signature_input_arg_type,
            }),
            order: aggregate_order_spec(call),
        });
    }

    let input_is_intermediate = functions.iter().all(|f| f.input_is_intermediate);
    let aggregate_node = LoweredNode {
        node: ExecNode {
            kind: ExecNodeKind::Aggregate(AggregateNode {
                input: Box::new(child.node),
                node_id: node.node_id,
                group_by,
                functions,
                need_finalize,
                input_is_intermediate,
                output_chunk_schema: aggregate_output_schema.clone(),
                topn_rf_specs: Vec::new(),
                streaming_preaggregation_mode: None,
            }),
        },
        layout: aggregate_layout,
        output_schema: aggregate_output_schema,
    };
    let visible_layout = layout_from_output_columns(visible_output_columns)?;
    if visible_layout.order() == aggregate_node.layout.order() {
        return Ok(aggregate_node);
    }
    build_slot_projection(
        "HashAggregateNode",
        aggregate_node,
        visible_output_columns,
        node.node_id,
        arena,
    )
}

fn aggregate_output_columns_from_layout(
    group_key_columns: &[common::OutputColumn],
    aggregate_columns: &[common::OutputColumn],
) -> Vec<common::OutputColumn> {
    let mut columns = Vec::with_capacity(group_key_columns.len() + aggregate_columns.len());
    columns.extend_from_slice(group_key_columns);
    columns.extend_from_slice(aggregate_columns);
    columns
}

fn aggregate_function_name(call: &plan::PlanAggregateCall) -> Result<String, String> {
    let name = call.name.to_ascii_lowercase();
    if call.distinct {
        return match name.as_str() {
            "count" => Ok("multi_distinct_count".to_string()),
            "sum" => Ok("multi_distinct_sum".to_string()),
            "array_agg" => Ok("array_agg_distinct".to_string()),
            _ => Ok(name),
        };
    }
    Ok(name)
}

fn aggregate_signature_arg_types(call: &plan::PlanAggregateCall) -> Result<Vec<DataType>, String> {
    let mut types = call
        .args
        .iter()
        .enumerate()
        .map(|(idx, expr)| {
            expr.r#type
                .as_ref()
                .ok_or_else(|| format!("aggregate {} argument {idx} type missing", call.name))
                .and_then(super::decode_type)
        })
        .collect::<Result<Vec<_>, _>>()?;
    for (idx, item) in call.order_by.iter().enumerate() {
        let expr = item
            .expr
            .as_ref()
            .ok_or_else(|| format!("aggregate {} order_by[{idx}] expr missing", call.name))?;
        let data_type = expr
            .r#type
            .as_ref()
            .ok_or_else(|| format!("aggregate {} order_by[{idx}] type missing", call.name))
            .and_then(super::decode_type)?;
        types.push(data_type);
    }
    Ok(types)
}

fn lower_aggregate_update_inputs(
    call: &plan::PlanAggregateCall,
    aggregate_idx: usize,
    child: &LoweredNode,
    arena: &mut ExprArena,
) -> Result<Vec<crate::exec::expr::ExprId>, String> {
    if call.name.eq_ignore_ascii_case("count_if") && !call.order_by.is_empty() {
        return Err(format!(
            "HashAggregateNode aggregate {aggregate_idx} count_if does not support ORDER BY"
        ));
    }
    let mut inputs = Vec::with_capacity(call.args.len() + call.order_by.len());
    for (arg_idx, expr) in call.args.iter().enumerate() {
        inputs.push(lower_proto_expr(expr, arena, &child.layout).map_err(|err| {
            format!("HashAggregateNode aggregate {aggregate_idx} arg {arg_idx}: {err}")
        })?);
    }
    for (order_idx, item) in call.order_by.iter().enumerate() {
        let expr = item.expr.as_ref().ok_or_else(|| {
            format!(
                "HashAggregateNode aggregate {aggregate_idx} order_by[{order_idx}] expr missing"
            )
        })?;
        inputs.push(lower_proto_expr(expr, arena, &child.layout).map_err(|err| {
            format!("HashAggregateNode aggregate {aggregate_idx} order_by[{order_idx}]: {err}")
        })?);
    }
    Ok(inputs)
}

fn aggregate_order_spec(call: &plan::PlanAggregateCall) -> AggOrderSpec {
    AggOrderSpec {
        is_asc_order: call.order_by.iter().map(|item| item.asc).collect(),
        nulls_first: call.order_by.iter().map(|item| item.nulls_first).collect(),
        is_distinct: call.distinct,
        group_concat_max_len: if call.name.eq_ignore_ascii_case("group_concat")
            || call.name.eq_ignore_ascii_case("string_agg")
        {
            Some(1024)
        } else {
            None
        },
    }
}

fn select_aggregate_inputs(
    fn_name: &str,
    is_merge: bool,
    args: Vec<crate::exec::expr::ExprId>,
    arena: &mut ExprArena,
) -> Result<Vec<crate::exec::expr::ExprId>, String> {
    if is_merge {
        return args
            .into_iter()
            .next()
            .map(|expr| vec![expr])
            .ok_or_else(|| format!("{fn_name} merge input missing"));
    }
    if fn_name == "count_if" {
        return match args.len() {
            1 => Ok(args),
            2 => Ok(vec![args[1]]),
            other => Err(format!("count_if expects 1 or 2 arguments, got {other}")),
        };
    }
    pack_struct_inputs(args, arena)
}

fn pack_struct_inputs(
    args: Vec<crate::exec::expr::ExprId>,
    arena: &mut ExprArena,
) -> Result<Vec<crate::exec::expr::ExprId>, String> {
    if args.len() <= 1 {
        return Ok(args);
    }
    let mut fields = Vec::with_capacity(args.len());
    for (idx, expr_id) in args.iter().enumerate() {
        let data_type = arena
            .data_type(*expr_id)
            .ok_or_else(|| "aggregate input type missing".to_string())?;
        fields.push(Field::new(format!("f{idx}"), data_type.clone(), true));
    }
    let struct_type = DataType::Struct(Fields::from(fields));
    let struct_expr = arena.push_typed(ExprNode::StructExpr { fields: args }, struct_type);
    Ok(vec![struct_expr])
}

fn lower_hash_join_node(
    node: &plan::DistributedNode,
    physical: &plan::PlanNode,
    join: &plan::HashJoinNode,
    children: Vec<LoweredNode>,
    arena: &mut ExprArena,
) -> Result<LoweredNode, String> {
    check_exact_arity("HashJoinNode", 2, children.len())?;
    let mut it = children.into_iter();
    let left = it.next().expect("left");
    let right = it.next().expect("right");
    if join.eq_conditions.is_empty() {
        return Err("HashJoinNode requires non-empty eq_conditions".to_string());
    }
    let join_type = proto_join_type(join.join_type, "HashJoinNode")?;
    let distribution_mode = hash_join_distribution_mode(join)?;
    let join_layout = concat_layouts(&left.layout, &right.layout)?;
    let join_scope_chunk_schema = Arc::new(ChunkSchema::concat(&[
        left.output_schema.clone(),
        right.output_schema.clone(),
    ])?);
    let output_schema =
        join_output_chunk_schema(physical, join_scope_chunk_schema.clone(), "HashJoinNode")?;

    let mut probe_keys = Vec::with_capacity(join.eq_conditions.len());
    let mut build_keys = Vec::with_capacity(join.eq_conditions.len());
    let mut eq_null_safe = Vec::with_capacity(join.eq_conditions.len());
    let right_semi_physical_right_probe = join_type == JoinType::RightSemi;
    for (idx, cond) in join.eq_conditions.iter().enumerate() {
        let left_expr = cond
            .left
            .as_ref()
            .ok_or_else(|| format!("HashJoinNode eq_conditions[{idx}] left missing"))?;
        let right_expr = cond
            .right
            .as_ref()
            .ok_or_else(|| format!("HashJoinNode eq_conditions[{idx}] right missing"))?;
        let probe_key = lower_proto_expr(left_expr, arena, &left.layout)
            .map_err(|err| format!("HashJoinNode eq_conditions[{idx}] left: {err}"))?;
        let build_key = lower_proto_expr(right_expr, arena, &right.layout)
            .map_err(|err| format!("HashJoinNode eq_conditions[{idx}] right: {err}"))?;
        if right_semi_physical_right_probe {
            probe_keys.push(build_key);
            build_keys.push(probe_key);
        } else {
            probe_keys.push(probe_key);
            build_keys.push(build_key);
        }
        eq_null_safe.push(cond.null_safe);
    }
    let raw_probe_keys = probe_keys.clone();
    let raw_build_keys = build_keys.clone();
    coerce_join_key_types(&mut probe_keys, &mut build_keys, arena)?;
    for key in probe_keys.iter().chain(build_keys.iter()) {
        if let Some(dt) = arena.data_type(*key)
            && matches!(dt, DataType::LargeBinary)
        {
            return Err("VARIANT is not supported in HASH_JOIN keys".to_string());
        }
    }

    let residual_predicate = join
        .other_condition
        .as_ref()
        .map(|expr| lower_proto_expr(expr, arena, &join_layout))
        .transpose()
        .map_err(|err| format!("HashJoinNode other_condition: {err}"))?;
    let runtime_filters = lower_join_runtime_filters(
        join,
        join_type,
        if right_semi_physical_right_probe {
            &right.layout
        } else {
            &left.layout
        },
        if right_semi_physical_right_probe {
            &left.layout
        } else {
            &right.layout
        },
        &raw_probe_keys,
        &raw_build_keys,
        &probe_keys,
        &build_keys,
        arena,
    )?;

    Ok(LoweredNode {
        node: ExecNode {
            kind: ExecNodeKind::Join(JoinNode {
                left: Box::new(left.node),
                right: Box::new(right.node),
                node_id: node.node_id,
                join_type,
                distribution_mode,
                left_chunk_schema: left.output_schema,
                right_chunk_schema: right.output_schema,
                join_scope_chunk_schema: output_schema.clone(),
                probe_keys,
                build_keys,
                eq_null_safe,
                residual_predicate,
                runtime_filters,
            }),
        },
        layout: join_layout,
        output_schema,
    })
}

fn join_output_chunk_schema(
    physical: &plan::PlanNode,
    fallback: ChunkSchemaRef,
    node_kind: &str,
) -> Result<ChunkSchemaRef, String> {
    if physical.output_columns.is_empty() {
        return Ok(fallback);
    }
    let output_schema = chunk_schema_from_output_columns(&physical.output_columns)
        .map_err(|err| format!("{node_kind} output_columns: {err}"))?;
    if output_schema.slot_ids() == fallback.slot_ids() {
        return Ok(output_schema);
    }
    Ok(fallback)
}

fn hash_join_distribution_mode(join: &plan::HashJoinNode) -> Result<JoinDistributionMode, String> {
    if let Some(mode) = join.execution_mode {
        return match plan::JoinExecutionMode::try_from(mode)
            .map_err(|_| format!("HashJoinNode unknown execution_mode {mode}"))?
        {
            plan::JoinExecutionMode::Broadcast => Ok(JoinDistributionMode::Broadcast),
            plan::JoinExecutionMode::Partitioned | plan::JoinExecutionMode::Colocate => {
                Ok(JoinDistributionMode::Partitioned)
            }
            plan::JoinExecutionMode::Unspecified => {
                Err("HashJoinNode execution_mode is unspecified".to_string())
            }
        };
    }

    match plan::JoinDistribution::try_from(join.distribution)
        .map_err(|_| format!("HashJoinNode unknown distribution {}", join.distribution))?
    {
        plan::JoinDistribution::Broadcast | plan::JoinDistribution::Unknown => {
            Ok(JoinDistributionMode::Broadcast)
        }
        plan::JoinDistribution::Shuffle | plan::JoinDistribution::Colocate => {
            Ok(JoinDistributionMode::Partitioned)
        }
        plan::JoinDistribution::Unspecified => {
            Err("HashJoinNode distribution is unspecified".to_string())
        }
    }
}

fn lower_join_runtime_filters(
    join: &plan::HashJoinNode,
    join_type: JoinType,
    probe_layout: &Layout,
    build_layout: &Layout,
    raw_probe_keys: &[crate::exec::expr::ExprId],
    raw_build_keys: &[crate::exec::expr::ExprId],
    probe_keys: &[crate::exec::expr::ExprId],
    build_keys: &[crate::exec::expr::ExprId],
    arena: &mut ExprArena,
) -> Result<Vec<JoinRuntimeFilterSpec>, String> {
    if !is_runtime_filter_safe_join_type(join_type) {
        return Ok(Vec::new());
    }
    let mut runtime_filters = Vec::new();
    for rf in &join.build_runtime_filters {
        let expr_order = rf.expr_order as usize;
        if expr_order >= probe_keys.len() || expr_order >= build_keys.len() {
            return Err(format!(
                "HashJoinNode runtime filter {} expr_order {} out of range",
                rf.filter_id, expr_order
            ));
        }
        validate_runtime_filter_intent(
            rf,
            expr_order,
            probe_layout,
            build_layout,
            raw_probe_keys[expr_order],
            raw_build_keys[expr_order],
            arena,
        )?;
        if join
            .eq_conditions
            .get(expr_order)
            .map(|cond| cond.null_safe)
            .unwrap_or(false)
        {
            continue;
        }
        let build_data_type = arena
            .data_type(build_keys[expr_order])
            .ok_or_else(|| format!("runtime filter {} build key type missing", rf.filter_id))?
            .clone();
        let Some(ExprNode::SlotId(probe_slot_id)) = arena.node(probe_keys[expr_order]) else {
            continue;
        };
        runtime_filters.push(JoinRuntimeFilterSpec {
            filter_id: rf.filter_id,
            expr_order,
            probe_expr_id: probe_keys[expr_order],
            build_expr_id: build_keys[expr_order],
            probe_slot_id: *probe_slot_id,
            build_data_type,
            merge_nodes: Vec::new(),
            has_remote_targets: false,
        });
    }
    Ok(runtime_filters)
}

fn is_runtime_filter_safe_join_type(join_type: JoinType) -> bool {
    matches!(
        join_type,
        JoinType::Inner | JoinType::LeftSemi | JoinType::RightSemi
    )
}

fn validate_runtime_filter_intent(
    rf: &plan::RuntimeFilterBuildIntent,
    expr_order: usize,
    probe_layout: &Layout,
    build_layout: &Layout,
    expected_probe_key: crate::exec::expr::ExprId,
    expected_build_key: crate::exec::expr::ExprId,
    arena: &mut ExprArena,
) -> Result<(), String> {
    let probe_expr = rf.probe_expr.as_ref().ok_or_else(|| {
        format!(
            "HashJoinNode runtime filter {} probe_expr missing",
            rf.filter_id
        )
    })?;
    let probe_expr_id = lower_proto_expr(probe_expr, arena, probe_layout).map_err(|err| {
        format!(
            "HashJoinNode runtime filter {} probe_expr: {err}",
            rf.filter_id
        )
    })?;
    if !exprs_equivalent(arena, probe_expr_id, expected_probe_key) {
        return Err(format!(
            "HashJoinNode runtime filter {} probe_expr does not match join key at expr_order {}",
            rf.filter_id, expr_order
        ));
    }

    let build_expr = rf.build_expr.as_ref().ok_or_else(|| {
        format!(
            "HashJoinNode runtime filter {} build_expr missing",
            rf.filter_id
        )
    })?;
    let build_expr_id = lower_proto_expr(build_expr, arena, build_layout).map_err(|err| {
        format!(
            "HashJoinNode runtime filter {} build_expr: {err}",
            rf.filter_id
        )
    })?;
    if !exprs_equivalent(arena, build_expr_id, expected_build_key) {
        return Err(format!(
            "HashJoinNode runtime filter {} build_expr does not match join key at expr_order {}",
            rf.filter_id, expr_order
        ));
    }

    Ok(())
}

fn exprs_equivalent(
    arena: &ExprArena,
    left: crate::exec::expr::ExprId,
    right: crate::exec::expr::ExprId,
) -> bool {
    if arena.data_type(left) != arena.data_type(right) {
        return false;
    }
    let Some(left_node) = arena.node(left) else {
        return false;
    };
    let Some(right_node) = arena.node(right) else {
        return false;
    };
    match (left_node, right_node) {
        (ExprNode::Literal(left), ExprNode::Literal(right)) => {
            format!("{left:?}") == format!("{right:?}")
        }
        (ExprNode::SlotId(left), ExprNode::SlotId(right)) => left == right,
        (ExprNode::ArrayExpr { elements: left }, ExprNode::ArrayExpr { elements: right })
        | (ExprNode::StructExpr { fields: left }, ExprNode::StructExpr { fields: right }) => {
            expr_id_slices_equivalent(arena, left, right)
        }
        (
            ExprNode::LambdaFunction {
                body: left_body,
                arg_slots: left_args,
                common_sub_exprs: left_common,
                is_nondeterministic: left_nondeterministic,
            },
            ExprNode::LambdaFunction {
                body: right_body,
                arg_slots: right_args,
                common_sub_exprs: right_common,
                is_nondeterministic: right_nondeterministic,
            },
        ) => {
            left_args == right_args
                && left_nondeterministic == right_nondeterministic
                && exprs_equivalent(arena, *left_body, *right_body)
                && common_sub_exprs_equivalent(arena, left_common, right_common)
        }
        (
            ExprNode::DictDecode {
                child: left,
                dict: left_dict,
            },
            ExprNode::DictDecode {
                child: right,
                dict: right_dict,
            },
        ) => Arc::ptr_eq(left_dict, right_dict) && exprs_equivalent(arena, *left, *right),
        (ExprNode::Cast(left), ExprNode::Cast(right))
        | (ExprNode::CastTime(left), ExprNode::CastTime(right))
        | (ExprNode::CastTimeFromDatetime(left), ExprNode::CastTimeFromDatetime(right))
        | (ExprNode::Not(left), ExprNode::Not(right))
        | (ExprNode::IsNull(left), ExprNode::IsNull(right))
        | (ExprNode::IsNotNull(left), ExprNode::IsNotNull(right))
        | (ExprNode::Clone(left), ExprNode::Clone(right)) => exprs_equivalent(arena, *left, *right),
        (ExprNode::Add(ll, lr), ExprNode::Add(rl, rr))
        | (ExprNode::Sub(ll, lr), ExprNode::Sub(rl, rr))
        | (ExprNode::Mul(ll, lr), ExprNode::Mul(rl, rr))
        | (ExprNode::Div(ll, lr), ExprNode::Div(rl, rr))
        | (ExprNode::Mod(ll, lr), ExprNode::Mod(rl, rr))
        | (ExprNode::Eq(ll, lr), ExprNode::Eq(rl, rr))
        | (ExprNode::EqForNull(ll, lr), ExprNode::EqForNull(rl, rr))
        | (ExprNode::Ne(ll, lr), ExprNode::Ne(rl, rr))
        | (ExprNode::Lt(ll, lr), ExprNode::Lt(rl, rr))
        | (ExprNode::Le(ll, lr), ExprNode::Le(rl, rr))
        | (ExprNode::Gt(ll, lr), ExprNode::Gt(rl, rr))
        | (ExprNode::Ge(ll, lr), ExprNode::Ge(rl, rr))
        | (ExprNode::And(ll, lr), ExprNode::And(rl, rr))
        | (ExprNode::Or(ll, lr), ExprNode::Or(rl, rr)) => {
            exprs_equivalent(arena, *ll, *rl) && exprs_equivalent(arena, *lr, *rr)
        }
        (
            ExprNode::In {
                child: left_child,
                values: left_values,
                is_not_in: left_not,
            },
            ExprNode::In {
                child: right_child,
                values: right_values,
                is_not_in: right_not,
            },
        ) => {
            left_not == right_not
                && exprs_equivalent(arena, *left_child, *right_child)
                && expr_id_slices_equivalent(arena, left_values, right_values)
        }
        (
            ExprNode::Case {
                has_case_expr: left_has_case,
                has_else_expr: left_has_else,
                children: left_children,
            },
            ExprNode::Case {
                has_case_expr: right_has_case,
                has_else_expr: right_has_else,
                children: right_children,
            },
        ) => {
            left_has_case == right_has_case
                && left_has_else == right_has_else
                && expr_id_slices_equivalent(arena, left_children, right_children)
        }
        (
            ExprNode::FunctionCall {
                kind: left_kind,
                args: left_args,
            },
            ExprNode::FunctionCall {
                kind: right_kind,
                args: right_args,
            },
        ) => left_kind == right_kind && expr_id_slices_equivalent(arena, left_args, right_args),
        _ => false,
    }
}

fn expr_id_slices_equivalent(
    arena: &ExprArena,
    left: &[crate::exec::expr::ExprId],
    right: &[crate::exec::expr::ExprId],
) -> bool {
    left.len() == right.len()
        && left
            .iter()
            .zip(right)
            .all(|(left, right)| exprs_equivalent(arena, *left, *right))
}

fn common_sub_exprs_equivalent(
    arena: &ExprArena,
    left: &[(SlotId, crate::exec::expr::ExprId)],
    right: &[(SlotId, crate::exec::expr::ExprId)],
) -> bool {
    left.len() == right.len()
        && left
            .iter()
            .zip(right)
            .all(|((left_slot, left_expr), (right_slot, right_expr))| {
                left_slot == right_slot && exprs_equivalent(arena, *left_expr, *right_expr)
            })
}

fn coerce_join_key_types(
    probe_keys: &mut [crate::exec::expr::ExprId],
    build_keys: &mut [crate::exec::expr::ExprId],
    arena: &mut ExprArena,
) -> Result<(), String> {
    for idx in 0..probe_keys.len() {
        let probe_expr = probe_keys[idx];
        let build_expr = build_keys[idx];
        let probe_type = arena
            .data_type(probe_expr)
            .ok_or_else(|| "HASH_JOIN probe key type missing".to_string())?
            .clone();
        let build_type = arena
            .data_type(build_expr)
            .ok_or_else(|| "HASH_JOIN build key type missing".to_string())?
            .clone();
        if probe_type == build_type {
            continue;
        }
        let common_type = common_join_key_type(&probe_type, &build_type)?;
        match common_type {
            Some(target_type) => {
                if probe_type != target_type {
                    probe_keys[idx] =
                        arena.push_typed(ExprNode::Cast(probe_expr), target_type.clone());
                }
                if build_type != target_type {
                    build_keys[idx] = arena.push_typed(ExprNode::Cast(build_expr), target_type);
                }
            }
            None => {
                build_keys[idx] = arena.push_typed(ExprNode::Cast(build_expr), probe_type);
            }
        }
    }
    Ok(())
}

fn common_join_key_type(left: &DataType, right: &DataType) -> Result<Option<DataType>, String> {
    if left == right {
        return Ok(Some(left.clone()));
    }
    match (left, right) {
        (
            DataType::Decimal128(_, _) | DataType::Decimal256(_, _),
            DataType::Decimal128(_, _) | DataType::Decimal256(_, _),
        ) => Ok(Some(crate::types::coercion::decimal_compare_type(
            left, right,
        )?)),
        (DataType::List(left_field), DataType::List(right_field)) => {
            let Some(elem_type) =
                common_join_key_type(left_field.data_type(), right_field.data_type())?
            else {
                return Ok(None);
            };
            Ok(Some(DataType::List(Arc::new(Field::new(
                left_field.name(),
                elem_type,
                left_field.is_nullable() || right_field.is_nullable(),
            )))))
        }
        _ => Ok(Some(wider_type(left, right))),
    }
}

fn lower_nest_loop_join_node(
    node: &plan::DistributedNode,
    physical: &plan::PlanNode,
    join: &plan::NestLoopJoinNode,
    children: Vec<LoweredNode>,
    arena: &mut ExprArena,
) -> Result<LoweredNode, String> {
    check_exact_arity("NestLoopJoinNode", 2, children.len())?;
    let mut it = children.into_iter();
    let mut left = it.next().expect("left");
    let mut right = it.next().expect("right");
    let join_kind = plan::JoinKind::try_from(join.join_type)
        .map_err(|_| format!("NestLoopJoinNode unknown join_type {}", join.join_type))?;
    let join_type = match join_kind {
        plan::JoinKind::RightSemi => {
            std::mem::swap(&mut left, &mut right);
            NestedLoopJoinType::LeftSemi
        }
        plan::JoinKind::RightAnti => {
            std::mem::swap(&mut left, &mut right);
            NestedLoopJoinType::LeftAnti
        }
        _ => proto_nested_loop_join_type(join.join_type, "NestLoopJoinNode")?,
    };
    let join_layout = concat_layouts(&left.layout, &right.layout)?;
    let join_scope_chunk_schema = Arc::new(ChunkSchema::concat(&[
        left.output_schema.clone(),
        right.output_schema.clone(),
    ])?);
    let is_semi_anti = matches!(
        join_type,
        NestedLoopJoinType::LeftSemi
            | NestedLoopJoinType::LeftAnti
            | NestedLoopJoinType::NullAwareLeftAnti
    );
    let output_schema = if is_semi_anti && !physical.output_columns.is_empty() {
        chunk_schema_from_output_columns(&physical.output_columns)
            .map_err(|err| format!("NestLoopJoinNode output_columns: {err}"))?
    } else {
        join_output_chunk_schema(
            physical,
            join_scope_chunk_schema.clone(),
            "NestLoopJoinNode",
        )?
    };
    let join_conjunct = join
        .condition
        .as_ref()
        .map(|expr| lower_proto_expr(expr, arena, &join_layout))
        .transpose()
        .map_err(|err| format!("NestLoopJoinNode condition: {err}"))?;
    let output_layout = if is_semi_anti {
        Layout::for_slots(output_schema.slot_ids().iter().copied())
    } else {
        join_layout.clone()
    };
    let execution_scope_chunk_schema = if is_semi_anti {
        join_scope_chunk_schema
    } else {
        output_schema.clone()
    };

    Ok(LoweredNode {
        node: ExecNode {
            kind: ExecNodeKind::NestedLoopJoin(NestedLoopJoinNode {
                left: Box::new(left.node),
                right: Box::new(right.node),
                node_id: node.node_id,
                join_type,
                join_conjunct,
                left_chunk_schema: left.output_schema,
                right_chunk_schema: right.output_schema,
                join_scope_chunk_schema: execution_scope_chunk_schema,
            }),
        },
        layout: output_layout,
        output_schema,
    })
}

fn proto_join_type(value: i32, node_kind: &str) -> Result<JoinType, String> {
    match plan::JoinKind::try_from(value)
        .map_err(|_| format!("{node_kind} unknown join_type {value}"))?
    {
        plan::JoinKind::Inner => Ok(JoinType::Inner),
        plan::JoinKind::LeftOuter => Ok(JoinType::LeftOuter),
        plan::JoinKind::RightOuter => Ok(JoinType::RightOuter),
        plan::JoinKind::FullOuter => Ok(JoinType::FullOuter),
        plan::JoinKind::LeftSemi => Ok(JoinType::LeftSemi),
        plan::JoinKind::RightSemi => Ok(JoinType::RightSemi),
        plan::JoinKind::LeftAnti => Ok(JoinType::LeftAnti),
        plan::JoinKind::RightAnti => Ok(JoinType::RightAnti),
        plan::JoinKind::NullAwareLeftAnti => Ok(JoinType::NullAwareLeftAnti),
        plan::JoinKind::Cross => Err(format!("{node_kind} CROSS join requires NestLoopJoinNode")),
        plan::JoinKind::Unspecified => Err(format!("{node_kind} join_type is unspecified")),
    }
}

fn proto_nested_loop_join_type(value: i32, node_kind: &str) -> Result<NestedLoopJoinType, String> {
    match plan::JoinKind::try_from(value)
        .map_err(|_| format!("{node_kind} unknown join_type {value}"))?
    {
        plan::JoinKind::Inner => Ok(NestedLoopJoinType::Inner),
        plan::JoinKind::Cross => Ok(NestedLoopJoinType::Cross),
        plan::JoinKind::LeftOuter => Ok(NestedLoopJoinType::LeftOuter),
        plan::JoinKind::RightOuter => Ok(NestedLoopJoinType::RightOuter),
        plan::JoinKind::FullOuter => Ok(NestedLoopJoinType::FullOuter),
        plan::JoinKind::LeftSemi => Ok(NestedLoopJoinType::LeftSemi),
        plan::JoinKind::LeftAnti => Ok(NestedLoopJoinType::LeftAnti),
        plan::JoinKind::NullAwareLeftAnti => Ok(NestedLoopJoinType::NullAwareLeftAnti),
        plan::JoinKind::RightSemi | plan::JoinKind::RightAnti => Err(format!(
            "{node_kind} right semi/anti must be rewritten before nested-loop join type lowering"
        )),
        plan::JoinKind::Unspecified => Err(format!("{node_kind} join_type is unspecified")),
    }
}

fn concat_layouts(left: &Layout, right: &Layout) -> Result<Layout, String> {
    let mut slots = Vec::with_capacity(left.order().len() + right.order().len());
    let mut seen = HashSet::with_capacity(left.order().len() + right.order().len());
    for slot in left.order().iter().chain(right.order().iter()).copied() {
        if !seen.insert(slot) {
            return Err(format!("duplicate slot id {} in joined layout", slot));
        }
        slots.push(slot);
    }
    Ok(Layout::for_slots(slots))
}

fn lower_repeat_node(
    node: &plan::DistributedNode,
    repeat: &plan::RepeatNode,
    mut children: Vec<LoweredNode>,
) -> Result<LoweredNode, String> {
    check_exact_arity("RepeatNode", 1, children.len())?;
    let child = children.pop().expect("child");
    let repeat_times = repeat.grouping_ids.len();
    if repeat_times == 0 {
        return Err("RepeatNode grouping_ids is empty".to_string());
    }
    if repeat.repeat_column_ref_ids.len() != repeat_times {
        return Err(format!(
            "RepeatNode repeat_column_ref_ids size mismatch: expected {}, got {}",
            repeat_times,
            repeat.repeat_column_ref_ids.len()
        ));
    }
    let all_slot_ids = repeat
        .all_rollup_column_ids
        .iter()
        .copied()
        .map(SlotId::new)
        .collect::<Vec<_>>();
    let all_slot_set = all_slot_ids.iter().copied().collect::<HashSet<_>>();
    let null_slot_ids = repeat
        .repeat_column_ref_ids
        .iter()
        .enumerate()
        .map(|(idx, keep_ids)| {
            let keep = keep_ids
                .values
                .iter()
                .copied()
                .map(SlotId::new)
                .collect::<HashSet<_>>();
            for slot in &keep {
                if !all_slot_set.contains(slot) {
                    return Err(format!(
                        "RepeatNode keep set {idx} contains unknown rollup slot {}",
                        slot
                    ));
                }
            }
            let mut nulls = all_slot_ids
                .iter()
                .copied()
                .filter(|slot| !keep.contains(slot))
                .collect::<Vec<_>>();
            nulls.sort_by_key(|slot| slot.as_u32());
            Ok(nulls)
        })
        .collect::<Result<Vec<_>, String>>()?;
    let grouping_slot_ids = repeat
        .grouping_fn_ids
        .iter()
        .map(|entry| SlotId::new(entry.value))
        .collect::<Vec<_>>();
    let grouping_list = repeat_grouping_values(repeat)?;
    let (layout, output_schema) =
        repeat_output_layout_and_schema(&child, &repeat.grouping_fn_ids, &grouping_slot_ids)?;

    Ok(LoweredNode {
        node: ExecNode {
            kind: ExecNodeKind::Repeat(RepeatNode {
                input: Box::new(child.node),
                node_id: node.node_id,
                null_slot_ids,
                grouping_slot_ids,
                grouping_list,
                repeat_times,
            }),
        },
        layout,
        output_schema,
    })
}

fn repeat_output_layout_and_schema(
    child: &LoweredNode,
    grouping_fn_ids: &[plan::NamedUInt32],
    grouping_slot_ids: &[SlotId],
) -> Result<(Layout, ChunkSchemaRef), String> {
    let mut slots = child.output_schema.slots().to_vec();
    let mut output_slot_ids = child.layout.order().to_vec();
    for (idx, slot_id) in grouping_slot_ids.iter().copied().enumerate() {
        if child.layout.contains_slot(slot_id) || output_slot_ids.contains(&slot_id) {
            return Err(format!(
                "RepeatNode grouping slot {} duplicates input slot",
                slot_id
            ));
        }
        let name = grouping_fn_ids
            .get(idx)
            .map(|entry| entry.name.as_str())
            .filter(|name| !name.is_empty())
            .unwrap_or("__grouping_fn");
        let field = Field::new(name, DataType::Int64, true);
        slots.push(ChunkSlotSchema::new_with_field(slot_id, field, None, None));
        output_slot_ids.push(slot_id);
    }
    let layout = Layout::for_slots(output_slot_ids);
    let output_schema = Arc::new(ChunkSchema::try_new(slots)?);
    Ok((layout, output_schema))
}

fn repeat_grouping_values(repeat: &plan::RepeatNode) -> Result<Vec<Vec<i64>>, String> {
    if repeat.grouping_fn_ids.len() != repeat.grouping_fn_arg_ids.len() {
        return Err(format!(
            "RepeatNode grouping fn length mismatch: ids={} arg_ids={}",
            repeat.grouping_fn_ids.len(),
            repeat.grouping_fn_arg_ids.len()
        ));
    }
    let repeat_times = repeat.grouping_ids.len();
    let keep_sets = repeat
        .repeat_column_ref_ids
        .iter()
        .map(|ids| ids.values.iter().copied().collect::<HashSet<_>>())
        .collect::<Vec<_>>();
    repeat
        .grouping_fn_arg_ids
        .iter()
        .enumerate()
        .map(|(idx, args)| {
            if args.values.len() > 63 {
                return Err(format!(
                    "RepeatNode grouping_fn_arg_ids[{idx}] has too many arguments: {}",
                    args.values.len()
                ));
            }
            let mut values = Vec::with_capacity(repeat_times);
            for (repeat_idx, keep) in keep_sets.iter().enumerate() {
                let mut value = 0i64;
                for (arg_idx, column_id) in args.values.iter().enumerate() {
                    if !keep.contains(column_id) {
                        let reverse_bit_pos = args.values.len() - 1 - arg_idx;
                        value |= 1i64 << reverse_bit_pos;
                    }
                }
                if repeat_idx >= repeat_times {
                    return Err("RepeatNode internal repeat index overflow".to_string());
                }
                values.push(value);
            }
            Ok(values)
        })
        .collect()
}

fn lower_change_event_expand_node(
    node: &plan::DistributedNode,
    physical: &plan::PlanNode,
    expand: &plan::ChangeEventExpandNode,
    mut children: Vec<LoweredNode>,
    arena: &mut ExprArena,
) -> Result<LoweredNode, String> {
    check_exact_arity("ChangeEventExpandNode", 1, children.len())?;
    let child = children.pop().expect("child");
    let output_columns = if expand.output_columns.is_empty() {
        &physical.output_columns
    } else {
        &expand.output_columns
    };
    let layout = layout_from_output_columns(output_columns)?;
    let output_schema = chunk_schema_from_output_columns(output_columns)?;
    let output_slot_ids = layout.order().to_vec();
    let output_set = output_slot_ids.iter().copied().collect::<HashSet<_>>();
    let change_op_slot_id = SlotId::new(expand.change_op_column_id);
    if !output_set.contains(&change_op_slot_id) {
        return Err(format!(
            "ChangeEventExpandNode change_op_column_id {} is not in outputs",
            expand.change_op_column_id
        ));
    }
    let change_op_field = output_schema.slot(change_op_slot_id).ok_or_else(|| {
        format!(
            "ChangeEventExpandNode change_op_column_id {} missing from output schema",
            expand.change_op_column_id
        )
    })?;
    if change_op_field.data_type() != &DataType::Int8 {
        return Err(format!(
            "ChangeEventExpandNode change_op_column_id {} must be Int8, got {:?}",
            expand.change_op_column_id,
            change_op_field.data_type()
        ));
    }
    let data_route_slot_id = expand.data_route_column_id.map(SlotId::new);
    if let Some(slot_id) = data_route_slot_id {
        if slot_id == change_op_slot_id {
            return Err(format!(
                "ChangeEventExpandNode data_route_column_id {} must differ from change_op_column_id {}",
                slot_id, change_op_slot_id
            ));
        }
        if !output_set.contains(&slot_id) {
            return Err(format!(
                "ChangeEventExpandNode data_route_column_id {} is not in outputs",
                slot_id
            ));
        }
        let route_field = output_schema.slot(slot_id).ok_or_else(|| {
            format!(
                "ChangeEventExpandNode data_route_column_id {} missing from output schema",
                slot_id
            )
        })?;
        if !is_signed_integer_route_type(route_field.data_type()) {
            return Err(format!(
                "ChangeEventExpandNode data_route_column_id {} must be a signed integer route type, got {:?}",
                slot_id,
                route_field.data_type()
            ));
        }
    }

    let mut events = Vec::with_capacity(expand.events.len());
    for (event_idx, event) in expand.events.iter().enumerate() {
        let branch_kind = change_event_branch_kind(event.branch_kind)?;
        if matches!(
            branch_kind,
            ChangeStreamBranchKind::ReuseData | ChangeStreamBranchKind::FreshData
        ) && data_route_slot_id.is_none()
        {
            return Err(format!(
                "ChangeEventExpandNode data branch {:?} requires data_route_column_id",
                branch_kind
            ));
        }
        let predicate = event
            .predicate
            .as_ref()
            .map(|expr| lower_proto_expr(expr, arena, &child.layout))
            .transpose()
            .map_err(|err| format!("ChangeEventExpandNode event {event_idx} predicate: {err}"))?;
        let assignments = event
            .assignments
            .iter()
            .enumerate()
            .map(|(assign_idx, assignment)| {
                let slot_id = SlotId::new(assignment.output_column_id);
                if !output_set.contains(&slot_id) {
                    return Err(format!(
                        "ChangeEventExpandNode event {event_idx} assignment {assign_idx} output column {} is not in outputs",
                        assignment.output_column_id
                    ));
                }
                let expr = assignment
                    .expr
                    .as_ref()
                    .map(|expr| lower_proto_expr(expr, arena, &child.layout))
                    .transpose()
                    .map_err(|err| {
                        format!(
                            "ChangeEventExpandNode event {event_idx} assignment {assign_idx}: {err}"
                        )
                    })?;
                Ok(ChangeEventRuntimeOutputExpr {
                    output_slot_id: slot_id,
                    expr,
                })
            })
            .collect::<Result<Vec<_>, String>>()?;
        events.push(ChangeEventRuntimeSpec {
            predicate,
            branch_kind,
            assignments,
        });
    }

    Ok(LoweredNode {
        node: ExecNode {
            kind: ExecNodeKind::ChangeEventExpand(ChangeEventExpandNode {
                input: Box::new(child.node),
                node_id: node.node_id,
                events,
                output_slot_ids,
                output_chunk_schema: output_schema.clone(),
                change_op_slot_id,
                data_route_slot_id,
            }),
        },
        layout,
        output_schema,
    })
}

fn is_signed_integer_route_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
    )
}

fn change_event_branch_kind(value: i32) -> Result<ChangeStreamBranchKind, String> {
    match plan::ChangeStreamBranchKind::try_from(value)
        .map_err(|_| format!("unknown change event branch kind {value}"))?
    {
        plan::ChangeStreamBranchKind::DeleteDv => Ok(ChangeStreamBranchKind::DeleteDv),
        plan::ChangeStreamBranchKind::ReuseData => Ok(ChangeStreamBranchKind::ReuseData),
        plan::ChangeStreamBranchKind::FreshData => Ok(ChangeStreamBranchKind::FreshData),
        plan::ChangeStreamBranchKind::Unspecified => {
            Err("change event branch kind is unspecified".to_string())
        }
    }
}

fn lower_redistribute_node(
    physical: &plan::PlanNode,
    redistribute: &plan::RedistributeNode,
    mut children: Vec<LoweredNode>,
    arena: &mut ExprArena,
) -> Result<LoweredNode, String> {
    check_exact_arity("RedistributeNode", 1, children.len())?;
    let child = children.pop().expect("child");
    let mode = redistribute
        .mode
        .as_ref()
        .and_then(|mode| mode.mode.as_ref())
        .ok_or_else(|| "RedistributeNode mode missing".to_string())?;
    match mode {
        plan::redistribute_mode::Mode::Gather(true)
        | plan::redistribute_mode::Mode::Broadcast(true) => {}
        plan::redistribute_mode::Mode::Hash(hash) => {
            if hash.cols.is_empty() {
                return Err("RedistributeNode hash mode requires cols".to_string());
            }
            for col in &hash.cols {
                child.layout.resolve_column_id(*col)?;
            }
        }
        plan::redistribute_mode::Mode::Gather(false)
        | plan::redistribute_mode::Mode::Broadcast(false) => {
            return Err("RedistributeNode boolean mode must be true".to_string());
        }
    }
    for (idx, expr) in redistribute.partition_exprs.iter().enumerate() {
        lower_proto_expr(expr, arena, &child.layout)
            .map_err(|err| format!("RedistributeNode partition_exprs[{idx}]: {err}"))?;
    }
    let output_columns = if redistribute.output_columns.is_empty() {
        &physical.output_columns
    } else {
        &redistribute.output_columns
    };
    if output_columns.is_empty() {
        return Ok(child);
    }
    let layout = layout_from_output_columns(output_columns)?;
    if layout.order() != child.layout.order() {
        return Err(format!(
            "RedistributeNode output columns must preserve child order: child={:?} output={:?}",
            child.layout.order(),
            layout.order()
        ));
    }
    let output_schema = chunk_schema_from_output_columns(output_columns)?;
    Ok(LoweredNode {
        node: child.node,
        layout,
        output_schema,
    })
}

fn lower_assert_one_row_node(
    node: &plan::DistributedNode,
    assert: &plan::AssertOneRowNode,
    mut children: Vec<LoweredNode>,
) -> Result<LoweredNode, String> {
    check_exact_arity("AssertOneRowNode", 1, children.len())?;
    let child = children.pop().expect("child");
    let desired_num_rows = parse_optional_nonnegative_i64(
        assert.desired_num_rows,
        "AssertOneRowNode.desired_num_rows",
    )?
    .or(Some(1));
    let assertion = lower_row_count_assertion(assert.assertion)?;
    let mode = if assert.group_key_column_ids.is_empty() {
        if !assert.group_key_labels.is_empty() || assert.keyed_message_prefix.is_some() {
            return Err(
                "AssertOneRowNode group_key_column_ids is required when keyed metadata is present"
                    .to_string(),
            );
        }
        AssertNumRowsMode::Global {
            desired_num_rows,
            assertion,
            subquery_string: Some(assert.subquery_text.clone()),
        }
    } else {
        if desired_num_rows != Some(1) || !matches!(assertion, Assertion::Le) {
            return Err(
                "AssertOneRowNode keyed assertions only support desired_num_rows <= 1".to_string(),
            );
        }
        if !assert.group_key_labels.is_empty()
            && assert.group_key_labels.len() != assert.group_key_column_ids.len()
        {
            return Err(format!(
                "AssertOneRowNode group_key_labels length mismatch: key_columns={} labels={}",
                assert.group_key_column_ids.len(),
                assert.group_key_labels.len()
            ));
        }
        let key_slots = assert
            .group_key_column_ids
            .iter()
            .map(|column_id| {
                child
                    .layout
                    .resolve_column_id(*column_id)
                    .map_err(|err| format!("AssertOneRowNode group key: {err}"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let key_labels = if assert.group_key_labels.is_empty() {
            assert
                .group_key_column_ids
                .iter()
                .map(|column_id| format!("column_{column_id}"))
                .collect()
        } else {
            assert.group_key_labels.clone()
        };
        AssertNumRowsMode::PerKeyAtMostOne {
            key_slots,
            key_labels,
            message_prefix: assert
                .keyed_message_prefix
                .clone()
                .unwrap_or_else(|| "assert_num_rows failed".to_string()),
        }
    };
    Ok(LoweredNode {
        node: ExecNode {
            kind: ExecNodeKind::AssertNumRows(AssertNumRowsNode {
                input: Box::new(child.node),
                node_id: node.node_id,
                mode,
            }),
        },
        layout: child.layout,
        output_schema: child.output_schema,
    })
}

fn lower_row_count_assertion(value: i32) -> Result<Assertion, String> {
    match value {
        value if value == plan::RowCountAssertion::Unspecified as i32 => Ok(Assertion::Le),
        value if value == plan::RowCountAssertion::Eq as i32 => Ok(Assertion::Eq),
        value if value == plan::RowCountAssertion::Ne as i32 => Ok(Assertion::Ne),
        value if value == plan::RowCountAssertion::Lt as i32 => Ok(Assertion::Lt),
        value if value == plan::RowCountAssertion::Le as i32 => Ok(Assertion::Le),
        value if value == plan::RowCountAssertion::Gt as i32 => Ok(Assertion::Gt),
        value if value == plan::RowCountAssertion::Ge as i32 => Ok(Assertion::Ge),
        other => Err(format!(
            "AssertOneRowNode assertion {other} is not supported"
        )),
    }
}

fn parse_optional_nonnegative_i64(
    value: Option<i64>,
    label: &str,
) -> Result<Option<usize>, String> {
    value
        .map(|value| {
            if value < 0 {
                Err(format!("{label} must be >= 0, got {value}"))
            } else {
                Ok(value as usize)
            }
        })
        .transpose()
}

fn parse_distributed_limit(value: i64, label: &str) -> Result<Option<usize>, String> {
    if value == -1 {
        Ok(None)
    } else if value < 0 {
        Err(format!("{label} must be -1 or >= 0, got {value}"))
    } else {
        Ok(Some(value as usize))
    }
}

fn merge_limits(
    node_kind: &str,
    payload_limit: Option<usize>,
    outer_limit: Option<usize>,
) -> Result<Option<usize>, String> {
    match (payload_limit, outer_limit) {
        (Some(left), Some(right)) if left != right => Err(format!(
            "{node_kind} payload limit {left} conflicts with DistributedNode.limit {right}"
        )),
        (Some(value), _) | (_, Some(value)) => Ok(Some(value)),
        (None, None) => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field};

    use super::{NodeLoweringContext, lower_proto_node};
    use crate::common::ids::SlotId;
    use crate::exec::expr::ExprArena;
    use crate::exec::node::ExecNodeKind;
    use crate::exec::node::assert::{AssertNumRowsMode, Assertion};
    use crate::exec::node::set_op::SetOpKind;
    use crate::exec::node::sort::SortTopNType;
    use crate::exec::node::table_function::TableFunctionOutputSlot;
    use crate::proto::{common, expr, plan};
    use crate::runtime::exchange::ExchangeKey;
    use crate::sql::codegen::proto_encode::types::encode_type;

    fn type_desc(data_type: &DataType) -> common::TypeDesc {
        encode_type(data_type).expect("encode type")
    }

    fn output_column_with_nullable(
        column_id: u32,
        name: &str,
        data_type: DataType,
        nullable: bool,
    ) -> common::OutputColumn {
        common::OutputColumn {
            column_id,
            name: name.to_string(),
            r#type: Some(type_desc(&data_type)),
            nullable,
            is_internal: false,
        }
    }

    fn output_column(column_id: u32, name: &str, data_type: DataType) -> common::OutputColumn {
        output_column_with_nullable(column_id, name, data_type, true)
    }

    fn int_literal(value: i64) -> expr::Expr {
        expr::Expr {
            r#type: Some(type_desc(&DataType::Int64)),
            nullable: false,
            kind: Some(expr::expr::Kind::Literal(expr::LiteralExpr {
                value: Some(common::LiteralValue {
                    value: Some(common::literal_value::Value::IntValue(value)),
                }),
            })),
        }
    }

    fn string_literal(value: &str) -> expr::Expr {
        expr::Expr {
            r#type: Some(type_desc(&DataType::Utf8)),
            nullable: false,
            kind: Some(expr::expr::Kind::Literal(expr::LiteralExpr {
                value: Some(common::LiteralValue {
                    value: Some(common::literal_value::Value::StringValue(value.to_string())),
                }),
            })),
        }
    }

    fn bool_literal(value: bool) -> expr::Expr {
        expr::Expr {
            r#type: Some(type_desc(&DataType::Boolean)),
            nullable: false,
            kind: Some(expr::expr::Kind::Literal(expr::LiteralExpr {
                value: Some(common::LiteralValue {
                    value: Some(common::literal_value::Value::BoolValue(value)),
                }),
            })),
        }
    }

    fn null_literal(data_type: DataType) -> expr::Expr {
        expr::Expr {
            r#type: Some(type_desc(&data_type)),
            nullable: true,
            kind: Some(expr::expr::Kind::Literal(expr::LiteralExpr {
                value: Some(common::LiteralValue {
                    value: Some(common::literal_value::Value::NullValue(true)),
                }),
            })),
        }
    }

    fn column_ref(column_id: u32, data_type: DataType) -> expr::Expr {
        expr::Expr {
            r#type: Some(type_desc(&data_type)),
            nullable: true,
            kind: Some(expr::expr::Kind::ColumnRef(expr::ColumnRef {
                column_id,
                qualifier: None,
                column: None,
            })),
        }
    }

    fn sort_item(column_id: u32) -> expr::SortItem {
        expr::SortItem {
            expr: Some(column_ref(column_id, DataType::Int64)),
            asc: true,
            nulls_first: false,
        }
    }

    fn topn_exchange_node(node_id: i32) -> plan::DistributedNode {
        plan::DistributedNode {
            node_id,
            fragment_id: 1,
            tuple_ids: Vec::new(),
            nullable_tuple_ids: Vec::new(),
            limit: -1,
            build_runtime_filters: Vec::new(),
            probe_runtime_filters: Vec::new(),
            children: Vec::new(),
            payload: Some(plan::distributed_node::Payload::Exchange(
                plan::ExchangeReceiver {
                    partition_type: plan::PartitionType::Hash as i32,
                    partition_exprs: Vec::new(),
                    source_fragment_id: 7,
                    output_columns: vec![output_column(1, "id", DataType::Int64)],
                    output_qualifier: None,
                    flavor: Some(plan::ExchangeFlavor {
                        kind: Some(plan::exchange_flavor::Kind::TopnSplit(
                            plan::TopNSplitFlavor {
                                items: vec![sort_item(1)],
                                limit: Some(3),
                                offset: Some(1),
                            },
                        )),
                    }),
                },
            )),
        }
    }

    fn limit_offset_exchange_node(
        node_id: i32,
        limit: Option<i64>,
        offset: Option<i64>,
    ) -> plan::DistributedNode {
        plan::DistributedNode {
            node_id,
            fragment_id: 1,
            tuple_ids: Vec::new(),
            nullable_tuple_ids: Vec::new(),
            limit: limit.unwrap_or(-1),
            build_runtime_filters: Vec::new(),
            probe_runtime_filters: Vec::new(),
            children: Vec::new(),
            payload: Some(plan::distributed_node::Payload::Exchange(
                plan::ExchangeReceiver {
                    partition_type: plan::PartitionType::Unpartitioned as i32,
                    partition_exprs: Vec::new(),
                    source_fragment_id: 7,
                    output_columns: vec![output_column(1, "id", DataType::Int64)],
                    output_qualifier: None,
                    flavor: Some(plan::ExchangeFlavor {
                        kind: Some(plan::exchange_flavor::Kind::LimitOffset(
                            plan::LimitOffsetFlavor { limit, offset },
                        )),
                    }),
                },
            )),
        }
    }

    fn cte_multicast_exchange_node(node_id: i32) -> plan::DistributedNode {
        plan::DistributedNode {
            node_id,
            fragment_id: 1,
            tuple_ids: Vec::new(),
            nullable_tuple_ids: Vec::new(),
            limit: -1,
            build_runtime_filters: Vec::new(),
            probe_runtime_filters: Vec::new(),
            children: Vec::new(),
            payload: Some(plan::distributed_node::Payload::Exchange(
                plan::ExchangeReceiver {
                    partition_type: plan::PartitionType::Unpartitioned as i32,
                    partition_exprs: Vec::new(),
                    source_fragment_id: 7,
                    output_columns: vec![output_column(1, "id", DataType::Int64)],
                    output_qualifier: None,
                    flavor: Some(plan::ExchangeFlavor {
                        kind: Some(plan::exchange_flavor::Kind::CteMulticast(
                            plan::CteMulticastFlavor {
                                cte_id: 3,
                                receive_producer_column_ids: vec![1],
                            },
                        )),
                    }),
                },
            )),
        }
    }

    fn physical_node(
        node_id: i32,
        kind: plan::plan_node::Kind,
        output_columns: Vec<common::OutputColumn>,
        children: Vec<plan::DistributedNode>,
    ) -> plan::DistributedNode {
        plan::DistributedNode {
            node_id,
            fragment_id: 1,
            tuple_ids: Vec::new(),
            nullable_tuple_ids: Vec::new(),
            limit: -1,
            build_runtime_filters: Vec::new(),
            probe_runtime_filters: Vec::new(),
            children,
            payload: Some(plan::distributed_node::Payload::Physical(plan::PlanNode {
                output_columns,
                kind: Some(kind),
            })),
        }
    }

    fn values_node(node_id: i32) -> plan::DistributedNode {
        let columns = vec![
            output_column(1, "id", DataType::Int64),
            output_column(2, "name", DataType::Utf8),
        ];
        physical_node(
            node_id,
            plan::plan_node::Kind::Values(plan::ValuesNode {
                rows: vec![
                    plan::ExprList {
                        values: vec![int_literal(10), string_literal("alice")],
                    },
                    plan::ExprList {
                        values: vec![int_literal(20), string_literal("bob")],
                    },
                ],
                columns: columns.clone(),
            }),
            columns,
            Vec::new(),
        )
    }

    fn one_col_values_node(node_id: i32) -> plan::DistributedNode {
        one_col_values_node_with(node_id, 1, "id", 10)
    }

    fn one_col_values_node_with(
        node_id: i32,
        column_id: u32,
        name: &str,
        value: i64,
    ) -> plan::DistributedNode {
        one_col_values_node_with_nullable(node_id, column_id, name, value, true)
    }

    fn one_col_values_node_with_nullable(
        node_id: i32,
        column_id: u32,
        name: &str,
        value: i64,
        nullable: bool,
    ) -> plan::DistributedNode {
        let columns = vec![output_column_with_nullable(
            column_id,
            name,
            DataType::Int64,
            nullable,
        )];
        physical_node(
            node_id,
            plan::plan_node::Kind::Values(plan::ValuesNode {
                rows: vec![plan::ExprList {
                    values: vec![int_literal(value)],
                }],
                columns: columns.clone(),
            }),
            columns,
            Vec::new(),
        )
    }

    fn two_col_values_node(node_id: i32) -> plan::DistributedNode {
        let columns = vec![
            output_column(1, "a", DataType::Int64),
            output_column(2, "b", DataType::Int64),
        ];
        physical_node(
            node_id,
            plan::plan_node::Kind::Values(plan::ValuesNode {
                rows: vec![plan::ExprList {
                    values: vec![int_literal(10), int_literal(20)],
                }],
                columns: columns.clone(),
            }),
            columns,
            Vec::new(),
        )
    }

    fn three_col_values_node(node_id: i32) -> plan::DistributedNode {
        let columns = vec![
            output_column(1, "a", DataType::Int64),
            output_column(2, "b", DataType::Int64),
            output_column(3, "c", DataType::Int64),
        ];
        physical_node(
            node_id,
            plan::plan_node::Kind::Values(plan::ValuesNode {
                rows: vec![plan::ExprList {
                    values: vec![int_literal(10), int_literal(20), int_literal(30)],
                }],
                columns: columns.clone(),
            }),
            columns,
            Vec::new(),
        )
    }

    fn lower(node: &plan::DistributedNode) -> super::LoweredNode {
        let mut arena = ExprArena::default();
        lower_proto_node(node, &mut arena, &NodeLoweringContext::default()).expect("lower node")
    }

    #[test]
    fn lowers_values_rows_into_chunk_schema() {
        let lowered = lower(&values_node(10));
        let ExecNodeKind::Values(values) = lowered.node.kind else {
            panic!("expected Values");
        };
        assert_eq!(values.node_id, 10);
        assert_eq!(values.chunk.len(), 2);
        assert_eq!(
            values.chunk.chunk_schema().slot_ids(),
            &[SlotId::new(1), SlotId::new(2)]
        );
        assert_eq!(lowered.layout.order(), &[SlotId::new(1), SlotId::new(2)]);
        assert_eq!(
            lowered.output_schema.slot_ids(),
            &[SlotId::new(1), SlotId::new(2)]
        );

        let id_column = values
            .chunk
            .column_by_slot_id(SlotId::new(1))
            .expect("id column");
        let id = id_column
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("int64 id");
        assert_eq!(id.value(0), 10);
        assert_eq!(id.value(1), 20);

        let name_column = values
            .chunk
            .column_by_slot_id(SlotId::new(2))
            .expect("name column");
        let name = name_column
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("utf8 name");
        assert_eq!(name.value(0), "alice");
        assert_eq!(name.value(1), "bob");
    }

    #[test]
    fn values_casts_null_rows_to_declared_column_type_before_concat() {
        let columns = vec![output_column(1, "id", DataType::Int64)];
        let node = physical_node(
            10,
            plan::plan_node::Kind::Values(plan::ValuesNode {
                rows: vec![
                    plan::ExprList {
                        values: vec![int_literal(10)],
                    },
                    plan::ExprList {
                        values: vec![null_literal(DataType::Null)],
                    },
                ],
                columns: columns.clone(),
            }),
            columns,
            Vec::new(),
        );

        let lowered = lower(&node);
        let ExecNodeKind::Values(values) = lowered.node.kind else {
            panic!("expected Values");
        };
        let id_column = values
            .chunk
            .column_by_slot_id(SlotId::new(1))
            .expect("id column");
        assert_eq!(id_column.data_type(), &DataType::Int64);
        let id = id_column
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("int64 id");
        assert_eq!(id.value(0), 10);
        assert!(id.is_null(1));
    }

    #[test]
    fn lowers_zero_column_values_rows_as_seed_rows() {
        let node = physical_node(
            10,
            plan::plan_node::Kind::Values(plan::ValuesNode {
                rows: vec![plan::ExprList { values: vec![] }],
                columns: vec![],
            }),
            vec![],
            Vec::new(),
        );

        let lowered = lower(&node);
        let ExecNodeKind::Values(values) = lowered.node.kind else {
            panic!("expected Values");
        };
        assert_eq!(values.chunk.len(), 1);
        assert!(values.chunk.chunk_schema().slot_ids().is_empty());
        assert!(lowered.layout.order().is_empty());
        assert!(lowered.output_schema.slot_ids().is_empty());
    }

    #[test]
    fn lowers_empty_zero_column_values_as_single_seed_row() {
        let node = physical_node(
            10,
            plan::plan_node::Kind::Values(plan::ValuesNode {
                rows: vec![],
                columns: vec![],
            }),
            vec![],
            Vec::new(),
        );

        let lowered = lower(&node);
        let ExecNodeKind::Values(values) = lowered.node.kind else {
            panic!("expected Values");
        };
        assert_eq!(values.chunk.len(), 1);
        assert!(values.chunk.chunk_schema().slot_ids().is_empty());
        assert!(lowered.layout.order().is_empty());
        assert!(lowered.output_schema.slot_ids().is_empty());
    }

    #[test]
    fn lowers_generate_series_to_table_function_exec_node() {
        let node = physical_node(
            20,
            plan::plan_node::Kind::GenerateSeries(plan::GenerateSeriesNode {
                start: 1,
                end: 5,
                step: 2,
                column_name: "x".to_string(),
                alias: Some("gs".to_string()),
                output_column_id: 9,
            }),
            Vec::new(),
            Vec::new(),
        );

        let lowered = lower(&node);
        let ExecNodeKind::TableFunction(table_function) = lowered.node.kind else {
            panic!("expected TableFunction");
        };
        assert_eq!(table_function.node_id, 20);
        assert_eq!(table_function.function_name, "generate_series");
        assert_eq!(table_function.param_slots.len(), 3);
        assert!(table_function.outer_slots.is_empty());
        assert_eq!(table_function.fn_result_slots, vec![SlotId::new(9)]);
        assert!(table_function.fn_result_required);
        assert!(!table_function.is_left_join);
        assert_eq!(
            table_function.param_types,
            vec![DataType::Int64, DataType::Int64, DataType::Int64]
        );
        assert_eq!(table_function.ret_types, vec![DataType::Int64]);
        assert_eq!(
            table_function.output_chunk_schema.slot_ids(),
            &[SlotId::new(9)]
        );
        assert_eq!(
            table_function.output_chunk_schema.field(0).unwrap().name(),
            "x"
        );
        assert_eq!(lowered.layout.order(), &[SlotId::new(9)]);
        assert_eq!(lowered.output_schema.slot_ids(), &[SlotId::new(9)]);
        assert!(matches!(
            table_function.output_slot_sources.as_slice(),
            [crate::exec::node::table_function::TableFunctionOutputSlot::Result { index: 0 }]
        ));

        let ExecNodeKind::Values(input) = table_function.input.kind else {
            panic!("expected synthetic Values input");
        };
        assert_eq!(input.chunk.len(), 1);
        assert_eq!(input.chunk.chunk_schema().slot_ids().len(), 3);
        for (slot, expected) in table_function.param_slots.iter().zip([1, 5, 2]) {
            let column = input.chunk.column_by_slot_id(*slot).expect("param column");
            let values = column.as_any().downcast_ref::<Int64Array>().unwrap();
            assert_eq!(values.value(0), expected);
        }
    }

    #[test]
    fn lowers_native_table_function_with_outer_and_result_slots() {
        let array_type = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));
        let child_columns = vec![
            output_column(1, "id", DataType::Int64),
            output_column(2, "arr", array_type.clone()),
        ];
        let child = physical_node(
            10,
            plan::plan_node::Kind::Values(plan::ValuesNode {
                rows: Vec::new(),
                columns: child_columns.clone(),
            }),
            child_columns,
            Vec::new(),
        );
        let result_columns = vec![output_column(3, "unnest", DataType::Int64)];
        let node = physical_node(
            20,
            plan::plan_node::Kind::TableFunction(plan::TableFunctionNode {
                function_name: "unnest".to_string(),
                args: vec![column_ref(2, array_type.clone())],
                output_columns: result_columns.clone(),
                alias: Some("u".to_string()),
                is_left_join: false,
            }),
            result_columns,
            vec![child],
        );

        let lowered = lower(&node);
        assert_eq!(
            lowered.layout.order(),
            &[SlotId::new(1), SlotId::new(2), SlotId::new(3)]
        );
        assert_eq!(
            lowered.output_schema.slot_ids(),
            &[SlotId::new(1), SlotId::new(2), SlotId::new(3)]
        );

        let ExecNodeKind::TableFunction(table_function) = lowered.node.kind else {
            panic!("expected TableFunction");
        };
        assert_eq!(table_function.node_id, 20);
        assert_eq!(table_function.function_name, "unnest");
        assert_eq!(table_function.param_types, vec![array_type]);
        assert_eq!(table_function.ret_types, vec![DataType::Int64]);
        assert_eq!(
            table_function.outer_slots,
            vec![SlotId::new(1), SlotId::new(2)]
        );
        assert_eq!(table_function.fn_result_slots, vec![SlotId::new(3)]);
        assert!(table_function.fn_result_required);
        assert!(!table_function.is_left_join);
        assert_eq!(table_function.param_slots.len(), 1);
        assert_ne!(table_function.param_slots[0], SlotId::new(1));
        assert_ne!(table_function.param_slots[0], SlotId::new(2));
        assert_ne!(table_function.param_slots[0], SlotId::new(3));
        assert_eq!(
            table_function.output_chunk_schema.slot_ids(),
            &[SlotId::new(1), SlotId::new(2), SlotId::new(3)]
        );
        assert_eq!(table_function.output_slot_sources.len(), 3);
        match &table_function.output_slot_sources[0] {
            TableFunctionOutputSlot::Outer { slot } => assert_eq!(*slot, SlotId::new(1)),
            other => panic!("expected first outer slot, got {other:?}"),
        }
        match &table_function.output_slot_sources[1] {
            TableFunctionOutputSlot::Outer { slot } => assert_eq!(*slot, SlotId::new(2)),
            other => panic!("expected second outer slot, got {other:?}"),
        }
        match &table_function.output_slot_sources[2] {
            TableFunctionOutputSlot::Result { index } => assert_eq!(*index, 0),
            other => panic!("expected result slot, got {other:?}"),
        }

        let ExecNodeKind::Project(project) = table_function.input.kind else {
            panic!("expected derived Project input");
        };
        assert!(project.is_subordinate);
        assert_eq!(
            project.expr_slot_ids,
            vec![
                SlotId::new(1),
                SlotId::new(2),
                table_function.param_slots[0],
            ]
        );
        assert_eq!(
            project.output_chunk_schema.slot_ids(),
            &[
                SlotId::new(1),
                SlotId::new(2),
                table_function.param_slots[0],
            ]
        );
        assert!(matches!(project.input.kind, ExecNodeKind::Values(_)));
    }

    #[test]
    fn generate_series_rejects_zero_step_and_children() {
        let zero_step = physical_node(
            20,
            plan::plan_node::Kind::GenerateSeries(plan::GenerateSeriesNode {
                start: 1,
                end: 5,
                step: 0,
                column_name: "x".to_string(),
                alias: None,
                output_column_id: 9,
            }),
            Vec::new(),
            Vec::new(),
        );
        let mut arena = ExprArena::default();
        let err =
            lower_proto_node(&zero_step, &mut arena, &NodeLoweringContext::default()).unwrap_err();
        assert!(err.contains("step must not be zero"), "{err}");

        let with_child = physical_node(
            21,
            plan::plan_node::Kind::GenerateSeries(plan::GenerateSeriesNode {
                start: 1,
                end: 5,
                step: 1,
                column_name: "x".to_string(),
                alias: None,
                output_column_id: 9,
            }),
            Vec::new(),
            vec![one_col_values_node(10)],
        );
        let err =
            lower_proto_node(&with_child, &mut arena, &NodeLoweringContext::default()).unwrap_err();
        assert!(
            err.contains("GenerateSeriesNode expected 0 children"),
            "{err}"
        );
    }

    #[test]
    fn lowers_project_items_to_output_slots_and_schema() {
        let project = physical_node(
            20,
            plan::plan_node::Kind::Project(plan::ProjectNode {
                items: vec![plan::ProjectItem {
                    expr: Some(column_ref(1, DataType::Int64)),
                    output_name: "projected_id".to_string(),
                    output_column_id: 7,
                }],
                output_qualifier: None,
            }),
            Vec::new(),
            vec![one_col_values_node(10)],
        );

        let lowered = lower(&project);
        let ExecNodeKind::Project(project) = lowered.node.kind else {
            panic!("expected Project");
        };
        assert_eq!(project.node_id, 20);
        assert_eq!(project.expr_slot_ids, vec![SlotId::new(1)]);
        assert_eq!(project.output_chunk_schema.slot_ids(), &[SlotId::new(7)]);
        assert_eq!(
            project.output_chunk_schema.field(0).unwrap().name(),
            "projected_id"
        );
        assert_eq!(lowered.layout.order(), &[SlotId::new(7)]);
        assert!(matches!(project.input.kind, ExecNodeKind::Values(_)));
    }

    #[test]
    fn wraps_project_distributed_limit_as_limit_node() {
        let mut project = physical_node(
            20,
            plan::plan_node::Kind::Project(plan::ProjectNode {
                items: vec![plan::ProjectItem {
                    expr: Some(column_ref(1, DataType::Int64)),
                    output_name: "projected_id".to_string(),
                    output_column_id: 7,
                }],
                output_qualifier: None,
            }),
            Vec::new(),
            vec![one_col_values_node(10)],
        );
        project.limit = 1;

        let lowered = lower(&project);
        let ExecNodeKind::Limit(limit) = lowered.node.kind else {
            panic!("expected Limit");
        };
        assert_eq!(limit.node_id, 20);
        assert_eq!(limit.limit, Some(1));
        assert_eq!(limit.offset, 0);
        assert!(matches!(limit.input.kind, ExecNodeKind::Project(_)));
        assert_eq!(lowered.layout.order(), &[SlotId::new(7)]);
        assert_eq!(lowered.output_schema.slot_ids(), &[SlotId::new(7)]);
    }

    #[test]
    fn parent_project_can_reference_child_project_output_column_id() {
        let inner = physical_node(
            20,
            plan::plan_node::Kind::Project(plan::ProjectNode {
                items: vec![plan::ProjectItem {
                    expr: Some(column_ref(1, DataType::Int64)),
                    output_name: "projected_id".to_string(),
                    output_column_id: 7,
                }],
                output_qualifier: None,
            }),
            Vec::new(),
            vec![one_col_values_node(10)],
        );
        let outer = physical_node(
            21,
            plan::plan_node::Kind::Project(plan::ProjectNode {
                items: vec![plan::ProjectItem {
                    expr: Some(column_ref(7, DataType::Int64)),
                    output_name: "outer_id".to_string(),
                    output_column_id: 9,
                }],
                output_qualifier: None,
            }),
            Vec::new(),
            vec![inner],
        );

        let lowered = lower(&outer);
        let ExecNodeKind::Project(project) = lowered.node.kind else {
            panic!("expected Project");
        };
        assert_eq!(project.expr_slot_ids, vec![SlotId::new(7)]);
        assert_eq!(project.output_chunk_schema.slot_ids(), &[SlotId::new(9)]);
        assert_eq!(lowered.layout.order(), &[SlotId::new(9)]);
    }

    #[test]
    fn lowers_project_reused_input_slots_with_output_indices_when_output_ids_change() {
        let project = physical_node(
            20,
            plan::plan_node::Kind::Project(plan::ProjectNode {
                items: vec![
                    plan::ProjectItem {
                        expr: Some(column_ref(1, DataType::Int64)),
                        output_name: "left_out".to_string(),
                        output_column_id: 7,
                    },
                    plan::ProjectItem {
                        expr: Some(column_ref(2, DataType::Int64)),
                        output_name: "right_out".to_string(),
                        output_column_id: 8,
                    },
                ],
                output_qualifier: None,
            }),
            Vec::new(),
            vec![two_col_values_node(10)],
        );

        let lowered = lower(&project);
        let ExecNodeKind::Project(project) = lowered.node.kind else {
            panic!("expected Project");
        };
        assert_eq!(project.expr_slot_ids, vec![SlotId::new(1), SlotId::new(2)]);
        assert_eq!(project.output_indices, Some(vec![0, 1]));
        assert_eq!(
            project.output_chunk_schema.slot_ids(),
            &[SlotId::new(7), SlotId::new(8)]
        );
        assert_eq!(lowered.layout.order(), &[SlotId::new(7), SlotId::new(8)]);
    }

    #[test]
    fn lowers_project_duplicate_output_ids_with_output_indices() {
        let project = physical_node(
            20,
            plan::plan_node::Kind::Project(plan::ProjectNode {
                items: vec![
                    plan::ProjectItem {
                        expr: Some(column_ref(1, DataType::Int64)),
                        output_name: "left_copy".to_string(),
                        output_column_id: 7,
                    },
                    plan::ProjectItem {
                        expr: Some(column_ref(1, DataType::Int64)),
                        output_name: "right_copy".to_string(),
                        output_column_id: 7,
                    },
                ],
                output_qualifier: None,
            }),
            Vec::new(),
            vec![one_col_values_node(10)],
        );

        let lowered = lower(&project);
        let ExecNodeKind::Project(project) = lowered.node.kind else {
            panic!("expected Project");
        };
        assert_eq!(project.exprs.len(), 1);
        assert_eq!(project.expr_slot_ids, vec![SlotId::new(1)]);
        assert_eq!(project.output_indices, Some(vec![0, 0]));
        assert_eq!(
            project.output_chunk_schema.slot_ids(),
            &[SlotId::new(7), SlotId::new(8)]
        );
        assert_eq!(
            project.output_chunk_schema.field(0).unwrap().name(),
            "left_copy"
        );
        assert_eq!(
            project.output_chunk_schema.field(1).unwrap().name(),
            "right_copy"
        );
        assert_eq!(lowered.layout.order(), &[SlotId::new(7), SlotId::new(8)]);
    }

    #[test]
    fn lowers_filter_limit_shape() {
        let filter = physical_node(
            20,
            plan::plan_node::Kind::Filter(plan::FilterNode {
                predicate: Some(bool_literal(true)),
            }),
            Vec::new(),
            vec![one_col_values_node(10)],
        );
        let limit = physical_node(
            30,
            plan::plan_node::Kind::Limit(plan::LimitNode {
                limit: Some(5),
                offset: Some(1),
            }),
            Vec::new(),
            vec![filter],
        );

        let lowered = lower(&limit);
        let ExecNodeKind::Limit(limit) = lowered.node.kind else {
            panic!("expected Limit");
        };
        assert_eq!(limit.node_id, 30);
        assert_eq!(limit.limit, Some(5));
        assert_eq!(limit.offset, 1);
        assert!(matches!(limit.input.kind, ExecNodeKind::Filter(_)));
        assert_eq!(lowered.layout.order(), &[SlotId::new(1)]);
    }

    #[test]
    fn lowers_sort_and_topn_shapes() {
        let mut sort = physical_node(
            20,
            plan::plan_node::Kind::Sort(plan::SortNode {
                items: vec![sort_item(1)],
                analytic_partition_by: Vec::new(),
                output_columns: vec![output_column(1, "id", DataType::Int64)],
                offset: Some(2),
                partition_limit: None,
                topn_type: None,
            }),
            vec![output_column(1, "id", DataType::Int64)],
            vec![one_col_values_node(10)],
        );
        sort.limit = 9;
        let lowered_sort = lower(&sort);
        let ExecNodeKind::Sort(sort) = lowered_sort.node.kind else {
            panic!("expected Sort");
        };
        assert!(!sort.use_top_n);
        assert_eq!(sort.limit, Some(9));
        assert_eq!(sort.offset, 2);
        assert_eq!(sort.order_by.len(), 1);

        let topn = physical_node(
            30,
            plan::plan_node::Kind::Topn(plan::TopNNode {
                items: vec![sort_item(1)],
                limit: Some(3),
                offset: Some(0),
                phase: plan::TopNPhase::TopnPhaseFinal as i32,
                is_split: false,
            }),
            Vec::new(),
            vec![one_col_values_node(10)],
        );
        let lowered_topn = lower(&topn);
        let ExecNodeKind::Sort(topn) = lowered_topn.node.kind else {
            panic!("expected TopN as Sort");
        };
        assert!(topn.use_top_n);
        assert_eq!(topn.limit, Some(3));
        assert_eq!(topn.offset, 0);
        assert_eq!(topn.topn_type, SortTopNType::RowNumber);
    }

    #[test]
    fn lowers_sort_output_reorder_as_subordinate_project() {
        let sort = physical_node(
            20,
            plan::plan_node::Kind::Sort(plan::SortNode {
                items: vec![sort_item(1)],
                analytic_partition_by: Vec::new(),
                output_columns: vec![
                    output_column(2, "b", DataType::Int64),
                    output_column(1, "a", DataType::Int64),
                ],
                offset: None,
                partition_limit: None,
                topn_type: None,
            }),
            vec![
                output_column(2, "b", DataType::Int64),
                output_column(1, "a", DataType::Int64),
            ],
            vec![two_col_values_node(10)],
        );

        let lowered = lower(&sort);
        let ExecNodeKind::Project(project) = lowered.node.kind else {
            panic!("expected reorder project");
        };
        assert!(project.is_subordinate);
        assert_eq!(project.node_id, 20);
        assert_eq!(project.expr_slot_ids, vec![SlotId::new(2), SlotId::new(1)]);
        assert_eq!(
            project.output_chunk_schema.slot_ids(),
            &[SlotId::new(2), SlotId::new(1)]
        );
        assert_eq!(lowered.layout.order(), &[SlotId::new(2), SlotId::new(1)]);
        let ExecNodeKind::Sort(sort) = project.input.kind else {
            panic!("expected Sort below reorder project");
        };
        assert_eq!(sort.order_by.len(), 1);
        assert!(matches!(sort.input.kind, ExecNodeKind::Values(_)));
    }

    #[test]
    fn lowers_partial_split_topn() {
        let topn = physical_node(
            30,
            plan::plan_node::Kind::Topn(plan::TopNNode {
                items: vec![sort_item(1)],
                limit: Some(3),
                offset: Some(0),
                phase: plan::TopNPhase::TopnPhasePartial as i32,
                is_split: true,
            }),
            Vec::new(),
            vec![one_col_values_node(10)],
        );
        let lowered = lower(&topn);
        let ExecNodeKind::Sort(topn) = lowered.node.kind else {
            panic!("expected split TopN as Sort");
        };
        assert!(topn.use_top_n);
        assert_eq!(topn.limit, Some(3));
        assert_eq!(topn.offset, 0);
    }

    #[test]
    fn rejects_final_split_topn_physical_node() {
        let topn = physical_node(
            30,
            plan::plan_node::Kind::Topn(plan::TopNNode {
                items: vec![sort_item(1)],
                limit: Some(3),
                offset: Some(0),
                phase: plan::TopNPhase::TopnPhaseFinal as i32,
                is_split: true,
            }),
            Vec::new(),
            vec![one_col_values_node(10)],
        );
        let mut arena = ExprArena::default();
        let err = lower_proto_node(&topn, &mut arena, &NodeLoweringContext::default()).unwrap_err();
        assert!(err.contains("TopNNode final split"));
        assert!(err.contains("ExchangeReceiver TopNSplit"));
    }

    #[test]
    fn exchange_receiver_requires_sender_count() {
        let exchange = plan::DistributedNode {
            node_id: 40,
            fragment_id: 1,
            tuple_ids: Vec::new(),
            nullable_tuple_ids: Vec::new(),
            limit: -1,
            build_runtime_filters: Vec::new(),
            probe_runtime_filters: Vec::new(),
            children: Vec::new(),
            payload: Some(plan::distributed_node::Payload::Exchange(
                plan::ExchangeReceiver {
                    partition_type: plan::PartitionType::Hash as i32,
                    partition_exprs: Vec::new(),
                    source_fragment_id: 7,
                    output_columns: vec![output_column(1, "id", DataType::Int64)],
                    output_qualifier: None,
                    flavor: Some(plan::ExchangeFlavor {
                        kind: Some(plan::exchange_flavor::Kind::Distribution(true)),
                    }),
                },
            )),
        };

        let mut arena = ExprArena::default();
        let err =
            lower_proto_node(&exchange, &mut arena, &NodeLoweringContext::default()).unwrap_err();
        assert!(err.contains("ExchangeReceiver"));
        assert!(err.contains("sender count"));

        let lowered = lower_proto_node(
            &exchange,
            &mut arena,
            &NodeLoweringContext::default().with_exchange_sender_count(
                ExchangeKey {
                    finst_id_hi: 0,
                    finst_id_lo: 0,
                    node_id: 40,
                },
                2,
            ),
        )
        .expect("plain exchange");
        let ExecNodeKind::ExchangeSource(exchange) = lowered.node.kind else {
            panic!("expected ExchangeSource");
        };
        assert_eq!(exchange.expected_senders, 2);
        assert_eq!(exchange.expected_chunk_schema.slot_ids(), &[SlotId::new(1)]);
    }

    #[test]
    fn lowers_topn_split_exchange_receiver_as_merging_sort() {
        let mut arena = ExprArena::default();
        let lowered = lower_proto_node(
            &topn_exchange_node(41),
            &mut arena,
            &NodeLoweringContext::default().with_exchange_sender_count(
                ExchangeKey {
                    finst_id_hi: 0,
                    finst_id_lo: 0,
                    node_id: 41,
                },
                2,
            ),
        )
        .expect("TopNSplit exchange receiver");

        let ExecNodeKind::Sort(sort) = lowered.node.kind else {
            panic!("expected Sort");
        };
        assert_eq!(sort.node_id, 41);
        assert_eq!(sort.limit, Some(3));
        assert_eq!(sort.offset, 1);
        assert_eq!(sort.order_by.len(), 1);
        let ExecNodeKind::ExchangeSource(exchange) = sort.input.kind else {
            panic!("expected ExchangeSource under Sort");
        };
        assert_eq!(exchange.expected_senders, 2);
        assert_eq!(exchange.expected_chunk_schema.slot_ids(), &[SlotId::new(1)]);
        assert_eq!(lowered.layout.order(), &[SlotId::new(1)]);
    }

    #[test]
    fn lowers_limit_offset_exchange_receiver_as_limit_node() {
        let mut arena = ExprArena::default();
        let lowered = lower_proto_node(
            &limit_offset_exchange_node(42, Some(3), Some(1)),
            &mut arena,
            &NodeLoweringContext::default().with_exchange_sender_count(
                ExchangeKey {
                    finst_id_hi: 0,
                    finst_id_lo: 0,
                    node_id: 42,
                },
                2,
            ),
        )
        .expect("LimitOffset exchange receiver");

        let ExecNodeKind::Limit(limit) = lowered.node.kind else {
            panic!("expected Limit");
        };
        assert_eq!(limit.node_id, 42);
        assert_eq!(limit.limit, Some(3));
        assert_eq!(limit.offset, 1);
        let ExecNodeKind::ExchangeSource(exchange) = limit.input.kind else {
            panic!("expected ExchangeSource under Limit");
        };
        assert_eq!(exchange.expected_senders, 2);
        assert_eq!(exchange.expected_chunk_schema.slot_ids(), &[SlotId::new(1)]);
        assert_eq!(lowered.layout.order(), &[SlotId::new(1)]);
    }

    #[test]
    fn lowers_cte_multicast_exchange_receiver_as_exchange_source() {
        let mut arena = ExprArena::default();
        let lowered = lower_proto_node(
            &cte_multicast_exchange_node(43),
            &mut arena,
            &NodeLoweringContext::default().with_exchange_sender_count(
                ExchangeKey {
                    finst_id_hi: 0,
                    finst_id_lo: 0,
                    node_id: 43,
                },
                2,
            ),
        )
        .expect("CTE multicast exchange receiver");

        let ExecNodeKind::ExchangeSource(exchange) = lowered.node.kind else {
            panic!("expected ExchangeSource");
        };
        assert_eq!(exchange.expected_senders, 2);
        assert_eq!(exchange.expected_chunk_schema.slot_ids(), &[SlotId::new(1)]);
        assert_eq!(lowered.layout.order(), &[SlotId::new(1)]);
    }

    #[test]
    fn lowers_window_node_to_analytic_exec_node() {
        let output_columns = vec![
            output_column(1, "id", DataType::Int64),
            output_column(2, "rn", DataType::Int64),
        ];
        let window = physical_node(
            80,
            plan::plan_node::Kind::Window(plan::WindowNode {
                window_exprs: vec![plan::WindowExpr {
                    name: "row_number".to_string(),
                    args: Vec::new(),
                    distinct: false,
                    partition_by: Vec::new(),
                    order_by: vec![sort_item(1)],
                    window_frame: Some(expr::WindowFrame {
                        frame_type: expr::WindowFrameType::Rows as i32,
                        start: Some(expr::WindowBound {
                            bound: Some(expr::window_bound::Bound::UnboundedPreceding(true)),
                        }),
                        end: Some(expr::WindowBound {
                            bound: Some(expr::window_bound::Bound::CurrentRow(true)),
                        }),
                    }),
                    result_type: Some(type_desc(&DataType::Int64)),
                    output_name: "rn".to_string(),
                    output_column_id: 2,
                    ignore_nulls: false,
                }],
                output_columns: output_columns.clone(),
            }),
            output_columns,
            vec![one_col_values_node(10)],
        );

        let lowered = lower(&window);
        let ExecNodeKind::Analytic(analytic) = lowered.node.kind else {
            panic!("expected Analytic");
        };
        assert_eq!(analytic.node_id, 80);
        assert_eq!(analytic.functions.len(), 1);
        assert!(matches!(
            analytic.functions[0].kind,
            crate::exec::node::analytic::WindowFunctionKind::RowNumber
        ));
        assert_eq!(analytic.order_by_exprs.len(), 1);
        assert_eq!(
            analytic.output_chunk_schema.slot_ids(),
            &[SlotId::new(1), SlotId::new(2)]
        );
        assert_eq!(lowered.layout.order(), &[SlotId::new(1), SlotId::new(2)]);
    }

    #[test]
    fn lowers_window_node_with_multiple_specs_as_analytic_chain() {
        let output_columns = vec![
            output_column(1, "id", DataType::Int64),
            output_column(2, "rn", DataType::Int64),
            output_column(3, "rnk", DataType::Int64),
        ];
        let mut descending_id = sort_item(1);
        descending_id.asc = false;
        let window = physical_node(
            81,
            plan::plan_node::Kind::Window(plan::WindowNode {
                window_exprs: vec![
                    plan::WindowExpr {
                        name: "row_number".to_string(),
                        args: Vec::new(),
                        distinct: false,
                        partition_by: Vec::new(),
                        order_by: vec![sort_item(1)],
                        window_frame: Some(expr::WindowFrame {
                            frame_type: expr::WindowFrameType::Rows as i32,
                            start: Some(expr::WindowBound {
                                bound: Some(expr::window_bound::Bound::UnboundedPreceding(true)),
                            }),
                            end: Some(expr::WindowBound {
                                bound: Some(expr::window_bound::Bound::CurrentRow(true)),
                            }),
                        }),
                        result_type: Some(type_desc(&DataType::Int64)),
                        output_name: "rn".to_string(),
                        output_column_id: 2,
                        ignore_nulls: false,
                    },
                    plan::WindowExpr {
                        name: "rank".to_string(),
                        args: Vec::new(),
                        distinct: false,
                        partition_by: Vec::new(),
                        order_by: vec![descending_id],
                        window_frame: Some(expr::WindowFrame {
                            frame_type: expr::WindowFrameType::Rows as i32,
                            start: Some(expr::WindowBound {
                                bound: Some(expr::window_bound::Bound::UnboundedPreceding(true)),
                            }),
                            end: Some(expr::WindowBound {
                                bound: Some(expr::window_bound::Bound::CurrentRow(true)),
                            }),
                        }),
                        result_type: Some(type_desc(&DataType::Int64)),
                        output_name: "rnk".to_string(),
                        output_column_id: 3,
                        ignore_nulls: false,
                    },
                ],
                output_columns: output_columns.clone(),
            }),
            output_columns,
            vec![one_col_values_node(10)],
        );

        let lowered = lower(&window);
        let ExecNodeKind::Analytic(second) = lowered.node.kind else {
            panic!("expected final Analytic");
        };
        assert_eq!(second.node_id, 83);
        assert_eq!(second.functions.len(), 1);
        assert!(matches!(
            second.functions[0].kind,
            crate::exec::node::analytic::WindowFunctionKind::Rank
        ));
        assert_eq!(
            second.output_chunk_schema.slot_ids(),
            &[SlotId::new(1), SlotId::new(2), SlotId::new(3)]
        );

        let ExecNodeKind::Sort(sort) = second.input.kind else {
            panic!("expected Sort under final Analytic");
        };
        assert_eq!(sort.node_id, 82);
        assert_eq!(sort.order_by.len(), 1);
        assert!(!sort.order_by[0].asc);

        let ExecNodeKind::Analytic(first) = sort.input.kind else {
            panic!("expected first Analytic under Sort");
        };
        assert_eq!(first.node_id, 81);
        assert_eq!(first.functions.len(), 1);
        assert!(matches!(
            first.functions[0].kind,
            crate::exec::node::analytic::WindowFunctionKind::RowNumber
        ));
        assert_eq!(
            first.output_chunk_schema.slot_ids(),
            &[SlotId::new(1), SlotId::new(2)]
        );
        assert_eq!(
            lowered.layout.order(),
            &[SlotId::new(1), SlotId::new(2), SlotId::new(3)]
        );
    }

    #[test]
    fn rejects_scan_without_context_and_union_distinct() {
        let scan = physical_node(
            50,
            plan::plan_node::Kind::Scan(plan::ScanNode::default()),
            Vec::new(),
            Vec::new(),
        );
        let mut arena = ExprArena::default();
        let err = lower_proto_node(&scan, &mut arena, &NodeLoweringContext::default()).unwrap_err();
        assert!(err.contains("Scan"));
        assert!(err.contains("table missing"));

        let union_distinct = physical_node(
            60,
            plan::plan_node::Kind::SetOp(plan::SetOpNode {
                kind: plan::PlanSetOpKind::UnionDistinct as i32,
                output_columns: vec![output_column(1, "id", DataType::Int64)],
                child_output_columns: Vec::new(),
            }),
            Vec::new(),
            vec![one_col_values_node(10), one_col_values_node(11)],
        );
        let err = lower_proto_node(&union_distinct, &mut arena, &NodeLoweringContext::default())
            .unwrap_err();
        assert!(err.contains("UnionDistinct"));
        assert!(err.contains("not implemented"));
    }

    #[test]
    fn lowers_union_all_intersect_except_and_assert_one_row() {
        let output_columns = vec![output_column(1, "id", DataType::Int64)];
        let union_all = physical_node(
            60,
            plan::plan_node::Kind::SetOp(plan::SetOpNode {
                kind: plan::PlanSetOpKind::UnionAll as i32,
                output_columns: output_columns.clone(),
                child_output_columns: Vec::new(),
            }),
            output_columns.clone(),
            vec![one_col_values_node(10), one_col_values_node(11)],
        );
        let lowered = lower(&union_all);
        assert!(matches!(lowered.node.kind, ExecNodeKind::UnionAll(_)));

        for (kind, expected) in [
            (plan::PlanSetOpKind::Intersect, SetOpKind::Intersect),
            (plan::PlanSetOpKind::Except, SetOpKind::Except),
        ] {
            let set_op = physical_node(
                61,
                plan::plan_node::Kind::SetOp(plan::SetOpNode {
                    kind: kind as i32,
                    output_columns: output_columns.clone(),
                    child_output_columns: Vec::new(),
                }),
                output_columns.clone(),
                vec![one_col_values_node(10), one_col_values_node(11)],
            );
            let lowered = lower(&set_op);
            let ExecNodeKind::SetOp(set_op) = lowered.node.kind else {
                panic!("expected SetOp");
            };
            assert_eq!(
                std::mem::discriminant(&set_op.kind),
                std::mem::discriminant(&expected)
            );
            assert_eq!(set_op.output_chunk_schema.slot_ids(), &[SlotId::new(1)]);
        }

        let assert_one_row = physical_node(
            70,
            plan::plan_node::Kind::AssertOneRow(plan::AssertOneRowNode {
                subquery_text: "select id from t".to_string(),
                desired_num_rows: Some(1),
                assertion: plan::RowCountAssertion::Le as i32,
                group_key_column_ids: Vec::new(),
                group_key_labels: Vec::new(),
                keyed_message_prefix: None,
            }),
            Vec::new(),
            vec![one_col_values_node(10)],
        );
        let lowered = lower(&assert_one_row);
        let ExecNodeKind::AssertNumRows(assert) = lowered.node.kind else {
            panic!("expected AssertNumRows");
        };
        match assert.mode {
            AssertNumRowsMode::Global {
                desired_num_rows,
                assertion,
                subquery_string,
            } => {
                assert_eq!(desired_num_rows, Some(1));
                assert!(matches!(assertion, Assertion::Le));
                assert_eq!(subquery_string.as_deref(), Some("select id from t"));
            }
            AssertNumRowsMode::PerKeyAtMostOne { .. } => panic!("expected global assert"),
        }
    }

    #[test]
    fn lowers_keyed_assert_num_rows_from_native_proto() {
        let assert_node = physical_node(
            70,
            plan::plan_node::Kind::AssertOneRow(plan::AssertOneRowNode {
                subquery_text: "DML change-stream matched row uniqueness".to_string(),
                desired_num_rows: Some(1),
                assertion: plan::RowCountAssertion::Le as i32,
                group_key_column_ids: vec![1],
                group_key_labels: vec!["_row_id".to_string()],
                keyed_message_prefix: Some("MOR UPDATE matched target row".to_string()),
            }),
            Vec::new(),
            vec![one_col_values_node(10)],
        );
        let lowered = lower(&assert_node);
        let ExecNodeKind::AssertNumRows(assert) = lowered.node.kind else {
            panic!("expected AssertNumRows");
        };
        match assert.mode {
            AssertNumRowsMode::PerKeyAtMostOne {
                key_slots,
                key_labels,
                message_prefix,
            } => {
                assert_eq!(key_slots, vec![SlotId::new(1)]);
                assert_eq!(key_labels, vec!["_row_id".to_string()]);
                assert_eq!(message_prefix, "MOR UPDATE matched target row");
            }
            AssertNumRowsMode::Global { .. } => panic!("expected keyed assert"),
        }
    }

    #[test]
    fn union_all_retags_child_slots_when_sidecar_is_missing() {
        let output_columns = vec![output_column(1, "id", DataType::Int64)];
        let union_all = physical_node(
            60,
            plan::plan_node::Kind::SetOp(plan::SetOpNode {
                kind: plan::PlanSetOpKind::UnionAll as i32,
                output_columns: output_columns.clone(),
                child_output_columns: Vec::new(),
            }),
            output_columns,
            vec![
                one_col_values_node_with(10, 11, "lhs_id", 10),
                one_col_values_node_with(11, 21, "rhs_id", 20),
            ],
        );
        let lowered = lower(&union_all);
        let ExecNodeKind::UnionAll(union) = lowered.node.kind else {
            panic!("expected UnionAll");
        };
        assert_eq!(union.inputs.len(), 2);
        for input in union.inputs {
            let ExecNodeKind::Project(project) = input.kind else {
                panic!("expected retagging Project");
            };
            assert!(project.is_subordinate);
            assert_eq!(project.expr_slot_ids, vec![SlotId::new(1)]);
            assert_eq!(project.output_chunk_schema.slot_ids(), &[SlotId::new(1)]);
        }
        assert_eq!(lowered.layout.order(), &[SlotId::new(1)]);
        assert_eq!(lowered.output_schema.slot_ids(), &[SlotId::new(1)]);
    }

    #[test]
    fn lowers_hash_aggregate_and_join_shapes() {
        let output_columns = vec![
            output_column(1, "id", DataType::Int64),
            output_column(2, "cnt", DataType::Int64),
        ];
        let aggregate = physical_node(
            20,
            plan::plan_node::Kind::HashAggregate(plan::HashAggregateNode {
                mode: plan::AggMode::Single as i32,
                group_by: vec![column_ref(1, DataType::Int64)],
                aggregates: vec![plan::PlanAggregateCall {
                    name: "count".to_string(),
                    args: Vec::new(),
                    distinct: false,
                    result_type: Some(type_desc(&DataType::Int64)),
                    order_by: Vec::new(),
                    output_column_id: 2,
                }],
                is_merge: vec![false],
                output_layout: Some(plan::AggregateOutputLayout {
                    group_key_columns: vec![output_columns[0].clone()],
                    aggregate_columns: vec![output_columns[1].clone()],
                }),
                output_columns: output_columns.clone(),
            }),
            output_columns,
            vec![one_col_values_node(10)],
        );
        let lowered = lower(&aggregate);
        let ExecNodeKind::Aggregate(aggregate) = lowered.node.kind else {
            panic!("expected Aggregate");
        };
        assert_eq!(aggregate.node_id, 20);
        assert_eq!(aggregate.group_by.len(), 1);
        assert_eq!(aggregate.functions.len(), 1);
        assert!(aggregate.need_finalize);
        assert_eq!(
            aggregate.output_chunk_schema.slot_ids(),
            &[SlotId::new(1), SlotId::new(2)]
        );

        let join = physical_node(
            30,
            plan::plan_node::Kind::HashJoin(plan::HashJoinNode {
                join_type: plan::JoinKind::Inner as i32,
                eq_conditions: vec![plan::HashJoinEqCondition {
                    left: Some(column_ref(1, DataType::Int64)),
                    right: Some(column_ref(2, DataType::Int64)),
                    null_safe: false,
                }],
                other_condition: None,
                distribution: plan::JoinDistribution::Broadcast as i32,
                execution_mode: None,
                build_runtime_filters: Vec::new(),
            }),
            Vec::new(),
            vec![
                one_col_values_node_with(10, 1, "lhs", 10),
                one_col_values_node_with(11, 2, "rhs", 10),
            ],
        );
        let lowered = lower(&join);
        let ExecNodeKind::Join(join) = lowered.node.kind else {
            panic!("expected Join");
        };
        assert_eq!(join.probe_keys.len(), 1);
        assert_eq!(join.build_keys.len(), 1);
        assert_eq!(join.eq_null_safe, vec![false]);
        assert_eq!(
            join.join_scope_chunk_schema.slot_ids(),
            &[SlotId::new(1), SlotId::new(2)]
        );
        assert!(matches!(
            join.join_type,
            crate::exec::node::join::JoinType::Inner
        ));
    }

    #[test]
    fn hash_join_output_schema_uses_plan_output_nullability() {
        let output_columns = vec![
            output_column_with_nullable(1, "lhs", DataType::Int64, false),
            output_column_with_nullable(2, "rhs", DataType::Int64, true),
        ];
        let join = physical_node(
            30,
            plan::plan_node::Kind::HashJoin(plan::HashJoinNode {
                join_type: plan::JoinKind::LeftOuter as i32,
                eq_conditions: vec![plan::HashJoinEqCondition {
                    left: Some(column_ref(1, DataType::Int64)),
                    right: Some(column_ref(2, DataType::Int64)),
                    null_safe: false,
                }],
                other_condition: None,
                distribution: plan::JoinDistribution::Broadcast as i32,
                execution_mode: None,
                build_runtime_filters: Vec::new(),
            }),
            output_columns,
            vec![
                one_col_values_node_with_nullable(10, 1, "lhs", 10, false),
                one_col_values_node_with_nullable(11, 2, "rhs", 10, false),
            ],
        );

        let lowered = lower(&join);
        assert_eq!(
            lowered.output_schema.slot_ids(),
            &[SlotId::new(1), SlotId::new(2)]
        );
        assert!(!lowered.output_schema.slots()[0].nullable());
        assert!(lowered.output_schema.slots()[1].nullable());
        let ExecNodeKind::Join(join) = lowered.node.kind else {
            panic!("expected Join");
        };
        assert!(!join.join_scope_chunk_schema.slots()[0].nullable());
        assert!(join.join_scope_chunk_schema.slots()[1].nullable());
    }

    #[test]
    fn nested_loop_join_output_schema_uses_plan_output_nullability() {
        let output_columns = vec![
            output_column_with_nullable(1, "lhs", DataType::Int64, false),
            output_column_with_nullable(2, "rhs", DataType::Int64, true),
        ];
        let join = physical_node(
            30,
            plan::plan_node::Kind::NestLoopJoin(plan::NestLoopJoinNode {
                join_type: plan::JoinKind::LeftOuter as i32,
                condition: Some(bool_literal(true)),
            }),
            output_columns,
            vec![
                one_col_values_node_with_nullable(10, 1, "lhs", 10, false),
                one_col_values_node_with_nullable(11, 2, "rhs", 10, false),
            ],
        );

        let lowered = lower(&join);
        assert_eq!(
            lowered.output_schema.slot_ids(),
            &[SlotId::new(1), SlotId::new(2)]
        );
        assert!(!lowered.output_schema.slots()[0].nullable());
        assert!(lowered.output_schema.slots()[1].nullable());
        let ExecNodeKind::NestedLoopJoin(join) = lowered.node.kind else {
            panic!("expected NestedLoopJoin");
        };
        assert!(!join.join_scope_chunk_schema.slots()[0].nullable());
        assert!(join.join_scope_chunk_schema.slots()[1].nullable());
    }

    #[test]
    fn nested_loop_right_semi_swaps_inputs_for_left_semi_execution() {
        let right_output = vec![output_column(2, "rhs", DataType::Int64)];
        let join = physical_node(
            30,
            plan::plan_node::Kind::NestLoopJoin(plan::NestLoopJoinNode {
                join_type: plan::JoinKind::RightSemi as i32,
                condition: Some(bool_literal(true)),
            }),
            right_output,
            vec![
                one_col_values_node_with(10, 1, "lhs", 10),
                one_col_values_node_with(11, 2, "rhs", 20),
            ],
        );

        let lowered = lower(&join);
        assert_eq!(lowered.output_schema.slot_ids(), &[SlotId::new(2)]);
        let ExecNodeKind::NestedLoopJoin(join) = lowered.node.kind else {
            panic!("expected NestedLoopJoin");
        };
        assert!(matches!(
            join.join_type,
            crate::exec::node::nljoin::NestedLoopJoinType::LeftSemi
        ));
        assert_eq!(join.left_chunk_schema.slot_ids(), &[SlotId::new(2)]);
        assert_eq!(join.right_chunk_schema.slot_ids(), &[SlotId::new(1)]);
        assert_eq!(
            join.join_scope_chunk_schema.slot_ids(),
            &[SlotId::new(2), SlotId::new(1)]
        );
    }

    #[test]
    fn hash_aggregate_derives_output_columns_from_layout_sidecar() {
        let group_column = output_column(1, "id", DataType::Int64);
        let aggregate = physical_node(
            20,
            plan::plan_node::Kind::HashAggregate(plan::HashAggregateNode {
                mode: plan::AggMode::Single as i32,
                group_by: vec![column_ref(1, DataType::Int64)],
                aggregates: Vec::new(),
                is_merge: Vec::new(),
                output_layout: Some(plan::AggregateOutputLayout {
                    group_key_columns: vec![group_column],
                    aggregate_columns: Vec::new(),
                }),
                output_columns: Vec::new(),
            }),
            Vec::new(),
            vec![one_col_values_node(10)],
        );

        let lowered = lower(&aggregate);
        let ExecNodeKind::Aggregate(aggregate) = lowered.node.kind else {
            panic!("expected Aggregate");
        };
        assert_eq!(aggregate.group_by.len(), 1);
        assert!(aggregate.functions.is_empty());
        assert_eq!(aggregate.output_chunk_schema.slot_ids(), &[SlotId::new(1)]);
        assert_eq!(lowered.layout.order(), &[SlotId::new(1)]);
    }

    #[test]
    fn hash_aggregate_projects_visible_subset_after_full_layout_output() {
        let group_a = output_column(1, "a", DataType::Int64);
        let group_c = output_column(3, "c", DataType::Int64);
        let sum_b = output_column(4, "sum_b", DataType::Int64);
        let visible_output = vec![sum_b.clone()];
        let aggregate = physical_node(
            20,
            plan::plan_node::Kind::HashAggregate(plan::HashAggregateNode {
                mode: plan::AggMode::Single as i32,
                group_by: vec![
                    column_ref(1, DataType::Int64),
                    column_ref(3, DataType::Int64),
                ],
                aggregates: vec![plan::PlanAggregateCall {
                    name: "sum".to_string(),
                    args: vec![column_ref(2, DataType::Int64)],
                    distinct: false,
                    result_type: Some(type_desc(&DataType::Int64)),
                    order_by: Vec::new(),
                    output_column_id: 4,
                }],
                is_merge: vec![false],
                output_layout: Some(plan::AggregateOutputLayout {
                    group_key_columns: vec![group_a, group_c],
                    aggregate_columns: vec![sum_b],
                }),
                output_columns: visible_output.clone(),
            }),
            visible_output,
            vec![three_col_values_node(10)],
        );

        let lowered = lower(&aggregate);
        assert_eq!(lowered.layout.order(), &[SlotId::new(4)]);
        let ExecNodeKind::Project(project) = lowered.node.kind else {
            panic!("expected visible-output projection");
        };
        assert!(project.is_subordinate);
        assert_eq!(project.expr_slot_ids, vec![SlotId::new(4)]);
        assert_eq!(project.output_chunk_schema.slot_ids(), &[SlotId::new(4)]);
        let ExecNodeKind::Aggregate(aggregate) = project.input.kind else {
            panic!("expected Aggregate below visible-output projection");
        };
        assert_eq!(aggregate.group_by.len(), 2);
        assert_eq!(aggregate.functions.len(), 1);
        assert_eq!(
            aggregate.output_chunk_schema.slot_ids(),
            &[SlotId::new(1), SlotId::new(3), SlotId::new(4)]
        );
    }

    #[test]
    fn hash_join_execution_mode_overrides_distribution_and_unknown_defaults_broadcast() {
        let mut join = physical_node(
            30,
            plan::plan_node::Kind::HashJoin(plan::HashJoinNode {
                join_type: plan::JoinKind::Inner as i32,
                eq_conditions: vec![plan::HashJoinEqCondition {
                    left: Some(column_ref(1, DataType::Int64)),
                    right: Some(column_ref(2, DataType::Int64)),
                    null_safe: false,
                }],
                other_condition: None,
                distribution: plan::JoinDistribution::Broadcast as i32,
                execution_mode: Some(plan::JoinExecutionMode::Partitioned as i32),
                build_runtime_filters: Vec::new(),
            }),
            Vec::new(),
            vec![
                one_col_values_node_with(10, 1, "lhs", 10),
                one_col_values_node_with(11, 2, "rhs", 10),
            ],
        );
        let lowered = lower(&join);
        let ExecNodeKind::Join(join_node) = lowered.node.kind else {
            panic!("expected Join");
        };
        assert_eq!(
            join_node.distribution_mode,
            crate::exec::node::join::JoinDistributionMode::Partitioned
        );

        let plan::distributed_node::Payload::Physical(physical) =
            join.payload.as_mut().expect("physical")
        else {
            panic!("expected physical");
        };
        let Some(plan::plan_node::Kind::HashJoin(hash_join)) = physical.kind.as_mut() else {
            panic!("expected hash join");
        };
        hash_join.distribution = plan::JoinDistribution::Unknown as i32;
        hash_join.execution_mode = None;
        let lowered = lower(&join);
        let ExecNodeKind::Join(join_node) = lowered.node.kind else {
            panic!("expected Join");
        };
        assert_eq!(
            join_node.distribution_mode,
            crate::exec::node::join::JoinDistributionMode::Broadcast
        );
    }

    #[test]
    fn hash_aggregate_uses_inferred_intermediate_type() {
        let output_columns = vec![output_column(2, "avg_id", DataType::Float64)];
        let aggregate = physical_node(
            20,
            plan::plan_node::Kind::HashAggregate(plan::HashAggregateNode {
                mode: plan::AggMode::Single as i32,
                group_by: Vec::new(),
                aggregates: vec![plan::PlanAggregateCall {
                    name: "avg".to_string(),
                    args: vec![column_ref(1, DataType::Int64)],
                    distinct: false,
                    result_type: Some(type_desc(&DataType::Float64)),
                    order_by: Vec::new(),
                    output_column_id: 2,
                }],
                is_merge: vec![false],
                output_layout: Some(plan::AggregateOutputLayout {
                    group_key_columns: Vec::new(),
                    aggregate_columns: output_columns.clone(),
                }),
                output_columns: output_columns.clone(),
            }),
            output_columns,
            vec![one_col_values_node(10)],
        );
        let lowered = lower(&aggregate);
        let ExecNodeKind::Aggregate(aggregate) = lowered.node.kind else {
            panic!("expected Aggregate");
        };
        let types = aggregate.functions[0]
            .types
            .as_ref()
            .expect("aggregate type signature");
        assert_eq!(types.intermediate_type, Some(DataType::Utf8));
        assert_eq!(types.output_type, Some(DataType::Float64));
        assert_eq!(types.input_arg_type, Some(DataType::Int64));
    }

    #[test]
    fn hash_aggregate_local_avg_signature_keeps_final_output_type() {
        let output_columns = vec![output_column(2, "avg_id", DataType::Utf8)];
        let aggregate = physical_node(
            20,
            plan::plan_node::Kind::HashAggregate(plan::HashAggregateNode {
                mode: plan::AggMode::Local as i32,
                group_by: Vec::new(),
                aggregates: vec![plan::PlanAggregateCall {
                    name: "avg".to_string(),
                    args: vec![column_ref(1, DataType::Int64)],
                    distinct: false,
                    result_type: Some(type_desc(&DataType::Utf8)),
                    order_by: Vec::new(),
                    output_column_id: 2,
                }],
                is_merge: vec![false],
                output_layout: Some(plan::AggregateOutputLayout {
                    group_key_columns: Vec::new(),
                    aggregate_columns: output_columns.clone(),
                }),
                output_columns: output_columns.clone(),
            }),
            output_columns,
            vec![one_col_values_node(10)],
        );
        let lowered = lower(&aggregate);
        let ExecNodeKind::Aggregate(aggregate) = lowered.node.kind else {
            panic!("expected Aggregate");
        };
        let types = aggregate.functions[0]
            .types
            .as_ref()
            .expect("aggregate type signature");
        assert_eq!(types.intermediate_type, Some(DataType::Utf8));
        assert_eq!(types.output_type, Some(DataType::Float64));
        assert_eq!(types.input_arg_type, Some(DataType::Int64));
        assert_eq!(
            aggregate
                .output_chunk_schema
                .field(0)
                .expect("avg output field")
                .data_type(),
            &DataType::Utf8
        );
    }

    #[test]
    fn hash_aggregate_ordered_inputs_pack_order_by_exprs() {
        let output_columns = vec![output_column(3, "gc", DataType::Utf8)];
        let aggregate = physical_node(
            20,
            plan::plan_node::Kind::HashAggregate(plan::HashAggregateNode {
                mode: plan::AggMode::Local as i32,
                group_by: Vec::new(),
                aggregates: vec![plan::PlanAggregateCall {
                    name: "group_concat".to_string(),
                    args: vec![column_ref(2, DataType::Utf8), string_literal("|")],
                    distinct: true,
                    result_type: Some(type_desc(&DataType::Utf8)),
                    order_by: vec![sort_item(1)],
                    output_column_id: 3,
                }],
                is_merge: vec![false],
                output_layout: Some(plan::AggregateOutputLayout {
                    group_key_columns: Vec::new(),
                    aggregate_columns: output_columns.clone(),
                }),
                output_columns: output_columns.clone(),
            }),
            output_columns,
            vec![values_node(10)],
        );

        let mut arena = ExprArena::default();
        let lowered = lower_proto_node(&aggregate, &mut arena, &NodeLoweringContext::default())
            .expect("lower ordered aggregate");
        let ExecNodeKind::Aggregate(aggregate) = lowered.node.kind else {
            panic!("expected Aggregate");
        };
        assert_eq!(aggregate.functions[0].inputs.len(), 1);
        let input_type = arena
            .data_type(aggregate.functions[0].inputs[0])
            .expect("packed input type");
        let DataType::Struct(fields) = input_type else {
            panic!("expected packed struct input, got {input_type:?}");
        };

        assert_eq!(fields.len(), 3);
        assert_eq!(fields[0].data_type(), &DataType::Utf8);
        assert_eq!(fields[1].data_type(), &DataType::Utf8);
        assert_eq!(fields[2].data_type(), &DataType::Int64);
        assert_eq!(aggregate.functions[0].order.is_asc_order, vec![true]);
        assert_eq!(aggregate.functions[0].order.nulls_first, vec![false]);
        assert!(aggregate.functions[0].order.is_distinct);
    }

    #[test]
    fn hash_aggregate_rejects_count_if_order_by_before_input_selection() {
        let output_columns = vec![output_column(3, "cnt", DataType::Int64)];
        let aggregate = physical_node(
            21,
            plan::plan_node::Kind::HashAggregate(plan::HashAggregateNode {
                mode: plan::AggMode::Local as i32,
                group_by: Vec::new(),
                aggregates: vec![plan::PlanAggregateCall {
                    name: "count_if".to_string(),
                    args: vec![bool_literal(true)],
                    distinct: false,
                    result_type: Some(type_desc(&DataType::Int64)),
                    order_by: vec![sort_item(1)],
                    output_column_id: 3,
                }],
                is_merge: vec![false],
                output_layout: Some(plan::AggregateOutputLayout {
                    group_key_columns: Vec::new(),
                    aggregate_columns: output_columns.clone(),
                }),
                output_columns: output_columns.clone(),
            }),
            output_columns,
            vec![values_node(10)],
        );

        let mut arena = ExprArena::default();
        let err = lower_proto_node(&aggregate, &mut arena, &NodeLoweringContext::default())
            .expect_err("count_if ORDER BY should be rejected before input selection");
        assert!(
            err.contains("count_if does not support ORDER BY"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn hash_join_runtime_filter_skips_unsafe_join_type_and_rejects_mismatched_exprs() {
        let outer_with_rf = physical_node(
            30,
            plan::plan_node::Kind::HashJoin(plan::HashJoinNode {
                join_type: plan::JoinKind::LeftOuter as i32,
                eq_conditions: vec![plan::HashJoinEqCondition {
                    left: Some(column_ref(1, DataType::Int64)),
                    right: Some(column_ref(3, DataType::Int64)),
                    null_safe: false,
                }],
                other_condition: None,
                distribution: plan::JoinDistribution::Broadcast as i32,
                execution_mode: None,
                build_runtime_filters: vec![plan::RuntimeFilterBuildIntent {
                    filter_id: 1,
                    build_expr: Some(column_ref(3, DataType::Int64)),
                    probe_expr: Some(column_ref(1, DataType::Int64)),
                    expr_order: 0,
                    execution_mode: plan::JoinExecutionMode::Broadcast as i32,
                }],
            }),
            Vec::new(),
            vec![
                two_col_values_node(10),
                one_col_values_node_with(11, 3, "rhs", 10),
            ],
        );
        let mut arena = ExprArena::default();
        let lowered = lower_proto_node(&outer_with_rf, &mut arena, &NodeLoweringContext::default())
            .expect("outer join runtime filters should be skipped");
        let ExecNodeKind::Join(join) = lowered.node.kind else {
            panic!("expected Join");
        };
        assert!(join.runtime_filters.is_empty());

        let mismatched_probe = physical_node(
            31,
            plan::plan_node::Kind::HashJoin(plan::HashJoinNode {
                join_type: plan::JoinKind::Inner as i32,
                eq_conditions: vec![plan::HashJoinEqCondition {
                    left: Some(column_ref(1, DataType::Int64)),
                    right: Some(column_ref(3, DataType::Int64)),
                    null_safe: false,
                }],
                other_condition: None,
                distribution: plan::JoinDistribution::Broadcast as i32,
                execution_mode: None,
                build_runtime_filters: vec![plan::RuntimeFilterBuildIntent {
                    filter_id: 2,
                    build_expr: Some(column_ref(3, DataType::Int64)),
                    probe_expr: Some(column_ref(2, DataType::Int64)),
                    expr_order: 0,
                    execution_mode: plan::JoinExecutionMode::Broadcast as i32,
                }],
            }),
            Vec::new(),
            vec![
                two_col_values_node(10),
                one_col_values_node_with(11, 3, "rhs", 10),
            ],
        );
        let mut arena = ExprArena::default();
        let err = lower_proto_node(
            &mismatched_probe,
            &mut arena,
            &NodeLoweringContext::default(),
        )
        .unwrap_err();
        assert!(err.contains("probe_expr does not match"));
    }

    #[test]
    fn lowers_repeat_change_event_and_redistribute_shapes() {
        let repeat = physical_node(
            20,
            plan::plan_node::Kind::Repeat(plan::RepeatNode {
                repeat_column_ref_list: Vec::new(),
                repeat_column_ref_ids: vec![
                    plan::UInt32List { values: vec![1] },
                    plan::UInt32List { values: Vec::new() },
                ],
                grouping_ids: vec![0, 1],
                all_rollup_columns: vec!["id".to_string()],
                all_rollup_column_ids: vec![1],
                grouping_key_aliases: Vec::new(),
                grouping_fn_args: Vec::new(),
                grouping_fn_arg_ids: vec![plan::UInt32List { values: vec![1] }],
                grouping_fn_ids: vec![plan::NamedUInt32 {
                    name: "__grouping_fn_0".to_string(),
                    value: 9,
                }],
                virtual_tuple_id: Some(7),
            }),
            Vec::new(),
            vec![one_col_values_node(10)],
        );
        let lowered = lower(&repeat);
        let ExecNodeKind::Repeat(repeat) = lowered.node.kind else {
            panic!("expected Repeat");
        };
        assert_eq!(repeat.repeat_times, 2);
        assert_eq!(repeat.null_slot_ids, vec![vec![], vec![SlotId::new(1)]]);
        assert_eq!(repeat.grouping_slot_ids, vec![SlotId::new(9)]);
        assert_eq!(repeat.grouping_list, vec![vec![0, 1]]);
        assert_eq!(lowered.layout.order(), &[SlotId::new(1), SlotId::new(9)]);
        assert_eq!(
            lowered.output_schema.slot_ids(),
            &[SlotId::new(1), SlotId::new(9)]
        );

        let change_event = physical_node(
            30,
            plan::plan_node::Kind::ChangeEventExpand(plan::ChangeEventExpandNode {
                events: vec![plan::DistributedChangeEventSpec {
                    predicate: None,
                    branch_kind: plan::ChangeStreamBranchKind::DeleteDv as i32,
                    assignments: vec![plan::DistributedChangeEventOutputExpr {
                        output_column_id: 2,
                        expr: None,
                    }],
                }],
                output_columns: vec![
                    output_column(1, "id", DataType::Int64),
                    output_column(2, "op", DataType::Int8),
                ],
                change_op_column_id: 2,
                data_route_column_id: None,
            }),
            Vec::new(),
            vec![one_col_values_node(10)],
        );
        let lowered = lower(&change_event);
        let ExecNodeKind::ChangeEventExpand(change_event) = lowered.node.kind else {
            panic!("expected ChangeEventExpand");
        };
        assert_eq!(
            change_event.output_slot_ids,
            vec![SlotId::new(1), SlotId::new(2)]
        );
        assert_eq!(change_event.change_op_slot_id, SlotId::new(2));
        assert_eq!(change_event.events.len(), 1);

        let redistribute = physical_node(
            40,
            plan::plan_node::Kind::Redistribute(plan::RedistributeNode {
                mode: Some(plan::RedistributeMode {
                    mode: Some(plan::redistribute_mode::Mode::Gather(true)),
                }),
                partition_exprs: Vec::new(),
                output_columns: vec![output_column(1, "id", DataType::Int64)],
            }),
            Vec::new(),
            vec![one_col_values_node(10)],
        );
        let lowered = lower(&redistribute);
        assert!(matches!(lowered.node.kind, ExecNodeKind::Values(_)));
        assert_eq!(lowered.layout.order(), &[SlotId::new(1)]);
    }

    #[test]
    fn repeat_grouping_function_uses_sql_reverse_bit_order() {
        let repeat = physical_node(
            20,
            plan::plan_node::Kind::Repeat(plan::RepeatNode {
                repeat_column_ref_list: Vec::new(),
                repeat_column_ref_ids: vec![
                    plan::UInt32List { values: vec![1, 2] },
                    plan::UInt32List { values: vec![1] },
                    plan::UInt32List { values: vec![2] },
                    plan::UInt32List { values: Vec::new() },
                ],
                grouping_ids: vec![0, 1, 2, 3],
                all_rollup_columns: vec!["a".to_string(), "b".to_string()],
                all_rollup_column_ids: vec![1, 2],
                grouping_key_aliases: Vec::new(),
                grouping_fn_args: Vec::new(),
                grouping_fn_arg_ids: vec![plan::UInt32List { values: vec![1, 2] }],
                grouping_fn_ids: vec![plan::NamedUInt32 {
                    name: "__grouping_fn_0".to_string(),
                    value: 9,
                }],
                virtual_tuple_id: Some(7),
            }),
            Vec::new(),
            vec![two_col_values_node(10)],
        );
        let lowered = lower(&repeat);
        let ExecNodeKind::Repeat(repeat) = lowered.node.kind else {
            panic!("expected Repeat");
        };
        assert_eq!(repeat.grouping_list, vec![vec![0, 1, 2, 3]]);
    }

    #[test]
    fn change_event_rejects_invalid_data_route_slot() {
        let same_slot = physical_node(
            30,
            plan::plan_node::Kind::ChangeEventExpand(plan::ChangeEventExpandNode {
                events: vec![plan::DistributedChangeEventSpec {
                    predicate: None,
                    branch_kind: plan::ChangeStreamBranchKind::ReuseData as i32,
                    assignments: Vec::new(),
                }],
                output_columns: vec![output_column(2, "op", DataType::Int8)],
                change_op_column_id: 2,
                data_route_column_id: Some(2),
            }),
            Vec::new(),
            vec![one_col_values_node(10)],
        );
        let mut arena = ExprArena::default();
        let err =
            lower_proto_node(&same_slot, &mut arena, &NodeLoweringContext::default()).unwrap_err();
        assert!(err.contains("must differ"));

        let non_integer = physical_node(
            31,
            plan::plan_node::Kind::ChangeEventExpand(plan::ChangeEventExpandNode {
                events: vec![plan::DistributedChangeEventSpec {
                    predicate: None,
                    branch_kind: plan::ChangeStreamBranchKind::ReuseData as i32,
                    assignments: Vec::new(),
                }],
                output_columns: vec![
                    output_column(2, "op", DataType::Int8),
                    output_column(3, "route", DataType::Utf8),
                ],
                change_op_column_id: 2,
                data_route_column_id: Some(3),
            }),
            Vec::new(),
            vec![one_col_values_node(10)],
        );
        let mut arena = ExprArena::default();
        let err = lower_proto_node(&non_integer, &mut arena, &NodeLoweringContext::default())
            .unwrap_err();
        assert!(err.contains("signed integer route type"));
    }
}
