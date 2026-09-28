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

//! Flat, checked local-program graph. Every edge names a prior node, so neither
//! construction nor Drop recursively owns the plan tree.

use std::collections::BTreeSet;
use std::fmt;
use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;

use arrow_schema::{DataType, Field};
use novarocks_connector_contract::{ConnectorRowMutationEffect, WriteTargetOrdinal};
use novarocks_functions::ResolvedAggregateSignature;
use novarocks_types::SlotId;

use crate::{
    BindingRequirement, BindingRequirements, CompileProfile, ImmutableExpressions, ProgramExprId,
    ProgramNodeId, ScanSourceKind, StaticConnectorScan, StaticFieldSchema, StaticFilterConsumer,
    StaticFilterProducer, StaticLayout, StaticSinkProgram, StaticValues,
};

/// Matches the native task-codec preflight, which runs before protobuf decode.
pub const MAX_PROGRAM_NODE_DEPTH: usize = 64;
pub const MAX_PROGRAM_NODES: usize = 65_536;
/// A flat DAG must also bound its expanded execution shape; sharing nodes must
/// not permit exponential pipeline construction.
pub const MAX_PROGRAM_EXPANDED_OCCURRENCES: usize = 65_536;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RowAssertion {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

#[derive(Clone, Debug)]
pub enum AssertRowsMode {
    Global {
        desired_num_rows: Option<usize>,
        assertion: RowAssertion,
        subquery_string: Option<Arc<str>>,
    },
    PerKeyAtMostOne {
        key_slots: Vec<SlotId>,
        key_labels: Vec<Arc<str>>,
        message_prefix: Arc<str>,
    },
}

#[derive(Clone, Debug)]
pub struct ProjectExpressionSlot {
    pub slot_id: SlotId,
    pub field: Field,
    pub field_schema: StaticFieldSchema,
    pub unique_id: Option<i32>,
}

#[derive(Clone, Debug)]
pub struct UnpivotPassthrough {
    pub input_slot_id: SlotId,
    pub output_slot_id: SlotId,
}

#[derive(Clone, Debug)]
pub enum UnpivotConstant {
    Scalar {
        expr_id: ProgramExprId,
        nullable: bool,
    },
    Int32List(Vec<i32>),
    Utf8Map(Vec<(Arc<str>, Arc<str>)>),
}

#[derive(Clone, Debug)]
pub struct UnpivotMapping {
    pub input_value_slot_id: SlotId,
    pub constants: Vec<UnpivotConstant>,
}

#[derive(Clone, Debug)]
pub struct ChangeEventOutputExpr {
    pub output_slot_id: SlotId,
    pub expr: Option<ProgramExprId>,
}

#[derive(Clone, Debug)]
pub struct ChangeEventSpec {
    pub predicate: Option<ProgramExprId>,
    pub effect: ConnectorRowMutationEffect,
    pub assignments: Vec<ChangeEventOutputExpr>,
}

#[derive(Clone, Debug)]
pub struct FilterConsumerAtExpr {
    pub expr_id: ProgramExprId,
    pub consumer: StaticFilterConsumer,
}

#[derive(Clone, Debug)]
pub struct FilterProducerAtExpr {
    pub expr_id: ProgramExprId,
    pub key_ordinal: usize,
    pub producer: StaticFilterProducer,
}

#[derive(Clone, Debug)]
pub struct StaticAggregateTypeSignature {
    pub intermediate_type: Option<DataType>,
    pub output_type: Option<DataType>,
    pub input_arg_type: Option<DataType>,
}

#[derive(Clone, Debug, Default)]
pub struct StaticAggregateOrder {
    pub is_asc_order: Vec<bool>,
    pub nulls_first: Vec<bool>,
    pub is_distinct: bool,
    pub group_concat_max_len: Option<i64>,
}

#[derive(Clone, Debug)]
pub struct StaticAggregateCall {
    pub name: Arc<str>,
    pub inputs: Vec<ProgramExprId>,
    pub input_is_intermediate: bool,
    pub types: Option<StaticAggregateTypeSignature>,
    pub order: StaticAggregateOrder,
    pub resolved: ResolvedAggregateSignature,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StreamingPreaggregationMode {
    Auto,
    ForceStreaming,
    ForcePreaggregation,
    LimitedMem,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum JoinType {
    Inner,
    LeftOuter,
    RightOuter,
    FullOuter,
    LeftSemi,
    RightSemi,
    LeftAnti,
    RightAnti,
    NullAwareLeftAnti,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum JoinDistributionMode {
    Broadcast,
    Partitioned,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NestedLoopJoinType {
    Inner,
    Cross,
    LeftOuter,
    RightOuter,
    FullOuter,
    LeftSemi,
    LeftAnti,
    NullAwareLeftAnti,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WindowType {
    Rows,
    Range,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WindowBoundary {
    CurrentRow,
    Preceding(i64),
    Following(i64),
}

#[derive(Clone, Debug)]
pub struct WindowFrame {
    pub start: Option<WindowBoundary>,
    pub end: Option<WindowBoundary>,
    pub window_type: WindowType,
}

#[derive(Clone, Debug)]
pub enum WindowFunctionKind {
    RowNumber,
    Rank,
    DenseRank,
    CumeDist,
    PercentRank,
    Ntile,
    FirstValue {
        ignore_nulls: bool,
    },
    FirstValueRewrite {
        ignore_nulls: bool,
    },
    LastValue {
        ignore_nulls: bool,
    },
    Lead {
        ignore_nulls: bool,
    },
    Lag {
        ignore_nulls: bool,
    },
    SessionNumber,
    Count,
    Sum,
    Avg,
    Min,
    Max,
    BitmapUnion,
    BitmapUnionCount,
    MaxBy,
    MinBy,
    VarianceSamp,
    StddevSamp,
    BoolOr,
    CovarPop,
    CovarSamp,
    Corr,
    ArrayAgg {
        is_distinct: bool,
        is_asc_order: Vec<bool>,
        nulls_first: Vec<bool>,
    },
    ApproxTopK,
}

#[derive(Clone, Debug)]
pub struct StaticWindowFunction {
    pub kind: WindowFunctionKind,
    pub args: Vec<ProgramExprId>,
    pub return_type: DataType,
    pub aggregate_binding: Option<(Arc<str>, ResolvedAggregateSignature)>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AnalyticOutputColumn {
    InputSlotId(SlotId),
    Window(usize),
}

#[derive(Clone, Debug)]
pub struct WriterPartialAggregateCall {
    pub input_slot_id: SlotId,
    pub function_name: Arc<str>,
    pub resolved: ResolvedAggregateSignature,
    pub intermediate_slot_id: SlotId,
}

#[derive(Clone, Debug)]
pub struct WriterFinalAggregateCall {
    pub function_name: Arc<str>,
    pub resolved: ResolvedAggregateSignature,
    pub intermediate_input_slot_id: SlotId,
    pub final_output_slot_id: SlotId,
}

#[derive(Clone, Debug)]
pub struct WriterGroupedUnpivotMapping {
    pub grouping_key: u32,
    pub input_value_slot_id: SlotId,
    pub constants: Vec<UnpivotConstant>,
}

#[derive(Clone, Debug)]
pub struct WriterGroupedUnpivotPlan {
    pub grouping_input_slot_id: SlotId,
    pub grouping_output_slot_id: SlotId,
    pub passthrough_output_slot_id: SlotId,
    pub value_output_slot_id: SlotId,
    pub literal_output_slot_ids: Vec<SlotId>,
    pub mappings: Vec<WriterGroupedUnpivotMapping>,
    pub max_output_rows: usize,
    pub max_output_bytes: usize,
}

#[derive(Clone, Debug)]
pub struct WriterFinalAggregatePlan {
    pub calls: Vec<WriterFinalAggregateCall>,
    pub unpivot: Option<WriterGroupedUnpivotPlan>,
}

#[derive(Clone, Debug)]
pub struct AggregateTopNFilter {
    pub group_key_expr: ProgramExprId,
    pub group_key_ordinal: usize,
    pub limit: NonZeroU32,
    pub producer: StaticFilterProducer,
}

#[derive(Clone, Debug)]
pub struct StaticWriterProjection {
    /// Writer projection owns a separate expression arena in the current plan.
    pub arena: Arc<ImmutableExpressions>,
    pub expressions: Vec<ProgramExprId>,
    pub layout: StaticLayout,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SortTopNType {
    RowNumber,
    Rank,
    DenseRank,
}

#[derive(Clone, Copy, Debug)]
pub struct SortExpression {
    pub expr: ProgramExprId,
    pub asc: bool,
    pub nulls_first: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SetOpKind {
    Intersect,
    Except,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TableFunctionOutputSlot {
    Outer { slot: SlotId },
    Result { index: usize },
}

/// Every execution node has a closed static representation. Task-owned inputs,
/// exchange receivers, writer handles, and filter sessions are requirements.
#[derive(Clone, Debug)]
pub enum ProgramNodeKind {
    AssertNumRows {
        input: ProgramNodeId,
        mode: AssertRowsMode,
    },
    Values {
        values: StaticValues,
    },
    Project {
        input: ProgramNodeId,
        is_subordinate: bool,
        exprs: Vec<ProgramExprId>,
        expr_slot_ids: Vec<SlotId>,
        expr_slot_schemas: Option<Vec<ProjectExpressionSlot>>,
        output_indices: Option<Vec<usize>>,
    },
    Unpivot {
        input: ProgramNodeId,
        passthrough_columns: Vec<UnpivotPassthrough>,
        value_output_slot_id: SlotId,
        literal_output_slot_ids: Vec<SlotId>,
        value_mappings: Vec<UnpivotMapping>,
        max_output_rows: usize,
        max_output_bytes: usize,
    },
    Filter {
        input: ProgramNodeId,
        predicate: ProgramExprId,
    },
    Repeat {
        input: ProgramNodeId,
        null_slot_ids: Vec<Vec<SlotId>>,
        grouping_slot_ids: Vec<SlotId>,
        grouping_list: Vec<Vec<i64>>,
        repeat_times: usize,
    },
    ChangeEventExpand {
        input: ProgramNodeId,
        events: Vec<ChangeEventSpec>,
        output_slot_ids: Vec<SlotId>,
        effect_slot_id: SlotId,
    },
    UnionAll {
        inputs: Vec<ProgramNodeId>,
    },
    Limit {
        input: ProgramNodeId,
        limit: Option<usize>,
        offset: usize,
    },
    Scan {
        source: StaticConnectorScan,
        runtime_filters: Vec<FilterConsumerAtExpr>,
        conjunct_predicate: Option<ProgramExprId>,
        connector_io_tasks_per_scan_operator: Option<i32>,
        limit: Option<usize>,
        accept_empty_scan_ranges: bool,
    },
    ExchangeSource {
        timeout: Duration,
        runtime_filters: Vec<FilterConsumerAtExpr>,
        hash_partition_exprs: Vec<ProgramExprId>,
    },
    Aggregate {
        input: ProgramNodeId,
        group_by: Vec<ProgramExprId>,
        functions: Vec<StaticAggregateCall>,
        need_finalize: bool,
        input_is_intermediate: bool,
        topn_filters: Vec<AggregateTopNFilter>,
        streaming_preaggregation_mode: Option<StreamingPreaggregationMode>,
    },
    Join {
        left: ProgramNodeId,
        right: ProgramNodeId,
        join_type: JoinType,
        distribution_mode: JoinDistributionMode,
        left_layout: StaticLayout,
        right_layout: StaticLayout,
        join_scope_layout: StaticLayout,
        probe_keys: Vec<ProgramExprId>,
        build_keys: Vec<ProgramExprId>,
        eq_null_safe: Vec<bool>,
        residual_predicate: Option<ProgramExprId>,
        runtime_filters: Vec<FilterProducerAtExpr>,
    },
    NestedLoopJoin {
        left: ProgramNodeId,
        right: ProgramNodeId,
        join_type: NestedLoopJoinType,
        join_conjunct: Option<ProgramExprId>,
        left_layout: StaticLayout,
        right_layout: StaticLayout,
        join_scope_layout: StaticLayout,
    },
    Analytic {
        input: ProgramNodeId,
        partition_exprs: Vec<ProgramExprId>,
        order_by_exprs: Vec<ProgramExprId>,
        functions: Vec<StaticWindowFunction>,
        window: Option<WindowFrame>,
        output_columns: Vec<AnalyticOutputColumn>,
    },
    RuntimeFilterConsumer {
        input: ProgramNodeId,
        bindings: Vec<FilterConsumerAtExpr>,
    },
    TableWriter {
        input: ProgramNodeId,
        target: WriteTargetOrdinal,
        expected_layout: StaticLayout,
        projection: StaticWriterProjection,
        writer_multiplex_layout: StaticLayout,
        partial_aggregates: Vec<WriterPartialAggregateCall>,
    },
    TableFinish {
        inputs: Vec<ProgramNodeId>,
        expected_targets: Vec<WriteTargetOrdinal>,
        writer_multiplex_layout: StaticLayout,
        root_result_layout: StaticLayout,
        final_aggregates: WriterFinalAggregatePlan,
    },
    Sort {
        input: ProgramNodeId,
        use_top_n: bool,
        order_by: Vec<SortExpression>,
        limit: Option<usize>,
        offset: usize,
        topn_type: SortTopNType,
        max_buffered_rows: Option<usize>,
        max_buffered_bytes: Option<usize>,
        partition_exprs: Vec<SortExpression>,
        partition_limit: Option<usize>,
    },
    TableFunction {
        input: ProgramNodeId,
        function_name: Arc<str>,
        param_slots: Vec<SlotId>,
        outer_slots: Vec<SlotId>,
        fn_result_slots: Vec<SlotId>,
        fn_result_required: bool,
        is_left_join: bool,
        param_types: Vec<DataType>,
        ret_types: Vec<DataType>,
        output_slot_sources: Vec<TableFunctionOutputSlot>,
    },
    SetOp {
        kind: SetOpKind,
        inputs: Vec<ProgramNodeId>,
    },
}

impl ProgramNodeKind {
    fn children(&self) -> Vec<ProgramNodeId> {
        match self {
            Self::Values { .. } | Self::Scan { .. } | Self::ExchangeSource { .. } => Vec::new(),
            Self::AssertNumRows { input, .. }
            | Self::Project { input, .. }
            | Self::Unpivot { input, .. }
            | Self::Filter { input, .. }
            | Self::Repeat { input, .. }
            | Self::ChangeEventExpand { input, .. }
            | Self::Limit { input, .. }
            | Self::Sort { input, .. }
            | Self::TableFunction { input, .. }
            | Self::Aggregate { input, .. }
            | Self::Analytic { input, .. }
            | Self::RuntimeFilterConsumer { input, .. }
            | Self::TableWriter { input, .. } => vec![*input],
            Self::UnionAll { inputs }
            | Self::SetOp { inputs, .. }
            | Self::TableFinish { inputs, .. } => inputs.clone(),
            Self::Join { left, right, .. } | Self::NestedLoopJoin { left, right, .. } => {
                vec![*left, *right]
            }
        }
    }

    fn expression_ids(&self) -> Vec<ProgramExprId> {
        match self {
            Self::Values { .. }
            | Self::AssertNumRows { .. }
            | Self::Repeat { .. }
            | Self::UnionAll { .. }
            | Self::Limit { .. }
            | Self::TableFunction { .. }
            | Self::SetOp { .. }
            | Self::TableFinish { .. } => Vec::new(),
            Self::Project { exprs, .. } => exprs.clone(),
            Self::Unpivot { value_mappings, .. } => value_mappings
                .iter()
                .flat_map(|mapping| &mapping.constants)
                .filter_map(|value| match value {
                    UnpivotConstant::Scalar { expr_id, .. } => Some(*expr_id),
                    _ => None,
                })
                .collect(),
            Self::Filter { predicate, .. } => vec![*predicate],
            Self::Scan {
                runtime_filters,
                conjunct_predicate,
                ..
            } => conjunct_predicate
                .iter()
                .copied()
                .chain(runtime_filters.iter().map(|binding| binding.expr_id))
                .collect(),
            Self::ExchangeSource {
                runtime_filters,
                hash_partition_exprs,
                ..
            } => hash_partition_exprs
                .iter()
                .copied()
                .chain(runtime_filters.iter().map(|binding| binding.expr_id))
                .collect(),
            Self::Aggregate {
                group_by,
                functions,
                topn_filters,
                ..
            } => group_by
                .iter()
                .copied()
                .chain(
                    functions
                        .iter()
                        .flat_map(|function| function.inputs.iter().copied()),
                )
                .chain(topn_filters.iter().map(|filter| filter.group_key_expr))
                .collect(),
            Self::Join {
                probe_keys,
                build_keys,
                residual_predicate,
                runtime_filters,
                ..
            } => probe_keys
                .iter()
                .chain(build_keys)
                .copied()
                .chain(residual_predicate.iter().copied())
                .chain(runtime_filters.iter().map(|filter| filter.expr_id))
                .collect(),
            Self::NestedLoopJoin { join_conjunct, .. } => join_conjunct.iter().copied().collect(),
            Self::Analytic {
                partition_exprs,
                order_by_exprs,
                functions,
                ..
            } => partition_exprs
                .iter()
                .chain(order_by_exprs)
                .copied()
                .chain(
                    functions
                        .iter()
                        .flat_map(|function| function.args.iter().copied()),
                )
                .collect(),
            Self::RuntimeFilterConsumer { bindings, .. } => {
                bindings.iter().map(|binding| binding.expr_id).collect()
            }
            Self::TableWriter { .. } => Vec::new(),
            Self::ChangeEventExpand { events, .. } => events
                .iter()
                .flat_map(|event| {
                    event
                        .predicate
                        .into_iter()
                        .chain(event.assignments.iter().filter_map(|output| output.expr))
                })
                .collect(),
            Self::Sort {
                order_by,
                partition_exprs,
                ..
            } => order_by
                .iter()
                .chain(partition_exprs)
                .map(|sort| sort.expr)
                .collect(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct ProgramNode {
    native_node_id: i32,
    kind: ProgramNodeKind,
    output_layout: StaticLayout,
}

impl ProgramNode {
    pub fn new(native_node_id: i32, kind: ProgramNodeKind, output_layout: StaticLayout) -> Self {
        Self {
            native_node_id,
            kind,
            output_layout,
        }
    }

    pub const fn native_node_id(&self) -> i32 {
        self.native_node_id
    }

    pub const fn kind(&self) -> &ProgramNodeKind {
        &self.kind
    }

    pub const fn output_layout(&self) -> &StaticLayout {
        &self.output_layout
    }
}

#[derive(Clone, Debug)]
pub struct LocalProgram {
    nodes: Arc<[ProgramNode]>,
    root: ProgramNodeId,
    expressions: Arc<ImmutableExpressions>,
    profile: CompileProfile,
    requirements: BindingRequirements,
    sink: Option<StaticSinkProgram>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LocalProgramError {
    Empty,
    TooManyNodes,
    InvalidRoot,
    InvalidChild,
    UnreachableNode,
    TooDeep,
    ExpandedLimit,
    DuplicateNativeNode,
    InvalidExpression,
    InvalidNodeShape,
    LayoutMismatch,
    InvalidRequirement,
    InvalidSink,
}

impl fmt::Display for LocalProgramError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "invalid local program: {self:?}")
    }
}

impl std::error::Error for LocalProgramError {}

impl LocalProgram {
    pub fn try_new(
        nodes: Vec<ProgramNode>,
        root: ProgramNodeId,
        expressions: Arc<ImmutableExpressions>,
        profile: CompileProfile,
        requirements: BindingRequirements,
    ) -> Result<Self, LocalProgramError> {
        Self::try_new_with_sink(nodes, root, expressions, profile, requirements, None)
    }

    pub fn try_new_with_sink(
        nodes: Vec<ProgramNode>,
        root: ProgramNodeId,
        expressions: Arc<ImmutableExpressions>,
        profile: CompileProfile,
        requirements: BindingRequirements,
        sink: Option<StaticSinkProgram>,
    ) -> Result<Self, LocalProgramError> {
        if nodes.is_empty() {
            return Err(LocalProgramError::Empty);
        }
        if nodes.len() > MAX_PROGRAM_NODES {
            return Err(LocalProgramError::TooManyNodes);
        }
        if root.index() >= nodes.len() {
            return Err(LocalProgramError::InvalidRoot);
        }
        let mut depths = Vec::with_capacity(nodes.len());
        let mut expansions = Vec::with_capacity(nodes.len());
        let mut native_ids = BTreeSet::new();
        for (index, node) in nodes.iter().enumerate() {
            // Native lowering may insert a wrapper with the same wire node ID
            // as its input. Only nodes addressed by per-Task sidecars require
            // unique native IDs; ProgramNodeId identifies every arena entry.
            if matches!(
                node.kind,
                ProgramNodeKind::Scan { .. }
                    | ProgramNodeKind::ExchangeSource { .. }
                    | ProgramNodeKind::TableWriter { .. }
                    | ProgramNodeKind::TableFinish { .. }
            ) && !native_ids.insert(node.native_node_id)
            {
                return Err(LocalProgramError::DuplicateNativeNode);
            }
            let mut depth = 1_usize;
            let mut expanded = 1_usize;
            for child in node.kind.children() {
                if child.index() >= index {
                    return Err(LocalProgramError::InvalidChild);
                }
                depth = depth.max(depths[child.index()] + 1);
                expanded = expanded
                    .checked_add(expansions[child.index()])
                    .ok_or(LocalProgramError::ExpandedLimit)?;
            }
            if depth > MAX_PROGRAM_NODE_DEPTH {
                return Err(LocalProgramError::TooDeep);
            }
            if expanded > MAX_PROGRAM_EXPANDED_OCCURRENCES {
                return Err(LocalProgramError::ExpandedLimit);
            }
            depths.push(depth);
            expansions.push(expanded);
            if node
                .kind
                .expression_ids()
                .iter()
                .any(|expr| expressions.node(*expr).is_none())
            {
                return Err(LocalProgramError::InvalidExpression);
            }
            validate_shape(node)?;
            validate_relationships(node, &nodes)?;
        }
        let mut reachable = vec![false; nodes.len()];
        let mut pending = vec![root];
        while let Some(node) = pending.pop() {
            if std::mem::replace(&mut reachable[node.index()], true) {
                continue;
            }
            pending.extend(nodes[node.index()].kind.children());
        }
        if reachable.iter().any(|seen| !seen) {
            return Err(LocalProgramError::UnreachableNode);
        }
        if nodes[root.index()]
            .output_layout
            .identity()
            .map_err(|_| LocalProgramError::LayoutMismatch)?
            != profile.layout()
        {
            return Err(LocalProgramError::LayoutMismatch);
        }
        let mut required_scans = BTreeSet::new();
        let mut required_exchanges = BTreeSet::new();
        let mut required_writers = BTreeSet::new();
        let mut required_finishes = BTreeSet::new();
        let mut required_filters = BTreeSet::new();
        for requirement in requirements.entries() {
            match requirement {
                BindingRequirement::ResultSink { layout }
                    if layout
                        .identity()
                        .map_err(|_| LocalProgramError::LayoutMismatch)?
                        == profile.layout() => {}
                BindingRequirement::Scan { node, kind, layout } => {
                    let Some(ProgramNode {
                        kind: ProgramNodeKind::Scan { source, .. },
                        output_layout,
                        ..
                    }) = nodes.get(node.index())
                    else {
                        return Err(LocalProgramError::InvalidRequirement);
                    };
                    let ScanSourceKind::TypedConnector { relation } = kind else {
                        return Err(LocalProgramError::InvalidRequirement);
                    };
                    if relation != source.recipe().draft().relation().table().header()
                        || layout
                            .identity()
                            .map_err(|_| LocalProgramError::LayoutMismatch)?
                            != output_layout
                                .identity()
                                .map_err(|_| LocalProgramError::LayoutMismatch)?
                    {
                        return Err(LocalProgramError::InvalidRequirement);
                    }
                    required_scans.insert(node.index());
                }
                BindingRequirement::RuntimeFilter { binding_id } => {
                    required_filters.insert(*binding_id);
                }
                BindingRequirement::ExchangeInput { node, layout } => {
                    check_node_layout(&nodes, *node, layout, |kind| {
                        matches!(kind, ProgramNodeKind::ExchangeSource { .. })
                    })?;
                    required_exchanges.insert(node.index());
                }
                BindingRequirement::TableWriter { node, layout } => {
                    check_node_layout(&nodes, *node, layout, |kind| {
                        matches!(kind, ProgramNodeKind::TableWriter { .. })
                    })?;
                    required_writers.insert(node.index());
                }
                BindingRequirement::TableFinish { node, layout } => {
                    check_node_layout(&nodes, *node, layout, |kind| {
                        matches!(kind, ProgramNodeKind::TableFinish { .. })
                    })?;
                    required_finishes.insert(node.index());
                }
                BindingRequirement::ExchangeOutput { branch: _, layout } => {
                    layout
                        .identity()
                        .map_err(|_| LocalProgramError::LayoutMismatch)?;
                }
                _ => return Err(LocalProgramError::InvalidRequirement),
            }
        }
        for (index, node) in nodes.iter().enumerate() {
            let required = match &node.kind {
                ProgramNodeKind::Scan { .. } => required_scans.contains(&index),
                ProgramNodeKind::ExchangeSource { .. } => required_exchanges.contains(&index),
                ProgramNodeKind::TableWriter { .. } => required_writers.contains(&index),
                ProgramNodeKind::TableFinish { .. } => required_finishes.contains(&index),
                _ => true,
            };
            if !required
                || node_filter_ids(&node.kind)?
                    .iter()
                    .any(|id| !required_filters.contains(id))
            {
                return Err(LocalProgramError::InvalidRequirement);
            }
        }
        if let Some(sink) = &sink {
            sink.validate()
                .map_err(|_| LocalProgramError::InvalidSink)?;
            let result_count = requirements
                .entries()
                .iter()
                .filter(|requirement| matches!(requirement, BindingRequirement::ResultSink { .. }))
                .count();
            let outputs = requirements
                .entries()
                .iter()
                .filter_map(|requirement| match requirement {
                    BindingRequirement::ExchangeOutput { branch, .. } => Some(*branch),
                    _ => None,
                })
                .collect::<BTreeSet<_>>();
            match sink {
                StaticSinkProgram::Result if result_count != 1 || !outputs.is_empty() => {
                    return Err(LocalProgramError::InvalidSink);
                }
                StaticSinkProgram::Noop if result_count != 0 || !outputs.is_empty() => {
                    return Err(LocalProgramError::InvalidSink);
                }
                StaticSinkProgram::DataStream { .. }
                | StaticSinkProgram::MultiCastDataStream { .. }
                | StaticSinkProgram::SplitDataStream { .. }
                    if result_count != 0 || outputs != (0..sink.branches().len()).collect() =>
                {
                    return Err(LocalProgramError::InvalidSink);
                }
                _ => {}
            }
            for requirement in requirements.entries() {
                if let BindingRequirement::ExchangeOutput { branch, layout } = requirement {
                    let branch = sink
                        .branches()
                        .get(*branch)
                        .ok_or(LocalProgramError::InvalidSink)?;
                    let projected = nodes[root.index()]
                        .output_layout
                        .project_by_slots(branch.output_columns())
                        .map_err(|_| LocalProgramError::InvalidSink)?;
                    if projected
                        .identity()
                        .map_err(|_| LocalProgramError::InvalidSink)?
                        != layout
                            .identity()
                            .map_err(|_| LocalProgramError::InvalidSink)?
                    {
                        return Err(LocalProgramError::InvalidSink);
                    }
                }
            }
        }
        Ok(Self {
            nodes: Arc::from(nodes),
            root,
            expressions,
            profile,
            requirements,
            sink,
        })
    }

    pub fn nodes(&self) -> &[ProgramNode] {
        &self.nodes
    }

    pub const fn root(&self) -> ProgramNodeId {
        self.root
    }

    pub const fn expressions(&self) -> &Arc<ImmutableExpressions> {
        &self.expressions
    }

    pub const fn profile(&self) -> CompileProfile {
        self.profile
    }

    pub const fn requirements(&self) -> &BindingRequirements {
        &self.requirements
    }

    pub const fn sink(&self) -> Option<&StaticSinkProgram> {
        self.sink.as_ref()
    }
}

fn check_node_layout(
    nodes: &[ProgramNode],
    node: ProgramNodeId,
    layout: &StaticLayout,
    expected_kind: impl FnOnce(&ProgramNodeKind) -> bool,
) -> Result<(), LocalProgramError> {
    let Some(found) = nodes.get(node.index()) else {
        return Err(LocalProgramError::InvalidRequirement);
    };
    if !expected_kind(&found.kind)
        || layout
            .identity()
            .map_err(|_| LocalProgramError::LayoutMismatch)?
            != found
                .output_layout
                .identity()
                .map_err(|_| LocalProgramError::LayoutMismatch)?
    {
        return Err(LocalProgramError::InvalidRequirement);
    }
    Ok(())
}

fn node_filter_ids(kind: &ProgramNodeKind) -> Result<Vec<i32>, LocalProgramError> {
    let consumer_ids = |bindings: &[FilterConsumerAtExpr]| {
        bindings
            .iter()
            .map(|binding| {
                i32::try_from(binding.consumer.binding_id())
                    .map_err(|_| LocalProgramError::InvalidRequirement)
            })
            .collect::<Result<Vec<_>, _>>()
    };
    match kind {
        ProgramNodeKind::Scan {
            runtime_filters, ..
        }
        | ProgramNodeKind::ExchangeSource {
            runtime_filters, ..
        } => consumer_ids(runtime_filters),
        ProgramNodeKind::RuntimeFilterConsumer { bindings, .. } => consumer_ids(bindings),
        ProgramNodeKind::Aggregate { topn_filters, .. } => topn_filters
            .iter()
            .map(|filter| {
                i32::try_from(filter.producer.binding_id())
                    .map_err(|_| LocalProgramError::InvalidRequirement)
            })
            .collect(),
        ProgramNodeKind::Join {
            runtime_filters, ..
        } => runtime_filters
            .iter()
            .map(|filter| {
                i32::try_from(filter.producer.binding_id())
                    .map_err(|_| LocalProgramError::InvalidRequirement)
            })
            .collect(),
        _ => Ok(Vec::new()),
    }
}

fn validate_relationships(
    node: &ProgramNode,
    nodes: &[ProgramNode],
) -> Result<(), LocalProgramError> {
    let layout_matches = |child: ProgramNodeId, expected: &StaticLayout| {
        Ok::<bool, LocalProgramError>(
            nodes[child.index()]
                .output_layout
                .identity()
                .map_err(|_| LocalProgramError::LayoutMismatch)?
                == expected
                    .identity()
                    .map_err(|_| LocalProgramError::LayoutMismatch)?,
        )
    };
    match &node.kind {
        ProgramNodeKind::Join {
            left,
            right,
            left_layout,
            right_layout,
            ..
        }
        | ProgramNodeKind::NestedLoopJoin {
            left,
            right,
            left_layout,
            right_layout,
            ..
        } => {
            if !layout_matches(*left, left_layout)? || !layout_matches(*right, right_layout)? {
                return Err(LocalProgramError::LayoutMismatch);
            }
        }
        ProgramNodeKind::RuntimeFilterConsumer { input, .. } => {
            if !layout_matches(*input, &node.output_layout)? {
                return Err(LocalProgramError::LayoutMismatch);
            }
        }
        ProgramNodeKind::TableFinish {
            inputs,
            writer_multiplex_layout,
            root_result_layout,
            ..
        } => {
            for input in inputs {
                if !layout_matches(*input, writer_multiplex_layout)? {
                    return Err(LocalProgramError::LayoutMismatch);
                }
            }
            if node
                .output_layout
                .identity()
                .map_err(|_| LocalProgramError::LayoutMismatch)?
                != root_result_layout
                    .identity()
                    .map_err(|_| LocalProgramError::LayoutMismatch)?
            {
                return Err(LocalProgramError::LayoutMismatch);
            }
        }
        _ => {}
    }
    Ok(())
}

fn validate_shape(node: &ProgramNode) -> Result<(), LocalProgramError> {
    match &node.kind {
        ProgramNodeKind::Values { values } => {
            if values
                .layout()
                .identity()
                .map_err(|_| LocalProgramError::LayoutMismatch)?
                != node
                    .output_layout
                    .identity()
                    .map_err(|_| LocalProgramError::LayoutMismatch)?
            {
                return Err(LocalProgramError::LayoutMismatch);
            }
        }
        ProgramNodeKind::Project {
            exprs,
            expr_slot_ids,
            expr_slot_schemas,
            output_indices,
            ..
        } => {
            if exprs.len() != expr_slot_ids.len()
                || expr_slot_schemas
                    .as_ref()
                    .is_some_and(|slots| slots.len() != exprs.len())
                || output_indices
                    .as_ref()
                    .is_some_and(|indices| indices.iter().any(|index| *index >= exprs.len()))
            {
                return Err(LocalProgramError::InvalidNodeShape);
            }
        }
        ProgramNodeKind::Scan {
            connector_io_tasks_per_scan_operator: Some(0),
            ..
        } => return Err(LocalProgramError::InvalidNodeShape),
        ProgramNodeKind::Unpivot {
            max_output_rows,
            max_output_bytes,
            ..
        } if *max_output_rows == 0 || *max_output_bytes == 0 => {
            return Err(LocalProgramError::InvalidNodeShape);
        }
        ProgramNodeKind::Repeat {
            null_slot_ids,
            grouping_slot_ids,
            grouping_list,
            repeat_times,
            ..
        } if *repeat_times == 0
            || null_slot_ids.len() != *repeat_times
            || grouping_list.len() != grouping_slot_ids.len()
            || grouping_list
                .iter()
                .any(|values| values.len() != *repeat_times) =>
        {
            return Err(LocalProgramError::InvalidNodeShape);
        }
        ProgramNodeKind::SetOp { inputs, .. } if inputs.len() < 2 => {
            return Err(LocalProgramError::InvalidNodeShape);
        }
        ProgramNodeKind::Sort {
            order_by,
            partition_exprs,
            ..
        } if order_by.is_empty() && partition_exprs.is_empty() => {
            return Err(LocalProgramError::InvalidNodeShape);
        }
        ProgramNodeKind::TableFunction { function_name, .. } if function_name.is_empty() => {
            return Err(LocalProgramError::InvalidNodeShape);
        }
        ProgramNodeKind::Aggregate {
            group_by,
            functions,
            topn_filters,
            ..
        } => {
            if functions.iter().any(|function| {
                function.name.is_empty()
                    || function.order.is_asc_order.len() != function.order.nulls_first.len()
            }) || topn_filters.iter().any(|filter| filter.group_key_ordinal >= group_by.len())
            {
                return Err(LocalProgramError::InvalidNodeShape);
            }
        }
        ProgramNodeKind::Join {
            probe_keys,
            build_keys,
            eq_null_safe,
            runtime_filters,
            ..
        } => {
            if probe_keys.len() != build_keys.len()
                || probe_keys.len() != eq_null_safe.len()
                || runtime_filters
                    .iter()
                    .any(|filter| filter.key_ordinal >= build_keys.len())
            {
                return Err(LocalProgramError::InvalidNodeShape);
            }
        }
        ProgramNodeKind::Analytic {
            functions,
            output_columns,
            ..
        } => {
            if output_columns.len() != node.output_layout.slots().len()
                || output_columns.iter().any(|column| {
                    matches!(column, AnalyticOutputColumn::Window(index) if *index >= functions.len())
                })
            {
                return Err(LocalProgramError::InvalidNodeShape);
            }
        }
        ProgramNodeKind::TableWriter {
            projection,
            expected_layout,
            ..
        } => {
            if projection.expressions.is_empty()
                || projection.expressions.len() != projection.layout.slots().len()
                || projection
                    .expressions
                    .iter()
                    .any(|expr| projection.arena.node(*expr).is_none())
                || projection
                    .layout
                    .identity()
                    .map_err(|_| LocalProgramError::LayoutMismatch)?
                    != expected_layout
                        .identity()
                        .map_err(|_| LocalProgramError::LayoutMismatch)?
            {
                return Err(LocalProgramError::InvalidNodeShape);
            }
        }
        ProgramNodeKind::TableFinish {
            inputs,
            expected_targets,
            final_aggregates,
            ..
        } => {
            let unique_targets: BTreeSet<_> = expected_targets.iter().copied().collect();
            if inputs.is_empty()
                || expected_targets.is_empty()
                || unique_targets.len() != expected_targets.len()
                || final_aggregates.unpivot.as_ref().is_some_and(|unpivot| {
                    unpivot.max_output_rows == 0 || unpivot.max_output_bytes == 0
                })
            {
                return Err(LocalProgramError::InvalidNodeShape);
            }
        }
        _ => {}
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::num::{NonZeroU32, NonZeroUsize};

    use arrow_array::{Int64Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};

    use super::*;
    use crate::{KernelAbiVersion, LayoutIdentity, StaticExprKind, StaticExprNode};

    fn values() -> (StaticValues, StaticLayout) {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![1]))],
        )
        .unwrap();
        let layout = StaticLayout::try_new(schema, Arc::from([SlotId::new(1)])).unwrap();
        (
            StaticValues::try_new(batch, layout.clone()).unwrap(),
            layout,
        )
    }

    fn profile(layout: &StaticLayout) -> CompileProfile {
        CompileProfile::new(
            NonZeroUsize::new(1).unwrap(),
            None,
            layout.identity().unwrap(),
            KernelAbiVersion::new(NonZeroU32::new(1).unwrap()),
        )
    }

    #[test]
    fn partition_only_sort_requires_a_valid_partition_expression() {
        let make_program = |partition_exprs: Vec<SortExpression>| {
            let partition_limit = (!partition_exprs.is_empty()).then_some(2);
            let (values, layout) = values();
            let expressions = Arc::new(
                ImmutableExpressions::try_new(
                    vec![StaticExprNode::new(
                        StaticExprKind::SlotId(SlotId::new(1)),
                        DataType::Int64,
                        None,
                    )],
                    false,
                    HashMap::new(),
                    None,
                )
                .unwrap(),
            );
            LocalProgram::try_new(
                vec![
                    ProgramNode::new(1, ProgramNodeKind::Values { values }, layout.clone()),
                    ProgramNode::new(
                        2,
                        ProgramNodeKind::Sort {
                            input: ProgramNodeId::new(0),
                            use_top_n: partition_limit.is_some(),
                            order_by: vec![],
                            limit: None,
                            offset: 0,
                            topn_type: SortTopNType::RowNumber,
                            max_buffered_rows: None,
                            max_buffered_bytes: None,
                            partition_exprs,
                            partition_limit,
                        },
                        layout.clone(),
                    ),
                ],
                ProgramNodeId::new(1),
                expressions,
                profile(&layout),
                BindingRequirements::try_new(vec![BindingRequirement::ResultSink { layout }])
                    .unwrap(),
            )
        };
        let partition = |index| SortExpression {
            expr: ProgramExprId::new(index),
            asc: true,
            nulls_first: true,
        };
        assert!(make_program(vec![partition(0)]).is_ok());
        assert!(matches!(
            make_program(vec![]),
            Err(LocalProgramError::InvalidNodeShape)
        ));
        assert!(matches!(
            make_program(vec![partition(1)]),
            Err(LocalProgramError::InvalidExpression)
        ));
    }

    #[test]
    fn shares_flat_values_and_expressions_across_instances() {
        let (values, layout) = values();
        let exprs = Arc::new(
            ImmutableExpressions::try_new(
                vec![StaticExprNode::new(
                    StaticExprKind::SlotId(SlotId::new(1)),
                    DataType::Int64,
                    None,
                )],
                false,
                HashMap::new(),
                None,
            )
            .unwrap(),
        );
        let nodes = vec![
            ProgramNode::new(1, ProgramNodeKind::Values { values }, layout.clone()),
            ProgramNode::new(
                2,
                ProgramNodeKind::Filter {
                    input: ProgramNodeId::new(0),
                    predicate: ProgramExprId::new(0),
                },
                layout.clone(),
            ),
        ];
        let program = LocalProgram::try_new(
            nodes,
            ProgramNodeId::new(1),
            exprs,
            profile(&layout),
            BindingRequirements::try_new(vec![BindingRequirement::ResultSink {
                layout: layout.clone(),
            }])
            .unwrap(),
        )
        .unwrap();
        let shared = program.clone();
        assert!(Arc::ptr_eq(&program.nodes, &shared.nodes));
        assert!(Arc::ptr_eq(program.expressions(), shared.expressions()));
    }

    #[test]
    fn rejects_cycle_before_any_runtime_binding() {
        let (values, layout) = values();
        let exprs =
            Arc::new(ImmutableExpressions::try_new(vec![], false, HashMap::new(), None).unwrap());
        let nodes = vec![
            ProgramNode::new(1, ProgramNodeKind::Values { values }, layout.clone()),
            ProgramNode::new(
                2,
                ProgramNodeKind::Limit {
                    input: ProgramNodeId::new(1),
                    limit: Some(1),
                    offset: 0,
                },
                layout.clone(),
            ),
        ];
        assert!(matches!(
            LocalProgram::try_new(
                nodes,
                ProgramNodeId::new(1),
                exprs,
                profile(&layout),
                BindingRequirements::try_new(vec![]).unwrap(),
            ),
            Err(LocalProgramError::InvalidChild)
        ));
    }

    #[test]
    fn validates_profile_layout_identity() {
        let (values, layout) = values();
        let program = LocalProgram::try_new(
            vec![ProgramNode::new(
                1,
                ProgramNodeKind::Values { values },
                layout.clone(),
            )],
            ProgramNodeId::new(0),
            Arc::new(ImmutableExpressions::try_new(vec![], false, HashMap::new(), None).unwrap()),
            CompileProfile::new(
                NonZeroUsize::new(1).unwrap(),
                None,
                LayoutIdentity::from_sha256([9; 32]),
                KernelAbiVersion::new(NonZeroU32::new(1).unwrap()),
            ),
            BindingRequirements::try_new(vec![]).unwrap(),
        );
        assert!(matches!(program, Err(LocalProgramError::LayoutMismatch)));
    }

    #[test]
    fn rejects_exponential_dag_expansion_before_instantiation() {
        let (values, layout) = values();
        let mut nodes = vec![ProgramNode::new(
            1,
            ProgramNodeKind::Values { values },
            layout.clone(),
        )];
        for index in 1..=17 {
            nodes.push(ProgramNode::new(
                i32::try_from(index + 1).unwrap(),
                ProgramNodeKind::UnionAll {
                    inputs: vec![ProgramNodeId::new(index - 1); 2],
                },
                layout.clone(),
            ));
        }
        let program = LocalProgram::try_new(
            nodes,
            ProgramNodeId::new(17),
            Arc::new(ImmutableExpressions::try_new(vec![], false, HashMap::new(), None).unwrap()),
            profile(&layout),
            BindingRequirements::try_new(vec![]).unwrap(),
        );
        assert!(matches!(program, Err(LocalProgramError::ExpandedLimit)));
    }

    #[test]
    fn rejects_unreachable_nodes_and_unbound_exchange_source() {
        let (values, layout) = values();
        let empty_expressions = || {
            Arc::new(ImmutableExpressions::try_new(vec![], false, HashMap::new(), None).unwrap())
        };
        let unreachable = LocalProgram::try_new(
            vec![
                ProgramNode::new(1, ProgramNodeKind::Values { values }, layout.clone()),
                ProgramNode::new(
                    2,
                    ProgramNodeKind::ExchangeSource {
                        timeout: Duration::from_secs(1),
                        runtime_filters: vec![],
                        hash_partition_exprs: vec![],
                    },
                    layout.clone(),
                ),
            ],
            ProgramNodeId::new(0),
            empty_expressions(),
            profile(&layout),
            BindingRequirements::try_new(vec![]).unwrap(),
        );
        assert!(matches!(
            unreachable,
            Err(LocalProgramError::UnreachableNode)
        ));

        let unbound = LocalProgram::try_new(
            vec![ProgramNode::new(
                2,
                ProgramNodeKind::ExchangeSource {
                    timeout: Duration::from_secs(1),
                    runtime_filters: vec![],
                    hash_partition_exprs: vec![],
                },
                layout.clone(),
            )],
            ProgramNodeId::new(0),
            empty_expressions(),
            profile(&layout),
            BindingRequirements::try_new(vec![]).unwrap(),
        );
        assert!(matches!(
            unbound,
            Err(LocalProgramError::InvalidRequirement)
        ));
    }

    #[test]
    fn flat_program_node_counts_scale_n_2n_4n() {
        for width in [8usize, 16, 32] {
            let (values, layout) = values();
            let mut nodes = vec![ProgramNode::new(
                1,
                ProgramNodeKind::Values { values },
                layout.clone(),
            )];
            for ordinal in 0..width {
                nodes.push(ProgramNode::new(
                    i32::try_from(ordinal + 2).unwrap(),
                    ProgramNodeKind::Limit {
                        input: ProgramNodeId::new(ordinal),
                        limit: Some(1),
                        offset: 0,
                    },
                    layout.clone(),
                ));
            }
            let program = LocalProgram::try_new(
                nodes,
                ProgramNodeId::new(width),
                Arc::new(
                    ImmutableExpressions::try_new(vec![], false, HashMap::new(), None).unwrap(),
                ),
                profile(&layout),
                BindingRequirements::try_new(vec![]).unwrap(),
            )
            .unwrap();
            assert_eq!(program.nodes().len(), width + 1);
            assert_eq!(program.expressions().nodes().len(), 0);
            let shared = program.clone();
            assert!(Arc::ptr_eq(&program.nodes, &shared.nodes));
        }
    }

    #[test]
    fn static_sink_rejects_missing_branch_binding() {
        let (values, layout) = values();
        let sink = StaticSinkProgram::try_data_stream(
            crate::StaticStreamBranch::try_new(
                7,
                novarocks_execution_contract::DataStreamPartitionType::Random,
                vec![],
                vec![SlotId::new(1)],
                None,
            )
            .unwrap(),
            Arc::new(ImmutableExpressions::try_new(vec![], false, HashMap::new(), None).unwrap()),
        )
        .unwrap();
        let result = LocalProgram::try_new_with_sink(
            vec![ProgramNode::new(
                1,
                ProgramNodeKind::Values { values },
                layout.clone(),
            )],
            ProgramNodeId::new(0),
            Arc::new(ImmutableExpressions::try_new(vec![], false, HashMap::new(), None).unwrap()),
            profile(&layout),
            BindingRequirements::try_new(vec![]).unwrap(),
            Some(sink),
        );
        assert!(matches!(result, Err(LocalProgramError::InvalidSink)));
    }
}
