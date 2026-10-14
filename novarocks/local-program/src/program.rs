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

use arrow_schema::{DataType, FieldRef};
use novarocks_connector_contract::{
    ConnectorEnvelopeHeader, ConnectorReadProgramRecipe, ConnectorRowMutationEffect,
    WriteTargetOrdinal,
};
use novarocks_functions::ResolvedAggregateSignature;
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, PureCompileControl,
};
use novarocks_types::SlotId;

use crate::{
    BindingRequirement, BindingRequirements, CompileProfile, DiagnosticSourceNodeId,
    ImmutableExpressions, LayoutCompileError, ProgramExprId, ProgramNodeId, ScanSourceKind,
    SinkCompileError, StaticConnectorScan, StaticFieldSchema, StaticFilterConsumer,
    StaticFilterProducer, StaticLayout, StaticSinkProgram, StaticValues,
};

/// Matches the native task-codec preflight, which runs before protobuf decode.
pub const MAX_PROGRAM_NODE_DEPTH: usize = 64;
pub const MAX_PROGRAM_NODES: usize = 65_536;
/// A flat DAG must also bound its expanded execution shape; sharing nodes must
/// not permit exponential pipeline construction.
pub const MAX_PROGRAM_EXPANDED_OCCURRENCES: usize =
    novarocks_type_contract::MAX_CONTROL_USE_REFERENCES;

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
    pub field: FieldRef,
    pub metadata_origins: Option<novarocks_types::arrow_metadata_owner::FieldMetadataOrigins>,
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

/// An admitted consumer of exactly one key of its owning hash join.
#[derive(Clone, Debug)]
pub struct FilterConsumerAtJoinKey {
    pub expr_id: ProgramExprId,
    pub key_ordinal: usize,
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
    pub state_interpretation: Option<novarocks_type_contract::AggregateStateInterpretation>,
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
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
    FirstValue,
    FirstValueRewrite,
    LastValue,
    Lead,
    Lag,
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
    /// Compiled programs only: the call is exactly the prepared window kernel
    /// attached at its `ProgramCallSite::Window`. The compiler never derives a
    /// legacy kind from a function name, and the legacy runtime refuses it.
    Prepared,
}
impl WindowFunctionKind {
    /// Whether IGNORE NULLS can be a fact of this call kind.
    pub const fn admits_ignore_nulls(&self) -> bool {
        matches!(
            self,
            Self::FirstValue
                | Self::FirstValueRewrite
                | Self::LastValue
                | Self::Lead
                | Self::Lag
                | Self::Prepared
        )
    }
}

/// One window call with its own frame and NULL treatment: calls of one
/// Analytic node share only partition and order keys. A legacy call copies
/// its node's frame, where `None` is the legacy absence of a frame. A
/// `Prepared` call always carries the explicit frame its compiler froze.
#[derive(Clone, Debug)]
pub struct StaticWindowFunction {
    pub kind: WindowFunctionKind,
    pub args: Vec<ProgramExprId>,
    pub return_type: DataType,
    pub aggregate_binding: Option<(Arc<str>, ResolvedAggregateSignature)>,
    pub frame: Option<WindowFrame>,
    pub ignore_nulls: bool,
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
    /// Original integer-series computation over the subordinate bounds Values.
    GenerateSeries {
        input: ProgramNodeId,
        parameter_slots: Arc<[SlotId]>,
    },
    Project {
        input: ProgramNodeId,
        is_subordinate: bool,
        /// Check runtime identity input domains at the final result boundary.
        validate_final_result_input: bool,
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
        predicates: Box<[ProgramExprId]>,
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
        source: ProgramScanSource,
        runtime_filters: Vec<FilterConsumerAtExpr>,
        residuals: Vec<ProgramExprId>,
        limit: Option<usize>,
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
    /// A complete-key budget over intermediate state contributions.
    GroupedTopN {
        input: ProgramNodeId,
        group_by: Vec<ProgramExprId>,
        order_by: Vec<SortExpression>,
        rank_group_ordinals: Vec<usize>,
        limit: usize,
        comparator: novarocks_type_contract::OrderedComparisonAlgorithm,
        functions: Vec<StaticAggregateCall>,
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
        runtime_filter_consumers: Vec<FilterConsumerAtJoinKey>,
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
    fn try_for_each_child<E>(
        &self,
        mut visit: impl FnMut(ProgramNodeId) -> Result<(), E>,
    ) -> Result<(), E> {
        match self {
            Self::Values { .. } | Self::Scan { .. } | Self::ExchangeSource { .. } => {}
            Self::AssertNumRows { input, .. }
            | Self::Project { input, .. }
            | Self::Unpivot { input, .. }
            | Self::Filter { input, .. }
            | Self::Repeat { input, .. }
            | Self::ChangeEventExpand { input, .. }
            | Self::Limit { input, .. }
            | Self::Sort { input, .. }
            | Self::TableFunction { input, .. }
            | Self::GenerateSeries { input, .. }
            | Self::Aggregate { input, .. }
            | Self::GroupedTopN { input, .. }
            | Self::Analytic { input, .. }
            | Self::RuntimeFilterConsumer { input, .. }
            | Self::TableWriter { input, .. } => visit(*input)?,
            Self::UnionAll { inputs }
            | Self::SetOp { inputs, .. }
            | Self::TableFinish { inputs, .. } => {
                for child in inputs {
                    visit(*child)?;
                }
            }
            Self::Join { left, right, .. } | Self::NestedLoopJoin { left, right, .. } => {
                visit(*left)?;
                visit(*right)?;
            }
        }
        Ok(())
    }
    // None observes a real container/constant/assignment inspection that emits
    // no expression edge; wide empty containers still perform bounded work.
    fn try_for_each_expression<E>(
        &self,
        mut visit: impl FnMut(Option<ProgramExprId>) -> Result<(), E>,
    ) -> Result<(), E> {
        match self {
            Self::Values { .. }
            | Self::AssertNumRows { .. }
            | Self::Repeat { .. }
            | Self::UnionAll { .. }
            | Self::Limit { .. }
            | Self::TableFunction { .. }
            | Self::GenerateSeries { .. }
            | Self::SetOp { .. }
            | Self::TableWriter { .. } => {}
            Self::Project { exprs, .. } => {
                for id in exprs {
                    visit(Some(*id))?;
                }
            }
            Self::Unpivot { value_mappings, .. } => {
                for mapping in value_mappings {
                    visit(None)?;
                    for value in &mapping.constants {
                        visit(match value {
                            UnpivotConstant::Scalar { expr_id, .. } => Some(*expr_id),
                            _ => None,
                        })?;
                    }
                }
            }
            Self::Filter { predicates, .. } => {
                for predicate in predicates {
                    visit(Some(*predicate))?;
                }
            }
            Self::Scan {
                runtime_filters,
                residuals,
                ..
            } => {
                for id in residuals {
                    visit(Some(*id))?;
                }
                for binding in runtime_filters {
                    visit(Some(binding.expr_id))?;
                }
            }
            Self::ExchangeSource {
                runtime_filters,
                hash_partition_exprs,
                ..
            } => {
                for id in hash_partition_exprs {
                    visit(Some(*id))?;
                }
                for binding in runtime_filters {
                    visit(Some(binding.expr_id))?;
                }
            }
            Self::Aggregate {
                group_by,
                functions,
                topn_filters,
                ..
            } => {
                for id in group_by {
                    visit(Some(*id))?;
                }
                for function in functions {
                    visit(None)?;
                    for id in &function.inputs {
                        visit(Some(*id))?;
                    }
                }
                for filter in topn_filters {
                    visit(Some(filter.group_key_expr))?;
                }
            }
            Self::GroupedTopN {
                group_by,
                order_by,
                functions,
                ..
            } => {
                for id in group_by {
                    visit(Some(*id))?;
                }
                for key in order_by {
                    visit(Some(key.expr))?;
                }
                for function in functions {
                    visit(None)?;
                    for id in &function.inputs {
                        visit(Some(*id))?;
                    }
                }
            }
            Self::Join {
                probe_keys,
                build_keys,
                residual_predicate,
                runtime_filters,
                runtime_filter_consumers,
                ..
            } => {
                for id in probe_keys
                    .iter()
                    .chain(build_keys)
                    .chain(residual_predicate)
                {
                    visit(Some(*id))?;
                }
                for filter in runtime_filter_consumers {
                    visit(Some(filter.expr_id))?;
                }
                for filter in runtime_filters {
                    visit(Some(filter.expr_id))?;
                }
            }
            Self::NestedLoopJoin { join_conjunct, .. } => {
                if let Some(id) = join_conjunct {
                    visit(Some(*id))?;
                }
            }
            Self::Analytic {
                partition_exprs,
                order_by_exprs,
                functions,
                ..
            } => {
                for id in partition_exprs.iter().chain(order_by_exprs) {
                    visit(Some(*id))?;
                }
                for function in functions {
                    visit(None)?;
                    for id in &function.args {
                        visit(Some(*id))?;
                    }
                }
            }
            Self::RuntimeFilterConsumer { bindings, .. } => {
                for binding in bindings {
                    visit(Some(binding.expr_id))?;
                }
            }
            Self::TableFinish {
                final_aggregates, ..
            } => {
                if let Some(unpivot) = &final_aggregates.unpivot {
                    for mapping in &unpivot.mappings {
                        visit(None)?;
                        for value in &mapping.constants {
                            visit(match value {
                                UnpivotConstant::Scalar { expr_id, .. } => Some(*expr_id),
                                _ => None,
                            })?;
                        }
                    }
                }
            }
            Self::ChangeEventExpand { events, .. } => {
                for event in events {
                    visit(None)?;
                    if let Some(id) = event.predicate {
                        visit(Some(id))?;
                    }
                    for assignment in &event.assignments {
                        visit(assignment.expr)?;
                    }
                }
            }
            Self::Sort {
                order_by,
                partition_exprs,
                ..
            } => {
                for sort in order_by.iter().chain(partition_exprs) {
                    visit(Some(sort.expr))?;
                }
            }
        }
        Ok(())
    }
}

/// Migration input only. A final compiled program accepts the complete-input
/// seal; the payload-only legacy branch is retired with the Execution bridge.
#[derive(Clone, Debug)]
pub enum ProgramScanSource {
    Legacy(Arc<StaticConnectorScan>),
    Compiled(Arc<ConnectorReadProgramRecipe>),
}
impl From<StaticConnectorScan> for ProgramScanSource {
    fn from(source: StaticConnectorScan) -> Self {
        Self::Legacy(Arc::new(source))
    }
}
impl From<ConnectorReadProgramRecipe> for ProgramScanSource {
    fn from(source: ConnectorReadProgramRecipe) -> Self {
        Self::Compiled(Arc::new(source))
    }
}
impl ProgramScanSource {
    pub fn compiled(&self) -> Option<&ConnectorReadProgramRecipe> {
        match self {
            Self::Compiled(recipe) => Some(recipe),
            Self::Legacy(_) => None,
        }
    }
    pub fn relation_header(&self) -> &ConnectorEnvelopeHeader {
        match self {
            Self::Legacy(scan) => scan.recipe().draft().relation().table().header(),
            Self::Compiled(recipe) => recipe.frozen().scan().recipe().relation().table().header(),
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum ProgramNodeIdentity {
    LegacyNative(i32),
    Local(ProgramNodeId),
}

#[derive(Clone, Debug)]
pub struct ProgramNode {
    identity: ProgramNodeIdentity,
    physical_sources: Arc<[DiagnosticSourceNodeId]>,
    kind: ProgramNodeKind,
    output_layout: StaticLayout,
}

impl ProgramNode {
    /// Existing construction bridge only; it is not a compiled node identity.
    pub fn new(native_node_id: i32, kind: ProgramNodeKind, output_layout: StaticLayout) -> Self {
        Self {
            identity: ProgramNodeIdentity::LegacyNative(native_node_id),
            physical_sources: Arc::from([]),
            kind,
            output_layout,
        }
    }
    /// Exact local binding identity and diagnostic physical sources occupy
    /// separate namespaces. A sparse u32 source is never cast to a legacy ID.
    pub fn new_local(
        id: ProgramNodeId,
        sources: Vec<DiagnosticSourceNodeId>,
        kind: ProgramNodeKind,
        output_layout: StaticLayout,
    ) -> Self {
        Self {
            identity: ProgramNodeIdentity::Local(id),
            physical_sources: Arc::from(sources),
            kind,
            output_layout,
        }
    }
    pub const fn local_id(&self) -> Option<ProgramNodeId> {
        match self.identity {
            ProgramNodeIdentity::Local(id) => Some(id),
            ProgramNodeIdentity::LegacyNative(_) => None,
        }
    }
    pub const fn legacy_native_node_id(&self) -> Option<i32> {
        match self.identity {
            ProgramNodeIdentity::LegacyNative(id) => Some(id),
            ProgramNodeIdentity::Local(_) => None,
        }
    }
    pub fn physical_sources(&self) -> &[DiagnosticSourceNodeId] {
        &self.physical_sources
    }

    pub const fn kind(&self) -> &ProgramNodeKind {
        &self.kind
    }

    pub const fn output_layout(&self) -> &StaticLayout {
        &self.output_layout
    }
}

#[derive(Clone, Debug)]
pub struct LocalProgramGraph {
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
    UnsupportedKernelAbi,
}

impl fmt::Display for LocalProgramError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "invalid local program: {self:?}")
    }
}

impl std::error::Error for LocalProgramError {}

/// A graph compilation failure preserves the original request-control cause.
/// No control capability or allocation grant enters the completed graph.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProgramCompileError {
    Program(LocalProgramError),
    Control(CompileControlError),
}
impl From<LocalProgramError> for ProgramCompileError {
    fn from(error: LocalProgramError) -> Self {
        Self::Program(error)
    }
}
impl From<CompileControlError> for ProgramCompileError {
    fn from(error: CompileControlError) -> Self {
        Self::Control(error)
    }
}
impl fmt::Display for ProgramCompileError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Program(error) => error.fmt(formatter),
            Self::Control(error) => error.fmt(formatter),
        }
    }
}
impl std::error::Error for ProgramCompileError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Program(error) => Some(error),
            Self::Control(error) => Some(error),
        }
    }
}
struct ProgramWork<'a>(Option<CompileCheckpoints<'a>>);
impl ProgramWork<'_> {
    fn step(&mut self) -> Result<(), ProgramCompileError> {
        if let Some(work) = &mut self.0 {
            work.step()?;
        }
        Ok(())
    }
    fn flush(&mut self) -> Result<(), ProgramCompileError> {
        if let Some(work) = &mut self.0 {
            work.flush()?;
        }
        Ok(())
    }
    // Only external header comparison and Arc allocation are opaque here.
    // Own graph traversal is observed inside its actual loops below.
    fn opaque<T>(&mut self, operation: impl FnOnce() -> T) -> Result<T, ProgramCompileError> {
        self.flush()?;
        let result = operation();
        self.flush()?;
        Ok(result)
    }
    fn identity(
        &mut self,
        layout: &StaticLayout,
        ordinary: LocalProgramError,
    ) -> Result<crate::LayoutIdentity, ProgramCompileError> {
        self.flush()?;
        let result = if let Some(work) = &self.0 {
            layout
                .identity_for_compile(work.control())
                .map_err(|error| match error {
                    LayoutCompileError::Control(cause) => ProgramCompileError::Control(cause),
                    LayoutCompileError::Layout(_) => ProgramCompileError::Program(ordinary),
                })
        } else {
            layout.identity().map_err(|_| ordinary.into())
        };
        if matches!(&result, Err(ProgramCompileError::Control(_))) {
            return result;
        }
        self.flush()?;
        result
    }
    fn project(
        &mut self,
        layout: &StaticLayout,
        columns: &[SlotId],
    ) -> Result<StaticLayout, ProgramCompileError> {
        self.flush()?;
        let result = if let Some(work) = &self.0 {
            layout
                .project_by_slots_for_compile(columns, work.control())
                .map_err(|error| match error {
                    LayoutCompileError::Control(cause) => ProgramCompileError::Control(cause),
                    LayoutCompileError::Layout(_) => {
                        ProgramCompileError::Program(LocalProgramError::InvalidSink)
                    }
                })
        } else {
            layout
                .project_by_slots(columns)
                .map_err(|_| LocalProgramError::InvalidSink.into())
        };
        if matches!(&result, Err(ProgramCompileError::Control(_))) {
            return result;
        }
        self.flush()?;
        result
    }
    fn sink(&mut self, sink: &StaticSinkProgram) -> Result<(), ProgramCompileError> {
        self.flush()?;
        let result = if let Some(work) = &self.0 {
            sink.validate_for_compile(work.control())
                .map_err(|error| match error {
                    SinkCompileError::Control(cause) => ProgramCompileError::Control(cause),
                    SinkCompileError::Sink(_) => {
                        ProgramCompileError::Program(LocalProgramError::InvalidSink)
                    }
                })
        } else {
            sink.validate()
                .map_err(|_| LocalProgramError::InvalidSink.into())
        };
        if matches!(&result, Err(ProgramCompileError::Control(_))) {
            return result;
        }
        self.flush()?;
        result
    }
}
impl LocalProgramGraph {
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
        match Self::try_new_core(
            nodes,
            root,
            expressions,
            profile,
            requirements,
            sink,
            &mut ProgramWork(None),
        ) {
            Ok(program) => Ok(program),
            Err(ProgramCompileError::Program(error)) => Err(error),
            Err(ProgramCompileError::Control(_)) => {
                unreachable!("legacy graph has no compile observer")
            }
        }
    }
    pub fn try_new_for_compile(
        nodes: Vec<ProgramNode>,
        root: ProgramNodeId,
        expressions: Arc<ImmutableExpressions>,
        profile: CompileProfile,
        requirements: BindingRequirements,
        control: &dyn PureCompileControl,
    ) -> Result<Self, ProgramCompileError> {
        Self::try_new_with_sink_for_compile(
            nodes,
            root,
            expressions,
            profile,
            requirements,
            None,
            control,
        )
    }
    pub fn try_new_with_sink_for_compile(
        nodes: Vec<ProgramNode>,
        root: ProgramNodeId,
        expressions: Arc<ImmutableExpressions>,
        profile: CompileProfile,
        requirements: BindingRequirements,
        sink: Option<StaticSinkProgram>,
        control: &dyn PureCompileControl,
    ) -> Result<Self, ProgramCompileError> {
        let mut work = ProgramWork(Some(CompileCheckpoints::try_new(
            control,
            CompilePhase::LowerProgram,
        )?));
        let result = Self::try_new_core(
            nodes,
            root,
            expressions,
            profile,
            requirements,
            sink,
            &mut work,
        );
        if matches!(&result, Err(ProgramCompileError::Control(_))) {
            return result;
        }
        work.flush()?;
        result
    }
    fn try_new_core(
        nodes: Vec<ProgramNode>,
        root: ProgramNodeId,
        expressions: Arc<ImmutableExpressions>,
        profile: CompileProfile,
        requirements: BindingRequirements,
        sink: Option<StaticSinkProgram>,
        work: &mut ProgramWork<'_>,
    ) -> Result<Self, ProgramCompileError> {
        let bad_abi = profile.kernel_abi() != crate::KernelAbiVersion::CURRENT;
        work.step()?;
        if bad_abi {
            return Err(LocalProgramError::UnsupportedKernelAbi.into());
        }
        let empty = nodes.is_empty();
        work.step()?;
        if empty {
            return Err(LocalProgramError::Empty.into());
        }
        let too_many = nodes.len() > MAX_PROGRAM_NODES;
        work.step()?;
        if too_many {
            return Err(LocalProgramError::TooManyNodes.into());
        }
        let bad_root = root.index() >= nodes.len();
        work.step()?;
        if bad_root {
            return Err(LocalProgramError::InvalidRoot.into());
        }
        let mut depths = Vec::with_capacity(nodes.len());
        let mut expansions = Vec::with_capacity(nodes.len());
        let mut native_ids = BTreeSet::new();
        for (index, node) in nodes.iter().enumerate() {
            // Only per-Task sidecar nodes require unique native IDs; wrappers
            // retain the original allowance to share a diagnostic wire ID.
            if node.local_id().is_some_and(|id| id.index() != index) {
                return Err(LocalProgramError::InvalidNodeShape.into());
            }
            let duplicate = matches!(
                node.kind,
                ProgramNodeKind::Scan { .. }
                    | ProgramNodeKind::ExchangeSource { .. }
                    | ProgramNodeKind::TableWriter { .. }
                    | ProgramNodeKind::TableFinish { .. }
            ) && node
                .legacy_native_node_id()
                .is_some_and(|id| !native_ids.insert(id));
            work.step()?;
            if duplicate {
                return Err(LocalProgramError::DuplicateNativeNode.into());
            }
            let mut depth = 1usize;
            let mut expanded = 1usize;
            node.kind.try_for_each_child(|child| {
                let valid = child.index() < index;
                work.step()?;
                if !valid {
                    return Err(LocalProgramError::InvalidChild.into());
                }
                depth = depth.max(depths[child.index()] + 1);
                let next = expanded.checked_add(expansions[child.index()]);
                work.step()?;
                expanded = next.ok_or(LocalProgramError::ExpandedLimit)?;
                Ok::<(), ProgramCompileError>(())
            })?;
            let too_deep = depth > MAX_PROGRAM_NODE_DEPTH;
            work.step()?;
            if too_deep {
                return Err(LocalProgramError::TooDeep.into());
            }
            let too_expanded = expanded > MAX_PROGRAM_EXPANDED_OCCURRENCES;
            work.step()?;
            if too_expanded {
                return Err(LocalProgramError::ExpandedLimit.into());
            }
            depths.push(depth);
            expansions.push(expanded);
            work.step()?;
            node.kind.try_for_each_expression(|expr| {
                let valid = expr.is_none_or(|id| expressions.node(id).is_some());
                work.step()?;
                if !valid {
                    return Err(LocalProgramError::InvalidExpression.into());
                }
                Ok::<(), ProgramCompileError>(())
            })?;
            validate_shape(node, work)?;
            validate_relationships(node, &nodes, work)?;
        }
        let mut reachable = vec![false; nodes.len()];
        let mut pending = vec![root];
        while let Some(node) = pending.pop() {
            let seen = std::mem::replace(&mut reachable[node.index()], true);
            work.step()?;
            if seen {
                continue;
            }
            nodes[node.index()].kind.try_for_each_child(|child| {
                pending.push(child);
                work.step()
            })?;
        }
        for seen in &reachable {
            let seen = *seen;
            work.step()?;
            if !seen {
                return Err(LocalProgramError::UnreachableNode.into());
            }
        }
        if work.identity(
            &nodes[root.index()].output_layout,
            LocalProgramError::LayoutMismatch,
        )? != profile.layout()
        {
            return Err(LocalProgramError::LayoutMismatch.into());
        }
        let mut required_scans = BTreeSet::new();
        let mut required_exchanges = BTreeSet::new();
        let mut required_writers = BTreeSet::new();
        let mut required_finishes = BTreeSet::new();
        let mut required_filters = BTreeSet::new();
        for requirement in requirements.entries() {
            // The iterator yielded this exact requirement. Its lookup,
            // comparison and insertion work is observed separately below.
            work.step()?;
            match requirement {
                BindingRequirement::ResultSink { layout } => {
                    if work.identity(layout, LocalProgramError::LayoutMismatch)? != profile.layout()
                    {
                        return Err(LocalProgramError::InvalidRequirement.into());
                    }
                }
                BindingRequirement::Scan { node, kind, layout } => {
                    let found = nodes.get(node.index());
                    work.step()?;
                    let Some(ProgramNode {
                        kind: ProgramNodeKind::Scan { source, .. },
                        output_layout,
                        ..
                    }) = found
                    else {
                        return Err(LocalProgramError::InvalidRequirement.into());
                    };
                    let ScanSourceKind::TypedConnector { relation } = kind else {
                        return Err(LocalProgramError::InvalidRequirement.into());
                    };
                    let same_header = work.opaque(|| relation == source.relation_header())?;
                    if !same_header
                        || work.identity(layout, LocalProgramError::LayoutMismatch)?
                            != work.identity(output_layout, LocalProgramError::LayoutMismatch)?
                    {
                        return Err(LocalProgramError::InvalidRequirement.into());
                    }
                    required_scans.insert(node.index());
                    work.step()?;
                }
                BindingRequirement::RuntimeFilter { binding_id } => {
                    required_filters.insert(*binding_id);
                    work.step()?;
                }
                BindingRequirement::ExchangeInput { node, layout } => {
                    check_node_layout(
                        &nodes,
                        *node,
                        layout,
                        |kind| matches!(kind, ProgramNodeKind::ExchangeSource { .. }),
                        work,
                    )?;
                    required_exchanges.insert(node.index());
                    work.step()?;
                }
                BindingRequirement::TableWriter { node, layout } => {
                    check_node_layout(
                        &nodes,
                        *node,
                        layout,
                        |kind| matches!(kind, ProgramNodeKind::TableWriter { .. }),
                        work,
                    )?;
                    required_writers.insert(node.index());
                    work.step()?;
                }
                BindingRequirement::TableFinish { node, layout } => {
                    check_node_layout(
                        &nodes,
                        *node,
                        layout,
                        |kind| matches!(kind, ProgramNodeKind::TableFinish { .. }),
                        work,
                    )?;
                    required_finishes.insert(node.index());
                    work.step()?;
                }
                BindingRequirement::ExchangeOutput { layout, .. } => {
                    work.identity(layout, LocalProgramError::LayoutMismatch)?;
                }
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
            work.step()?;
            if !required {
                return Err(LocalProgramError::InvalidRequirement.into());
            }
            for id in node_filter_ids(&node.kind, work)? {
                let present = required_filters.contains(&id);
                work.step()?;
                if !present {
                    return Err(LocalProgramError::InvalidRequirement.into());
                }
            }
        }
        if let Some(sink) = &sink {
            work.sink(sink)?;
            let mut result_count = 0usize;
            for requirement in requirements.entries() {
                if matches!(requirement, BindingRequirement::ResultSink { .. }) {
                    result_count += 1;
                }
                work.step()?;
            }
            let mut outputs = BTreeSet::new();
            for requirement in requirements.entries() {
                if let BindingRequirement::ExchangeOutput { branch, .. } = requirement {
                    outputs.insert(*branch);
                }
                work.step()?;
            }
            match sink {
                StaticSinkProgram::Result | StaticSinkProgram::RootResult(_)
                    if result_count != 1 || !outputs.is_empty() =>
                {
                    return Err(LocalProgramError::InvalidSink.into());
                }
                StaticSinkProgram::Noop if result_count != 0 || !outputs.is_empty() => {
                    return Err(LocalProgramError::InvalidSink.into());
                }
                StaticSinkProgram::DataStream { .. }
                | StaticSinkProgram::MultiCastDataStream { .. }
                | StaticSinkProgram::SplitDataStream { .. } => {
                    if result_count != 0 {
                        return Err(LocalProgramError::InvalidSink.into());
                    }
                    let mut expected = BTreeSet::new();
                    for branch in 0..sink.branches().len() {
                        expected.insert(branch);
                        work.step()?;
                    }
                    if !same_set(&outputs, &expected, work)? {
                        return Err(LocalProgramError::InvalidSink.into());
                    }
                }
                _ => {}
            }
            if let StaticSinkProgram::RootResult(contract) = sink {
                contract
                    .validate_purpose()
                    .map_err(|_| LocalProgramError::InvalidSink)?;
            }
            if let StaticSinkProgram::RootResult(contract) = sink
                && let novarocks_result_contract::FrozenRootOutput::ClientRows(schema) =
                    contract.output()
            {
                let slots = nodes[root.index()]
                    .output_layout
                    .slots()
                    .iter()
                    .map(|slot| slot.as_u32())
                    .collect::<Vec<_>>();
                schema
                    .validate_native_slots(&slots)
                    .map_err(|_| LocalProgramError::InvalidSink)?;
                let fields = nodes[root.index()].output_layout.schema().fields();
                for column in schema.columns() {
                    let field = fields
                        .get(column.source_ordinal as usize)
                        .ok_or(LocalProgramError::InvalidSink)?;
                    if !novarocks_type_contract::result_render_type::render_field_matches_storage(
                        &column.field,
                        field.data_type(),
                        field.is_nullable(),
                    ) {
                        return Err(LocalProgramError::InvalidSink.into());
                    }
                }
            }
            if let StaticSinkProgram::RootResult(contract) = sink
                && let novarocks_result_contract::FrozenRootOutput::ScalarValue(schema) =
                    contract.output()
            {
                let layout = &nodes[root.index()].output_layout;
                let [slot] = layout.slots() else {
                    return Err(LocalProgramError::InvalidSink.into());
                };
                schema
                    .validate_native_slots(&[slot.as_u32()])
                    .map_err(|_| LocalProgramError::InvalidSink)?;
                let [field] = layout.schema().fields().as_ref() else {
                    return Err(LocalProgramError::InvalidSink.into());
                };
                if !novarocks_type_contract::result_scalar_type::scalar_field_matches_storage(
                    schema.field(),
                    field.data_type(),
                    field.is_nullable(),
                ) {
                    return Err(LocalProgramError::InvalidSink.into());
                }
            }
            for requirement in requirements.entries() {
                // Account the completed iteration before optional field work.
                work.step()?;
                if let BindingRequirement::ExchangeOutput { branch, layout } = requirement {
                    let branch = sink.branches().get(*branch);
                    work.step()?;
                    let branch = branch.ok_or(LocalProgramError::InvalidSink)?;
                    let projected =
                        work.project(&nodes[root.index()].output_layout, branch.output_columns())?;
                    if work.identity(&projected, LocalProgramError::InvalidSink)?
                        != work.identity(layout, LocalProgramError::InvalidSink)?
                    {
                        return Err(LocalProgramError::InvalidSink.into());
                    }
                }
            }
        }
        let nodes = work.opaque(|| Arc::from(nodes))?;
        Ok(Self {
            nodes,
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
    work: &mut ProgramWork<'_>,
) -> Result<(), ProgramCompileError> {
    let found = nodes.get(node.index());
    work.step()?;
    let found = found.ok_or(LocalProgramError::InvalidRequirement)?;
    let correct_kind = expected_kind(&found.kind);
    work.step()?;
    if !correct_kind
        || work.identity(layout, LocalProgramError::LayoutMismatch)?
            != work.identity(&found.output_layout, LocalProgramError::LayoutMismatch)?
    {
        return Err(LocalProgramError::InvalidRequirement.into());
    }
    Ok(())
}
fn node_filter_ids(
    kind: &ProgramNodeKind,
    work: &mut ProgramWork<'_>,
) -> Result<Vec<i32>, ProgramCompileError> {
    let mut ids = Vec::new();
    let mut add = |id: u32| -> Result<(), ProgramCompileError> {
        let id = i32::try_from(id);
        work.step()?;
        ids.push(id.map_err(|_| LocalProgramError::InvalidRequirement)?);
        work.step()?;
        Ok(())
    };
    match kind {
        ProgramNodeKind::Scan {
            runtime_filters, ..
        }
        | ProgramNodeKind::ExchangeSource {
            runtime_filters, ..
        } => {
            for binding in runtime_filters {
                add(binding.consumer.binding_id())?;
            }
        }
        ProgramNodeKind::RuntimeFilterConsumer { bindings, .. } => {
            for binding in bindings {
                add(binding.consumer.binding_id())?;
            }
        }
        ProgramNodeKind::Aggregate { topn_filters, .. } => {
            for filter in topn_filters {
                add(filter.producer.binding_id())?;
            }
        }
        ProgramNodeKind::Join {
            runtime_filters,
            runtime_filter_consumers,
            ..
        } => {
            for filter in runtime_filter_consumers {
                add(filter.consumer.binding_id())?;
            }
            for filter in runtime_filters {
                add(filter.producer.binding_id())?;
            }
        }
        _ => {}
    }
    Ok(ids)
}
fn same_set(
    left: &BTreeSet<usize>,
    right: &BTreeSet<usize>,
    work: &mut ProgramWork<'_>,
) -> Result<bool, ProgramCompileError> {
    let same_length = left.len() == right.len();
    work.step()?;
    if !same_length {
        return Ok(false);
    }
    for (left, right) in left.iter().zip(right) {
        let same = left == right;
        work.step()?;
        if !same {
            return Ok(false);
        }
    }
    Ok(true)
}
fn layout_matches(
    nodes: &[ProgramNode],
    child: ProgramNodeId,
    expected: &StaticLayout,
    work: &mut ProgramWork<'_>,
) -> Result<bool, ProgramCompileError> {
    Ok(work.identity(
        &nodes[child.index()].output_layout,
        LocalProgramError::LayoutMismatch,
    )? == work.identity(expected, LocalProgramError::LayoutMismatch)?)
}
fn validate_relationships(
    node: &ProgramNode,
    nodes: &[ProgramNode],
    work: &mut ProgramWork<'_>,
) -> Result<(), ProgramCompileError> {
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
            if !layout_matches(nodes, *left, left_layout, work)?
                || !layout_matches(nodes, *right, right_layout, work)?
            {
                return Err(LocalProgramError::LayoutMismatch.into());
            }
        }
        ProgramNodeKind::GenerateSeries {
            input,
            parameter_slots,
        } => {
            let child = &nodes[input.index()];
            let ProgramNodeKind::Values { values } = child.kind() else {
                return Err(LocalProgramError::InvalidNodeShape.into());
            };
            let same =
                values.num_rows() == 1 && child.output_layout().slots() == parameter_slots.as_ref();
            work.step()?;
            if !same {
                return Err(LocalProgramError::LayoutMismatch.into());
            }
        }
        ProgramNodeKind::RuntimeFilterConsumer { input, .. } => {
            if !layout_matches(nodes, *input, &node.output_layout, work)? {
                return Err(LocalProgramError::LayoutMismatch.into());
            }
        }
        ProgramNodeKind::TableFinish {
            inputs,
            writer_multiplex_layout,
            root_result_layout,
            ..
        } => {
            for input in inputs {
                let matches = layout_matches(nodes, *input, writer_multiplex_layout, work)?;
                work.step()?;
                if !matches {
                    return Err(LocalProgramError::LayoutMismatch.into());
                }
            }
            if work.identity(&node.output_layout, LocalProgramError::LayoutMismatch)?
                != work.identity(root_result_layout, LocalProgramError::LayoutMismatch)?
            {
                return Err(LocalProgramError::LayoutMismatch.into());
            }
        }
        _ => {}
    }
    Ok(())
}
fn validate_shape(
    node: &ProgramNode,
    work: &mut ProgramWork<'_>,
) -> Result<(), ProgramCompileError> {
    match &node.kind {
        ProgramNodeKind::Filter { predicates, .. } => {
            let empty = predicates.is_empty();
            work.step()?;
            if empty {
                return Err(LocalProgramError::InvalidNodeShape.into());
            }
        }
        ProgramNodeKind::Values { values } => {
            if work.identity(values.layout(), LocalProgramError::LayoutMismatch)?
                != work.identity(&node.output_layout, LocalProgramError::LayoutMismatch)?
            {
                return Err(LocalProgramError::LayoutMismatch.into());
            }
        }
        ProgramNodeKind::GenerateSeries {
            parameter_slots, ..
        } => {
            let bad =
                !matches!(parameter_slots.len(), 2 | 3) || node.output_layout.slots().len() != 1;
            work.step()?;
            if bad {
                return Err(LocalProgramError::InvalidNodeShape.into());
            }
        }
        ProgramNodeKind::Project {
            exprs,
            expr_slot_ids,
            expr_slot_schemas,
            output_indices,
            ..
        } => {
            let wrong_arity = exprs.len() != expr_slot_ids.len()
                || expr_slot_schemas
                    .as_ref()
                    .is_some_and(|slots| slots.len() != exprs.len());
            work.step()?;
            if wrong_arity {
                return Err(LocalProgramError::InvalidNodeShape.into());
            }
            if let Some(indices) = output_indices {
                for index in indices {
                    let valid = *index < exprs.len();
                    work.step()?;
                    if !valid {
                        return Err(LocalProgramError::InvalidNodeShape.into());
                    }
                }
            }
        }
        ProgramNodeKind::Unpivot {
            max_output_rows,
            max_output_bytes,
            ..
        } if *max_output_rows == 0 || *max_output_bytes == 0 => {
            return Err(LocalProgramError::InvalidNodeShape.into());
        }
        ProgramNodeKind::Repeat {
            null_slot_ids,
            grouping_slot_ids,
            grouping_list,
            repeat_times,
            ..
        } => {
            let bad = *repeat_times == 0
                || null_slot_ids.len() != *repeat_times
                || grouping_list.len() != grouping_slot_ids.len();
            work.step()?;
            if bad {
                return Err(LocalProgramError::InvalidNodeShape.into());
            }
            for values in grouping_list {
                let bad = values.len() != *repeat_times;
                work.step()?;
                if bad {
                    return Err(LocalProgramError::InvalidNodeShape.into());
                }
            }
        }
        ProgramNodeKind::SetOp { inputs, .. } if inputs.len() < 2 => {
            return Err(LocalProgramError::InvalidNodeShape.into());
        }
        ProgramNodeKind::Sort {
            order_by,
            partition_exprs,
            ..
        } if order_by.is_empty() && partition_exprs.is_empty() => {
            return Err(LocalProgramError::InvalidNodeShape.into());
        }
        ProgramNodeKind::TableFunction { function_name, .. } if function_name.is_empty() => {
            return Err(LocalProgramError::InvalidNodeShape.into());
        }
        ProgramNodeKind::GroupedTopN {
            group_by,
            order_by,
            rank_group_ordinals,
            functions,
            ..
        } => {
            let width = group_by.len().checked_add(functions.len());
            if group_by.is_empty()
                || order_by.len() != group_by.len()
                || rank_group_ordinals.len() != group_by.len()
                || width != Some(node.output_layout.slots().len())
            {
                return Err(LocalProgramError::InvalidNodeShape.into());
            }
            let mut permutation = std::collections::BTreeSet::new();
            for ordinal in rank_group_ordinals {
                work.step()?;
                if *ordinal >= group_by.len() || !permutation.insert(*ordinal) {
                    return Err(LocalProgramError::InvalidNodeShape.into());
                }
            }
            for function in functions {
                work.step()?;
                if function.name.is_empty()
                    || !function.input_is_intermediate
                    || function.inputs.len() != 1
                    || function.order.is_distinct
                    || !function.order.is_asc_order.is_empty()
                    || !function.order.nulls_first.is_empty()
                {
                    return Err(LocalProgramError::InvalidNodeShape.into());
                }
            }
        }
        ProgramNodeKind::Aggregate {
            group_by,
            functions,
            topn_filters,
            ..
        } => {
            for function in functions {
                let bad = function.name.is_empty()
                    || function.order.is_asc_order.len() != function.order.nulls_first.len();
                work.step()?;
                if bad {
                    return Err(LocalProgramError::InvalidNodeShape.into());
                }
            }
            for filter in topn_filters {
                let bad = group_by.get(filter.group_key_ordinal) != Some(&filter.group_key_expr);
                work.step()?;
                if bad {
                    return Err(LocalProgramError::InvalidNodeShape.into());
                }
            }
        }
        ProgramNodeKind::Join {
            join_type,
            probe_keys,
            build_keys,
            eq_null_safe,
            runtime_filters,
            runtime_filter_consumers,
            ..
        } => {
            let bad = (!runtime_filter_consumers.is_empty()
                && !matches!(join_type, JoinType::Inner | JoinType::LeftSemi))
                || probe_keys.len() != build_keys.len()
                || probe_keys.len() != eq_null_safe.len();
            work.step()?;
            if bad {
                return Err(LocalProgramError::InvalidNodeShape.into());
            }
            for filter in runtime_filter_consumers {
                let bad = probe_keys.get(filter.key_ordinal) != Some(&filter.expr_id);
                work.step()?;
                if bad {
                    return Err(LocalProgramError::InvalidNodeShape.into());
                }
            }
            for filter in runtime_filters {
                let bad = build_keys.get(filter.key_ordinal) != Some(&filter.expr_id);
                work.step()?;
                if bad {
                    return Err(LocalProgramError::InvalidNodeShape.into());
                }
            }
        }
        ProgramNodeKind::Analytic {
            functions,
            output_columns,
            ..
        } => {
            let bad = output_columns.len() != node.output_layout.slots().len();
            work.step()?;
            if bad {
                return Err(LocalProgramError::InvalidNodeShape.into());
            }
            for column in output_columns {
                let bad = matches!(column, AnalyticOutputColumn::Window(index) if *index >= functions.len());
                work.step()?;
                if bad {
                    return Err(LocalProgramError::InvalidNodeShape.into());
                }
            }
            for function in functions {
                let prepared = matches!(function.kind, WindowFunctionKind::Prepared);
                let bad = (function.ignore_nulls && !function.kind.admits_ignore_nulls())
                    || (prepared && function.frame.is_none());
                work.step()?;
                if bad {
                    return Err(LocalProgramError::InvalidNodeShape.into());
                }
            }
        }
        ProgramNodeKind::TableWriter {
            projection,
            expected_layout,
            ..
        } => {
            let bad = projection.expressions.is_empty()
                || projection.expressions.len() != projection.layout.slots().len();
            work.step()?;
            if bad {
                return Err(LocalProgramError::InvalidNodeShape.into());
            }
            for expr in &projection.expressions {
                let bad = projection.arena.node(*expr).is_none();
                work.step()?;
                if bad {
                    return Err(LocalProgramError::InvalidNodeShape.into());
                }
            }
            if work.identity(&projection.layout, LocalProgramError::LayoutMismatch)?
                != work.identity(expected_layout, LocalProgramError::LayoutMismatch)?
            {
                return Err(LocalProgramError::InvalidNodeShape.into());
            }
        }
        ProgramNodeKind::TableFinish {
            inputs,
            expected_targets,
            final_aggregates,
            ..
        } => {
            let mut unique_targets = BTreeSet::new();
            for target in expected_targets {
                unique_targets.insert(*target);
                work.step()?;
            }
            let bad = inputs.is_empty()
                || expected_targets.is_empty()
                || unique_targets.len() != expected_targets.len()
                || final_aggregates.unpivot.as_ref().is_some_and(|unpivot| {
                    unpivot.max_output_rows == 0 || unpivot.max_output_bytes == 0
                });
            work.step()?;
            if bad {
                return Err(LocalProgramError::InvalidNodeShape.into());
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
            KernelAbiVersion::CURRENT,
        )
    }

    #[test]
    fn grouped_topn_shape_requires_a_complete_rank_permutation_even_at_zero_limit() {
        // The graph owner rejects an invalid rank mapping before an executor
        // can index group keys. This local input needs no external resource;
        // LocalProgram owns the small affected-node regression. Installed
        // call/type/root correspondence is covered by the compiler fixture.
        let make = |rank_group_ordinals| {
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
            LocalProgramGraph::try_new(
                vec![
                    ProgramNode::new(1, ProgramNodeKind::Values { values }, layout.clone()),
                    ProgramNode::new(2, ProgramNodeKind::GroupedTopN {
                        input: ProgramNodeId::new(0), group_by: vec![ProgramExprId::new(0)],
                        order_by: vec![SortExpression { expr: ProgramExprId::new(0), asc: true, nulls_first: false }],
                        rank_group_ordinals, limit: 0,
                        comparator: novarocks_type_contract::OrderedComparisonAlgorithm::NativeScalarOrderV1,
                        functions: vec![],
                    }, layout.clone()),
                ], ProgramNodeId::new(1), expressions, profile(&layout), BindingRequirements::try_new(Vec::new()).unwrap(),
            )
        };
        assert!(make(vec![0]).is_ok());
        for permutation in [vec![], vec![1], vec![0, 0]] {
            assert!(matches!(
                make(permutation),
                Err(LocalProgramError::InvalidNodeShape)
            ));
        }
    }

    #[test]
    fn table_finish_checks_scalar_constants_in_every_grouped_unpivot_mapping() {
        let make_program = |last_constant: UnpivotConstant| {
            let (values, layout) = values();
            let expressions = Arc::new(
                ImmutableExpressions::try_new(
                    vec![StaticExprNode::new(
                        StaticExprKind::Literal(crate::StaticLiteral::Int64(7)),
                        DataType::Int64,
                        None,
                    )],
                    false,
                    HashMap::new(),
                    None,
                )
                .unwrap(),
            );
            let unpivot = WriterGroupedUnpivotPlan {
                grouping_input_slot_id: SlotId::new(1),
                grouping_output_slot_id: SlotId::new(2),
                passthrough_output_slot_id: SlotId::new(3),
                value_output_slot_id: SlotId::new(4),
                literal_output_slot_ids: vec![SlotId::new(5)],
                mappings: vec![
                    WriterGroupedUnpivotMapping {
                        grouping_key: 0,
                        input_value_slot_id: SlotId::new(1),
                        constants: vec![UnpivotConstant::Int32List(vec![2])],
                    },
                    WriterGroupedUnpivotMapping {
                        grouping_key: 1,
                        input_value_slot_id: SlotId::new(1),
                        constants: vec![UnpivotConstant::Scalar {
                            expr_id: ProgramExprId::new(0),
                            nullable: false,
                        }],
                    },
                    WriterGroupedUnpivotMapping {
                        grouping_key: 2,
                        input_value_slot_id: SlotId::new(1),
                        constants: vec![last_constant],
                    },
                ],
                max_output_rows: 16,
                max_output_bytes: 1024,
            };
            LocalProgramGraph::try_new(
                vec![
                    ProgramNode::new(1, ProgramNodeKind::Values { values }, layout.clone()),
                    ProgramNode::new(
                        2,
                        ProgramNodeKind::TableFinish {
                            inputs: vec![ProgramNodeId::new(0)],
                            expected_targets: vec![WriteTargetOrdinal::try_new(0).unwrap()],
                            writer_multiplex_layout: layout.clone(),
                            root_result_layout: layout.clone(),
                            final_aggregates: WriterFinalAggregatePlan {
                                calls: vec![],
                                unpivot: Some(unpivot),
                            },
                        },
                        layout.clone(),
                    ),
                ],
                ProgramNodeId::new(1),
                expressions,
                profile(&layout),
                BindingRequirements::try_new(vec![BindingRequirement::TableFinish {
                    node: ProgramNodeId::new(1),
                    layout,
                }])
                .unwrap(),
            )
        };
        assert!(
            make_program(UnpivotConstant::Scalar {
                expr_id: ProgramExprId::new(0),
                nullable: false,
            })
            .is_ok()
        );
        assert!(make_program(UnpivotConstant::Utf8Map(vec![])).is_ok());
        for index in [1, usize::MAX] {
            assert!(matches!(
                make_program(UnpivotConstant::Scalar {
                    expr_id: ProgramExprId::new(index),
                    nullable: false,
                }),
                Err(LocalProgramError::InvalidExpression)
            ));
        }
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
            LocalProgramGraph::try_new(
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
                    predicates: vec![ProgramExprId::new(0)].into_boxed_slice(),
                },
                layout.clone(),
            ),
        ];
        let program = LocalProgramGraph::try_new(
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
            LocalProgramGraph::try_new(
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
        let program = LocalProgramGraph::try_new(
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
                KernelAbiVersion::CURRENT,
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
        let program = LocalProgramGraph::try_new(
            nodes,
            ProgramNodeId::new(17),
            Arc::new(ImmutableExpressions::try_new(vec![], false, HashMap::new(), None).unwrap()),
            profile(&layout),
            BindingRequirements::try_new(vec![]).unwrap(),
        );
        assert!(matches!(program, Err(LocalProgramError::ExpandedLimit)));
    }

    #[test]
    fn local_identity_matches_actual_dense_position_independently_of_sparse_source() {
        for id in [0usize, 1, usize::MAX] {
            let (values, layout) = values();
            let result = LocalProgramGraph::try_new(
                vec![ProgramNode::new_local(
                    ProgramNodeId::new(id),
                    vec![DiagnosticSourceNodeId::new(u32::MAX)],
                    ProgramNodeKind::Values { values },
                    layout.clone(),
                )],
                ProgramNodeId::new(0),
                Arc::new(
                    ImmutableExpressions::try_new(vec![], false, HashMap::new(), None).unwrap(),
                ),
                profile(&layout),
                BindingRequirements::try_new(vec![]).unwrap(),
            );
            if id == 0 {
                let graph = result.unwrap();
                assert_eq!(graph.nodes()[0].local_id(), Some(ProgramNodeId::new(0)));
                assert_eq!(
                    graph.nodes()[0].physical_sources(),
                    &[DiagnosticSourceNodeId::new(u32::MAX)]
                );
            } else {
                assert_eq!(result.unwrap_err(), LocalProgramError::InvalidNodeShape);
            }
        }
    }

    #[test]
    fn rejects_unreachable_nodes_and_unbound_exchange_source() {
        let (values, layout) = values();
        let empty_expressions = || {
            Arc::new(ImmutableExpressions::try_new(vec![], false, HashMap::new(), None).unwrap())
        };
        let unreachable = LocalProgramGraph::try_new(
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

        let unbound = LocalProgramGraph::try_new(
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
            let program = LocalProgramGraph::try_new(
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
        let result = LocalProgramGraph::try_new_with_sink(
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

    #[test]
    fn local_program_rejects_the_pre_policy_kernel_abi() {
        let (_, layout) = values();
        let expressions =
            Arc::new(ImmutableExpressions::try_new(vec![], false, HashMap::new(), None).unwrap());
        let requirements = BindingRequirements::try_new(vec![]).unwrap();
        let old = CompileProfile::new(
            NonZeroUsize::new(1).unwrap(),
            None,
            layout.identity().unwrap(),
            KernelAbiVersion::new(NonZeroU32::new(1).unwrap()),
        );
        assert!(matches!(
            LocalProgramGraph::try_new(
                vec![],
                ProgramNodeId::new(0),
                expressions.clone(),
                old,
                requirements.clone()
            ),
            Err(LocalProgramError::UnsupportedKernelAbi)
        ));
        assert!(matches!(
            LocalProgramGraph::try_new(
                vec![],
                ProgramNodeId::new(0),
                expressions,
                profile(&layout),
                requirements
            ),
            Err(LocalProgramError::Empty)
        ));
        assert_eq!(KernelAbiVersion::CURRENT.get(), 2);
    }

    #[derive(Default)]
    struct OriginalControl {
        trace: std::sync::Mutex<Vec<(CompilePhase, u32)>>,
        stop: Option<(usize, CompileControlError)>,
    }
    impl PureCompileControl for OriginalControl {
        fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
            let mut trace = self.trace.lock().unwrap();
            let index = trace.len();
            trace.push((phase, units));
            if let Some((at, cause)) = self.stop
                && at == index
            {
                return Err(cause);
            }
            Ok(())
        }
    }
    fn causes() -> [CompileControlError; 3] {
        [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ]
    }
    fn exchange_fixture(edges: usize, children: bool) -> LocalProgramGraph {
        let (_, layout) = values();
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
        let mut nodes = vec![ProgramNode::new(
            7,
            ProgramNodeKind::ExchangeSource {
                timeout: Duration::from_secs(1),
                runtime_filters: vec![],
                hash_partition_exprs: if children {
                    vec![]
                } else {
                    vec![ProgramExprId::new(0); edges]
                },
            },
            layout.clone(),
        )];
        if children {
            nodes.push(ProgramNode::new(
                8,
                ProgramNodeKind::UnionAll {
                    inputs: vec![ProgramNodeId::new(0); edges],
                },
                layout.clone(),
            ));
        }
        let root = ProgramNodeId::new(nodes.len() - 1);
        LocalProgramGraph::try_new_with_sink(
            nodes,
            root,
            expressions,
            profile(&layout),
            BindingRequirements::try_new(vec![
                BindingRequirement::ExchangeInput {
                    node: ProgramNodeId::new(0),
                    layout: layout.clone(),
                },
                BindingRequirement::ResultSink { layout },
            ])
            .unwrap(),
            Some(StaticSinkProgram::Result),
        )
        .unwrap()
    }
    fn compile_fixture(
        source: &LocalProgramGraph,
        control: &dyn PureCompileControl,
    ) -> Result<LocalProgramGraph, ProgramCompileError> {
        LocalProgramGraph::try_new_with_sink_for_compile(
            source.nodes().to_vec(),
            source.root(),
            source.expressions().clone(),
            source.profile(),
            source.requirements().clone(),
            source.sink().cloned(),
            control,
        )
    }

    #[test]
    fn compile_program_preserves_real_child_and_expression_edges_and_owned_backing() {
        for children in [false, true] {
            let source = exchange_fixture(320, children);
            let control = OriginalControl::default();
            let actual = compile_fixture(&source, &control).unwrap();
            assert_eq!(actual.root(), source.root());
            assert_eq!(actual.nodes().len(), source.nodes().len());
            assert_eq!(actual.profile(), source.profile());
            assert!(Arc::ptr_eq(actual.expressions(), source.expressions()));
            assert_eq!(
                actual.nodes()[actual.root().index()]
                    .output_layout()
                    .identity()
                    .unwrap(),
                source.profile().layout()
            );
            if children {
                let ProgramNodeKind::UnionAll { inputs } = actual.nodes()[1].kind() else {
                    panic!("expected exact union graph");
                };
                assert_eq!(inputs, &vec![ProgramNodeId::new(0); 320]);
            } else {
                let ProgramNodeKind::ExchangeSource {
                    hash_partition_exprs,
                    ..
                } = actual.nodes()[0].kind()
                else {
                    panic!("expected exact exchange graph");
                };
                assert_eq!(hash_partition_exprs, &vec![ProgramExprId::new(0); 320]);
            }
            let trace = control.trace.lock().unwrap();
            assert!(trace.iter().any(|(_, units)| *units == 256));
            assert!(
                trace
                    .iter()
                    .all(|(phase, units)| *phase == CompilePhase::LowerProgram && *units <= 256)
            );
        }
    }

    #[test]
    fn compile_program_original_control_refuses_entry_quantum_delegate_and_final_tail() {
        for children in [false, true] {
            let source = exchange_fixture(320, children);
            let baseline = OriginalControl::default();
            compile_fixture(&source, &baseline).unwrap();
            let trace = baseline.trace.lock().unwrap().clone();
            let quantum = trace.iter().position(|(_, units)| *units == 256).unwrap();
            let delegate = trace
                .iter()
                .enumerate()
                .skip(quantum + 1)
                .find(|(_, (_, units))| *units == 0)
                .unwrap()
                .0;
            for cause in causes() {
                for at in [0, quantum, delegate, trace.len() - 1] {
                    let control = OriginalControl {
                        trace: Default::default(),
                        stop: Some((at, cause)),
                    };
                    assert!(
                        matches!(compile_fixture(&source, &control), Err(ProgramCompileError::Control(actual)) if actual == cause)
                    );
                    assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
                }
            }
        }
    }

    #[test]
    fn compile_program_empty_error_observes_tail_and_keeps_typed_cause() {
        use std::error::Error;
        let source = exchange_fixture(0, false);
        let baseline = OriginalControl::default();
        let run = |control: &dyn PureCompileControl| {
            LocalProgramGraph::try_new_for_compile(
                vec![],
                ProgramNodeId::new(0),
                source.expressions().clone(),
                source.profile(),
                BindingRequirements::try_new(vec![]).unwrap(),
                control,
            )
        };
        assert!(matches!(
            run(&baseline),
            Err(ProgramCompileError::Program(LocalProgramError::Empty))
        ));
        let trace = baseline.trace.lock().unwrap().clone();
        assert_eq!(
            trace,
            vec![
                (CompilePhase::LowerProgram, 0),
                (CompilePhase::LowerProgram, 2)
            ]
        );
        for cause in causes() {
            let control = OriginalControl {
                trace: Default::default(),
                stop: Some((1, cause)),
            };
            let error = run(&control).unwrap_err();
            assert_eq!(error, ProgramCompileError::Control(cause));
            assert_eq!(
                error
                    .source()
                    .unwrap()
                    .downcast_ref::<CompileControlError>(),
                Some(&cause)
            );
            assert_eq!(*control.trace.lock().unwrap(), trace);
        }
    }

    #[test]
    fn compile_program_preserves_shape_layout_requirement_and_reference_error_order() {
        let source = exchange_fixture(0, false);
        let layout = source.nodes()[0].output_layout().clone();
        for (kind, expected) in [
            (
                ProgramNodeKind::Filter {
                    input: ProgramNodeId::new(1),
                    predicates: vec![ProgramExprId::new(999)].into_boxed_slice(),
                },
                LocalProgramError::InvalidChild,
            ),
            (
                ProgramNodeKind::Filter {
                    input: ProgramNodeId::new(0),
                    predicates: vec![ProgramExprId::new(999)].into_boxed_slice(),
                },
                LocalProgramError::InvalidExpression,
            ),
            (
                ProgramNodeKind::Project {
                    input: ProgramNodeId::new(0),
                    is_subordinate: false,
                    validate_final_result_input: false,
                    exprs: vec![ProgramExprId::new(0)],
                    expr_slot_ids: vec![],
                    expr_slot_schemas: None,
                    output_indices: None,
                },
                LocalProgramError::InvalidNodeShape,
            ),
        ] {
            let nodes = vec![
                source.nodes()[0].clone(),
                ProgramNode::new(8, kind, layout.clone()),
            ];
            assert!(
                matches!(LocalProgramGraph::try_new(nodes.clone(), ProgramNodeId::new(1), source.expressions().clone(), source.profile(), source.requirements().clone()), Err(actual) if actual == expected)
            );
            let control = OriginalControl::default();
            assert!(
                matches!(LocalProgramGraph::try_new_for_compile(nodes, ProgramNodeId::new(1), source.expressions().clone(), source.profile(), source.requirements().clone(), &control), Err(ProgramCompileError::Program(actual)) if actual == expected)
            );
        }
        let bad_profile = CompileProfile::new(
            NonZeroUsize::new(1).unwrap(),
            None,
            LayoutIdentity::from_sha256([9; 32]),
            KernelAbiVersion::CURRENT,
        );
        assert!(matches!(
            LocalProgramGraph::try_new_for_compile(
                source.nodes().to_vec(),
                source.root(),
                source.expressions().clone(),
                bad_profile,
                BindingRequirements::try_new(vec![]).unwrap(),
                &OriginalControl::default()
            ),
            Err(ProgramCompileError::Program(
                LocalProgramError::LayoutMismatch
            ))
        ));
        assert!(matches!(
            LocalProgramGraph::try_new_for_compile(
                source.nodes().to_vec(),
                source.root(),
                source.expressions().clone(),
                source.profile(),
                BindingRequirements::try_new(vec![]).unwrap(),
                &OriginalControl::default()
            ),
            Err(ProgramCompileError::Program(
                LocalProgramError::InvalidRequirement
            ))
        ));
    }

    #[test]
    fn compile_program_stream_sink_uses_same_control_and_exact_projected_layout() {
        let source = exchange_fixture(0, false);
        let layout = source.nodes()[0].output_layout().clone();
        let sink = StaticSinkProgram::try_data_stream(
            crate::StaticStreamBranch::try_new(
                9,
                novarocks_execution_contract::DataStreamPartitionType::Random,
                vec![],
                vec![SlotId::new(1)],
                None,
            )
            .unwrap(),
            source.expressions().clone(),
        )
        .unwrap();
        let requirements = BindingRequirements::try_new(vec![
            BindingRequirement::ExchangeInput {
                node: ProgramNodeId::new(0),
                layout: layout.clone(),
            },
            BindingRequirement::ExchangeOutput { branch: 0, layout },
        ])
        .unwrap();
        let control = OriginalControl::default();
        let actual = LocalProgramGraph::try_new_with_sink_for_compile(
            source.nodes().to_vec(),
            source.root(),
            source.expressions().clone(),
            source.profile(),
            requirements.clone(),
            Some(sink.clone()),
            &control,
        )
        .unwrap();
        assert_eq!(
            actual.sink().unwrap().branches()[0].output_columns(),
            &[SlotId::new(1)]
        );
        LocalProgramGraph::try_new_with_sink(
            source.nodes().to_vec(),
            source.root(),
            source.expressions().clone(),
            source.profile(),
            requirements,
            Some(sink.clone()),
        )
        .unwrap();
        let run = |control: &dyn PureCompileControl| {
            LocalProgramGraph::try_new_with_sink_for_compile(
                source.nodes().to_vec(),
                source.root(),
                source.expressions().clone(),
                source.profile(),
                BindingRequirements::try_new(vec![BindingRequirement::ExchangeInput {
                    node: ProgramNodeId::new(0),
                    layout: source.nodes()[0].output_layout().clone(),
                }])
                .unwrap(),
                Some(sink.clone()),
                control,
            )
        };
        let ordinary = OriginalControl::default();
        assert!(matches!(
            run(&ordinary),
            Err(ProgramCompileError::Program(LocalProgramError::InvalidSink))
        ));
        let trace = ordinary.trace.lock().unwrap().clone();
        for cause in causes() {
            let control = OriginalControl {
                trace: Default::default(),
                stop: Some((trace.len() - 1, cause)),
            };
            assert!(
                matches!(run(&control), Err(ProgramCompileError::Control(actual)) if actual == cause)
            );
            assert_eq!(*control.trace.lock().unwrap(), trace);
        }
    }
}
