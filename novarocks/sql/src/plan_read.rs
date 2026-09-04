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

//! Read-only public projections of a sealed distributed SQL plan.
// Design: ADR-0050 (docs/adr/ADR-0050-sealed-plan-logical-mutation-effects-and-opaque-routes.md)
//!
//! This is the single public SQL plan reading surface. Construction, draft
//! mutation, sealing, and validation remain private to the SQL compiler.

pub use crate::analysis::{ExprKind, SortItem, TypedExpr, UnpivotConstant};
pub use crate::column_id::ColumnId;
pub use crate::common::CteId;
pub use crate::common::expr::{
    BinOp, JoinKind, LiteralValue, UnOp, WindowBound, WindowFrame, WindowFrameType,
};
pub use crate::common::plan_hints::{ScanVariantColumn, SqlTopNType};
pub use crate::common::schema::OutputColumn;
pub use crate::planner::distributed::write::{
    ChangeStreamRouterSink, ConnectorWriteInputBinding, TableFinishNode, TableWriterNode,
    WriteUnpivotMapping, WriterFinalAggregateCall, WriterFinalAggregatePlan,
    WriterPartialAggregateCall, WriterPartialAggregatePlan, WriterUnpivotPlan,
};
pub use crate::planner::distributed::{
    BoundaryColumn, BoundaryContract, BoundaryKind, ConnectorWriteOutputContract, DataPartition,
    DataSink, DistributedNode, DistributedNodeKind, DistributedPlan, ExchangeFlavor,
    ExchangeReceiver, ExecutionColumnId, FinalizedWriteTargetColumn, FragmentEdge,
    FragmentEdgeKind, FragmentEdgeOutputCatalog, FragmentId, FragmentStreamKind,
    NodeExecutionColumn, NodeExecutionOutput, NodeOutputCatalog, PartitionKind, PlanFragment,
    WriteContractCatalog, distributed_kind_to_physical,
};
pub use crate::planner::payload::{PlanRowCountAssertion, PlanScanNode};
pub use crate::planner::physical::node::{PhysicalPlanKind, PlanSetOpKind, RedistributeMode};
pub use crate::planner::physical::runtime_filter::JoinExecutionMode;
pub use crate::planner::physical::vocab::{AggMode, HashSource, JoinDistribution, TopNPhase};
pub use novarocks_spi::connector::{
    ConnectorMutationRouteInput, ConnectorRowMutationEffect, ConnectorWriteRouteId,
};

impl DistributedNode {
    /// Project sealed runtime-filter binding identifiers for protocol encoding.
    /// The typed SQL binding identifiers remain internal to the planner.
    pub fn runtime_filter_binding_ids(&self) -> impl ExactSizeIterator<Item = u32> + '_ {
        self.runtime_filter_binding_ids
            .iter()
            .map(|binding_id| binding_id.get())
    }
}

/// Read-only access to one sealed NCP-6 dataflow table writer.
///
/// The writer emits the SPI-owned writer relation
/// (`novarocks_spi::connector::write_stack::writer_output_schema`) rather than
/// terminating the plan; readers can only observe its query-local writer
/// ordinal, its Arrow input binding, and the sealed SQL/Arrow output contract
/// it feeds the connector writer.
impl TableWriterNode {
    /// The dense, query-local writer-handle ordinal every row this writer emits
    /// is tagged with.
    pub const fn write_target_ordinal(
        &self,
    ) -> novarocks_spi::connector::write_stack::WriteTargetOrdinal {
        self.write_target_ordinal
    }

    /// The Arrow input selection that feeds the connector writer.
    pub fn input(&self) -> &ConnectorWriteInputBinding {
        &self.input
    }

    /// The sealed write output expressions, evaluated against the writer's
    /// execution input.
    pub fn output_exprs(&self) -> &[TypedExpr] {
        &self.output_contract.output_exprs
    }

    /// The sealed write target schema, carrying positional target-field ids.
    pub fn target_schema(&self) -> &[FinalizedWriteTargetColumn] {
        &self.output_contract.target_schema
    }

    pub const fn writer_multiplex_schema(
        &self,
    ) -> &novarocks_spi::connector::write_stack::WriterMultiplexSchema {
        &self.writer_multiplex_schema
    }

    pub const fn partial_aggregate_plan(&self) -> &WriterPartialAggregatePlan {
        &self.partial_aggregate_plan
    }
}

/// Read-only access to the single sealed NCP-6 write finish node.
impl TableFinishNode {
    /// The dense `0..n` writer ordinals this finish node must observe.
    pub fn expected_target_ordinals(
        &self,
    ) -> &[novarocks_spi::connector::write_stack::WriteTargetOrdinal] {
        &self.expected_target_ordinals
    }

    pub const fn writer_multiplex_schema(
        &self,
    ) -> &novarocks_spi::connector::write_stack::WriterMultiplexSchema {
        &self.writer_multiplex_schema
    }

    pub const fn root_result_schema(
        &self,
    ) -> &novarocks_spi::connector::write_stack::RootWriteResultSchema {
        &self.root_result_schema
    }

    pub const fn final_aggregate_plan(&self) -> &WriterFinalAggregatePlan {
        &self.final_aggregate_plan
    }
}

/// Borrowed, read-only projection of one sealed change-stream router route.
/// Route construction and validation remain private to SQL planning.
pub struct ChangeStreamRouterRouteRead<'a>(
    &'a crate::planner::distributed::write::change_stream::ChangeStreamRoute,
);

impl ChangeStreamRouterRouteRead<'_> {
    pub fn route_id(&self) -> ConnectorWriteRouteId {
        self.0.route_id
    }

    /// The dense, query-local logical write target this branch feeds.
    pub const fn write_target_ordinal(
        &self,
    ) -> novarocks_spi::connector::write_stack::WriteTargetOrdinal {
        self.0.write_target_ordinal
    }

    pub fn accepted_effects(&self) -> &[ConnectorRowMutationEffect] {
        &self.0.accepted_effects
    }

    pub fn input_ordinals(&self) -> &[ConnectorMutationRouteInput] {
        &self.0.input_ordinals
    }

    pub const fn target_fragment_id(&self) -> FragmentId {
        self.0.target_fragment_id
    }

    pub const fn target_exchange_node_id(&self) -> i32 {
        self.0.target_exchange_node_id
    }

    pub fn output_partition_ordinals(&self) -> &[usize] {
        &self.0.output_partition_ordinals
    }
}

impl ChangeStreamRouterSink {
    pub const fn group_id(&self) -> i32 {
        self.group_id
    }

    pub const fn effect_output_ordinal(&self) -> usize {
        self.effect_output_ordinal
    }

    pub fn routes(&self) -> impl ExactSizeIterator<Item = ChangeStreamRouterRouteRead<'_>> + '_ {
        self.routes.iter().map(ChangeStreamRouterRouteRead)
    }
}

/// Immutable expression projection for protocol encoders.
///
/// Expression construction and the analyzer-only fields of `ExprKind` remain
/// private to SQL. Consumers can recursively encode the sealed expression
/// values without naming analyzer payload types.
#[derive(Clone, Debug)]
pub struct SqlExpressionRead {
    pub data_type: arrow::datatypes::DataType,
    pub nullable: bool,
    pub kind: SqlExpressionReadKind,
}

#[derive(Clone, Debug)]
pub enum SqlExpressionReadKind {
    ColumnRef {
        column_id: ColumnId,
        qualifier: Option<String>,
        column: String,
    },
    LambdaParamRef {
        name: String,
        slot_id: i32,
    },
    Literal(LiteralValue),
    BinaryOp {
        left: Box<TypedExpr>,
        op: BinOp,
        right: Box<TypedExpr>,
    },
    UnaryOp {
        op: UnOp,
        expr: Box<TypedExpr>,
    },
    FunctionCall {
        name: String,
        args: Vec<TypedExpr>,
        distinct: bool,
    },
    LambdaFunction {
        params: Vec<SqlLambdaParameterRead>,
        body: Box<TypedExpr>,
    },
    AggregateCall {
        name: String,
        args: Vec<TypedExpr>,
        distinct: bool,
        order_by: Vec<SortItem>,
        resolved: novarocks_functions::ResolvedAggregateSignature,
    },
    Cast {
        expr: Box<TypedExpr>,
        target: arrow::datatypes::DataType,
    },
    IsNull {
        expr: Box<TypedExpr>,
        negated: bool,
    },
    InList {
        expr: Box<TypedExpr>,
        list: Vec<TypedExpr>,
        negated: bool,
    },
    Between {
        expr: Box<TypedExpr>,
        low: Box<TypedExpr>,
        high: Box<TypedExpr>,
        negated: bool,
    },
    Like {
        expr: Box<TypedExpr>,
        pattern: Box<TypedExpr>,
        negated: bool,
    },
    Case {
        operand: Option<Box<TypedExpr>>,
        when_then: Vec<(TypedExpr, TypedExpr)>,
        else_expr: Option<Box<TypedExpr>>,
    },
    IsTruthValue {
        expr: Box<TypedExpr>,
        value: bool,
        negated: bool,
    },
    Nested(Box<TypedExpr>),
    WindowCall {
        name: String,
        args: Vec<TypedExpr>,
        distinct: bool,
        function_order_by: Vec<SortItem>,
        aggregate_binding: Option<novarocks_functions::ResolvedAggregateSignature>,
        partition_by: Vec<TypedExpr>,
        order_by: Vec<SortItem>,
        window_frame: Option<WindowFrame>,
        ignore_nulls: bool,
    },
    SubqueryPlaceholder {
        id: usize,
    },
    Lambda,
}

#[derive(Clone, Debug)]
pub struct SqlLambdaParameterRead {
    pub name: String,
    pub slot_id: i32,
    pub data_type: arrow::datatypes::DataType,
    pub nullable: bool,
}

pub fn expression_read(expr: &TypedExpr) -> SqlExpressionRead {
    use crate::analysis::ExprKind;

    let kind = match &expr.kind {
        ExprKind::ColumnRef {
            column_id,
            qualifier,
            column,
        } => SqlExpressionReadKind::ColumnRef {
            column_id: *column_id,
            qualifier: qualifier.clone(),
            column: column.clone(),
        },
        ExprKind::LambdaParamRef { name, slot_id } => SqlExpressionReadKind::LambdaParamRef {
            name: name.clone(),
            slot_id: *slot_id,
        },
        ExprKind::Literal(value) => SqlExpressionReadKind::Literal(value.clone()),
        ExprKind::BinaryOp { left, op, right } => SqlExpressionReadKind::BinaryOp {
            left: left.clone(),
            op: *op,
            right: right.clone(),
        },
        ExprKind::UnaryOp { op, expr } => SqlExpressionReadKind::UnaryOp {
            op: *op,
            expr: expr.clone(),
        },
        ExprKind::FunctionCall {
            name,
            args,
            distinct,
            ..
        } => SqlExpressionReadKind::FunctionCall {
            name: name.clone(),
            args: args.clone(),
            distinct: *distinct,
        },
        ExprKind::LambdaFunction { params, body } => SqlExpressionReadKind::LambdaFunction {
            params: params
                .iter()
                .map(|param| SqlLambdaParameterRead {
                    name: param.name.clone(),
                    slot_id: param.slot_id,
                    data_type: param.data_type.clone(),
                    nullable: param.nullable,
                })
                .collect(),
            body: body.clone(),
        },
        ExprKind::AggregateCall {
            name,
            args,
            distinct,
            order_by,
            resolved,
        } => SqlExpressionReadKind::AggregateCall {
            name: name.clone(),
            args: args.clone(),
            distinct: *distinct,
            order_by: order_by.clone(),
            resolved: resolved.clone(),
        },
        ExprKind::Cast { expr, target } => SqlExpressionReadKind::Cast {
            expr: expr.clone(),
            target: target.clone(),
        },
        ExprKind::IsNull { expr, negated } => SqlExpressionReadKind::IsNull {
            expr: expr.clone(),
            negated: *negated,
        },
        ExprKind::InList {
            expr,
            list,
            negated,
        } => SqlExpressionReadKind::InList {
            expr: expr.clone(),
            list: list.clone(),
            negated: *negated,
        },
        ExprKind::Between {
            expr,
            low,
            high,
            negated,
        } => SqlExpressionReadKind::Between {
            expr: expr.clone(),
            low: low.clone(),
            high: high.clone(),
            negated: *negated,
        },
        ExprKind::Like {
            expr,
            pattern,
            negated,
        } => SqlExpressionReadKind::Like {
            expr: expr.clone(),
            pattern: pattern.clone(),
            negated: *negated,
        },
        ExprKind::Case {
            operand,
            when_then,
            else_expr,
        } => SqlExpressionReadKind::Case {
            operand: operand.clone(),
            when_then: when_then.clone(),
            else_expr: else_expr.clone(),
        },
        ExprKind::IsTruthValue {
            expr,
            value,
            negated,
        } => SqlExpressionReadKind::IsTruthValue {
            expr: expr.clone(),
            value: *value,
            negated: *negated,
        },
        ExprKind::Nested(expr) => SqlExpressionReadKind::Nested(expr.clone()),
        ExprKind::WindowCall {
            name,
            args,
            distinct,
            function_order_by,
            aggregate_binding,
            partition_by,
            order_by,
            window_frame,
            ignore_nulls,
        } => SqlExpressionReadKind::WindowCall {
            name: name.clone(),
            args: args.clone(),
            distinct: *distinct,
            function_order_by: function_order_by.clone(),
            aggregate_binding: aggregate_binding.clone(),
            partition_by: partition_by.clone(),
            order_by: order_by.clone(),
            window_frame: window_frame.clone(),
            ignore_nulls: *ignore_nulls,
        },
        ExprKind::SubqueryPlaceholder { id, .. } => {
            SqlExpressionReadKind::SubqueryPlaceholder { id: *id }
        }
        ExprKind::Lambda { .. } => SqlExpressionReadKind::Lambda,
    };

    SqlExpressionRead {
        data_type: expr.data_type.clone(),
        nullable: expr.nullable,
        kind,
    }
}

/// Immutable, wire-oriented projection of a sealed physical node.
///
/// SQL retains every physical payload type; consumers receive only copied
/// planning values needed to map the native protocol.
#[derive(Clone, Debug)]
pub enum SqlPhysicalPlanRead {
    Scan(SqlPlanScanNodeRead),
    Filter {
        predicate: TypedExpr,
    },
    Project(SqlProjectPlanRead),
    Unpivot(SqlUnpivotPlanRead),
    Sort(SqlSortPlanRead),
    Limit {
        limit: Option<i64>,
        offset: Option<i64>,
    },
    Values(SqlValuesPlanRead),
    Repeat(SqlRepeatPlanRead),
    Window(SqlWindowPlanRead),
    GenerateSeries(SqlGenerateSeriesPlanRead),
    TableFunction(SqlTableFunctionPlanRead),
    AssertOneRow(SqlAssertOneRowPlanRead),
    TopN(SqlTopNPlanRead),
    HashAggregate(SqlHashAggregatePlanRead),
    HashJoin(SqlHashJoinPlanRead),
    NestLoopJoin(SqlNestLoopJoinPlanRead),
    SetOp(SqlSetOpPlanRead),
    ChangeEventExpand(SqlChangeEventExpandPlanRead),
    CTEAnchor {
        cte_id: CteId,
    },
    CTEProduce(SqlCteProducePlanRead),
    CTEConsume(SqlCteConsumePlanRead),
    Redistribute(SqlRedistributePlanRead),
}

#[derive(Clone, Debug)]
pub struct SqlPlanScanNodeRead {
    pub database: String,
    pub table: SqlTableDefRead,
    pub alias: Option<String>,
    pub columns: Vec<OutputColumn>,
    pub predicates: Vec<TypedExpr>,
    pub required_columns: Option<Vec<String>>,
    pub variant_columns: Vec<ScanVariantColumn>,
    pub mv_rewritten_from: Option<String>,
}

#[derive(Clone, Debug)]
pub struct SqlTableDefRead {
    pub name: String,
    pub columns: Vec<novarocks_types::schema::ColumnDef>,
    pub iceberg_row_lineage_metadata_columns: Vec<novarocks_types::schema::ColumnDef>,
    pub source: SqlScanSourceRead,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SqlScanSourceRead {
    ConnectorRead,
    PinnedFileSet,
    TableExecute,
    Data,
    FrozenInputSet,
    Metadata,
    Delta {
        from_snapshot_id: i64,
        to_snapshot_id: i64,
    },
    MvTargetState,
    MvTargetLocator,
}

#[derive(Clone, Debug)]
pub struct SqlProjectPlanRead {
    pub items: Vec<SqlProjectItemRead>,
    pub output_qualifier: Option<String>,
}

#[derive(Clone, Debug)]
pub struct SqlProjectItemRead {
    pub expr: TypedExpr,
    pub output_name: String,
    pub output_column_id: ColumnId,
}

#[derive(Clone, Debug)]
pub struct SqlSortPlanRead {
    pub output_columns: Vec<OutputColumn>,
    pub items: Vec<SortItem>,
    pub analytic_partition_by: Vec<TypedExpr>,
    pub offset: Option<i64>,
    pub partition_limit: Option<usize>,
    pub topn_type: Option<SqlTopNType>,
}

#[derive(Clone, Debug)]
pub struct SqlValuesPlanRead {
    pub rows: Vec<Vec<TypedExpr>>,
    pub columns: Vec<OutputColumn>,
}

#[derive(Clone, Debug)]
pub struct SqlUnpivotPlanRead {
    pub passthrough_columns: Vec<SqlUnpivotPassthroughColumnRead>,
    pub value_output_column_id: ColumnId,
    pub literal_output_column_ids: Vec<ColumnId>,
    pub value_mappings: Vec<SqlUnpivotValueMappingRead>,
    pub output_columns: Vec<OutputColumn>,
    pub max_output_rows: usize,
    pub max_output_bytes: usize,
}

#[derive(Clone, Debug)]
pub struct SqlUnpivotPassthroughColumnRead {
    pub input_column_id: ColumnId,
    pub output_column_id: ColumnId,
}

#[derive(Clone, Debug)]
pub struct SqlUnpivotValueMappingRead {
    pub input_value_column_id: ColumnId,
    pub constants: Vec<crate::analysis::UnpivotConstant>,
}

#[derive(Clone, Debug)]
pub struct SqlRepeatPlanRead {
    pub repeat_column_ref_list: Vec<Vec<String>>,
    pub repeat_column_ref_ids: Vec<Vec<ColumnId>>,
    pub grouping_ids: Vec<u64>,
    pub all_rollup_columns: Vec<String>,
    pub all_rollup_column_ids: Vec<ColumnId>,
    pub grouping_key_aliases: Vec<(String, String)>,
    pub grouping_fn_args: Vec<(String, Vec<String>)>,
    pub grouping_fn_arg_ids: Vec<Vec<ColumnId>>,
    pub grouping_fn_ids: Vec<(String, ColumnId)>,
    pub virtual_tuple_id: Option<i32>,
}

#[derive(Clone, Debug)]
pub struct SqlWindowPlanRead {
    pub window_exprs: Vec<SqlWindowExprRead>,
    pub output_columns: Vec<OutputColumn>,
}

#[derive(Clone, Debug)]
pub struct SqlWindowExprRead {
    pub name: String,
    pub args: Vec<TypedExpr>,
    pub distinct: bool,
    pub function_order_by: Vec<SortItem>,
    pub aggregate_binding: Option<novarocks_functions::ResolvedAggregateSignature>,
    pub partition_by: Vec<TypedExpr>,
    pub order_by: Vec<SortItem>,
    pub window_frame: Option<WindowFrame>,
    pub result_type: arrow::datatypes::DataType,
    pub output_name: String,
    pub output_column_id: ColumnId,
    pub ignore_nulls: bool,
}

#[derive(Clone, Debug)]
pub struct SqlGenerateSeriesPlanRead {
    pub start: i64,
    pub end: i64,
    pub step: i64,
    pub column_name: String,
    pub alias: Option<String>,
    pub output_column_id: ColumnId,
}

#[derive(Clone, Debug)]
pub struct SqlTableFunctionPlanRead {
    pub function_name: String,
    pub args: Vec<TypedExpr>,
    pub output_columns: Vec<OutputColumn>,
    pub alias: Option<String>,
    pub is_left_join: bool,
}

#[derive(Clone, Debug)]
pub struct SqlAssertOneRowPlanRead {
    pub subquery_text: String,
    pub desired_num_rows: Option<i64>,
    pub assertion: PlanRowCountAssertion,
    pub group_key_column_ids: Vec<ColumnId>,
    pub group_key_labels: Vec<String>,
    pub keyed_message_prefix: Option<String>,
}

#[derive(Clone, Debug)]
pub struct SqlTopNPlanRead {
    pub items: Vec<SortItem>,
    pub limit: Option<i64>,
    pub offset: Option<i64>,
    pub phase: TopNPhase,
    pub is_split: bool,
}

#[derive(Clone, Debug)]
pub struct SqlHashAggregatePlanRead {
    pub mode: AggMode,
    pub group_by: Vec<TypedExpr>,
    pub aggregates: Vec<SqlAggregateCallRead>,
    pub is_merge: Vec<bool>,
    pub output_layout: SqlAggregateOutputLayoutRead,
    pub output_columns: Vec<OutputColumn>,
}

#[derive(Clone, Debug)]
pub struct SqlAggregateCallRead {
    pub name: String,
    pub args: Vec<TypedExpr>,
    pub distinct: bool,
    pub result_type: arrow::datatypes::DataType,
    pub order_by: Vec<SortItem>,
    pub output_column_id: ColumnId,
    pub resolved: novarocks_functions::ResolvedAggregateSignature,
}

#[derive(Clone, Debug)]
pub struct SqlAggregateOutputLayoutRead {
    pub group_key_columns: Vec<OutputColumn>,
    pub aggregate_columns: Vec<OutputColumn>,
}

#[derive(Clone, Debug)]
pub struct SqlHashJoinPlanRead {
    pub join_type: JoinKind,
    pub eq_conditions: Vec<SqlHashJoinEqConditionRead>,
    pub other_condition: Option<TypedExpr>,
    pub distribution: JoinDistribution,
    pub execution_mode: Option<JoinExecutionMode>,
    pub output_columns: Vec<OutputColumn>,
}

#[derive(Clone, Debug)]
pub struct SqlHashJoinEqConditionRead {
    pub left: TypedExpr,
    pub right: TypedExpr,
    pub null_safe: bool,
}

#[derive(Clone, Debug)]
pub struct SqlNestLoopJoinPlanRead {
    pub join_type: JoinKind,
    pub condition: Option<TypedExpr>,
    pub output_columns: Vec<OutputColumn>,
}

#[derive(Clone, Debug)]
pub struct SqlSetOpPlanRead {
    pub kind: PlanSetOpKind,
    pub output_columns: Vec<OutputColumn>,
    pub child_output_columns: Vec<Vec<OutputColumn>>,
}

#[derive(Clone, Debug)]
pub struct SqlChangeEventExpandPlanRead {
    pub events: Vec<SqlChangeEventSpecRead>,
    pub output_columns: Vec<OutputColumn>,
    pub effect_column_id: ColumnId,
}

#[derive(Clone, Debug)]
pub struct SqlChangeEventSpecRead {
    pub predicate: Option<TypedExpr>,
    pub effect: ConnectorRowMutationEffect,
    pub assignments: Vec<SqlChangeEventOutputExprRead>,
}

#[derive(Clone, Debug)]
pub struct SqlChangeEventOutputExprRead {
    pub output_column_id: ColumnId,
    pub expr: Option<TypedExpr>,
}

#[derive(Clone, Debug)]
pub struct SqlCteProducePlanRead {
    pub cte_id: CteId,
    pub output_columns: Vec<OutputColumn>,
}

#[derive(Clone, Debug)]
pub struct SqlCteConsumePlanRead {
    pub cte_id: CteId,
    pub alias: String,
    pub output_columns: Vec<OutputColumn>,
    pub producer_column_ids: Vec<ColumnId>,
}

#[derive(Clone, Debug)]
pub struct SqlRedistributePlanRead {
    pub mode: RedistributeMode,
    pub partition_exprs: Vec<TypedExpr>,
    pub output_columns: Vec<OutputColumn>,
}

pub fn physical_plan_read(src: &PhysicalPlanKind) -> SqlPhysicalPlanRead {
    use crate::planner::physical::node::PhysicalPlanKind as Node;

    match src {
        Node::Scan(node) => SqlPhysicalPlanRead::Scan(SqlPlanScanNodeRead {
            database: node.database.clone(),
            table: SqlTableDefRead {
                name: node.table.name.clone(),
                columns: node.table.columns.clone(),
                iceberg_row_lineage_metadata_columns: node
                    .table
                    .iceberg_row_lineage_metadata_columns
                    .clone(),
                source: sql_scan_source_read(&node.table.source),
            },
            alias: node.alias.clone(),
            columns: node.columns.clone(),
            predicates: node.predicates.clone(),
            required_columns: node.required_columns.clone(),
            variant_columns: node.variant_columns.clone(),
            mv_rewritten_from: node.mv_rewritten_from.clone(),
        }),
        Node::Filter(node) => SqlPhysicalPlanRead::Filter {
            predicate: node.predicate.clone(),
        },
        Node::Project(node) => SqlPhysicalPlanRead::Project(SqlProjectPlanRead {
            items: node
                .items
                .iter()
                .map(|item| SqlProjectItemRead {
                    expr: item.expr.clone(),
                    output_name: item.output_name.clone(),
                    output_column_id: item.output_column_id,
                })
                .collect(),
            output_qualifier: node.output_qualifier.clone(),
        }),
        Node::Unpivot(node) => SqlPhysicalPlanRead::Unpivot(SqlUnpivotPlanRead {
            passthrough_columns: node
                .passthrough_columns
                .iter()
                .map(|mapping| SqlUnpivotPassthroughColumnRead {
                    input_column_id: mapping.input_column_id,
                    output_column_id: mapping.output_column_id,
                })
                .collect(),
            value_output_column_id: node.value_output_column_id,
            literal_output_column_ids: node.literal_output_column_ids.clone(),
            value_mappings: node
                .value_mappings
                .iter()
                .map(|mapping| SqlUnpivotValueMappingRead {
                    input_value_column_id: mapping.input_value_column_id,
                    constants: mapping.constants.clone(),
                })
                .collect(),
            output_columns: node.output_columns.clone(),
            max_output_rows: node.max_output_rows,
            max_output_bytes: node.max_output_bytes,
        }),
        Node::Sort(node) => SqlPhysicalPlanRead::Sort(SqlSortPlanRead {
            output_columns: node.output_columns.clone(),
            items: node.items.clone(),
            analytic_partition_by: node.analytic_partition_by.clone(),
            offset: node.offset,
            partition_limit: node.partition_limit,
            topn_type: node.topn_type,
        }),
        Node::Limit(node) => SqlPhysicalPlanRead::Limit {
            limit: node.limit,
            offset: node.offset,
        },
        Node::Values(node) => SqlPhysicalPlanRead::Values(SqlValuesPlanRead {
            rows: node.rows.clone(),
            columns: node.columns.clone(),
        }),
        Node::Repeat(node) => SqlPhysicalPlanRead::Repeat(SqlRepeatPlanRead {
            repeat_column_ref_list: node.repeat_column_ref_list.clone(),
            repeat_column_ref_ids: node.repeat_column_ref_ids.clone(),
            grouping_ids: node.grouping_ids.clone(),
            all_rollup_columns: node.all_rollup_columns.clone(),
            all_rollup_column_ids: node.all_rollup_column_ids.clone(),
            grouping_key_aliases: node.grouping_key_aliases.clone(),
            grouping_fn_args: node.grouping_fn_args.clone(),
            grouping_fn_arg_ids: node.grouping_fn_arg_ids.clone(),
            grouping_fn_ids: node.grouping_fn_ids.clone(),
            virtual_tuple_id: node.virtual_tuple_id,
        }),
        Node::Window(node) => SqlPhysicalPlanRead::Window(SqlWindowPlanRead {
            window_exprs: node
                .window_exprs
                .iter()
                .map(|expr| SqlWindowExprRead {
                    name: expr.name.clone(),
                    args: expr.args.clone(),
                    distinct: expr.distinct,
                    function_order_by: expr.function_order_by.clone(),
                    aggregate_binding: expr.aggregate_binding.clone(),
                    partition_by: expr.partition_by.clone(),
                    order_by: expr.order_by.clone(),
                    window_frame: expr.window_frame.clone(),
                    result_type: expr.result_type.clone(),
                    output_name: expr.output_name.clone(),
                    output_column_id: expr.output_column_id,
                    ignore_nulls: expr.ignore_nulls,
                })
                .collect(),
            output_columns: node.output_columns.clone(),
        }),
        Node::GenerateSeries(node) => {
            SqlPhysicalPlanRead::GenerateSeries(SqlGenerateSeriesPlanRead {
                start: node.start,
                end: node.end,
                step: node.step,
                column_name: node.column_name.clone(),
                alias: node.alias.clone(),
                output_column_id: node.output_column_id,
            })
        }
        Node::TableFunction(node) => SqlPhysicalPlanRead::TableFunction(SqlTableFunctionPlanRead {
            function_name: node.function_name.clone(),
            args: node.args.clone(),
            output_columns: node.output_columns.clone(),
            alias: node.alias.clone(),
            is_left_join: node.is_left_join,
        }),
        Node::AssertOneRow(node) => SqlPhysicalPlanRead::AssertOneRow(SqlAssertOneRowPlanRead {
            subquery_text: node.subquery_text.clone(),
            desired_num_rows: node.desired_num_rows,
            assertion: node.assertion,
            group_key_column_ids: node.group_key_column_ids.clone(),
            group_key_labels: node.group_key_labels.clone(),
            keyed_message_prefix: node.keyed_message_prefix.clone(),
        }),
        Node::TopN(node) => SqlPhysicalPlanRead::TopN(SqlTopNPlanRead {
            items: node.items.clone(),
            limit: node.limit,
            offset: node.offset,
            phase: node.phase,
            is_split: node.is_split,
        }),
        Node::HashAggregate(node) => SqlPhysicalPlanRead::HashAggregate(SqlHashAggregatePlanRead {
            mode: node.mode,
            group_by: node.group_by.clone(),
            aggregates: node
                .aggregates
                .iter()
                .map(|call| SqlAggregateCallRead {
                    name: call.name.clone(),
                    args: call.args.clone(),
                    distinct: call.distinct,
                    result_type: call.result_type.clone(),
                    order_by: call.order_by.clone(),
                    output_column_id: call.output_column_id,
                    resolved: call.resolved.clone(),
                })
                .collect(),
            is_merge: node.is_merge.clone(),
            output_layout: SqlAggregateOutputLayoutRead {
                group_key_columns: node.output_layout.group_key_columns.clone(),
                aggregate_columns: node.output_layout.aggregate_columns.clone(),
            },
            output_columns: node.output_columns.clone(),
        }),
        Node::HashJoin(node) => SqlPhysicalPlanRead::HashJoin(SqlHashJoinPlanRead {
            join_type: node.join_type,
            eq_conditions: node
                .eq_conditions
                .iter()
                .map(|condition| SqlHashJoinEqConditionRead {
                    left: condition.left.clone(),
                    right: condition.right.clone(),
                    null_safe: condition.null_safe,
                })
                .collect(),
            other_condition: node.other_condition.clone(),
            distribution: node.distribution.clone(),
            execution_mode: node.execution_mode,
            output_columns: node.output_columns.clone(),
        }),
        Node::NestLoopJoin(node) => SqlPhysicalPlanRead::NestLoopJoin(SqlNestLoopJoinPlanRead {
            join_type: node.join_type,
            condition: node.condition.clone(),
            output_columns: node.output_columns.clone(),
        }),
        Node::SetOp(node) => SqlPhysicalPlanRead::SetOp(SqlSetOpPlanRead {
            kind: node.kind,
            output_columns: node.output_columns.clone(),
            child_output_columns: node.child_output_columns.clone(),
        }),
        Node::ChangeEventExpand(node) => {
            SqlPhysicalPlanRead::ChangeEventExpand(SqlChangeEventExpandPlanRead {
                events: node
                    .events
                    .iter()
                    .map(|event| SqlChangeEventSpecRead {
                        predicate: event.predicate.clone(),
                        effect: event.effect,
                        assignments: event
                            .assignments
                            .iter()
                            .map(|assignment| SqlChangeEventOutputExprRead {
                                output_column_id: assignment.output_column_id,
                                expr: assignment.expr.clone(),
                            })
                            .collect(),
                    })
                    .collect(),
                output_columns: node.output_columns.clone(),
                effect_column_id: node.effect_column_id,
            })
        }
        Node::CTEAnchor(node) => SqlPhysicalPlanRead::CTEAnchor {
            cte_id: node.cte_id,
        },
        Node::CTEProduce(node) => SqlPhysicalPlanRead::CTEProduce(SqlCteProducePlanRead {
            cte_id: node.cte_id,
            output_columns: node.output_columns.clone(),
        }),
        Node::CTEConsume(node) => SqlPhysicalPlanRead::CTEConsume(SqlCteConsumePlanRead {
            cte_id: node.cte_id,
            alias: node.alias.clone(),
            output_columns: node.output_columns.clone(),
            producer_column_ids: node.producer_column_ids.clone(),
        }),
        Node::Redistribute(node) => SqlPhysicalPlanRead::Redistribute(SqlRedistributePlanRead {
            mode: node.mode.clone(),
            partition_exprs: node.partition_exprs.clone(),
            output_columns: node.output_columns.clone(),
        }),
    }
}

#[cfg(any(test, feature = "test-support"))]
pub fn unpivot_physical_plan_for_test() -> PhysicalPlanKind {
    use crate::analysis::LiteralValue;
    use crate::planner::payload::{
        PlanUnpivotNode, PlanUnpivotPassthroughColumn, PlanUnpivotValueMapping,
    };

    let column = |id, name: &str, data_type, nullable| OutputColumn {
        column_id: ColumnId(id),
        name: name.to_string(),
        data_type,
        nullable,
        is_internal: false,
    };
    let input = vec![
        column(1, "group", arrow::datatypes::DataType::Utf8, false),
        column(2, "value_a", arrow::datatypes::DataType::Int64, true),
        column(3, "value_b", arrow::datatypes::DataType::Int64, false),
    ];
    let literal = |value: &str| TypedExpr {
        kind: ExprKind::Literal(LiteralValue::String(value.to_string())),
        data_type: arrow::datatypes::DataType::Utf8,
        nullable: false,
    };
    PhysicalPlanKind::Unpivot(
        PlanUnpivotNode::try_new(
            &input,
            vec![PlanUnpivotPassthroughColumn {
                input_column_id: ColumnId(1),
                output_column_id: ColumnId(11),
            }],
            ColumnId(13),
            vec![ColumnId(12)],
            vec![
                PlanUnpivotValueMapping {
                    input_value_column_id: ColumnId(2),
                    constants: vec![crate::analysis::UnpivotConstant::Scalar(literal("a"))],
                },
                PlanUnpivotValueMapping {
                    input_value_column_id: ColumnId(3),
                    constants: vec![crate::analysis::UnpivotConstant::Scalar(literal("b"))],
                },
            ],
            vec![
                column(11, "group", arrow::datatypes::DataType::Utf8, false),
                column(12, "label", arrow::datatypes::DataType::Utf8, false),
                column(13, "value", arrow::datatypes::DataType::Int64, true),
            ],
            128,
            4096,
        )
        .expect("valid unpivot test plan"),
    )
}

fn sql_scan_source_read(source: &crate::planner::table::ScanSource) -> SqlScanSourceRead {
    match source {
        crate::planner::table::ScanSource::Sql(source) => match &source.kind {
            crate::planner::table::SqlScanKind::ConnectorRead => SqlScanSourceRead::ConnectorRead,
            crate::planner::table::SqlScanKind::PinnedFileSet => SqlScanSourceRead::PinnedFileSet,
            crate::planner::table::SqlScanKind::TableExecute => SqlScanSourceRead::TableExecute,
            crate::planner::table::SqlScanKind::Data { .. } => SqlScanSourceRead::Data,
            crate::planner::table::SqlScanKind::FrozenInputSet { .. } => {
                SqlScanSourceRead::FrozenInputSet
            }
            crate::planner::table::SqlScanKind::Metadata { .. } => SqlScanSourceRead::Metadata,
            crate::planner::table::SqlScanKind::Delta {
                from_snapshot_id,
                to_snapshot_id,
                ..
            } => SqlScanSourceRead::Delta {
                from_snapshot_id: *from_snapshot_id,
                to_snapshot_id: *to_snapshot_id,
            },
            crate::planner::table::SqlScanKind::MvTargetState { .. } => {
                SqlScanSourceRead::MvTargetState
            }
            crate::planner::table::SqlScanKind::MvTargetLocator { .. } => {
                SqlScanSourceRead::MvTargetLocator
            }
        },
    }
}

/// Immutable boundary facts projected from the sealed plan catalog.
///
/// Boundary derivation and occurrence allocation remain SQL-owned. Consumers
/// may inspect the frozen result but cannot rebuild or mutate its catalog.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SqlBoundaryContractRead {
    pub fragment_id: FragmentId,
    pub node_id: Option<i32>,
    pub kind: SqlBoundaryKindRead,
    pub columns: Vec<SqlBoundaryColumnRead>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SqlBoundaryKindRead {
    ResultOutput,
    ExchangeSend,
    ExchangeReceive,
    ChangeStreamRouterInput,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SqlBoundaryColumnRead {
    pub execution_column_id: ExecutionColumnId,
    pub column_id: ColumnId,
    pub output_ordinal: usize,
    pub name: String,
    pub data_type: arrow::datatypes::DataType,
    pub nullable: bool,
}

pub fn boundary_contract_reads(plan: &DistributedPlan) -> Vec<SqlBoundaryContractRead> {
    plan.boundaries()
        .contracts()
        .iter()
        .map(|contract| SqlBoundaryContractRead {
            fragment_id: contract.fragment_id,
            node_id: contract.node_id,
            kind: match contract.kind {
                crate::planner::distributed::BoundaryKind::ResultOutput => {
                    SqlBoundaryKindRead::ResultOutput
                }
                crate::planner::distributed::BoundaryKind::ExchangeSend => {
                    SqlBoundaryKindRead::ExchangeSend
                }
                crate::planner::distributed::BoundaryKind::ExchangeReceive => {
                    SqlBoundaryKindRead::ExchangeReceive
                }
                crate::planner::distributed::BoundaryKind::ChangeStreamRouterInput => {
                    SqlBoundaryKindRead::ChangeStreamRouterInput
                }
            },
            columns: contract
                .columns
                .iter()
                .map(|column| SqlBoundaryColumnRead {
                    execution_column_id: column.execution_column_id,
                    column_id: column.column_id,
                    output_ordinal: column.output_ordinal,
                    name: column.name.clone(),
                    data_type: column.data_type.clone(),
                    nullable: column.nullable,
                })
                .collect(),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{
        SqlBoundaryKindRead, SqlExpressionReadKind, boundary_contract_reads, expression_read,
    };
    use crate::test_support::{
        NativeEncoderPlanFixture, native_encoder_plan, native_lambda_expression,
    };

    #[test]
    fn expression_read_projects_lambda_parameters_without_exposing_analyzer_payload() {
        let read = expression_read(&native_lambda_expression());
        let SqlExpressionReadKind::LambdaFunction { params, .. } = read.kind else {
            panic!("expected lambda expression projection");
        };
        assert_eq!(params.len(), 1);
        assert_eq!(params[0].name, "x");
        assert_eq!(params[0].slot_id, 3);
        assert!(params[0].nullable);
    }

    #[test]
    fn boundary_contract_reads_preserve_sealed_exchange_occurrences() {
        let plan = native_encoder_plan(NativeEncoderPlanFixture::HashExchange)
            .expect("sealed exchange fixture");
        let contracts = boundary_contract_reads(&plan);
        let send = contracts
            .iter()
            .find(|contract| contract.kind == SqlBoundaryKindRead::ExchangeSend)
            .expect("exchange send boundary");
        let receive = contracts
            .iter()
            .find(|contract| contract.kind == SqlBoundaryKindRead::ExchangeReceive)
            .expect("exchange receive boundary");
        assert_eq!(send.columns[0].column_id, receive.columns[0].column_id);
        assert_ne!(
            send.columns[0].execution_column_id,
            receive.columns[0].execution_column_id
        );
    }
}

/// Read-only SQL table facts used by plan encoders.
pub mod table {
    pub use crate::planner::table::{
        ScanSource, SqlMetadataTableKind, SqlMvTargetLocatorScan,
        SqlMvTargetStatePartitionConstraint, SqlMvTargetStateRowFilter, SqlMvTargetStateScan,
        SqlScanKind, SqlScanSource, SqlTableIdentity, SqlTableVersionSelector, TableDef,
    };

    impl SqlMvTargetStateScan {
        /// Return the target row-id column captured by SQL admission.
        pub fn row_id_column_name(&self) -> &str {
            &self.row_id_column_name
        }

        /// Return the target group-key columns captured by SQL admission.
        pub fn group_key_names(&self) -> &[String] {
            &self.group_key_names
        }

        /// Return the aggregate-state columns captured by SQL admission.
        pub fn aggregate_state_names(&self) -> &[String] {
            &self.aggregate_state_names
        }

        /// Return the optional branch-id column needed by this target-state scan.
        pub fn branch_id_column_name(&self) -> Option<&str> {
            match &self.row_filter {
                SqlMvTargetStateRowFilter::DeltaInputRowIds {
                    branch_scope: Some(scope),
                    ..
                } => Some(&scope.branch_id_column_name),
                SqlMvTargetStateRowFilter::DeltaInputRowIds {
                    branch_scope: None, ..
                } => None,
            }
        }
    }

    impl SqlMvTargetLocatorScan {
        /// Return the physical apply-key column captured by SQL admission.
        pub fn apply_key_column(&self) -> &str {
            &self.apply_key_column
        }

        /// Return the optional branch-id column captured by SQL admission.
        pub fn branch_id_column(&self) -> Option<&str> {
            self.branch_id_column.as_deref()
        }
    }

    impl SqlScanSource {
        /// Return the sealed scan kind selected by SQL planning. The binding
        /// token and table identity remain private to the scan source.
        pub fn kind(&self) -> &SqlScanKind {
            &self.kind
        }
    }
}

/// Read-only runtime-filter planning facts used by plan encoders.
pub mod runtime_filter {
    pub use crate::planner::runtime_filter::contract::{
        ArtifactCapability, CompletionFenceKind, CompletionRequirement, ConsumerActivation,
        ContributionKind, LateApplyGranularity,
    };
    pub use crate::planner::runtime_filter::graph::RuntimeFilterGraph;
    pub use crate::planner::runtime_filter::graph::{
        ApplyPoint, ConsumerBindingTarget, ProducerBindingTarget,
    };
    pub use crate::planner::runtime_filter::progress::JoinBuildProgressCatalog;
    pub use crate::planner::runtime_filter::sealed::SealedRuntimeFilterPlan;
}
