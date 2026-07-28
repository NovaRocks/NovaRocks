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

//! Planner-owned physical plan nodes.

use arrow::datatypes::DataType;

use crate::sql::analysis::{JoinKind, OutputColumn, SortItem, TypedExpr};
use crate::sql::column_id::ColumnId;
use crate::sql::common::ChangeStreamBranchKind;
use crate::sql::planner::payload::{
    AggregateCall, PlanAssertOneRowNode, PlanCTEAnchorNode, PlanCTEConsumeNode, PlanCTEProduceNode,
    PlanFilterNode, PlanGenerateSeriesNode, PlanLimitNode, PlanProjectNode, PlanRepeatNode,
    PlanScanNode, PlanSortNode, PlanTableFunctionNode, PlanValuesNode, PlanWindowNode,
};
use crate::sql::planner::physical::runtime_filter::{
    AggregateTopNRuntimeFilterBuildIntent, RuntimeFilterBuildIntent, RuntimeFilterProbeIntent,
};
use crate::sql::planner::physical::{
    AggMode, AggregateOutputLayout, HashSource, JoinDistribution, JoinExecutionMode,
    PhysicalPlanStats, TopNPhase,
};
use novarocks_types::aggregate::{infer_agg_function_types, mangle_distinct_aggregate_name};

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct PhysicalTopNNode {
    pub items: Vec<SortItem>,
    pub limit: Option<i64>,
    pub offset: Option<i64>,
    pub phase: TopNPhase,
    pub is_split: bool,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct PhysicalHashAggregateNode {
    pub mode: AggMode,
    pub group_by: Vec<TypedExpr>,
    pub aggregates: Vec<AggregateCall>,
    pub is_merge: Vec<bool>,
    pub output_layout: AggregateOutputLayout,
    pub output_columns: Vec<OutputColumn>,
    pub topn_runtime_filter_builds: Vec<AggregateTopNRuntimeFilterBuildIntent>,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct PhysicalHashJoinNode {
    pub join_type: JoinKind,
    pub eq_conditions: Vec<PhysicalHashJoinEqCondition>,
    pub other_condition: Option<TypedExpr>,
    pub distribution: JoinDistribution,
    pub execution_mode: Option<JoinExecutionMode>,
    pub build_runtime_filters: Vec<RuntimeFilterBuildIntent>,
    pub output_columns: Vec<OutputColumn>,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct PhysicalHashJoinEqCondition {
    pub left: TypedExpr,
    pub right: TypedExpr,
    pub null_safe: bool,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct PhysicalNestLoopJoinNode {
    pub join_type: JoinKind,
    pub condition: Option<TypedExpr>,
    pub output_columns: Vec<OutputColumn>,
}

#[allow(dead_code)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PlanSetOpKind {
    UnionAll,
    UnionDistinct,
    Intersect,
    Except,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct PhysicalSetOpNode {
    pub kind: PlanSetOpKind,
    pub output_columns: Vec<OutputColumn>,
    pub child_output_columns: Vec<Vec<OutputColumn>>,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct DistributedChangeEventExpandNode {
    pub(crate) events: Vec<DistributedChangeEventSpec>,
    pub(crate) output_columns: Vec<OutputColumn>,
    pub(crate) change_op_column_id: ColumnId,
    pub(crate) data_route_column_id: Option<ColumnId>,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct DistributedChangeEventSpec {
    pub(crate) predicate: Option<TypedExpr>,
    pub(crate) branch_kind: ChangeStreamBranchKind,
    pub(crate) assignments: Vec<DistributedChangeEventOutputExpr>,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct DistributedChangeEventOutputExpr {
    pub(crate) output_column_id: ColumnId,
    pub(crate) expr: Option<TypedExpr>,
}

#[derive(Clone, Debug)]
pub(crate) struct PreExpandKeyedAssertSpec {
    pub(crate) key_column_name: String,
    pub(crate) key_label: String,
    pub(crate) message_prefix: String,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct PhysicalPlanNode {
    pub kind: PhysicalPlanKind,
    pub children: Vec<PhysicalPlanNode>,
    pub output_columns: Vec<OutputColumn>,
    pub stats: PhysicalPlanStats,
    pub probe_runtime_filters: Vec<RuntimeFilterProbeIntent>,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) enum PhysicalPlanKind {
    Scan(PlanScanNode),
    Filter(PlanFilterNode),
    Project(PlanProjectNode),
    Sort(PlanSortNode),
    Limit(PlanLimitNode),
    Values(PlanValuesNode),
    Repeat(PlanRepeatNode),
    Window(PlanWindowNode),
    GenerateSeries(PlanGenerateSeriesNode),
    TableFunction(PlanTableFunctionNode),
    AssertOneRow(PlanAssertOneRowNode),
    TopN(PhysicalTopNNode),
    HashAggregate(Box<PhysicalHashAggregateNode>),
    HashJoin(Box<PhysicalHashJoinNode>),
    NestLoopJoin(PhysicalNestLoopJoinNode),
    SetOp(PhysicalSetOpNode),
    ChangeEventExpand(DistributedChangeEventExpandNode),
    CTEAnchor(PlanCTEAnchorNode),
    CTEProduce(PlanCTEProduceNode),
    CTEConsume(PlanCTEConsumeNode),
    Redistribute(RedistributeNode),
}

impl PhysicalPlanKind {
    #[cfg(test)]
    pub(crate) fn variant_names_for_test() -> &'static [&'static str] {
        &[
            "Scan",
            "Filter",
            "Project",
            "Sort",
            "Limit",
            "Values",
            "Repeat",
            "Window",
            "GenerateSeries",
            "TableFunction",
            "AssertOneRow",
            "TopN",
            "HashAggregate",
            "HashJoin",
            "NestLoopJoin",
            "SetOp",
            "ChangeEventExpand",
            "CTEAnchor",
            "CTEProduce",
            "CTEConsume",
            "Redistribute",
        ]
    }
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct RedistributeNode {
    pub mode: RedistributeMode,
    pub partition_exprs: Vec<TypedExpr>,
    pub output_columns: Vec<OutputColumn>,
}

#[allow(dead_code)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum RedistributeMode {
    Gather,
    Hash {
        cols: Vec<ColumnId>,
        source: HashSource,
    },
    Broadcast,
}

// ---------------------------------------------------------------------------
// Aggregate wire-type adapters
// ---------------------------------------------------------------------------
//
// Planner-typed adapters bridging a `PhysicalHashAggregateNode`'s calls/mode to
// the canonical aggregate type contract in `novarocks_types::aggregate` (which stays
// a pure, planner-free leaf). Consumed by the distributed planner's aggregate
// output finalization (`sql::planner::distributed::output::finalize_hash_aggregate_wire`).

/// Whether a `HashAggregate` running in `mode` emits intermediate aggregate-state
/// types on the wire. Partial modes (`Local` / `DistinctGlobal` / `DistinctLocal`)
/// emit intermediate state; the terminal modes (`Single` / `Global`) emit the
/// final result type.
pub(crate) fn hash_aggregate_outputs_intermediate(mode: AggMode) -> bool {
    !matches!(mode, AggMode::Single | AggMode::Global)
}

/// The intermediate aggregate-state Arrow type an aggregate `call` exposes,
/// derived from its canonical function name and argument types via
/// [`infer_agg_function_types`]. Errors when the function exposes no intermediate
/// type.
///
/// Only the call's positional `args` participate (matching the wire the encoder
/// historically emitted); `order_by` inputs are intentionally excluded.
pub(crate) fn aggregate_intermediate_type(call: &AggregateCall) -> Result<DataType, String> {
    let function_name = aggregate_function_name(call);
    let arg_types = call
        .args
        .iter()
        .map(|arg| arg.data_type.clone())
        .collect::<Vec<_>>();
    infer_agg_function_types(&function_name, &arg_types, call.distinct)?
        .1
        .ok_or_else(|| format!("{function_name} does not expose an intermediate type"))
}

/// The canonical aggregate function name for a `call`, delegating the DISTINCT
/// name-mangling table to the single source of truth
/// [`mangle_distinct_aggregate_name`].
pub(crate) fn aggregate_function_name(call: &AggregateCall) -> String {
    mangle_distinct_aggregate_name(&call.name, call.distinct)
}

#[cfg(test)]
mod aggregate_wire_tests {
    use arrow::datatypes::DataType;

    use super::{AggMode, hash_aggregate_outputs_intermediate};
    use super::{aggregate_function_name, aggregate_intermediate_type};
    use crate::sql::analysis::{ExprKind, LiteralValue, TypedExpr};
    use crate::sql::column_id::ColumnId;
    use crate::sql::planner::payload::AggregateCall;

    fn agg_call(name: &str, distinct: bool, args: Vec<DataType>) -> AggregateCall {
        AggregateCall {
            name: name.to_string(),
            args: args
                .into_iter()
                .map(|data_type| TypedExpr {
                    kind: ExprKind::Literal(LiteralValue::Int(1)),
                    data_type,
                    nullable: true,
                })
                .collect(),
            distinct,
            result_type: DataType::Int64,
            order_by: Vec::new(),
            output_column_id: ColumnId::new_for_test(1),
        }
    }

    #[test]
    fn intermediate_mode_covers_partial_and_distinct_modes_only() {
        assert!(!hash_aggregate_outputs_intermediate(AggMode::Single));
        assert!(!hash_aggregate_outputs_intermediate(AggMode::Global));
        assert!(hash_aggregate_outputs_intermediate(AggMode::Local));
        assert!(hash_aggregate_outputs_intermediate(AggMode::DistinctGlobal));
        assert!(hash_aggregate_outputs_intermediate(AggMode::DistinctLocal));
    }

    #[test]
    fn function_name_applies_distinct_mangling() {
        assert_eq!(
            aggregate_function_name(&agg_call("count", false, vec![])),
            "count"
        );
        assert_eq!(
            aggregate_function_name(&agg_call("COUNT", true, vec![DataType::Int64])),
            "multi_distinct_count"
        );
    }

    #[test]
    fn intermediate_type_follows_canonical_contract_using_args_only() {
        // avg's intermediate state is Utf8 regardless of its Float64 output.
        assert_eq!(
            aggregate_intermediate_type(&agg_call("avg", false, vec![DataType::Int64])).unwrap(),
            DataType::Utf8
        );
        // DISTINCT count mangles to multi_distinct_count -> Binary intermediate.
        assert_eq!(
            aggregate_intermediate_type(&agg_call("count", true, vec![DataType::Int64])).unwrap(),
            DataType::Binary
        );
        assert_eq!(
            aggregate_intermediate_type(&agg_call("sum", false, vec![DataType::Int32])).unwrap(),
            DataType::Int64
        );
    }
}

#[cfg(test)]
mod plan_tests {
    use super::*;

    #[test]
    fn physical_plan_kind_set_op_uses_plan_scoped_kind() {
        let set_op = PhysicalPlanKind::SetOp(PhysicalSetOpNode {
            kind: PlanSetOpKind::UnionAll,
            output_columns: vec![],
            child_output_columns: vec![vec![], vec![]],
        });

        let PhysicalPlanKind::SetOp(node) = set_op else {
            panic!("expected SetOp");
        };
        assert_eq!(node.kind, PlanSetOpKind::UnionAll);
        assert_eq!(node.child_output_columns.len(), 2);
    }

    #[test]
    fn physical_plan_kind_has_redistribute_but_no_exchange() {
        fn accepts_physical(_: PhysicalPlanKind) {}

        accepts_physical(PhysicalPlanKind::Redistribute(RedistributeNode {
            mode: RedistributeMode::Gather,
            partition_exprs: vec![],
            output_columns: vec![],
        }));

        assert!(
            !PhysicalPlanKind::variant_names_for_test().contains(&"Exchange"),
            "Exchange belongs to DistributedPlan, not PhysicalPlanKind"
        );
    }

    #[test]
    fn redistribute_mode_variants_are_frozen() {
        fn _exhaustive(mode: &RedistributeMode) {
            match mode {
                RedistributeMode::Gather => {}
                RedistributeMode::Hash { .. } => {}
                RedistributeMode::Broadcast => {}
            }
        }
    }

    #[test]
    fn physical_plan_kind_has_no_exchange_variant() {
        assert!(
            !PhysicalPlanKind::variant_names_for_test().contains(&"Exchange"),
            "Exchange must not be a PhysicalPlanKind variant"
        );
    }
}
