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

#[cfg(test)]
use arrow::datatypes::DataType;
use novarocks_physical_plan::ProviderReadOccurrenceId;
use std::ops::{Deref, DerefMut};

use crate::analysis::{JoinKind, OutputColumn, SortItem, TypedExpr};
use crate::column_id::ColumnId;
use crate::planner::payload::{
    AggregateCall, PlanAssertOneRowNode, PlanCTEAnchorNode, PlanCTEConsumeNode, PlanCTEProduceNode,
    PlanFilterNode, PlanGenerateSeriesNode, PlanLimitNode, PlanProjectNode, PlanRepeatNode,
    PlanScanNode, PlanSortNode, PlanTableFunctionNode, PlanUnpivotNode, PlanValuesNode,
    PlanWindowNode,
};
use crate::planner::physical::runtime_filter::{
    AggregateTopNRuntimeFilterBuildIntent, RuntimeFilterBuildIntent, RuntimeFilterProbeIntent,
};
use crate::planner::physical::{
    AggMode, AggregateOutputLayout, HashSource, JoinDistribution, JoinExecutionMode,
    PhysicalPlanStats, TopNPhase,
};
#[cfg(test)]
use novarocks_functions::aggregate_types::mangle_distinct_aggregate_name;

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
    pub condition:
        novarocks_physical_plan::HashJoinCondition<Vec<PhysicalHashJoinEqCondition>, TypedExpr>,
    pub build_side: PhysicalHashJoinBuildSide,
    pub distribution: JoinDistribution,
    pub execution_mode: Option<JoinExecutionMode>,
    pub build_runtime_filters: Vec<RuntimeFilterBuildIntent>,
    pub output_columns: Vec<OutputColumn>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PhysicalHashJoinBuildSide {
    Left,
    Right,
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
pub enum PlanSetOpKind {
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
    pub(crate) effect_column_id: ColumnId,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct DistributedChangeEventSpec {
    pub(crate) predicate: Option<TypedExpr>,
    pub(crate) effect: novarocks_spi::connector::ConnectorRowMutationEffect,
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
    pub(crate) declared_violation: Option<novarocks_failure_contract::FailureReason>,
    pub(crate) applicability_output_ordinal: Option<usize>,
    pub(crate) key_columns: Vec<(String, String)>,
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

/// A scan after optimizer materialization and before final contract lowering.
///
/// Provider completion freezes the query-local read occurrence directly on
/// this node. Later stages must read that identity from the scan rather than
/// reconstructing an association from traversal order or relation identity.
#[derive(Clone, Debug)]
pub(crate) enum PhysicalScanNode {
    Planning(PlanScanNode),
    Finalized(FinalPhysicalScanNode),
}

#[derive(Clone, Debug)]
pub(crate) struct FinalPhysicalScanNode {
    payload: PlanScanNode,
    provider_read_occurrence: ProviderReadOccurrenceId,
}

impl PhysicalScanNode {
    pub(crate) fn provider_read_occurrence(&self) -> Option<ProviderReadOccurrenceId> {
        match self {
            Self::Planning(_) => None,
            Self::Finalized(scan) => Some(scan.provider_read_occurrence),
        }
    }

    pub(crate) fn finalize_provider_read_occurrence(
        self,
        occurrence: ProviderReadOccurrenceId,
    ) -> Result<Self, String> {
        let Self::Planning(payload) = self else {
            let existing = self
                .provider_read_occurrence()
                .expect("finalized physical scan owns an occurrence");
            return Err(format!(
                "physical scan already owns provider read occurrence {} and cannot be reassigned to {}",
                existing.get(),
                occurrence.get()
            ));
        };
        Ok(Self::Finalized(FinalPhysicalScanNode {
            payload,
            provider_read_occurrence: occurrence,
        }))
    }
}

impl From<PlanScanNode> for PhysicalScanNode {
    fn from(payload: PlanScanNode) -> Self {
        Self::Planning(payload)
    }
}

impl Deref for PhysicalScanNode {
    type Target = PlanScanNode;

    fn deref(&self) -> &Self::Target {
        match self {
            Self::Planning(payload) => payload,
            Self::Finalized(scan) => &scan.payload,
        }
    }
}

impl DerefMut for PhysicalScanNode {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match self {
            Self::Planning(payload) => payload,
            Self::Finalized(scan) => &mut scan.payload,
        }
    }
}

#[allow(dead_code)]
#[expect(
    clippy::large_enum_variant,
    reason = "Physical plan payloads remain inline to avoid changing the established plan traversal representation."
)]
#[derive(Clone, Debug)]
pub enum PhysicalPlanKind {
    Scan(PhysicalScanNode),
    Filter(PlanFilterNode),
    Project(PlanProjectNode),
    Unpivot(PlanUnpivotNode),
    Sort(PlanSortNode),
    Limit(PlanLimitNode),
    Values(PlanValuesNode),
    SystemTable(crate::planner::system_relation::PhysicalSystemTableNode),
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

#[cfg(test)]
impl PhysicalPlanKind {
    pub(crate) fn variant_names_for_test() -> &'static [&'static str] {
        &[
            "Scan",
            "Filter",
            "Project",
            "Unpivot",
            "Sort",
            "Limit",
            "Values",
            "SystemTable",
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

#[cfg(test)]
pub(crate) fn hash_aggregate_outputs_intermediate(mode: AggMode) -> bool {
    !matches!(mode, AggMode::Single | AggMode::Global)
}

#[cfg(test)]
pub(crate) fn aggregate_intermediate_type(call: &AggregateCall) -> Result<DataType, String> {
    Ok(crate::functions::aggregate_selection(call.source.binding())
        .intermediate_type
        .data_type()
        .clone())
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
pub enum RedistributeMode {
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

/// The canonical aggregate function name for a `call`, delegating the DISTINCT
/// name-mangling table to the single source of truth
/// [`mangle_distinct_aggregate_name`].
#[cfg(test)]
pub(crate) fn aggregate_function_name(call: &AggregateCall) -> String {
    mangle_distinct_aggregate_name(&call.name, call.distinct)
}

#[cfg(test)]
mod aggregate_wire_tests {
    use arrow::datatypes::DataType;

    use super::{AggMode, hash_aggregate_outputs_intermediate};
    use super::{aggregate_function_name, aggregate_intermediate_type};
    use crate::analysis::{ExprKind, LiteralValue, TypedExpr};
    use crate::column_id::ColumnId;
    use crate::planner::payload::AggregateCall;

    fn agg_call(name: &str, distinct: bool, args: Vec<DataType>) -> AggregateCall {
        let resolved = crate::functions::test_resolved_aggregate(name, &args, distinct);
        AggregateCall {
            name: name.to_string(),
            distinct,
            result_type: novarocks_type_contract::FunctionValueType::new(DataType::Int64, true),
            output_column_id: ColumnId::new_for_test(1),
            source: crate::binding::AggregateArgumentSource::uncertified(
                args.into_iter()
                    .map(|data_type| TypedExpr {
                        kind: ExprKind::Literal(LiteralValue::Int(1)),
                        value_type: novarocks_type_contract::FunctionValueType::new(
                            data_type, true,
                        ),
                    })
                    .collect(),
                Vec::new(),
                resolved,
            ),
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
        // An integer SUM accumulates exactly and travels as DECIMAL(38, 0).
        assert_eq!(
            aggregate_intermediate_type(&agg_call("sum", false, vec![DataType::Int32])).unwrap(),
            DataType::Decimal128(38, 0)
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
            "Exchange belongs to final PhysicalPlan topology, not PhysicalPlanKind"
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
