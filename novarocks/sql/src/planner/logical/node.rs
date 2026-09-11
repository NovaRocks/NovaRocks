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

//! Logical planner IR node and logical-only payload ownership.

use std::collections::HashSet;

use crate::analysis::{JoinKind, OutputColumn, TypedExpr};
use crate::column_id::ColumnId;
use crate::common::{ApplyKind, ImvVersionRef};
use crate::planner::payload::{
    AggregateCall, PlanAssertOneRowNode, PlanCTEAnchorNode, PlanCTEConsumeNode, PlanCTEProduceNode,
    PlanFilterNode, PlanGenerateSeriesNode, PlanLimitNode, PlanProjectNode, PlanRepeatNode,
    PlanScanNode, PlanSortNode, PlanTableFunctionNode, PlanValuesNode, PlanWindowNode,
};

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct LogicalPlanNode {
    pub kind: LogicalPlanKind,
    pub children: Vec<LogicalPlanNode>,
    /// Set by the Phase-1 column-pruning tagging pass; `None` means all columns required.
    pub required_output_columns: Option<HashSet<ColumnId>>,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) enum LogicalPlanKind {
    Scan(PlanScanNode),
    Filter(PlanFilterNode),
    Project(PlanProjectNode),
    Sort(PlanSortNode),
    /// Logical-stage payload only; distributed IR encodes limits on the wrapper,
    /// Sort/TopN offsets, or ExchangeFlavor::LimitOffset instead of a Limit kind.
    Limit(PlanLimitNode),
    Values(PlanValuesNode),
    Repeat(PlanRepeatNode),
    Window(PlanWindowNode),
    GenerateSeries(PlanGenerateSeriesNode),
    TableFunction(PlanTableFunctionNode),
    AssertOneRow(PlanAssertOneRowNode),
    Aggregate(LogicalAggregateNode),
    Join(LogicalJoinNode),
    Union(LogicalUnionNode),
    Intersect(LogicalIntersectNode),
    Except(LogicalExceptNode),
    CTEAnchor(PlanCTEAnchorNode),
    CTEProduce(PlanCTEProduceNode),
    CTEConsume(PlanCTEConsumeNode),
    Apply(LogicalApplyNode),
    ImvDelta(LogicalImvDeltaNode),
    ImvVersion(LogicalImvVersionNode),
}

impl LogicalPlanKind {
    pub(crate) fn variant_name(&self) -> &'static str {
        match self {
            LogicalPlanKind::Scan(_) => "Scan",
            LogicalPlanKind::Filter(_) => "Filter",
            LogicalPlanKind::Project(_) => "Project",
            LogicalPlanKind::Sort(_) => "Sort",
            LogicalPlanKind::Limit(_) => "Limit",
            LogicalPlanKind::Values(_) => "Values",
            LogicalPlanKind::Repeat(_) => "Repeat",
            LogicalPlanKind::Window(_) => "Window",
            LogicalPlanKind::GenerateSeries(_) => "GenerateSeries",
            LogicalPlanKind::TableFunction(_) => "TableFunction",
            LogicalPlanKind::AssertOneRow(_) => "AssertOneRow",
            LogicalPlanKind::Aggregate(_) => "Aggregate",
            LogicalPlanKind::Join(_) => "Join",
            LogicalPlanKind::Union(_) => "Union",
            LogicalPlanKind::Intersect(_) => "Intersect",
            LogicalPlanKind::Except(_) => "Except",
            LogicalPlanKind::CTEAnchor(_) => "CTEAnchor",
            LogicalPlanKind::CTEProduce(_) => "CTEProduce",
            LogicalPlanKind::CTEConsume(_) => "CTEConsume",
            LogicalPlanKind::Apply(_) => "Apply",
            LogicalPlanKind::ImvDelta(_) => "ImvDelta",
            LogicalPlanKind::ImvVersion(_) => "ImvVersion",
        }
    }

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
            "Aggregate",
            "Join",
            "Union",
            "Intersect",
            "Except",
            "CTEAnchor",
            "CTEProduce",
            "CTEConsume",
            "Apply",
            "ImvDelta",
            "ImvVersion",
        ]
    }
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct LogicalApplyNode {
    pub kind: ApplyKind,
    pub subquery_expr: TypedExpr,
    pub output_column: OutputColumn,
    pub inner_output_column_id: ColumnId,
    pub correlation_column_ids: Vec<ColumnId>,
    pub correlation_conjuncts: Vec<TypedExpr>,
    pub residual_predicate: Option<TypedExpr>,
    pub need_check_max_rows: bool,
    pub use_semi_anti: bool,
    pub uncorrelated_outer_predicate_columns: HashSet<ColumnId>,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct LogicalAggregateNode {
    pub group_by: Vec<TypedExpr>,
    pub aggregates: Vec<AggregateCall>,
    pub output_columns: Vec<OutputColumn>,
    pub already_pushed: bool,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct LogicalJoinNode {
    pub join_type: JoinKind,
    pub condition: Option<TypedExpr>,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct LogicalUnionNode {
    pub all: bool,
    pub output_columns: Vec<OutputColumn>,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct LogicalIntersectNode {
    pub output_columns: Vec<OutputColumn>,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct LogicalExceptNode {
    pub output_columns: Vec<OutputColumn>,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct LogicalImvDeltaNode {
    pub is_root: bool,
    pub action_column: Option<ColumnId>,
    pub branch_scope: Option<crate::planner::table::BranchScope>,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct LogicalImvVersionNode {
    pub version_ref: ImvVersionRef,
}

impl LogicalPlanNode {
    pub(crate) fn new(
        kind: LogicalPlanKind,
        children: Vec<LogicalPlanNode>,
        required_output_columns: Option<HashSet<ColumnId>>,
    ) -> Self {
        Self {
            kind,
            children,
            required_output_columns,
        }
    }

    pub(crate) fn child(&self, index: usize) -> &LogicalPlanNode {
        self.children
            .get(index)
            .expect("logical plan node child index out of range")
    }

    pub(crate) fn unary_input(&self) -> &LogicalPlanNode {
        self.child(0)
    }

    pub(crate) fn left(&self) -> &LogicalPlanNode {
        self.child(0)
    }

    pub(crate) fn right(&self) -> &LogicalPlanNode {
        self.child(1)
    }

    #[allow(
        dead_code,
        reason = "Destructive child extraction remains available to rewrite rules compiled in feature-specific targets."
    )]
    pub(crate) fn take_child(&mut self, index: usize) -> LogicalPlanNode {
        self.children.remove(index)
    }

    #[allow(
        dead_code,
        reason = "Single-child extraction remains available to rewrite rules compiled in feature-specific targets."
    )]
    pub(crate) fn take_single_child(&mut self) -> LogicalPlanNode {
        assert_eq!(self.children.len(), 1, "expected one logical plan child");
        self.children.remove(0)
    }

    #[allow(
        dead_code,
        reason = "Two-child extraction remains available to rewrite rules compiled in feature-specific targets."
    )]
    pub(crate) fn take_two_children(&mut self) -> (LogicalPlanNode, LogicalPlanNode) {
        assert_eq!(self.children.len(), 2, "expected two logical plan children");
        let right = self.children.remove(1);
        let left = self.children.remove(0);
        (left, right)
    }

    #[allow(
        dead_code,
        reason = "Ownership-consuming extraction remains available to rewrite rules compiled in feature-specific targets."
    )]
    pub(crate) fn into_single_child(mut self) -> LogicalPlanNode {
        assert_eq!(self.children.len(), 1, "expected one logical plan child");
        self.children.remove(0)
    }
}

#[cfg(test)]
mod plan_tests {
    use super::*;
    use crate::planner::physical::TopNPhase;
    use crate::planner::physical::{PhysicalPlanKind, PhysicalTopNNode};

    fn empty_values_for_test() -> LogicalPlanNode {
        LogicalPlanNode::new(
            LogicalPlanKind::Values(PlanValuesNode {
                rows: vec![],
                columns: vec![],
            }),
            vec![],
            None,
        )
    }

    #[test]
    fn logical_plan_node_exposes_kind_and_children_uniformly() {
        let node = LogicalPlanNode::new(
            LogicalPlanKind::Project(PlanProjectNode {
                items: vec![],
                output_qualifier: None,
            }),
            vec![empty_values_for_test()],
            None,
        );

        assert!(matches!(node.kind, LogicalPlanKind::Project(_)));
        assert_eq!(node.children.len(), 1);
        assert!(node.required_output_columns.is_none());
    }

    #[test]
    fn logical_plan_node_uses_logical_kind() {
        fn accepts_logical_kind(_: &LogicalPlanKind) {}

        let node = empty_values_for_test();

        accepts_logical_kind(&node.kind);
    }

    #[test]
    fn imv_marker_keeps_input_in_children() {
        let node = LogicalPlanNode::new(
            LogicalPlanKind::ImvDelta(LogicalImvDeltaNode {
                is_root: true,
                action_column: Some(ColumnId::new_for_test(7)),
                branch_scope: None,
            }),
            vec![empty_values_for_test()],
            None,
        );
        match node.kind {
            LogicalPlanKind::ImvDelta(delta) => {
                assert!(delta.is_root);
                assert_eq!(delta.action_column, Some(ColumnId::new_for_test(7)));
            }
            other => panic!("expected ImvDelta, got {other:?}"),
        }
        assert_eq!(node.children.len(), 1);
        assert!(matches!(node.children[0].kind, LogicalPlanKind::Values(_)));
    }

    #[test]
    fn logical_aggregate_node_already_pushed_defaults_false_via_construction() {
        let node = LogicalAggregateNode {
            group_by: vec![],
            aggregates: vec![],
            output_columns: vec![],
            already_pushed: false,
        };
        assert!(!node.already_pushed);
    }

    #[test]
    fn wrapper_required_output_columns_defaults_none() {
        let node = LogicalPlanNode::new(
            LogicalPlanKind::Project(PlanProjectNode {
                items: vec![],
                output_qualifier: None,
            }),
            vec![empty_values_for_test()],
            None,
        );
        assert!(node.required_output_columns.is_none());
    }

    #[test]
    fn logical_union_node_carries_explicit_output_columns() {
        use crate::column_id::ColumnId;
        use arrow::datatypes::DataType;
        let cols = vec![OutputColumn {
            column_id: ColumnId::UNSET,
            name: "x".to_string(),
            data_type: DataType::Int32,
            nullable: false,
            is_internal: false,
        }];
        let node = LogicalUnionNode {
            all: true,
            output_columns: cols.clone(),
        };
        assert_eq!(node.output_columns.len(), 1);
        assert_eq!(node.output_columns[0].name, "x");
        assert_eq!(node.output_columns[0].data_type, DataType::Int32);
        assert!(!node.output_columns[0].nullable);
    }

    #[test]
    fn logical_intersect_node_carries_explicit_output_columns() {
        use crate::column_id::ColumnId;
        use arrow::datatypes::DataType;
        let cols = vec![OutputColumn {
            column_id: ColumnId::UNSET,
            name: "y".to_string(),
            data_type: DataType::Utf8,
            nullable: true,
            is_internal: false,
        }];
        let node = LogicalIntersectNode {
            output_columns: cols,
        };
        assert_eq!(node.output_columns.len(), 1);
        assert_eq!(node.output_columns[0].name, "y");
    }

    #[test]
    fn logical_except_node_carries_explicit_output_columns() {
        use crate::column_id::ColumnId;
        use arrow::datatypes::DataType;
        let cols = vec![OutputColumn {
            column_id: ColumnId::UNSET,
            name: "z".to_string(),
            data_type: DataType::Boolean,
            nullable: false,
            is_internal: false,
        }];
        let node = LogicalExceptNode {
            output_columns: cols,
        };
        assert_eq!(node.output_columns.len(), 1);
        assert_eq!(node.output_columns[0].name, "z");
    }

    #[test]
    fn logical_plan_kind_scan_carries_mv_rewrite_source() {
        let table = crate::planner::table::TableDef {
            name: "mv_orders".to_string(),
            columns: vec![],
            iceberg_row_lineage_metadata_columns: vec![],
            source: crate::compiler::mv_rewrite::test_scan_source(
                crate::planner::table::SqlScanKind::ConnectorRead,
            ),
        };
        let node = LogicalPlanKind::Scan(PlanScanNode {
            database: "default".to_string(),
            table,
            alias: None,
            columns: vec![],
            predicates: vec![],
            required_columns: None,
            variant_columns: vec![],
            mv_rewritten_from: Some(crate::planner::payload::MvRewriteSelection::unverified(
                "mv_orders_rollup".to_string(),
            )),
        });

        let LogicalPlanKind::Scan(scan) = node else {
            panic!("expected Scan");
        };
        assert_eq!(scan.mv_rewritten_from.as_deref(), Some("mv_orders_rollup"));
    }

    #[test]
    fn logical_limit_and_physical_topn_are_stage_split() {
        let limit = LogicalPlanKind::Limit(PlanLimitNode {
            limit: Some(10),
            offset: Some(3),
        });
        let topn = PhysicalPlanKind::TopN(PhysicalTopNNode {
            items: vec![],
            limit: Some(10),
            offset: Some(3),
            phase: TopNPhase::Final,
            is_split: false,
        });

        match limit {
            LogicalPlanKind::Limit(node) => {
                assert_eq!(node.limit, Some(10));
                assert_eq!(node.offset, Some(3));
            }
            other => panic!("expected Limit, got {other:?}"),
        }
        match topn {
            PhysicalPlanKind::TopN(node) => {
                assert_eq!(node.limit, Some(10));
                assert_eq!(node.offset, Some(3));
                assert!(!node.is_split);
            }
            other => panic!("expected TopN, got {other:?}"),
        }
    }

    #[test]
    fn logical_plan_kind_has_no_distributed_variants() {
        fn accepts_logical(_: LogicalPlanKind) {}

        accepts_logical(LogicalPlanKind::Values(PlanValuesNode {
            rows: vec![],
            columns: vec![],
        }));

        assert_eq!(
            [
                "TopN",
                "Exchange",
                "HashAggregate",
                "HashJoin",
                "NestLoopJoin",
                "SetOp",
            ]
            .iter()
            .filter(|name| LogicalPlanKind::variant_names_for_test().contains(name))
            .count(),
            0
        );
    }
}
