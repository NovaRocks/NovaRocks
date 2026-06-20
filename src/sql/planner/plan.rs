//! Logical Plan — a tree of relational algebra operators.
//!
//! This is the layer where a future optimizer would operate.
//! Expressions use [`TypedExpr`] from [`crate::sql::analysis`].

use std::collections::HashSet;

use arrow::datatypes::DataType;

use crate::sql::catalog::TableDef;

use crate::sql::analysis::cte::CteId;
use crate::sql::analysis::{JoinKind, OutputColumn, ProjectItem, SortItem, TypedExpr};
use crate::sql::column_id::ColumnId;
pub(crate) use crate::sql::common::{
    ApplyKind, DecodeMapping, ScanDictionaryColumn, ScanVariantColumn,
};
use crate::sql::optimizer::operator::{AggMode, JoinDistribution, TopNPhase};

// ---------------------------------------------------------------------------
// Logical plan tree
// ---------------------------------------------------------------------------

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct LogicalPlanNode {
    pub kind: PlanNodeKind,
    pub children: Vec<LogicalPlanNode>,
    /// Set by the Phase-1 column-pruning tagging pass; `None` means all columns required.
    pub required_output_columns: Option<HashSet<ColumnId>>,
}

// ---------------------------------------------------------------------------
// Unified planner-side plan node kind
// ---------------------------------------------------------------------------

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) enum PlanNodeKind {
    Scan(PlanScanNode),
    Filter(PlanFilterNode),
    Project(PlanProjectNode),
    Sort(PlanSortNode),
    /// Logical-stage payload only; distributed IR encodes limits on the wrapper,
    /// Sort/TopN offsets, or ExchangeFlavor::LimitOffset instead of a Limit kind.
    Limit(PlanLimitNode),
    Values(PlanValuesNode),
    Decode(PlanDecodeNode),
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
    CTEAnchor(LogicalCTEAnchorNode),
    CTEProduce(LogicalCTEProduceNode),
    CTEConsume(LogicalCTEConsumeNode),
    AggregateStateMerge(LogicalAggregateStateMergeNode),
    Apply(LogicalApplyNode),
    ImvDelta(LogicalImvDeltaNode),
    ImvVersion(LogicalImvVersionNode),
    TopN(DistributedTopNNode),
    Exchange(DistributedExchangeNode),
    HashAggregate(Box<DistributedHashAggregateNode>),
    HashJoin(Box<DistributedHashJoinNode>),
    NestLoopJoin(DistributedNestLoopJoinNode),
    SetOp(DistributedSetOpNode),
}

impl PlanNodeKind {
    pub(crate) fn variant_name(&self) -> &'static str {
        match self {
            PlanNodeKind::Scan(_) => "Scan",
            PlanNodeKind::Filter(_) => "Filter",
            PlanNodeKind::Project(_) => "Project",
            PlanNodeKind::Sort(_) => "Sort",
            PlanNodeKind::Limit(_) => "Limit",
            PlanNodeKind::Values(_) => "Values",
            PlanNodeKind::Decode(_) => "Decode",
            PlanNodeKind::Repeat(_) => "Repeat",
            PlanNodeKind::Window(_) => "Window",
            PlanNodeKind::GenerateSeries(_) => "GenerateSeries",
            PlanNodeKind::TableFunction(_) => "TableFunction",
            PlanNodeKind::AssertOneRow(_) => "AssertOneRow",
            PlanNodeKind::Aggregate(_) => "Aggregate",
            PlanNodeKind::Join(_) => "Join",
            PlanNodeKind::Union(_) => "Union",
            PlanNodeKind::Intersect(_) => "Intersect",
            PlanNodeKind::Except(_) => "Except",
            PlanNodeKind::CTEAnchor(_) => "CTEAnchor",
            PlanNodeKind::CTEProduce(_) => "CTEProduce",
            PlanNodeKind::CTEConsume(_) => "CTEConsume",
            PlanNodeKind::AggregateStateMerge(_) => "AggregateStateMerge",
            PlanNodeKind::Apply(_) => "Apply",
            PlanNodeKind::ImvDelta(_) => "ImvDelta",
            PlanNodeKind::ImvVersion(_) => "ImvVersion",
            PlanNodeKind::TopN(_) => "TopN",
            PlanNodeKind::Exchange(_) => "Exchange",
            PlanNodeKind::HashAggregate(_) => "HashAggregate",
            PlanNodeKind::HashJoin(_) => "HashJoin",
            PlanNodeKind::NestLoopJoin(_) => "NestLoopJoin",
            PlanNodeKind::SetOp(_) => "SetOp",
        }
    }
}

pub(crate) fn validate_logical_plan_stage(plan: &LogicalPlanNode) -> Result<(), String> {
    validate_logical_plan_stage_at(plan, "root")
}

fn validate_logical_plan_stage_at(plan: &LogicalPlanNode, path: &str) -> Result<(), String> {
    match &plan.kind {
        PlanNodeKind::TopN(_)
        | PlanNodeKind::Exchange(_)
        | PlanNodeKind::HashAggregate(_)
        | PlanNodeKind::HashJoin(_)
        | PlanNodeKind::NestLoopJoin(_)
        | PlanNodeKind::SetOp(_) => {
            return Err(format!(
                "distributed-only PlanNodeKind::{} is not valid in LogicalPlanNode at {path}",
                plan.kind.variant_name()
            ));
        }
        PlanNodeKind::Scan(scan) if scan.mv_rewritten_from.is_some() => {
            return Err(format!(
                "LogicalPlanNode at {path} has Scan.mv_rewritten_from set; \
                 MV rewrite source is a distributed-stage scan field"
            ));
        }
        PlanNodeKind::Sort(sort) if !sort.output_columns.is_empty() => {
            return Err(format!(
                "LogicalPlanNode at {path} has Sort.output_columns set; \
                 output_columns is a distributed-stage sort field"
            ));
        }
        PlanNodeKind::Sort(sort) if sort.offset.is_some() => {
            return Err(format!(
                "LogicalPlanNode at {path} has Sort.offset set; \
                 offset is a distributed-stage sort field"
            ));
        }
        PlanNodeKind::Repeat(repeat) if repeat.virtual_tuple_id.is_some() => {
            return Err(format!(
                "LogicalPlanNode at {path} has Repeat.virtual_tuple_id set; \
                 virtual_tuple_id is a distributed-stage repeat field"
            ));
        }
        _ => {}
    }

    for (idx, child) in plan.children.iter().enumerate() {
        validate_logical_plan_stage_at(child, &format!("{path}.children[{idx}]"))?;
    }
    Ok(())
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct PlanScanNode {
    pub database: String,
    pub table: TableDef,
    pub alias: Option<String>,
    pub columns: Vec<OutputColumn>,
    pub predicates: Vec<TypedExpr>,
    pub required_columns: Option<Vec<String>>,
    pub dict_columns: Vec<ScanDictionaryColumn>,
    pub variant_columns: Vec<ScanVariantColumn>,
    pub mv_rewritten_from: Option<String>,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct PlanFilterNode {
    pub predicate: TypedExpr,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct PlanProjectNode {
    pub items: Vec<ProjectItem>,
    pub output_qualifier: Option<String>,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct PlanSortNode {
    pub items: Vec<SortItem>,
    pub analytic_partition_by: Vec<TypedExpr>,
    pub output_columns: Vec<OutputColumn>,
    pub offset: Option<i64>,
    pub partition_limit: Option<usize>,
    pub topn_type: Option<crate::exec::node::sort::SortTopNType>,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct PlanLimitNode {
    pub limit: Option<i64>,
    pub offset: Option<i64>,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct PlanValuesNode {
    pub rows: Vec<Vec<TypedExpr>>,
    pub columns: Vec<OutputColumn>,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct PlanDecodeNode {
    pub mappings: Vec<DecodeMapping>,
    pub output_columns: Vec<OutputColumn>,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct PlanRepeatNode {
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

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct PlanWindowNode {
    pub window_exprs: Vec<WindowExpr>,
    pub output_columns: Vec<OutputColumn>,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct PlanGenerateSeriesNode {
    pub start: i64,
    pub end: i64,
    pub step: i64,
    pub column_name: String,
    pub alias: Option<String>,
    pub output_column_id: ColumnId,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct PlanTableFunctionNode {
    pub function_name: String,
    pub args: Vec<TypedExpr>,
    pub output_columns: Vec<OutputColumn>,
    pub alias: Option<String>,
    pub is_left_join: bool,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct PlanAssertOneRowNode {
    pub subquery_text: String,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct DistributedTopNNode {
    pub items: Vec<SortItem>,
    pub limit: Option<i64>,
    pub offset: Option<i64>,
    pub phase: TopNPhase,
    pub is_split: bool,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct DistributedExchangeNode {
    pub partition_type: crate::partitions::TPartitionType,
    pub partition_exprs: Vec<TypedExpr>,
    pub source_fragment_id: u32,
    pub output_columns: Vec<OutputColumn>,
    pub output_qualifier: Option<String>,
    pub flavor: ExchangeFlavor,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) enum ExchangeFlavor {
    Distribution,
    LimitOffset {
        limit: Option<i64>,
        offset: Option<i64>,
    },
    TopNSplit {
        items: Vec<SortItem>,
        limit: Option<i64>,
        offset: Option<i64>,
    },
    CteMulticast {
        cte_id: CteId,
    },
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct DistributedHashAggregateNode {
    pub mode: AggMode,
    pub group_by: Vec<TypedExpr>,
    pub aggregates: Vec<AggregateCall>,
    pub is_merge: Vec<bool>,
    pub output_columns: Vec<OutputColumn>,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct DistributedHashJoinNode {
    pub join_type: JoinKind,
    pub eq_conditions: Vec<DistributedHashJoinEqCondition>,
    pub other_condition: Option<TypedExpr>,
    pub distribution: JoinDistribution,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct DistributedHashJoinEqCondition {
    pub left: TypedExpr,
    pub right: TypedExpr,
    pub null_safe: bool,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct DistributedNestLoopJoinNode {
    pub join_type: JoinKind,
    pub condition: Option<TypedExpr>,
}

#[allow(dead_code)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PlanSetOpKind {
    UnionAll,
    Intersect,
    Except,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct DistributedSetOpNode {
    pub kind: PlanSetOpKind,
    pub output_columns: Vec<OutputColumn>,
    pub child_output_columns: Vec<Vec<OutputColumn>>,
}

pub(crate) type LogicalDecodeNode = PlanDecodeNode;

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct LogicalAggregateStateMergeNode {
    pub(crate) group_key_names: Vec<String>,
    pub(crate) aggregate_state_names: Vec<String>,
    pub(crate) change_op_column: String,
    pub(crate) output_columns: Vec<OutputColumn>,
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

pub(crate) type LogicalAssertOneRowNode = PlanAssertOneRowNode;
pub(crate) type LogicalRepeatNode = PlanRepeatNode;

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct LogicalCTEAnchorNode {
    pub cte_id: crate::sql::analysis::cte::CteId,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct LogicalCTEProduceNode {
    pub cte_id: crate::sql::analysis::cte::CteId,
    pub output_columns: Vec<crate::sql::analysis::OutputColumn>,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct LogicalCTEConsumeNode {
    pub cte_id: crate::sql::analysis::cte::CteId,
    pub alias: String,
    pub output_columns: Vec<crate::sql::analysis::OutputColumn>,
}

pub(crate) type LogicalWindowNode = PlanWindowNode;
pub(crate) type LogicalGenerateSeriesNode = PlanGenerateSeriesNode;
pub(crate) type LogicalTableFunctionNode = PlanTableFunctionNode;
pub(crate) type LogicalScanNode = PlanScanNode;
pub(crate) type LogicalValuesNode = PlanValuesNode;
pub(crate) type LogicalFilterNode = PlanFilterNode;
pub(crate) type LogicalProjectNode = PlanProjectNode;

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct LogicalAggregateNode {
    pub group_by: Vec<TypedExpr>,
    pub aggregates: Vec<AggregateCall>,
    pub output_columns: Vec<OutputColumn>,
    pub already_pushed: bool,
}

pub(crate) type LogicalSortNode = PlanSortNode;
pub(crate) type LogicalLimitNode = PlanLimitNode;

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
    pub branch_scope: Option<crate::sql::catalog::BranchScope>,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct LogicalImvVersionNode {
    pub version_ref: crate::sql::planner::imv_rewrite::marker::ImvVersionRef,
}

/// A single window function expression with its OVER specification.
#[derive(Clone, Debug)]
pub(crate) struct WindowExpr {
    pub name: String,
    pub args: Vec<TypedExpr>,
    pub distinct: bool,
    pub partition_by: Vec<TypedExpr>,
    pub order_by: Vec<SortItem>,
    pub window_frame: Option<crate::sql::analysis::WindowFrame>,
    pub result_type: DataType,
    /// Display label only (EXPLAIN / output schema). Identity is now
    /// `output_column_id`. (G1: `output_name` downgraded from a binding key.)
    pub output_name: String,
    /// G1: globally-unique id of this window function's output column.
    /// TODO(G1 P2/P3): remove this allow once parent Project/window references
    /// are rebound by id and downstream binding consumes the populated field.
    #[allow(dead_code)]
    pub output_column_id: crate::sql::column_id::ColumnId,
    /// `IGNORE NULLS` modifier. Currently honored by first_value / last_value
    /// / lead / lag; ignored for other window functions.
    pub ignore_nulls: bool,
}

#[derive(Clone, Debug)]
pub(crate) struct AggregateCall {
    pub name: String,
    pub args: Vec<TypedExpr>,
    pub distinct: bool,
    pub result_type: DataType,
    pub order_by: Vec<SortItem>,
    /// G1: id of THIS aggregate's output column. Planner-created calls are
    /// minted by `collect_aggregates`; rewrite paths should preserve existing
    /// ids or allocate ids for newly-defined aggregate outputs. Fixtures and
    /// transient adapters may use `UNSET` until they become executable
    /// bindings.
    pub output_column_id: crate::sql::column_id::ColumnId,
}

impl LogicalPlanNode {
    pub(crate) fn new(
        kind: PlanNodeKind,
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

    pub(crate) fn take_child(&mut self, index: usize) -> LogicalPlanNode {
        self.children.remove(index)
    }

    pub(crate) fn take_single_child(&mut self) -> LogicalPlanNode {
        assert_eq!(self.children.len(), 1, "expected one logical plan child");
        self.children.remove(0)
    }

    pub(crate) fn take_two_children(&mut self) -> (LogicalPlanNode, LogicalPlanNode) {
        assert_eq!(self.children.len(), 2, "expected two logical plan children");
        let right = self.children.remove(1);
        let left = self.children.remove(0);
        (left, right)
    }

    pub(crate) fn into_single_child(mut self) -> LogicalPlanNode {
        assert_eq!(self.children.len(), 1, "expected one logical plan child");
        self.children.remove(0)
    }
}

#[cfg(test)]
mod plan_tests {
    use super::*;

    fn empty_values_for_test() -> LogicalPlanNode {
        LogicalPlanNode::new(
            PlanNodeKind::Values(LogicalValuesNode {
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
            PlanNodeKind::Project(LogicalProjectNode {
                items: vec![],
                output_qualifier: None,
            }),
            vec![empty_values_for_test()],
            None,
        );

        assert!(matches!(node.kind, PlanNodeKind::Project(_)));
        assert_eq!(node.children.len(), 1);
        assert!(node.required_output_columns.is_none());
    }

    #[test]
    fn logical_plan_node_uses_unified_kind() {
        fn accepts_unified_kind(_: &PlanNodeKind) {}

        let node = empty_values_for_test();

        accepts_unified_kind(&node.kind);
    }

    #[test]
    fn imv_marker_keeps_input_in_children() {
        let node = LogicalPlanNode::new(
            PlanNodeKind::ImvDelta(LogicalImvDeltaNode {
                is_root: true,
                action_column: Some(ColumnId::new_for_test(7)),
                branch_scope: None,
            }),
            vec![empty_values_for_test()],
            None,
        );
        match node.kind {
            PlanNodeKind::ImvDelta(delta) => {
                assert!(delta.is_root);
                assert_eq!(delta.action_column, Some(ColumnId::new_for_test(7)));
            }
            other => panic!("expected ImvDelta, got {other:?}"),
        }
        assert_eq!(node.children.len(), 1);
        assert!(matches!(node.children[0].kind, PlanNodeKind::Values(_)));
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
            PlanNodeKind::Project(LogicalProjectNode {
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
        use crate::sql::column_id::ColumnId;
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
        use crate::sql::column_id::ColumnId;
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
        use crate::sql::column_id::ColumnId;
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
    fn logical_aggregate_state_merge_node_keeps_inputs_in_children() {
        use crate::sql::analysis::OutputColumn;
        use crate::sql::column_id::ColumnId;

        let old_input = empty_values_for_test();
        let delta_input = empty_values_for_test();
        let plan = LogicalPlanNode::new(
            PlanNodeKind::AggregateStateMerge(LogicalAggregateStateMergeNode {
                group_key_names: vec!["region".to_string()],
                aggregate_state_names: vec!["c".to_string(), "s".to_string()],
                change_op_column: "__change_op".to_string(),
                output_columns: vec![
                    OutputColumn {
                        column_id: ColumnId::new_for_test(1),
                        name: "region".to_string(),
                        data_type: arrow::datatypes::DataType::Utf8,
                        nullable: true,
                        is_internal: false,
                    },
                    OutputColumn {
                        column_id: ColumnId::new_for_test(2),
                        name: "c".to_string(),
                        data_type: arrow::datatypes::DataType::Int64,
                        nullable: true,
                        is_internal: false,
                    },
                ],
            }),
            vec![old_input, delta_input],
            None,
        );

        let PlanNodeKind::AggregateStateMerge(node) = plan.kind else {
            panic!("expected aggregate state merge");
        };
        assert_eq!(plan.children.len(), 2);
        assert_eq!(node.group_key_names, vec!["region"]);
        assert_eq!(node.aggregate_state_names, vec!["c", "s"]);
        assert_eq!(node.change_op_column, "__change_op");
        assert_eq!(node.output_columns.len(), 2);
    }

    #[test]
    fn unified_plan_node_kind_scan_carries_mv_rewrite_source() {
        let table = TableDef {
            name: "mv_orders".to_string(),
            columns: vec![],
            iceberg_row_lineage_metadata_columns: vec![],
            source: crate::sql::catalog::ScanSource::StarRocks {
                db_id: 1,
                table_id: 2,
            },
        };
        let node = PlanNodeKind::Scan(PlanScanNode {
            database: "default".to_string(),
            table,
            alias: None,
            columns: vec![],
            predicates: vec![],
            required_columns: None,
            dict_columns: vec![],
            variant_columns: vec![],
            mv_rewritten_from: Some("mv_orders_rollup".to_string()),
        });

        let PlanNodeKind::Scan(scan) = node else {
            panic!("expected Scan");
        };
        assert_eq!(scan.mv_rewritten_from.as_deref(), Some("mv_orders_rollup"));
    }

    #[test]
    fn unified_plan_node_kind_keeps_limit_and_topn_split_explicit() {
        let limit = PlanNodeKind::Limit(PlanLimitNode {
            limit: Some(10),
            offset: Some(3),
        });
        let topn = PlanNodeKind::TopN(DistributedTopNNode {
            items: vec![],
            limit: Some(10),
            offset: Some(3),
            phase: crate::sql::optimizer::operator::TopNPhase::Final,
            is_split: false,
        });

        match limit {
            PlanNodeKind::Limit(node) => {
                assert_eq!(node.limit, Some(10));
                assert_eq!(node.offset, Some(3));
            }
            other => panic!("expected Limit, got {other:?}"),
        }
        match topn {
            PlanNodeKind::TopN(node) => {
                assert_eq!(node.limit, Some(10));
                assert_eq!(node.offset, Some(3));
                assert!(!node.is_split);
            }
            other => panic!("expected TopN, got {other:?}"),
        }
    }

    #[test]
    fn unified_plan_node_kind_set_op_uses_plan_scoped_kind() {
        let set_op = PlanNodeKind::SetOp(DistributedSetOpNode {
            kind: PlanSetOpKind::UnionAll,
            output_columns: vec![],
            child_output_columns: vec![vec![], vec![]],
        });

        let PlanNodeKind::SetOp(node) = set_op else {
            panic!("expected SetOp");
        };
        assert_eq!(node.kind, PlanSetOpKind::UnionAll);
        assert_eq!(node.child_output_columns.len(), 2);
    }
}
