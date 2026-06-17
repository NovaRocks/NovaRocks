//! Logical Plan — a tree of relational algebra operators.
//!
//! This is the layer where a future optimizer would operate.
//! Expressions use [`TypedExpr`] from [`crate::sql::analysis`].

use std::collections::HashSet;

use arrow::datatypes::DataType;

use crate::sql::catalog::TableDef;

use crate::sql::analysis::{JoinKind, OutputColumn, ProjectItem, SortItem, TypedExpr};
use crate::sql::column_id::ColumnId;

// ---------------------------------------------------------------------------
// Logical plan tree
// ---------------------------------------------------------------------------

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct LogicalPlanNode {
    pub kind: LogicalPlanNodeKind,
    pub children: Vec<LogicalPlanNode>,
    /// Set by the Phase-1 column-pruning tagging pass; `None` means all columns required.
    pub required_output_columns: Option<HashSet<ColumnId>>,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) enum LogicalPlanNodeKind {
    Scan(LogicalScanNode),
    Filter(LogicalFilterNode),
    Project(LogicalProjectNode),
    Aggregate(LogicalAggregateNode),
    Join(LogicalJoinNode),
    Sort(LogicalSortNode),
    Limit(LogicalLimitNode),
    Union(LogicalUnionNode),
    Intersect(LogicalIntersectNode),
    Except(LogicalExceptNode),
    Values(LogicalValuesNode),
    GenerateSeries(LogicalGenerateSeriesNode),
    TableFunction(LogicalTableFunctionNode),
    Window(LogicalWindowNode),
    /// Repeat node for ROLLUP/CUBE/GROUPING SETS.
    /// Replicates each input row N times with different null patterns.
    Repeat(LogicalRepeatNode),
    /// Defines the scope of one CTE. The left child is the producer subtree;
    /// the right child is the query subtree that may consume it.
    CTEAnchor(LogicalCTEAnchorNode),
    /// Produces the analyzed CTE definition.
    CTEProduce(LogicalCTEProduceNode),
    /// Reference to a CTE definition. Leaf node.
    CTEConsume(LogicalCTEConsumeNode),
    /// Low-cardinality dictionary decode: rewrites string columns to their
    /// dictionary-encoded form upstream and decodes back to strings before
    /// emission. Inserted by the dictionary-rewrite optimizer rule (Task 7);
    /// today no optimizer pass produces this variant — Task 5 only adds the
    /// type-system plumbing.
    Decode(LogicalDecodeNode),
    /// Logical IMV aggregate-state reconciliation over old target state and
    /// delta state. Execution lowering is added by later tasks.
    AggregateStateMerge(LogicalAggregateStateMergeNode),
    /// Subquery glue node (outer ⋈ subquery). Eliminated by the
    /// SubqueryRewrite stage; see LogicalApplyNode.
    Apply(LogicalApplyNode),
    /// At-most-one-row runtime guard for scalar subqueries.
    AssertOneRow(LogicalAssertOneRowNode),
    /// IMV marker: "compute the incremental of input". Emitted by the
    /// `imv-delta-marker` stage; rejected by `imv-validation` if not
    /// consumed. Must never reach physical lowering. See
    /// `src/sql/optimizer/rewrite/imv/marker.rs`.
    ImvDelta(LogicalImvDeltaNode),
    /// IMV marker: "scan input over a snapshot window". Emitted by task 4
    /// scan-binding rules; consumed before lowering. Same panic-on-leak
    /// rule as `ImvDelta`.
    // PR-β scaffolding: task 4 constructs ImvVersion during scan-binding;
    // the variant exists here so the type is wired through the plan tree.
    #[allow(dead_code)]
    ImvVersion(LogicalImvVersionNode),
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct LogicalDecodeNode {
    pub mappings: Vec<DecodeMapping>,
    pub output_columns: Vec<OutputColumn>,
}

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

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct LogicalAssertOneRowNode {
    pub subquery_text: String,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct LogicalRepeatNode {
    pub repeat_column_ref_list: Vec<Vec<String>>,
    pub repeat_column_ref_ids: Vec<Vec<ColumnId>>,
    pub grouping_ids: Vec<u64>,
    pub all_rollup_columns: Vec<String>,
    pub all_rollup_column_ids: Vec<ColumnId>,
    pub grouping_key_aliases: Vec<(String, String)>,
    pub grouping_fn_args: Vec<(String, Vec<String>)>,
    pub grouping_fn_arg_ids: Vec<Vec<ColumnId>>,
    pub grouping_fn_ids: Vec<(String, ColumnId)>,
}

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

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct LogicalWindowNode {
    pub window_exprs: Vec<WindowExpr>,
    pub output_columns: Vec<OutputColumn>,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct LogicalGenerateSeriesNode {
    pub start: i64,
    pub end: i64,
    pub step: i64,
    pub column_name: String,
    pub alias: Option<String>,
    pub output_column_id: ColumnId,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct LogicalTableFunctionNode {
    pub function_name: String,
    pub args: Vec<TypedExpr>,
    pub output_columns: Vec<OutputColumn>,
    pub alias: Option<String>,
    pub is_left_join: bool,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct LogicalScanNode {
    pub database: String,
    pub table: TableDef,
    pub alias: Option<String>,
    pub columns: Vec<OutputColumn>,
    pub predicates: Vec<TypedExpr>,
    pub required_columns: Option<Vec<String>>,
    pub dict_columns: Vec<ScanDictionaryColumn>,
    pub variant_columns: Vec<ScanVariantColumn>,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct LogicalValuesNode {
    pub rows: Vec<Vec<TypedExpr>>,
    pub columns: Vec<OutputColumn>,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct LogicalFilterNode {
    pub predicate: TypedExpr,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct LogicalProjectNode {
    pub items: Vec<ProjectItem>,
    pub output_qualifier: Option<String>,
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
pub(crate) struct LogicalSortNode {
    pub items: Vec<SortItem>,
    /// Populated by `build_window_and_project` when this Sort was inserted
    /// as a precursor to a Window operator (PARTITION BY ...). Carries the
    /// window's partition_by columns, which become the analytic-partition
    /// tag on the downstream SortOp / SortOp / TSortNode.
    /// Empty for top-level `ORDER BY` sorts.
    pub analytic_partition_by: Vec<TypedExpr>,
    /// Set by RankingWindowPredicatePushdown: per-partition rank cap + ranking
    /// kind. `None` means ordinary sort. See OQ-13 ranking-window design spec.
    pub partition_limit: Option<usize>,
    pub topn_type: Option<crate::exec::node::sort::SortTopNType>,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct LogicalLimitNode {
    pub limit: Option<i64>,
    pub offset: Option<i64>,
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
    pub branch_scope: Option<crate::sql::catalog::BranchScope>,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct LogicalImvVersionNode {
    pub version_ref: crate::sql::optimizer::rewrite::imv::marker::ImvVersionRef,
}

/// Per-column mapping from the dictionary-encoded slot back to the original
/// string slot. `dict_column` is the input column produced by the upstream
/// dict-encoded plan; `string_column` is the string output exposed to the
/// rest of the plan.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct DecodeMapping {
    pub source_column_id: ColumnId,
    pub output_column_id: ColumnId,
    pub dict_column: String,
    pub string_column: String,
}

/// What the subquery expression looks like to its enclosing clause.
/// M1 consumes the non-Scalar variants; remove the allow then.
#[allow(dead_code)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ApplyKind {
    Scalar,
    Exists { negated: bool },
    In { negated: bool },
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

/// Plan hint for a single dict-encoded string column on a scan.
/// `source_column` is the original string column name in the scan
/// output; `dict_column` is the synthetic `Int32` slot name introduced
/// by the rewrite rule; `dictionary` is the snapshot whose `(id, bytes)`
/// pairs become a `TGlobalDict` payload at codegen time.
#[derive(Clone, Debug)]
pub(crate) struct ScanDictionaryColumn {
    pub source_column: String,
    pub dict_column: String,
    pub dictionary: std::sync::Arc<crate::engine::dictionary::model::DictionarySnapshot>,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct ScanVariantColumn {
    pub source_column_id: ColumnId,
    pub source_column: String,
    pub synthetic_column_id: ColumnId,
    pub synthetic_column: String,
    pub canonical_path: String,
    pub requested_type: DataType,
    pub strict: bool,
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
        kind: LogicalPlanNodeKind,
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
            LogicalPlanNodeKind::Values(LogicalValuesNode {
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
            LogicalPlanNodeKind::Project(LogicalProjectNode {
                items: vec![],
                output_qualifier: None,
            }),
            vec![empty_values_for_test()],
            None,
        );

        assert!(matches!(node.kind, LogicalPlanNodeKind::Project(_)));
        assert_eq!(node.children.len(), 1);
        assert!(node.required_output_columns.is_none());
    }

    #[test]
    fn imv_marker_keeps_input_in_children() {
        let node = LogicalPlanNode::new(
            LogicalPlanNodeKind::ImvDelta(LogicalImvDeltaNode {
                is_root: true,
                action_column: Some(ColumnId::new_for_test(7)),
                branch_scope: None,
            }),
            vec![empty_values_for_test()],
            None,
        );
        match node.kind {
            LogicalPlanNodeKind::ImvDelta(delta) => {
                assert!(delta.is_root);
                assert_eq!(delta.action_column, Some(ColumnId::new_for_test(7)));
            }
            other => panic!("expected ImvDelta, got {other:?}"),
        }
        assert_eq!(node.children.len(), 1);
        assert!(matches!(
            node.children[0].kind,
            LogicalPlanNodeKind::Values(_)
        ));
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
            LogicalPlanNodeKind::Project(LogicalProjectNode {
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
            LogicalPlanNodeKind::AggregateStateMerge(LogicalAggregateStateMergeNode {
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

        let LogicalPlanNodeKind::AggregateStateMerge(node) = plan.kind else {
            panic!("expected aggregate state merge");
        };
        assert_eq!(plan.children.len(), 2);
        assert_eq!(node.group_key_names, vec!["region"]);
        assert_eq!(node.aggregate_state_names, vec!["c", "s"]);
        assert_eq!(node.change_op_column, "__change_op");
        assert_eq!(node.output_columns.len(), 2);
    }
}
