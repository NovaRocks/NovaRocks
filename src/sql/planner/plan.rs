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

#[derive(Clone, Debug)]
pub(crate) enum LogicalPlan {
    Scan(ScanNode),
    Filter(FilterNode),
    Project(ProjectNode),
    Aggregate(AggregateNode),
    Join(JoinNode),
    Sort(SortNode),
    Limit(LimitNode),
    Union(UnionNode),
    Intersect(IntersectNode),
    Except(ExceptNode),
    Values(ValuesNode),
    GenerateSeries(GenerateSeriesNode),
    TableFunction(TableFunctionNode),
    Window(WindowNode),
    /// Repeat node for ROLLUP/CUBE/GROUPING SETS.
    /// Replicates each input row N times with different null patterns.
    Repeat(RepeatPlanNode),
    /// Defines the scope of one CTE. The left child is the producer subtree;
    /// the right child is the query subtree that may consume it.
    CTEAnchor(CTEAnchorNode),
    /// Produces the analyzed CTE definition.
    CTEProduce(CTEProduceNode),
    /// Reference to a CTE definition. Leaf node.
    CTEConsume(CTEConsumeNode),
    /// Low-cardinality dictionary decode: rewrites string columns to their
    /// dictionary-encoded form upstream and decodes back to strings before
    /// emission. Inserted by the dictionary-rewrite optimizer rule (Task 7);
    /// today no optimizer pass produces this variant — Task 5 only adds the
    /// type-system plumbing.
    Decode(DecodeNode),
    /// Logical IMV aggregate-state reconciliation over old target state and
    /// delta state. Execution lowering is added by later tasks.
    AggregateStateMerge(AggregateStateMergeNode),
    /// Subquery glue node (outer ⋈ subquery). Eliminated by the
    /// SubqueryRewrite stage; see ApplyNode.
    Apply(ApplyNode),
    /// At-most-one-row runtime guard for scalar subqueries.
    AssertOneRow(AssertOneRowNode),
    /// IMV marker: "compute the incremental of input". Emitted by the
    /// `imv-delta-marker` stage; rejected by `imv-validation` if not
    /// consumed. Must never reach physical lowering. See
    /// `src/sql/optimizer/rewrite/imv/marker.rs`.
    ImvDelta(crate::sql::optimizer::rewrite::imv::marker::ImvDeltaNode),
    /// IMV marker: "scan input over a snapshot window". Emitted by task 4
    /// scan-binding rules; consumed before lowering. Same panic-on-leak
    /// rule as `ImvDelta`.
    // PR-β scaffolding: task 4 constructs ImvVersion during scan-binding;
    // the variant exists here so the type is wired through the plan tree.
    #[allow(dead_code)]
    ImvVersion(crate::sql::optimizer::rewrite::imv::marker::ImvVersionNode),
}

#[derive(Clone, Debug)]
pub(crate) struct DecodeNode {
    pub input: Box<LogicalPlan>,
    pub mappings: Vec<DecodeMapping>,
    /// Output columns this Decode exposes upward. Mirrors the input's
    /// output columns with each `dict_column` swapped for its
    /// `string_column`. Populated by the rewrite rule that inserts
    /// Decode (Task 7) and preserved by every downstream pass. The
    /// optimizer's `derive_output_columns` returns this verbatim — without
    /// it the parent group would observe the child's `dict_column` name
    /// rather than `string_column`.
    pub output_columns: Vec<OutputColumn>,
    /// Set by the Phase-1 column-pruning tagging pass; `None` means all columns required.
    pub required_output_columns: Option<HashSet<ColumnId>>,
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

#[derive(Clone, Debug)]
pub(crate) struct AggregateStateMergeNode {
    pub(crate) old_input: Box<LogicalPlan>,
    pub(crate) delta_input: Box<LogicalPlan>,
    pub(crate) group_key_names: Vec<String>,
    pub(crate) aggregate_state_names: Vec<String>,
    pub(crate) change_op_column: String,
    pub(crate) output_columns: Vec<OutputColumn>,
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

/// Subquery glue node: left child = outer plan, right child = subquery plan.
/// Built by the planner from analyzer-collected subquery metadata (M1);
/// rewritten into join / aggregate / window shapes by the optimizer's
/// SubqueryRewrite stage. Must never survive past that stage — the
/// ApplyException rule and the optimize() backstop enforce this, and
/// memo conversion panics on a leaked Apply as defence in depth.
/// Field semantics mirror StarRocks LogicalApplyOperator; see the design doc
/// docs/design/specs/2026-06-10-apply-correlated-subquery-framework-design.md §5.1.
/// M1 consumes the remaining fields; remove the allow then.
#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct ApplyNode {
    pub left: Box<LogicalPlan>,
    /// Subquery plan. May reference outer columns from
    /// `correlation_column_ids` while the Apply is alive.
    pub right: Box<LogicalPlan>,
    pub kind: ApplyKind,
    /// The expression the Apply was built from, written over the inner plan's
    /// output columns (`lhs IN (inner_col)`, `EXISTS(inner_col)`, or a bare
    /// `ColumnRef(inner_col)` for scalar subqueries).
    pub subquery_expr: TypedExpr,
    /// Fresh column standing in for the subquery's value in outer expressions.
    pub output_column: OutputColumn,
    /// Outer-side columns referenced inside the subquery.
    pub correlation_column_ids: Vec<ColumnId>,
    /// Correlated conjuncts hoisted out of the inner plan by the
    /// SubqueryRewrite push-down rules (empty at construction).
    pub correlation_conjuncts: Vec<TypedExpr>,
    /// Uncorrelated residual predicate hoisted out of the inner plan.
    pub residual_predicate: Option<TypedExpr>,
    /// Scalar only: the subquery must still be runtime-checked to <= 1 row.
    pub need_check_max_rows: bool,
    /// True iff the subquery sits as a top-level AND conjunct of
    /// WHERE / HAVING / JOIN-ON, so it may collapse into a semi/anti join.
    pub use_semi_anti: bool,
    /// For uncorrelated scalar subqueries used inside a predicate: the outer
    /// sibling columns of that predicate (drives left-side Apply push-down).
    pub uncorrelated_outer_predicate_columns: HashSet<ColumnId>,
    /// Set by the Phase-1 column-pruning tagging pass; `None` means all columns required.
    pub required_output_columns: Option<HashSet<ColumnId>>,
}

/// Runtime guard asserting its input yields at most one row (SQL scalar
/// subquery cardinality rule). Lowered to thrift ASSERT_NUM_ROWS_NODE; the
/// exec operator and FE-compat lowering already exist. Must not be reordered
/// with Limit (a LIMIT above would mask the multi-row error).
/// M1 produces this node from ScalarApplyToJoin; remove the allow then.
#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct AssertOneRowNode {
    pub input: Box<LogicalPlan>,
    /// Original subquery text used in the runtime error message.
    pub subquery_text: String,
    /// Set by the Phase-1 column-pruning tagging pass; `None` means all columns required.
    pub required_output_columns: Option<HashSet<ColumnId>>,
}

/// Repeat node for ROLLUP/CUBE/GROUPING SETS.
/// Replicates each input row N times with different null patterns.
#[derive(Clone, Debug)]
pub(crate) struct RepeatPlanNode {
    pub input: Box<LogicalPlan>,
    pub repeat_column_ref_list: Vec<Vec<String>>,
    pub repeat_column_ref_ids: Vec<Vec<ColumnId>>,
    pub grouping_ids: Vec<u64>,
    pub all_rollup_columns: Vec<String>,
    pub all_rollup_column_ids: Vec<ColumnId>,
    pub grouping_key_aliases: Vec<(String, String)>,
    pub grouping_fn_args: Vec<(String, Vec<String>)>,
    pub grouping_fn_arg_ids: Vec<Vec<ColumnId>>,
    pub grouping_fn_ids: Vec<(String, ColumnId)>,
    /// Set by the Phase-1 column-pruning tagging pass; `None` means all columns required.
    pub required_output_columns: Option<HashSet<ColumnId>>,
}

#[derive(Clone, Debug)]
pub(crate) struct CTEAnchorNode {
    pub cte_id: crate::sql::analysis::cte::CteId,
    pub produce: Box<LogicalPlan>,
    pub consumer: Box<LogicalPlan>,
    /// Set by the Phase-1 column-pruning tagging pass; `None` means all columns required.
    pub required_output_columns: Option<HashSet<ColumnId>>,
}

#[derive(Clone, Debug)]
pub(crate) struct CTEProduceNode {
    pub cte_id: crate::sql::analysis::cte::CteId,
    pub input: Box<LogicalPlan>,
    pub output_columns: Vec<crate::sql::analysis::OutputColumn>,
    /// Set by the Phase-1 column-pruning tagging pass; `None` means all columns required.
    pub required_output_columns: Option<HashSet<ColumnId>>,
}

#[derive(Clone, Debug)]
pub(crate) struct CTEConsumeNode {
    pub cte_id: crate::sql::analysis::cte::CteId,
    pub alias: String,
    pub output_columns: Vec<crate::sql::analysis::OutputColumn>,
    /// Set by the Phase-1 column-pruning tagging pass; `None` means all columns required.
    pub required_output_columns: Option<HashSet<ColumnId>>,
}

/// Analytic/window function evaluation node.
#[derive(Clone, Debug)]
pub(crate) struct WindowNode {
    pub input: Box<LogicalPlan>,
    pub window_exprs: Vec<WindowExpr>,
    /// All output columns: base columns from input + window function results.
    pub output_columns: Vec<OutputColumn>,
    /// Set by the Phase-1 column-pruning tagging pass; `None` means all columns required.
    pub required_output_columns: Option<HashSet<ColumnId>>,
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

/// Inline table function: `TABLE(generate_series(start, end, step))`.
/// Emitted as a TABLE_FUNCTION_NODE over a one-row parameter input.
#[derive(Clone, Debug)]
pub(crate) struct GenerateSeriesNode {
    pub start: i64,
    pub end: i64,
    pub step: i64,
    pub column_name: String,
    pub alias: Option<String>,
    pub output_column_id: ColumnId,
    /// Set by the Phase-1 column-pruning tagging pass; `None` means all columns required.
    pub required_output_columns: Option<HashSet<ColumnId>>,
}

/// Lateral table function evaluation over each input row.
#[derive(Clone, Debug)]
pub(crate) struct TableFunctionNode {
    pub input: Box<LogicalPlan>,
    pub function_name: String,
    pub args: Vec<TypedExpr>,
    pub output_columns: Vec<OutputColumn>,
    pub alias: Option<String>,
    pub is_left_join: bool,
    /// Set by the Phase-1 column-pruning tagging pass; `None` means all columns required.
    pub required_output_columns: Option<HashSet<ColumnId>>,
}

// ---------------------------------------------------------------------------
// Leaf nodes
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub(crate) struct ScanNode {
    pub database: String,
    pub table: TableDef,
    pub alias: Option<String>,
    pub columns: Vec<OutputColumn>,
    /// Predicates pushed down from Filter nodes by the optimizer.
    pub predicates: Vec<TypedExpr>,
    /// Columns actually required by upstream operators (set by column pruning).
    /// `None` means all columns are required (no pruning applied).
    pub required_columns: Option<Vec<String>>,
    /// Per-scan dictionary plan hints. Populated by the Task 7
    /// `LowCardinalityDictionaryRewrite` rule when a string column on
    /// this scan is eligible for low-cardinality rewriting. Empty
    /// everywhere else. Mirrored onto `LogicalScanOp` and
    /// `PhysicalScanOp` by memo conversion and the `ScanToPhysical`
    /// implementation rule.
    pub dict_columns: Vec<ScanDictionaryColumn>,
    /// Set by the Phase-1 column-pruning tagging pass; `None` means all columns required.
    pub required_output_columns: Option<HashSet<ColumnId>>,
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

#[derive(Clone, Debug)]
pub(crate) struct ValuesNode {
    pub rows: Vec<Vec<TypedExpr>>,
    pub columns: Vec<OutputColumn>,
    /// Set by the Phase-1 column-pruning tagging pass; `None` means all columns required.
    pub required_output_columns: Option<HashSet<ColumnId>>,
}

// ---------------------------------------------------------------------------
// Unary nodes (single input)
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub(crate) struct FilterNode {
    pub input: Box<LogicalPlan>,
    pub predicate: TypedExpr,
    /// Set by the Phase-1 column-pruning tagging pass; `None` means all columns required.
    pub required_output_columns: Option<HashSet<ColumnId>>,
}

#[derive(Clone, Debug)]
pub(crate) struct ProjectNode {
    pub input: Box<LogicalPlan>,
    pub items: Vec<ProjectItem>,
    pub output_qualifier: Option<String>,
    /// Set by the Phase-1 column-pruning tagging pass; `None` means all columns required.
    pub required_output_columns: Option<HashSet<ColumnId>>,
}

#[derive(Clone, Debug)]
pub(crate) struct AggregateNode {
    pub input: Box<LogicalPlan>,
    pub group_by: Vec<TypedExpr>,
    pub aggregates: Vec<AggregateCall>,
    pub output_columns: Vec<OutputColumn>,
    /// Set to true by `AggregatePushdownRule`'s rewriter on the FINAL
    /// (top-level) aggregate after a partial aggregate has been spliced
    /// below. The collector treats `already_pushed = true` as a hard
    /// "skip" signal so the rule does not re-fire on its own output.
    /// Other rules (predicate pushdown, column pruning, cte rewrite,
    /// etc.) MUST preserve this flag when cloning `AggregateNode`.
    pub already_pushed: bool,
    /// Set by the Phase-1 column-pruning tagging pass; `None` means all columns required.
    pub required_output_columns: Option<HashSet<ColumnId>>,
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

#[derive(Clone, Debug)]
pub(crate) struct SortNode {
    pub input: Box<LogicalPlan>,
    pub items: Vec<SortItem>,
    /// Populated by `build_window_and_project` when this Sort was inserted
    /// as a precursor to a Window operator (PARTITION BY ...). Carries the
    /// window's partition_by columns, which become the analytic-partition
    /// tag on the downstream LogicalSortOp / PhysicalSortOp / TSortNode.
    /// Empty for top-level `ORDER BY` sorts.
    pub analytic_partition_by: Vec<TypedExpr>,
    /// Set by the Phase-1 column-pruning tagging pass; `None` means all columns required.
    pub required_output_columns: Option<HashSet<ColumnId>>,
}

#[derive(Clone, Debug)]
pub(crate) struct LimitNode {
    pub input: Box<LogicalPlan>,
    pub limit: Option<i64>,
    pub offset: Option<i64>,
    /// Set by the Phase-1 column-pruning tagging pass; `None` means all columns required.
    pub required_output_columns: Option<HashSet<ColumnId>>,
}

// ---------------------------------------------------------------------------
// Binary nodes
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub(crate) struct JoinNode {
    pub left: Box<LogicalPlan>,
    pub right: Box<LogicalPlan>,
    pub join_type: JoinKind,
    /// `None` for CROSS JOIN.
    pub condition: Option<TypedExpr>,
    /// Set by the Phase-1 column-pruning tagging pass; `None` means all columns required.
    pub required_output_columns: Option<HashSet<ColumnId>>,
}

// ---------------------------------------------------------------------------
// N-ary set operation nodes
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub(crate) struct UnionNode {
    pub inputs: Vec<LogicalPlan>,
    /// `true` = UNION ALL, `false` = UNION DISTINCT.
    pub all: bool,
    /// Position-aligned output schema. Column at index `i` describes the
    /// union's output slot at position `i`, using the first branch's
    /// ColumnId. Populated at planner construction time so that future
    /// column-pruning passes (Gap 4) can map parent ColumnId requests to
    /// branch positions without descending into inputs.
    pub output_columns: Vec<OutputColumn>,
    /// Set by the Phase-1 column-pruning tagging pass; `None` means all columns required.
    pub required_output_columns: Option<HashSet<ColumnId>>,
}

#[derive(Clone, Debug)]
pub(crate) struct IntersectNode {
    pub inputs: Vec<LogicalPlan>,
    /// Position-aligned output schema. Same semantics as `UnionNode::output_columns`.
    pub output_columns: Vec<OutputColumn>,
    /// Set by the Phase-1 column-pruning tagging pass; `None` means all columns required.
    pub required_output_columns: Option<HashSet<ColumnId>>,
}

#[derive(Clone, Debug)]
pub(crate) struct ExceptNode {
    pub inputs: Vec<LogicalPlan>,
    /// Position-aligned output schema. Same semantics as `UnionNode::output_columns`.
    pub output_columns: Vec<OutputColumn>,
    /// Set by the Phase-1 column-pruning tagging pass; `None` means all columns required.
    pub required_output_columns: Option<HashSet<ColumnId>>,
}

#[cfg(test)]
mod plan_tests {
    use super::*;

    #[test]
    fn aggregate_node_already_pushed_defaults_false_via_construction() {
        let node = AggregateNode {
            input: Box::new(LogicalPlan::Values(ValuesNode {
                rows: vec![],
                columns: vec![],
                required_output_columns: None,
            })),
            group_by: vec![],
            aggregates: vec![],
            output_columns: vec![],
            already_pushed: false,
            required_output_columns: None,
        };
        assert!(!node.already_pushed);
    }

    #[test]
    fn project_node_required_output_columns_defaults_none() {
        // Construct a ProjectNode with a minimal Values input and assert
        // that required_output_columns is None on a freshly-built node.
        let node = ProjectNode {
            input: Box::new(LogicalPlan::Values(ValuesNode {
                rows: vec![],
                columns: vec![],
                required_output_columns: None,
            })),
            items: vec![],
            output_qualifier: None,
            required_output_columns: None,
        };
        assert!(node.required_output_columns.is_none());
    }

    #[test]
    fn union_node_carries_explicit_output_columns() {
        use crate::sql::column_id::ColumnId;
        use arrow::datatypes::DataType;
        let cols = vec![OutputColumn {
            column_id: ColumnId::UNSET,
            name: "x".to_string(),
            data_type: DataType::Int32,
            nullable: false,
            is_internal: false,
        }];
        let node = UnionNode {
            inputs: vec![],
            all: true,
            output_columns: cols.clone(),
            required_output_columns: None,
        };
        assert_eq!(node.output_columns.len(), 1);
        assert_eq!(node.output_columns[0].name, "x");
        assert_eq!(node.output_columns[0].data_type, DataType::Int32);
        assert!(!node.output_columns[0].nullable);
    }

    #[test]
    fn intersect_node_carries_explicit_output_columns() {
        use crate::sql::column_id::ColumnId;
        use arrow::datatypes::DataType;
        let cols = vec![OutputColumn {
            column_id: ColumnId::UNSET,
            name: "y".to_string(),
            data_type: DataType::Utf8,
            nullable: true,
            is_internal: false,
        }];
        let node = IntersectNode {
            inputs: vec![],
            output_columns: cols,
            required_output_columns: None,
        };
        assert_eq!(node.output_columns.len(), 1);
        assert_eq!(node.output_columns[0].name, "y");
    }

    #[test]
    fn except_node_carries_explicit_output_columns() {
        use crate::sql::column_id::ColumnId;
        use arrow::datatypes::DataType;
        let cols = vec![OutputColumn {
            column_id: ColumnId::UNSET,
            name: "z".to_string(),
            data_type: DataType::Boolean,
            nullable: false,
            is_internal: false,
        }];
        let node = ExceptNode {
            inputs: vec![],
            output_columns: cols,
            required_output_columns: None,
        };
        assert_eq!(node.output_columns.len(), 1);
        assert_eq!(node.output_columns[0].name, "z");
    }

    #[test]
    fn aggregate_state_merge_node_preserves_inputs_and_output_columns() {
        use crate::sql::analysis::OutputColumn;
        use crate::sql::column_id::ColumnId;

        fn empty_values_for_test() -> LogicalPlan {
            LogicalPlan::Values(ValuesNode {
                rows: vec![],
                columns: vec![],
                required_output_columns: None,
            })
        }

        let old_input = empty_values_for_test();
        let delta_input = empty_values_for_test();
        let node = AggregateStateMergeNode {
            old_input: Box::new(old_input),
            delta_input: Box::new(delta_input),
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
        };

        assert_eq!(node.group_key_names, vec!["region"]);
        assert_eq!(node.aggregate_state_names, vec!["c", "s"]);
        assert_eq!(node.change_op_column, "__change_op");
        assert_eq!(node.output_columns.len(), 2);
    }
}
