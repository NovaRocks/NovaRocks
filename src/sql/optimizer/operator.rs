//! Operator enum for the Cascades optimizer.
//!
//! Logical operators mirror `LogicalPlanNode` node fields minus child references
//! (children are represented as `GroupId`s in `MExpr`).
//! Physical operators add physical execution decisions (distribution, agg mode).

use std::collections::HashSet;

use crate::sql::analysis::cte::CteId;
use crate::sql::analysis::{JoinKind, OutputColumn, WindowFrame};
use crate::sql::catalog::{BranchScope, TableDef};
use crate::sql::column_id::ColumnId;
use crate::sql::optimizer::scalar::{ColumnDisplay, ScalarId, SortKey};
use crate::sql::planner::imv_rewrite::marker::ImvVersionRef;
use crate::sql::planner::plan::{ApplyKind, DecodeMapping};

pub(crate) use crate::sql::planner::plan::{ScanDictionaryColumn, ScanVariantColumn};

// ---------------------------------------------------------------------------
// Physical decision enums
// ---------------------------------------------------------------------------

#[allow(dead_code)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum JoinDistribution {
    Unknown,
    Shuffle,
    Broadcast,
    Colocate,
}

#[allow(dead_code)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum AggMode {
    Single,
    Local,
    Global,
    /// Dedup by distinct-column + merge non-DISTINCT aggregate states across
    /// instances. Used as the shuffle-receive phase of 3- and 4-phase DISTINCT
    /// aggregation.
    DistinctGlobal,
    /// Per-instance scalar rollup of DISTINCT_GLOBAL output — emits
    /// `count(x)` (update) for each DISTINCT call and merges threaded
    /// non-DISTINCT states. Only used in 4-phase (scalar DISTINCT).
    DistinctLocal,
}

#[allow(dead_code)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub(crate) enum TopNPhase {
    Partial,
    #[default]
    Final,
}

// ---------------------------------------------------------------------------
// Logical operator structs
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum AggStage {
    Single,
    Local,
    Global,
}

impl AggStage {
    pub(crate) fn to_physical_mode(self) -> AggMode {
        match self {
            AggStage::Single => AggMode::Single,
            AggStage::Local => AggMode::Local,
            AggStage::Global => AggMode::Global,
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ScalarProjectItem {
    pub expr: ScalarId,
    pub output_name: String,
    pub output_column_id: ColumnId,
    pub expr_display: Option<ColumnDisplay>,
}

#[derive(Clone, Debug)]
pub(crate) struct ScalarAggregateSpec {
    pub name: String,
    pub args: Vec<ScalarId>,
    pub distinct: bool,
    pub order_by: Vec<SortKey>,
}

#[derive(Clone, Debug)]
pub(crate) struct ScalarWindowSpec {
    pub name: String,
    pub args: Vec<ScalarId>,
    pub distinct: bool,
    pub partition_by: Vec<ScalarId>,
    pub order_by: Vec<SortKey>,
    pub window_frame: Option<WindowFrame>,
    pub ignore_nulls: bool,
}

#[derive(Clone, Debug)]
pub(crate) struct ScanOp {
    pub database: String,
    pub table: TableDef,
    pub alias: Option<String>,
    pub columns: Vec<OutputColumn>,
    pub predicates: Vec<ScalarId>,
    pub required_columns: Option<Vec<String>>,
    /// Per-scan dictionary plan hints. Populated by the Task 7
    /// `LowCardinalityDictionaryRewrite` rule on the logical side and
    /// propagated to `ScanOp` by `ScanToPhysical`.
    pub dict_columns: Vec<ScanDictionaryColumn>,
    /// Synthetic typed columns materialized from variant paths during scan.
    /// Populated by `VariantPathPushdownRule` and propagated to
    /// `ScanOp` by `ScanToPhysical`.
    pub variant_columns: Vec<ScanVariantColumn>,
    /// When this scan was injected by the MvRewrite rule, the source MV name
    /// (shown in EXPLAIN as `rewritten with mv: <name>`). None for all
    /// user-written scans.
    pub mv_rewritten_from: Option<String>,
}

#[derive(Clone, Debug)]
pub(crate) struct FilterOp {
    pub predicate: ScalarId,
}

#[derive(Clone, Debug)]
pub(crate) struct ProjectOp {
    pub items: Vec<ScalarProjectItem>,
    pub output_qualifier: Option<String>,
}

#[derive(Clone, Debug)]
pub(crate) struct LogicalAggregateOp {
    pub stage: AggStage,
    pub group_by: Vec<ScalarId>,
    pub aggregates: Vec<ScalarAggregateSpec>,
    pub output_columns: Vec<OutputColumn>,
    pub is_merge: Vec<bool>,
    pub is_split: bool,
}

impl LogicalAggregateOp {
    pub(crate) fn single(
        group_by: Vec<ScalarId>,
        aggregates: Vec<ScalarAggregateSpec>,
        output_columns: Vec<OutputColumn>,
    ) -> Self {
        let is_merge = vec![false; aggregates.len()];
        Self {
            stage: AggStage::Single,
            group_by,
            aggregates,
            output_columns,
            is_merge,
            is_split: false,
        }
    }

    pub(crate) fn staged(
        stage: AggStage,
        group_by: Vec<ScalarId>,
        aggregates: Vec<ScalarAggregateSpec>,
        output_columns: Vec<OutputColumn>,
        is_merge: Vec<bool>,
        is_split: bool,
    ) -> Self {
        debug_assert_eq!(aggregates.len(), is_merge.len());
        Self {
            stage,
            group_by,
            aggregates,
            output_columns,
            is_merge,
            is_split,
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct AggregateStateMergeOp {
    pub(crate) group_key_names: Vec<String>,
    pub(crate) aggregate_state_names: Vec<String>,
    pub(crate) change_op_column: String,
    pub(crate) output_columns: Vec<OutputColumn>,
}

#[derive(Clone, Debug)]
pub(crate) struct LogicalJoinOp {
    pub join_type: JoinKind,
    pub condition: Option<ScalarId>,
}

#[derive(Clone, Debug)]
pub(crate) struct SortOp {
    pub items: Vec<SortKey>,
    /// Set by `build_window_and_project` when this Sort was inserted as a
    /// precursor to a Window (PARTITION BY + ORDER BY). Empty otherwise.
    /// When non-empty, the sort can be done locally per partition after a
    /// HASH EXCHANGE keyed on these columns — no global Gather needed.
    /// Mirrors StarRocks's `TSortNode.analytic_partition_exprs`. Stored as
    /// `ScalarId` handles into `Memo.scalars`; bridge/codegen phases materialize
    /// them when they need analyzer expressions or wire-level `TExpr`s.
    pub analytic_partition_exprs: Vec<ScalarId>,
    /// Set by RankingWindowPredicatePushdown: per-partition rank cap + ranking
    /// kind. `None` ⇒ ordinary sort. See OQ-13 ranking-window design spec §4.
    pub partition_limit: Option<usize>,
    pub topn_type: Option<crate::exec::node::sort::SortTopNType>,
}

#[derive(Clone, Debug)]
pub(crate) struct LimitOp {
    pub limit: Option<i64>,
    pub offset: Option<i64>,
}

#[derive(Clone, Debug)]
pub(crate) struct TopNOp {
    pub items: Vec<SortKey>,
    pub limit: Option<i64>,
    pub offset: Option<i64>,
    pub phase: TopNPhase,
    pub is_split: bool,
}

#[derive(Clone, Debug)]
pub(crate) struct WindowOp {
    pub window_exprs: Vec<ScalarWindowSpec>,
    pub output_columns: Vec<OutputColumn>,
}

#[derive(Clone, Debug)]
pub(crate) struct UnionOp {
    pub all: bool,
    pub output_columns: Vec<OutputColumn>,
    pub child_output_columns: Vec<Vec<OutputColumn>>,
}

#[derive(Clone, Debug)]
pub(crate) struct IntersectOp {
    pub output_columns: Vec<OutputColumn>,
    pub child_output_columns: Vec<Vec<OutputColumn>>,
}

#[derive(Clone, Debug)]
pub(crate) struct ExceptOp {
    pub output_columns: Vec<OutputColumn>,
    pub child_output_columns: Vec<Vec<OutputColumn>>,
}

#[derive(Clone, Debug)]
pub(crate) struct ValuesOp {
    pub rows: Vec<Vec<ScalarId>>,
    pub columns: Vec<OutputColumn>,
}

#[derive(Clone, Debug)]
pub(crate) struct GenerateSeriesOp {
    pub start: i64,
    pub end: i64,
    pub step: i64,
    pub column_name: String,
    pub alias: Option<String>,
    pub output_column_id: crate::sql::column_id::ColumnId,
}

#[derive(Clone, Debug)]
pub(crate) struct TableFunctionOp {
    pub function_name: String,
    pub args: Vec<ScalarId>,
    pub output_columns: Vec<OutputColumn>,
    pub alias: Option<String>,
    pub is_left_join: bool,
}

#[derive(Clone, Debug)]
pub(crate) struct RepeatOp {
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

#[derive(Clone, Debug)]
pub(crate) struct CTEAnchorOp {
    pub cte_id: CteId,
}

#[derive(Clone, Debug)]
pub(crate) struct CTEProduceOp {
    pub cte_id: CteId,
    pub output_columns: Vec<OutputColumn>,
}

#[derive(Clone, Debug)]
pub(crate) struct CTEConsumeOp {
    pub cte_id: CteId,
    pub alias: String,
    pub output_columns: Vec<OutputColumn>,
}

#[derive(Clone, Debug)]
pub(crate) struct AssertOneRowOp {
    /// Original subquery text used in the runtime error message.
    pub subquery_text: String,
}

/// Logical dictionary-decode operator. Maps dictionary-encoded child columns
/// back to their string form. Produced exclusively by the dictionary-rewrite
/// rule (Task 7); the implementation rule `DecodeToPhysical` lowers it to
/// `DecodeOp`.
///
/// `output_columns` mirrors the input group's output columns with each
/// `dict_column` swapped for its `string_column`. Without it
/// `derive_output_columns` would surface the child's pre-decode names
/// (the dict columns) to consumers, and parent lookups for the
/// string column would fail to resolve.
#[derive(Clone, Debug)]
pub(crate) struct DecodeOp {
    pub mappings: Vec<DecodeMapping>,
    pub output_columns: Vec<OutputColumn>,
}

/// Apply (correlated subquery) operator. Eliminated by the SubqueryRewrite
/// stage before memo conversion; must not reach derive/cost/codegen.
///
/// `TypedExpr` fields from `LogicalApplyNode` are interned as `ScalarId`.
/// `HashSet<ColumnId>` (non-scalar, pure metadata) is kept as-is.
#[derive(Clone, Debug)]
pub(crate) struct ApplyOp {
    pub kind: ApplyKind,
    /// Interned scalar for the subquery expression.
    pub subquery_expr: ScalarId,
    pub output_column: OutputColumn,
    pub inner_output_column_id: ColumnId,
    pub correlation_column_ids: Vec<ColumnId>,
    /// Interned scalars for the correlation conjuncts.
    pub correlation_conjuncts: Vec<ScalarId>,
    /// Interned scalar for the residual predicate, if any.
    pub residual_predicate: Option<ScalarId>,
    pub need_check_max_rows: bool,
    pub use_semi_anti: bool,
    pub uncorrelated_outer_predicate_columns: HashSet<ColumnId>,
}

/// IMV delta marker operator. Eliminated during the IMV rewrite stage;
/// must not reach derive/cost/codegen.
#[derive(Clone, Debug)]
pub(crate) struct ImvDeltaOp {
    pub is_root: bool,
    pub action_column: Option<ColumnId>,
    pub branch_scope: Option<BranchScope>,
}

/// IMV version marker operator. Eliminated during the IMV rewrite stage;
/// must not reach derive/cost/codegen.
#[derive(Clone, Debug)]
pub(crate) struct ImvVersionOp {
    pub version_ref: ImvVersionRef,
}

// ---------------------------------------------------------------------------
// Physical operator structs
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub(crate) struct PhysicalHashJoinOp {
    pub join_type: JoinKind,
    pub eq_conditions: Vec<PhysicalHashJoinEqCondition>,
    pub other_condition: Option<ScalarId>,
    pub distribution: JoinDistribution,
}

#[derive(Clone, Debug)]
pub(crate) struct PhysicalHashJoinEqCondition {
    pub left: ScalarId,
    pub right: ScalarId,
    pub null_safe: bool,
}

#[derive(Clone, Debug)]
pub(crate) struct PhysicalNestLoopJoinOp {
    pub join_type: JoinKind,
    pub condition: Option<ScalarId>,
}

#[derive(Clone, Debug)]
pub(crate) struct PhysicalHashAggregateOp {
    pub mode: AggMode,
    pub group_by: Vec<ScalarId>,
    pub aggregates: Vec<ScalarAggregateSpec>,
    pub output_columns: Vec<OutputColumn>,
    /// Per-aggregate merge flag. `true` → this phase applies the aggregate's
    /// merge function over an intermediate state slot from the child; `false`
    /// → this phase applies the update function over raw args from the child
    /// scope. Length must equal `aggregates.len()`.
    pub is_merge: Vec<bool>,
}

/// Distribution enforcer node.
#[derive(Clone, Debug)]
pub(crate) struct PhysicalDistributionOp {
    pub spec: super::property::DistributionSpec,
}

// ---------------------------------------------------------------------------
// Operator enum
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub(crate) enum Operator {
    // Logical operators
    LogicalScan(ScanOp),
    LogicalFilter(FilterOp),
    LogicalProject(ProjectOp),
    LogicalAggregate(LogicalAggregateOp),
    LogicalJoin(LogicalJoinOp),
    LogicalSort(SortOp),
    LogicalLimit(LimitOp),
    LogicalTopN(TopNOp),
    LogicalWindow(WindowOp),
    LogicalUnion(UnionOp),
    LogicalIntersect(IntersectOp),
    LogicalExcept(ExceptOp),
    LogicalValues(ValuesOp),
    LogicalGenerateSeries(GenerateSeriesOp),
    LogicalTableFunction(TableFunctionOp),
    LogicalRepeat(RepeatOp),
    LogicalCTEAnchor(CTEAnchorOp),
    LogicalCTEProduce(CTEProduceOp),
    LogicalCTEConsume(CTEConsumeOp),
    LogicalDecode(DecodeOp),
    LogicalAggregateStateMerge(AggregateStateMergeOp),
    LogicalAssertOneRow(AssertOneRowOp),
    /// Apply (correlated subquery). Eliminated by SubqueryRewrite before memo.
    LogicalApply(ApplyOp),
    /// IMV delta marker. Eliminated by the IMV rewrite stage before memo.
    LogicalImvDelta(ImvDeltaOp),
    /// IMV version marker. Eliminated by the IMV rewrite stage before memo.
    LogicalImvVersion(ImvVersionOp),

    // Physical operators
    PhysicalScan(ScanOp),
    PhysicalFilter(FilterOp),
    PhysicalProject(ProjectOp),
    PhysicalHashJoin(PhysicalHashJoinOp),
    PhysicalNestLoopJoin(PhysicalNestLoopJoinOp),
    PhysicalHashAggregate(PhysicalHashAggregateOp),
    PhysicalSort(SortOp),
    PhysicalLimit(LimitOp),
    PhysicalTopN(TopNOp),
    PhysicalWindow(WindowOp),
    PhysicalDistribution(PhysicalDistributionOp),
    PhysicalCTEAnchor(CTEAnchorOp),
    PhysicalCTEProduce(CTEProduceOp),
    PhysicalCTEConsume(CTEConsumeOp),
    PhysicalRepeat(RepeatOp),
    PhysicalUnion(UnionOp),
    PhysicalIntersect(IntersectOp),
    PhysicalExcept(ExceptOp),
    PhysicalValues(ValuesOp),
    PhysicalGenerateSeries(GenerateSeriesOp),
    PhysicalTableFunction(TableFunctionOp),
    PhysicalDecode(DecodeOp),
    PhysicalAggregateStateMerge(AggregateStateMergeOp),
    PhysicalAssertOneRow(AssertOneRowOp),
}

impl Operator {
    pub(crate) fn is_logical(&self) -> bool {
        matches!(
            self,
            Operator::LogicalScan(_)
                | Operator::LogicalFilter(_)
                | Operator::LogicalProject(_)
                | Operator::LogicalAggregate(_)
                | Operator::LogicalJoin(_)
                | Operator::LogicalSort(_)
                | Operator::LogicalLimit(_)
                | Operator::LogicalTopN(_)
                | Operator::LogicalWindow(_)
                | Operator::LogicalUnion(_)
                | Operator::LogicalIntersect(_)
                | Operator::LogicalExcept(_)
                | Operator::LogicalValues(_)
                | Operator::LogicalGenerateSeries(_)
                | Operator::LogicalTableFunction(_)
                | Operator::LogicalRepeat(_)
                | Operator::LogicalCTEAnchor(_)
                | Operator::LogicalCTEProduce(_)
                | Operator::LogicalCTEConsume(_)
                | Operator::LogicalDecode(_)
                | Operator::LogicalAggregateStateMerge(_)
                | Operator::LogicalAssertOneRow(_)
                | Operator::LogicalApply(_)
                | Operator::LogicalImvDelta(_)
                | Operator::LogicalImvVersion(_)
        )
    }

    pub(crate) fn is_physical(&self) -> bool {
        !self.is_logical()
    }
}

#[cfg(test)]
mod aggregate_stage_tests {
    use super::*;
    use crate::sql::analysis::{OutputColumn, TypedExpr};
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::scalar::{ScalarArena, intern_typed};

    fn output_column(id: u32, name: &str) -> OutputColumn {
        OutputColumn {
            column_id: ColumnId::new_for_test(id),
            name: name.to_string(),
            data_type: arrow::datatypes::DataType::Int64,
            nullable: false,
            is_internal: false,
        }
    }

    fn col_ref(id: u32, name: &str) -> TypedExpr {
        TypedExpr {
            kind: crate::sql::analysis::ExprKind::ColumnRef {
                column_id: ColumnId::new_for_test(id),
                qualifier: Some("t".to_string()),
                column: name.to_string(),
            },
            data_type: arrow::datatypes::DataType::Int64,
            nullable: false,
        }
    }

    fn scalar_col_ref(arena: &mut ScalarArena, id: u32, name: &str) -> ScalarId {
        intern_typed(arena, &col_ref(id, name))
    }

    fn count_call(arena: &mut ScalarArena) -> ScalarAggregateSpec {
        ScalarAggregateSpec {
            name: "count".to_string(),
            args: vec![scalar_col_ref(arena, 2, "v")],
            distinct: false,
            order_by: vec![],
        }
    }

    #[test]
    fn single_constructor_sets_unsplit_single_metadata() {
        let mut arena = ScalarArena::new();
        let op = LogicalAggregateOp::single(
            vec![scalar_col_ref(&mut arena, 1, "k")],
            vec![count_call(&mut arena)],
            vec![output_column(1, "k"), output_column(3, "count(v)")],
        );
        assert_eq!(op.stage, AggStage::Single);
        assert_eq!(op.stage.to_physical_mode(), AggMode::Single);
        assert_eq!(op.is_merge, vec![false]);
        assert!(!op.is_split);
    }

    #[test]
    fn staged_constructor_preserves_merge_flags_and_split_marker() {
        assert_eq!(AggStage::Local.to_physical_mode(), AggMode::Local);

        let mut arena = ScalarArena::new();
        let op = LogicalAggregateOp::staged(
            AggStage::Global,
            vec![scalar_col_ref(&mut arena, 1, "k")],
            vec![count_call(&mut arena)],
            vec![output_column(1, "k"), output_column(3, "count(v)")],
            vec![true],
            true,
        );
        assert_eq!(op.stage, AggStage::Global);
        assert_eq!(op.stage.to_physical_mode(), AggMode::Global);
        assert_eq!(op.is_merge, vec![true]);
        assert!(op.is_split);
    }
}
