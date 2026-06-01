//! Operator enum for the Cascades optimizer.
//!
//! Logical operators mirror `LogicalPlan` node fields minus child references
//! (children are represented as `GroupId`s in `MExpr`).
//! Physical operators add physical execution decisions (distribution, agg mode).

use crate::sql::analysis::cte::CteId;
use crate::sql::analysis::{JoinKind, OutputColumn, ProjectItem, SortItem, TypedExpr};
use crate::sql::catalog::TableDef;
use crate::sql::planner::plan::{AggregateCall, DecodeMapping, WindowExpr};

pub(crate) use crate::sql::planner::plan::ScanDictionaryColumn;

// ---------------------------------------------------------------------------
// Physical decision enums
// ---------------------------------------------------------------------------

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) enum JoinDistribution {
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
pub(crate) struct LogicalScanOp {
    pub database: String,
    pub table: TableDef,
    pub alias: Option<String>,
    pub columns: Vec<OutputColumn>,
    pub predicates: Vec<TypedExpr>,
    pub required_columns: Option<Vec<String>>,
    /// Per-scan dictionary plan hints. Populated by the Task 7
    /// `LowCardinalityDictionaryRewrite` rule on the logical side and
    /// propagated to `PhysicalScanOp` by `ScanToPhysical`.
    pub dict_columns: Vec<ScanDictionaryColumn>,
}

#[derive(Clone, Debug)]
pub(crate) struct LogicalFilterOp {
    pub predicate: TypedExpr,
}

#[derive(Clone, Debug)]
pub(crate) struct LogicalProjectOp {
    pub items: Vec<ProjectItem>,
}

#[derive(Clone, Debug)]
pub(crate) struct LogicalAggregateOp {
    pub stage: AggStage,
    pub group_by: Vec<TypedExpr>,
    pub aggregates: Vec<AggregateCall>,
    pub output_columns: Vec<OutputColumn>,
    pub is_merge: Vec<bool>,
    pub is_split: bool,
}

impl LogicalAggregateOp {
    pub(crate) fn single(
        group_by: Vec<TypedExpr>,
        aggregates: Vec<AggregateCall>,
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
        group_by: Vec<TypedExpr>,
        aggregates: Vec<AggregateCall>,
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
pub(crate) struct LogicalJoinOp {
    pub join_type: JoinKind,
    pub condition: Option<TypedExpr>,
}

#[derive(Clone, Debug)]
pub(crate) struct LogicalSortOp {
    pub items: Vec<SortItem>,
    /// Set by `build_window_and_project` when this Sort was inserted as a
    /// precursor to a Window (PARTITION BY + ORDER BY). Empty otherwise.
    /// When non-empty, the sort can be done locally per partition after a
    /// HASH EXCHANGE keyed on these columns — no global Gather needed.
    /// Mirrors StarRocks's `TSortNode.analytic_partition_exprs`. Stored as
    /// `TypedExpr` (not `ColumnRef`) so the fragment builder can compile
    /// them back to wire-level `TExpr`s; the optimizer converts to
    /// `ColumnRef` on demand for distribution-property matching.
    pub analytic_partition_exprs: Vec<crate::sql::analysis::TypedExpr>,
}

#[derive(Clone, Debug)]
pub(crate) struct LogicalLimitOp {
    pub limit: Option<i64>,
    pub offset: Option<i64>,
}

#[derive(Clone, Debug)]
pub(crate) struct LogicalTopNOp {
    pub items: Vec<SortItem>,
    pub limit: Option<i64>,
    pub offset: Option<i64>,
    pub phase: TopNPhase,
    pub is_split: bool,
}

#[derive(Clone, Debug)]
pub(crate) struct LogicalWindowOp {
    pub window_exprs: Vec<WindowExpr>,
    pub output_columns: Vec<OutputColumn>,
}

#[derive(Clone, Debug)]
pub(crate) struct LogicalUnionOp {
    pub all: bool,
    pub output_columns: Vec<OutputColumn>,
}

#[derive(Clone, Debug)]
pub(crate) struct LogicalIntersectOp {
    pub output_columns: Vec<OutputColumn>,
}

#[derive(Clone, Debug)]
pub(crate) struct LogicalExceptOp {
    pub output_columns: Vec<OutputColumn>,
}

#[derive(Clone, Debug)]
pub(crate) struct LogicalValuesOp {
    pub rows: Vec<Vec<TypedExpr>>,
    pub columns: Vec<OutputColumn>,
}

#[derive(Clone, Debug)]
pub(crate) struct LogicalGenerateSeriesOp {
    pub start: i64,
    pub end: i64,
    pub step: i64,
    pub column_name: String,
    pub alias: Option<String>,
}

#[derive(Clone, Debug)]
pub(crate) struct LogicalTableFunctionOp {
    pub function_name: String,
    pub args: Vec<TypedExpr>,
    pub output_columns: Vec<OutputColumn>,
    pub alias: Option<String>,
    pub is_left_join: bool,
}

#[derive(Clone, Debug)]
pub(crate) struct LogicalRepeatOp {
    pub repeat_column_ref_list: Vec<Vec<String>>,
    pub grouping_ids: Vec<u64>,
    pub all_rollup_columns: Vec<String>,
    pub grouping_key_aliases: Vec<(String, String)>,
    pub grouping_fn_args: Vec<(String, Vec<String>)>,
}

#[derive(Clone, Debug)]
pub(crate) struct LogicalCTEAnchorOp {
    pub cte_id: CteId,
}

#[derive(Clone, Debug)]
pub(crate) struct LogicalCTEProduceOp {
    pub cte_id: CteId,
    pub output_columns: Vec<OutputColumn>,
}

#[derive(Clone, Debug)]
pub(crate) struct LogicalCTEConsumeOp {
    pub cte_id: CteId,
    pub alias: String,
    pub output_columns: Vec<OutputColumn>,
}

/// Logical dictionary-decode operator. Maps dictionary-encoded child columns
/// back to their string form. Produced exclusively by the dictionary-rewrite
/// rule (Task 7); the implementation rule `DecodeToPhysical` lowers it to
/// `PhysicalDecodeOp`.
///
/// `output_columns` mirrors the input group's output columns with each
/// `dict_column` swapped for its `string_column`. Without it
/// `derive_output_columns` would surface the child's pre-decode names
/// (the dict columns) to consumers, and parent lookups for the
/// string column would fail to resolve.
#[derive(Clone, Debug)]
pub(crate) struct LogicalDecodeOp {
    pub mappings: Vec<DecodeMapping>,
    pub output_columns: Vec<OutputColumn>,
}

// ---------------------------------------------------------------------------
// Physical operator structs
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub(crate) struct PhysicalScanOp {
    pub database: String,
    pub table: TableDef,
    pub alias: Option<String>,
    pub columns: Vec<OutputColumn>,
    pub predicates: Vec<TypedExpr>,
    pub required_columns: Option<Vec<String>>,
    /// Per-scan dictionary plan hints. Populated by the Task 7
    /// `LowCardinalityDictionaryRewrite` rule when a string column on this
    /// scan is eligible for low-cardinality rewriting. Codegen reads this to
    /// emit a hidden INT dict slot, a `TGlobalDict` payload on the owning
    /// fragment, and (for StarRocks scans) the
    /// `TLakeScanNode.dict_string_id_to_int_ids` mapping. Empty in all
    /// production paths today.
    #[allow(dead_code)] // Read by codegen when Task 7 populates it.
    pub dict_columns: Vec<ScanDictionaryColumn>,
}

#[derive(Clone, Debug)]
pub(crate) struct PhysicalFilterOp {
    pub predicate: TypedExpr,
}

#[derive(Clone, Debug)]
pub(crate) struct PhysicalProjectOp {
    pub items: Vec<ProjectItem>,
}

#[derive(Clone, Debug)]
pub(crate) struct PhysicalHashJoinOp {
    pub join_type: JoinKind,
    pub eq_conditions: Vec<PhysicalHashJoinEqCondition>,
    pub other_condition: Option<TypedExpr>,
    pub distribution: JoinDistribution,
}

#[derive(Clone, Debug)]
pub(crate) struct PhysicalHashJoinEqCondition {
    pub left: TypedExpr,
    pub right: TypedExpr,
    pub null_safe: bool,
}

#[derive(Clone, Debug)]
pub(crate) struct PhysicalNestLoopJoinOp {
    pub join_type: JoinKind,
    pub condition: Option<TypedExpr>,
}

#[derive(Clone, Debug)]
pub(crate) struct PhysicalHashAggregateOp {
    pub mode: AggMode,
    pub group_by: Vec<TypedExpr>,
    pub aggregates: Vec<AggregateCall>,
    pub output_columns: Vec<OutputColumn>,
    /// Per-aggregate merge flag. `true` → this phase applies the aggregate's
    /// merge function over an intermediate state slot from the child; `false`
    /// → this phase applies the update function over raw args from the child
    /// scope. Length must equal `aggregates.len()`.
    pub is_merge: Vec<bool>,
}

#[derive(Clone, Debug)]
pub(crate) struct PhysicalSortOp {
    pub items: Vec<SortItem>,
    /// Propagated from `LogicalSortOp::analytic_partition_exprs`. See the
    /// LogicalSortOp doc-comment for semantics.
    pub analytic_partition_exprs: Vec<crate::sql::analysis::TypedExpr>,
}

#[derive(Clone, Debug)]
pub(crate) struct PhysicalLimitOp {
    pub limit: Option<i64>,
    pub offset: Option<i64>,
}

#[derive(Clone, Debug)]
pub(crate) struct PhysicalTopNOp {
    pub items: Vec<SortItem>,
    pub limit: Option<i64>,
    pub offset: Option<i64>,
    pub phase: TopNPhase,
    pub is_split: bool,
}

#[derive(Clone, Debug)]
pub(crate) struct PhysicalWindowOp {
    pub window_exprs: Vec<WindowExpr>,
    pub output_columns: Vec<OutputColumn>,
}

/// Distribution enforcer node.
#[derive(Clone, Debug)]
pub(crate) struct PhysicalDistributionOp {
    pub spec: super::property::DistributionSpec,
}

#[derive(Clone, Debug)]
pub(crate) struct PhysicalCTEAnchorOp {
    pub cte_id: CteId,
}

#[derive(Clone, Debug)]
pub(crate) struct PhysicalCTEProduceOp {
    pub cte_id: CteId,
    pub output_columns: Vec<OutputColumn>,
}

#[derive(Clone, Debug)]
pub(crate) struct PhysicalCTEConsumeOp {
    pub cte_id: CteId,
    pub alias: String,
    pub output_columns: Vec<OutputColumn>,
}

#[derive(Clone, Debug)]
pub(crate) struct PhysicalRepeatOp {
    pub repeat_column_ref_list: Vec<Vec<String>>,
    pub grouping_ids: Vec<u64>,
    pub all_rollup_columns: Vec<String>,
    pub grouping_key_aliases: Vec<(String, String)>,
    pub grouping_fn_args: Vec<(String, Vec<String>)>,
}

#[derive(Clone, Debug)]
pub(crate) struct PhysicalUnionOp {
    pub all: bool,
    pub output_columns: Vec<OutputColumn>,
}

#[derive(Clone, Debug)]
pub(crate) struct PhysicalIntersectOp {
    pub output_columns: Vec<OutputColumn>,
}

#[derive(Clone, Debug)]
pub(crate) struct PhysicalExceptOp {
    pub output_columns: Vec<OutputColumn>,
}

#[derive(Clone, Debug)]
pub(crate) struct PhysicalValuesOp {
    pub rows: Vec<Vec<TypedExpr>>,
    pub columns: Vec<OutputColumn>,
}

#[derive(Clone, Debug)]
pub(crate) struct PhysicalGenerateSeriesOp {
    pub start: i64,
    pub end: i64,
    pub step: i64,
    pub column_name: String,
    pub alias: Option<String>,
}

#[derive(Clone, Debug)]
pub(crate) struct PhysicalTableFunctionOp {
    pub function_name: String,
    pub args: Vec<TypedExpr>,
    pub output_columns: Vec<OutputColumn>,
    pub alias: Option<String>,
    pub is_left_join: bool,
}

/// Physical counterpart of [`LogicalDecodeOp`]. The codegen step (Task 6)
/// turns this into a dictionary-decode execution node; Task 5 only routes
/// the operator through the optimizer.
///
/// `output_columns` is propagated verbatim from `LogicalDecodeOp` by the
/// `DecodeToPhysical` implementation rule.
#[derive(Clone, Debug)]
pub(crate) struct PhysicalDecodeOp {
    pub mappings: Vec<DecodeMapping>,
    pub output_columns: Vec<OutputColumn>,
}

// ---------------------------------------------------------------------------
// Operator enum
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub(crate) enum Operator {
    // Logical operators
    LogicalScan(LogicalScanOp),
    LogicalFilter(LogicalFilterOp),
    LogicalProject(LogicalProjectOp),
    LogicalAggregate(LogicalAggregateOp),
    LogicalJoin(LogicalJoinOp),
    LogicalSort(LogicalSortOp),
    LogicalLimit(LogicalLimitOp),
    LogicalTopN(LogicalTopNOp),
    LogicalWindow(LogicalWindowOp),
    LogicalUnion(LogicalUnionOp),
    LogicalIntersect(LogicalIntersectOp),
    LogicalExcept(LogicalExceptOp),
    LogicalValues(LogicalValuesOp),
    LogicalGenerateSeries(LogicalGenerateSeriesOp),
    LogicalTableFunction(LogicalTableFunctionOp),
    LogicalRepeat(LogicalRepeatOp),
    LogicalCTEAnchor(LogicalCTEAnchorOp),
    LogicalCTEProduce(LogicalCTEProduceOp),
    LogicalCTEConsume(LogicalCTEConsumeOp),
    LogicalDecode(LogicalDecodeOp),

    // Physical operators
    PhysicalScan(PhysicalScanOp),
    PhysicalFilter(PhysicalFilterOp),
    PhysicalProject(PhysicalProjectOp),
    PhysicalHashJoin(PhysicalHashJoinOp),
    PhysicalNestLoopJoin(PhysicalNestLoopJoinOp),
    PhysicalHashAggregate(PhysicalHashAggregateOp),
    PhysicalSort(PhysicalSortOp),
    PhysicalLimit(PhysicalLimitOp),
    PhysicalTopN(PhysicalTopNOp),
    PhysicalWindow(PhysicalWindowOp),
    PhysicalDistribution(PhysicalDistributionOp),
    PhysicalCTEAnchor(PhysicalCTEAnchorOp),
    PhysicalCTEProduce(PhysicalCTEProduceOp),
    PhysicalCTEConsume(PhysicalCTEConsumeOp),
    PhysicalRepeat(PhysicalRepeatOp),
    PhysicalUnion(PhysicalUnionOp),
    PhysicalIntersect(PhysicalIntersectOp),
    PhysicalExcept(PhysicalExceptOp),
    PhysicalValues(PhysicalValuesOp),
    PhysicalGenerateSeries(PhysicalGenerateSeriesOp),
    PhysicalTableFunction(PhysicalTableFunctionOp),
    PhysicalDecode(PhysicalDecodeOp),
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
    use crate::sql::planner::plan::AggregateCall;

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

    fn count_call() -> AggregateCall {
        AggregateCall {
            name: "count".to_string(),
            args: vec![col_ref(2, "v")],
            distinct: false,
            result_type: arrow::datatypes::DataType::Int64,
            order_by: vec![],
        }
    }

    #[test]
    fn single_constructor_sets_unsplit_single_metadata() {
        let op = LogicalAggregateOp::single(
            vec![col_ref(1, "k")],
            vec![count_call()],
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

        let op = LogicalAggregateOp::staged(
            AggStage::Global,
            vec![col_ref(1, "k")],
            vec![count_call()],
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
