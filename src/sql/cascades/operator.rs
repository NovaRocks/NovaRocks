//! Operator enum for the Cascades optimizer.
//!
//! Logical operators mirror `LogicalPlan` node fields minus child references
//! (children are represented as `GroupId`s in `MExpr`).
//! Physical operators add physical execution decisions (distribution, agg mode).

use arrow::datatypes::DataType;

use crate::sql::catalog::TableDef;
use crate::sql::cte::CteId;
use crate::sql::ir::{JoinKind, OutputColumn, ProjectItem, SortItem, TypedExpr, WindowFrame};
use crate::sql::plan::{AggregateCall, WindowExpr};

// ---------------------------------------------------------------------------
// Physical decision enums
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub(crate) enum JoinDistribution {
    Shuffle,
    Broadcast,
    Colocate,
}

#[derive(Clone, Debug)]
pub(crate) enum AggMode {
    Single,
    Local,
    Global,
}

// ---------------------------------------------------------------------------
// Logical operator structs
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub(crate) struct LogicalScanOp {
    pub database: String,
    pub table: TableDef,
    pub alias: Option<String>,
    pub columns: Vec<OutputColumn>,
    pub predicates: Vec<TypedExpr>,
    pub required_columns: Option<Vec<String>>,
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
    pub group_by: Vec<TypedExpr>,
    pub aggregates: Vec<AggregateCall>,
    pub output_columns: Vec<OutputColumn>,
}

#[derive(Clone, Debug)]
pub(crate) struct LogicalJoinOp {
    pub join_type: JoinKind,
    pub condition: Option<TypedExpr>,
}

#[derive(Clone, Debug)]
pub(crate) struct LogicalSortOp {
    pub items: Vec<SortItem>,
}

#[derive(Clone, Debug)]
pub(crate) struct LogicalLimitOp {
    pub limit: Option<i64>,
    pub offset: Option<i64>,
}

#[derive(Clone, Debug)]
pub(crate) struct LogicalWindowOp {
    pub window_exprs: Vec<WindowExpr>,
    pub output_columns: Vec<OutputColumn>,
}

#[derive(Clone, Debug)]
pub(crate) struct LogicalUnionOp {
    pub all: bool,
}

#[derive(Clone, Debug)]
pub(crate) struct LogicalIntersectOp;

#[derive(Clone, Debug)]
pub(crate) struct LogicalExceptOp;

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
pub(crate) struct LogicalSubqueryAliasOp {
    pub alias: String,
    pub output_columns: Vec<OutputColumn>,
}

#[derive(Clone, Debug)]
pub(crate) struct LogicalRepeatOp {
    pub repeat_column_ref_list: Vec<Vec<String>>,
    pub grouping_ids: Vec<u64>,
    pub all_rollup_columns: Vec<String>,
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
    pub eq_conditions: Vec<(TypedExpr, TypedExpr)>,
    pub other_condition: Option<TypedExpr>,
    pub distribution: JoinDistribution,
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
}

#[derive(Clone, Debug)]
pub(crate) struct PhysicalSortOp {
    pub items: Vec<SortItem>,
}

#[derive(Clone, Debug)]
pub(crate) struct PhysicalLimitOp {
    pub limit: Option<i64>,
    pub offset: Option<i64>,
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
    pub grouping_fn_args: Vec<(String, Vec<String>)>,
}

#[derive(Clone, Debug)]
pub(crate) struct PhysicalUnionOp {
    pub all: bool,
}

#[derive(Clone, Debug)]
pub(crate) struct PhysicalIntersectOp;

#[derive(Clone, Debug)]
pub(crate) struct PhysicalExceptOp;

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
pub(crate) struct PhysicalSubqueryAliasOp {
    pub alias: String,
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
    LogicalWindow(LogicalWindowOp),
    LogicalUnion(LogicalUnionOp),
    LogicalIntersect(LogicalIntersectOp),
    LogicalExcept(LogicalExceptOp),
    LogicalValues(LogicalValuesOp),
    LogicalGenerateSeries(LogicalGenerateSeriesOp),
    LogicalSubqueryAlias(LogicalSubqueryAliasOp),
    LogicalRepeat(LogicalRepeatOp),
    LogicalCTEAnchor(LogicalCTEAnchorOp),
    LogicalCTEProduce(LogicalCTEProduceOp),
    LogicalCTEConsume(LogicalCTEConsumeOp),

    // Physical operators
    PhysicalScan(PhysicalScanOp),
    PhysicalFilter(PhysicalFilterOp),
    PhysicalProject(PhysicalProjectOp),
    PhysicalHashJoin(PhysicalHashJoinOp),
    PhysicalNestLoopJoin(PhysicalNestLoopJoinOp),
    PhysicalHashAggregate(PhysicalHashAggregateOp),
    PhysicalSort(PhysicalSortOp),
    PhysicalLimit(PhysicalLimitOp),
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
    PhysicalSubqueryAlias(PhysicalSubqueryAliasOp),
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
                | Operator::LogicalWindow(_)
                | Operator::LogicalUnion(_)
                | Operator::LogicalIntersect(_)
                | Operator::LogicalExcept(_)
                | Operator::LogicalValues(_)
                | Operator::LogicalGenerateSeries(_)
                | Operator::LogicalSubqueryAlias(_)
                | Operator::LogicalRepeat(_)
                | Operator::LogicalCTEAnchor(_)
                | Operator::LogicalCTEProduce(_)
                | Operator::LogicalCTEConsume(_)
        )
    }

    pub(crate) fn is_physical(&self) -> bool {
        !self.is_logical()
    }
}
