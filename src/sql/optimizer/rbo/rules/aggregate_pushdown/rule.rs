//! AggregatePushdownRule entry point.

use std::collections::HashMap;
use std::sync::Arc;

use crate::sql::optimizer::rbo::rule::RewriteRule;
use crate::sql::optimizer::statistics::TableStatistics;
use crate::sql::planner::plan::LogicalPlan;

#[allow(dead_code)]
pub(crate) struct AggregatePushdownRule {
    table_stats: Arc<HashMap<String, TableStatistics>>,
}

impl AggregatePushdownRule {
    #[allow(dead_code)]
    pub(crate) fn new(table_stats: Arc<HashMap<String, TableStatistics>>) -> Self {
        Self { table_stats }
    }
}

impl RewriteRule for AggregatePushdownRule {
    fn name(&self) -> &'static str {
        "AggregatePushdown"
    }

    fn matches(&self, plan: &LogicalPlan) -> bool {
        matches!(plan, LogicalPlan::Aggregate(_))
    }

    fn apply(&self, plan: LogicalPlan) -> Option<LogicalPlan> {
        let agg = match &plan {
            LogicalPlan::Aggregate(a) => a,
            _ => return None,
        };
        let push = super::collector::collect_push_plan(agg, &self.table_stats)?;
        if !super::cost::should_push(&push, &self.table_stats) {
            return None;
        }
        Some(super::rewriter::rewrite(agg, push))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::analysis::OutputColumn;
    use crate::sql::catalog::{TableDef, ScanSource};
    use crate::sql::column_id::ColumnId;
    use crate::sql::planner::plan::{AggregateNode, ScanNode};
    use arrow::datatypes::DataType;

    fn dummy_aggregate() -> LogicalPlan {
        LogicalPlan::Aggregate(AggregateNode {
            input: Box::new(LogicalPlan::Scan(ScanNode {
                database: "db".into(),
                table: TableDef {
                    name: "t".into(),
                    columns: vec![],
                    iceberg_row_lineage_metadata_columns: vec![],
                    iceberg_table: None,
                    source: ScanSource::ManagedLake,
                },
                alias: None,
                columns: vec![OutputColumn {
                    column_id: ColumnId::UNSET,
                    name: "id".into(),
                    data_type: DataType::Int32,
                    nullable: false,
                }],
                predicates: vec![],
                required_columns: None,
            })),
            group_by: vec![],
            aggregates: vec![],
            output_columns: vec![],
            already_pushed: false,
        })
    }

    #[test]
    fn stub_returns_none() {
        let rule = AggregatePushdownRule::new(Arc::new(HashMap::new()));
        let plan = dummy_aggregate();
        assert!(rule.matches(&plan));
        assert!(rule.apply(plan).is_none());
    }

    #[test]
    fn idempotent_does_not_repush_already_pushed_plan() {
        use crate::sql::analysis::{BinOp, ExprKind, JoinKind, TypedExpr};
        use crate::sql::planner::plan::{AggregateCall, JoinNode};

        fn col(name: &str) -> TypedExpr {
            TypedExpr {
                kind: ExprKind::ColumnRef {
                    column_id: ColumnId::UNSET,
                    qualifier: None,
                    column: name.into(),
                },
                data_type: DataType::Int64,
                nullable: true,
            }
        }

        fn scan(name: &str, cols: &[&str]) -> LogicalPlan {
            LogicalPlan::Scan(ScanNode {
                database: "db".into(),
                table: TableDef {
                    name: name.into(),
                    columns: vec![],
                    iceberg_row_lineage_metadata_columns: vec![],
                    iceberg_table: None,
                    source: ScanSource::ManagedLake,
                },
                alias: None,
                columns: cols
                    .iter()
                    .map(|n| OutputColumn {
                        column_id: ColumnId::UNSET,
                        name: (*n).into(),
                        data_type: DataType::Int64,
                        nullable: false,
                    })
                    .collect(),
                predicates: vec![],
                required_columns: None,
            })
        }

        // Build a plan with already_pushed = true. The rule must reject.
        let plan = LogicalPlan::Aggregate(AggregateNode {
            input: Box::new(LogicalPlan::Join(JoinNode {
                left: Box::new(scan("a", &["k", "v"])),
                right: Box::new(scan("b", &["k"])),
                join_type: JoinKind::Inner,
                condition: Some(TypedExpr {
                    kind: ExprKind::BinaryOp {
                        left: Box::new(col("k")),
                        op: BinOp::Eq,
                        right: Box::new(col("k")),
                    },
                    data_type: DataType::Boolean,
                    nullable: false,
                }),
            })),
            group_by: vec![col("k")],
            aggregates: vec![AggregateCall {
                name: "sum".into(),
                args: vec![col("v")],
                distinct: false,
                result_type: DataType::Int64,
                order_by: vec![],
            }],
            output_columns: vec![],
            already_pushed: true, // <- key invariant
        });

        let rule = AggregatePushdownRule::new(Arc::new(HashMap::new()));
        assert!(
            rule.apply(plan).is_none(),
            "must not re-fire on already_pushed"
        );
    }
}
