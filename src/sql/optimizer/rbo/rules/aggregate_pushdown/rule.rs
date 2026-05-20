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
        Some(super::rewriter::rewrite(agg, push))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::analysis::OutputColumn;
    use crate::sql::catalog::{TableDef, TableStorage};
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
                    storage: TableStorage::LocalParquetFile {
                        path: std::path::PathBuf::from("/tmp/t.parquet"),
                    },
                },
                alias: None,
                columns: vec![OutputColumn {
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
}
