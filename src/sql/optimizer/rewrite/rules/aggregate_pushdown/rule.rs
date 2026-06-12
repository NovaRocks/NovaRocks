//! AggregatePushdownRule entry point.

use std::collections::HashMap;
use std::sync::Arc;

use crate::sql::optimizer::rewrite::context::RewriteContext;
use crate::sql::optimizer::rewrite::phase::RewritePhase;
use crate::sql::optimizer::rewrite::result::RewriteResult;
use crate::sql::optimizer::rewrite::rule::LogicalRewriteRule as RewriteRule;
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

    fn phase(&self) -> RewritePhase {
        RewritePhase::StructuralRewrite
    }

    fn matches(&self, plan: &LogicalPlan, _ctx: &RewriteContext) -> bool {
        matches!(plan, LogicalPlan::Aggregate(_))
    }

    fn apply(&self, plan: LogicalPlan, ctx: &mut RewriteContext) -> Result<RewriteResult, String> {
        let agg = match &plan {
            LogicalPlan::Aggregate(a) => a,
            _ => return Ok(RewriteResult::Unchanged),
        };
        let Some(push) = super::collector::collect_push_plan(agg, &self.table_stats) else {
            return Ok(RewriteResult::Unchanged);
        };
        if !super::cost::should_push(&push, &self.table_stats) {
            return Ok(RewriteResult::Unchanged);
        }
        let factory = ctx
            .column_ref_factory()
            .ok_or_else(|| "AggregatePushdown requires ColumnRefFactory".to_string())?;
        let mut factory = factory.borrow_mut();
        Ok(RewriteResult::Changed(super::rewriter::rewrite(
            agg,
            push,
            &mut factory,
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::analysis::OutputColumn;
    use crate::sql::catalog::{ScanSource, TableDef};
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::rewrite::context::{RewriteConsumer, RewriteContext};
    use crate::sql::optimizer::rewrite::result::RewriteResult;
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
                    source: ScanSource::StarRocks {
                        db_id: 0,
                        table_id: 0,
                    },
                },
                alias: None,
                columns: vec![OutputColumn {
                    column_id: ColumnId::UNSET,
                    name: "id".into(),
                    data_type: DataType::Int32,
                    nullable: false,
                    is_internal: false,
                }],
                predicates: vec![],
                required_columns: None,
                dict_columns: vec![],
                variant_columns: vec![],
                required_output_columns: None,
            })),
            group_by: vec![],
            aggregates: vec![],
            output_columns: vec![],
            required_output_columns: None,
            already_pushed: false,
        })
    }

    #[test]
    fn stub_returns_none() {
        let rule = AggregatePushdownRule::new(Arc::new(HashMap::new()));
        let plan = dummy_aggregate();
        let mut ctx = RewriteContext::new(RewriteConsumer::Query);
        assert!(rule.matches(&plan, &ctx));
        assert!(matches!(
            rule.apply(plan, &mut ctx).unwrap(),
            RewriteResult::Unchanged
        ));
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
                    source: ScanSource::StarRocks {
                        db_id: 0,
                        table_id: 0,
                    },
                },
                alias: None,
                columns: cols
                    .iter()
                    .map(|n| OutputColumn {
                        column_id: ColumnId::UNSET,
                        name: (*n).into(),
                        data_type: DataType::Int64,
                        nullable: false,
                        is_internal: false,
                    })
                    .collect(),
                predicates: vec![],
                required_columns: None,
                dict_columns: vec![],
                variant_columns: vec![],
                required_output_columns: None,
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
                required_output_columns: None,
            })),
            group_by: vec![col("k")],
            aggregates: vec![AggregateCall {
                name: "sum".into(),
                args: vec![col("v")],
                distinct: false,
                result_type: DataType::Int64,
                order_by: vec![],
                output_column_id: ColumnId::UNSET,
            }],
            output_columns: vec![],
            required_output_columns: None,
            already_pushed: true, // <- key invariant
        });

        let rule = AggregatePushdownRule::new(Arc::new(HashMap::new()));
        assert!(
            matches!(
                rule.apply(plan, &mut RewriteContext::new(RewriteConsumer::Query))
                    .unwrap(),
                RewriteResult::Unchanged
            ),
            "must not re-fire on already_pushed"
        );
    }
}
