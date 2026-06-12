//! `ExistentialApplyToJoin` - EXISTS / NOT EXISTS -> LeftSemi / LeftAnti join.
//!
//! Self-contained: reads the inner subquery's WHERE directly (no dependency on
//! PushDownApplyFilter). Correlated EXISTS becomes
//! `outer LEFT SEMI JOIN inner ON <normalized correlation predicate>`;
//! NOT EXISTS -> LEFT ANTI; uncorrelated -> semi/anti ON true.

use super::predicate_apply_util::{lift_correlated_inner, literal_true};
use crate::sql::analysis::JoinKind;
use crate::sql::optimizer::rewrite::context::RewriteContext;
use crate::sql::optimizer::rewrite::phase::RewritePhase;
use crate::sql::optimizer::rewrite::result::RewriteResult;
use crate::sql::optimizer::rewrite::rule::LogicalRewriteRule;
use crate::sql::planner::plan::{ApplyKind, JoinNode, LogicalPlan};

#[allow(dead_code)] // Registered by Task 6.
pub(crate) struct ExistentialApplyToJoin;

impl LogicalRewriteRule for ExistentialApplyToJoin {
    fn name(&self) -> &'static str {
        "ExistentialApplyToJoin"
    }

    fn phase(&self) -> RewritePhase {
        RewritePhase::StructuralRewrite
    }

    fn matches(&self, plan: &LogicalPlan, _ctx: &RewriteContext) -> bool {
        matches!(plan, LogicalPlan::Apply(a) if matches!(a.kind, ApplyKind::Exists { .. }))
    }

    fn apply(&self, plan: LogicalPlan, _ctx: &mut RewriteContext) -> Result<RewriteResult, String> {
        let LogicalPlan::Apply(a) = plan else {
            return Ok(RewriteResult::Unchanged);
        };
        let negated = match a.kind {
            ApplyKind::Exists { negated } => negated,
            _ => return Ok(RewriteResult::Unchanged),
        };
        let join_type = if negated {
            JoinKind::LeftAnti
        } else {
            JoinKind::LeftSemi
        };

        let (right, condition) = if a.correlation_column_ids.is_empty() {
            (*a.right, literal_true())
        } else {
            let Some(lifted) = lift_correlated_inner(*a.right, &a.correlation_column_ids) else {
                return Ok(RewriteResult::Unchanged);
            };
            let Some(pred) = lifted.on_predicate else {
                return Ok(RewriteResult::Unchanged);
            };
            (lifted.right, pred)
        };

        Ok(RewriteResult::Changed(LogicalPlan::Join(JoinNode {
            left: a.left,
            right: Box::new(right),
            join_type,
            condition: Some(condition),
            required_output_columns: None,
        })))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use arrow::datatypes::DataType;

    use super::*;
    use crate::sql::analysis::{
        BinOp, ExprKind, JoinKind, LiteralValue, OutputColumn, ProjectItem, TypedExpr,
    };
    use crate::sql::catalog::{ScanSource, TableDef};
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::rewrite::result::RewriteResult;
    use crate::sql::planner::plan::{
        ApplyKind, ApplyNode, FilterNode, JoinNode, ProjectNode, ScanNode,
    };

    const OUTER_K: ColumnId = ColumnId(1);
    const INNER_K: ColumnId = ColumnId(2);
    const EXISTS_OUT: ColumnId = ColumnId(3);
    const CONST_ONE: ColumnId = ColumnId(4);

    fn ctx() -> RewriteContext {
        RewriteContext::for_query(Vec::<String>::new())
    }

    fn output_column(id: ColumnId, name: &str, data_type: DataType) -> OutputColumn {
        OutputColumn {
            column_id: id,
            name: name.to_string(),
            data_type,
            nullable: false,
            is_internal: false,
        }
    }

    fn table_def(name: &str) -> TableDef {
        TableDef {
            name: name.to_string(),
            columns: vec![],
            iceberg_row_lineage_metadata_columns: vec![],
            source: ScanSource::StarRocks {
                db_id: 0,
                table_id: 0,
            },
        }
    }

    fn scan(table: &str, id: ColumnId) -> LogicalPlan {
        LogicalPlan::Scan(ScanNode {
            database: "default".to_string(),
            table: table_def(table),
            alias: None,
            columns: vec![output_column(id, "k", DataType::Int64)],
            predicates: vec![],
            required_columns: None,
            dict_columns: vec![],
            variant_columns: vec![],
            required_output_columns: None,
        })
    }

    fn col_ref(id: ColumnId, name: &str) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: id,
                qualifier: None,
                column: name.to_string(),
            },
            data_type: DataType::Int64,
            nullable: false,
        }
    }

    fn bool_expr() -> TypedExpr {
        TypedExpr {
            kind: ExprKind::Literal(LiteralValue::Bool(true)),
            data_type: DataType::Boolean,
            nullable: false,
        }
    }

    fn exists_output() -> OutputColumn {
        OutputColumn {
            column_id: EXISTS_OUT,
            name: "exists".to_string(),
            data_type: DataType::Boolean,
            nullable: false,
            is_internal: true,
        }
    }

    fn correlation_predicate() -> TypedExpr {
        TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(col_ref(INNER_K, "k")),
                op: BinOp::Eq,
                right: Box::new(col_ref(OUTER_K, "k")),
            },
            data_type: DataType::Boolean,
            nullable: false,
        }
    }

    fn correlated_inner() -> LogicalPlan {
        LogicalPlan::Project(ProjectNode {
            input: Box::new(LogicalPlan::Filter(FilterNode {
                input: Box::new(scan("inner", INNER_K)),
                predicate: correlation_predicate(),
                required_output_columns: None,
            })),
            items: vec![ProjectItem {
                expr: col_ref(INNER_K, "k"),
                output_name: "k".to_string(),
                output_column_id: INNER_K,
            }],
            output_qualifier: None,
            required_output_columns: None,
        })
    }

    fn correlated_select_one_inner() -> LogicalPlan {
        LogicalPlan::Project(ProjectNode {
            input: Box::new(LogicalPlan::Filter(FilterNode {
                input: Box::new(scan("inner", INNER_K)),
                predicate: correlation_predicate(),
                required_output_columns: None,
            })),
            items: vec![ProjectItem {
                expr: TypedExpr {
                    kind: ExprKind::Literal(LiteralValue::Int(1)),
                    data_type: DataType::Int64,
                    nullable: false,
                },
                output_name: "1".to_string(),
                output_column_id: CONST_ONE,
            }],
            output_qualifier: None,
            required_output_columns: None,
        })
    }

    fn correlated_project_scan_inner() -> LogicalPlan {
        LogicalPlan::Project(ProjectNode {
            input: Box::new(scan("inner", INNER_K)),
            items: vec![ProjectItem {
                expr: col_ref(INNER_K, "k"),
                output_name: "k".to_string(),
                output_column_id: INNER_K,
            }],
            output_qualifier: None,
            required_output_columns: None,
        })
    }

    fn exists_apply(negated: bool, right: LogicalPlan, correlated: bool) -> LogicalPlan {
        LogicalPlan::Apply(ApplyNode {
            left: Box::new(scan("outer", OUTER_K)),
            right: Box::new(right),
            kind: ApplyKind::Exists { negated },
            subquery_expr: bool_expr(),
            output_column: exists_output(),
            inner_output_column_id: INNER_K,
            correlation_column_ids: if correlated { vec![OUTER_K] } else { vec![] },
            correlation_conjuncts: vec![],
            residual_predicate: None,
            need_check_max_rows: false,
            use_semi_anti: true,
            uncorrelated_outer_predicate_columns: HashSet::new(),
            required_output_columns: None,
        })
    }

    fn rewrite(plan: LogicalPlan) -> LogicalPlan {
        let rule = ExistentialApplyToJoin;
        let mut ctx = ctx();
        assert!(rule.matches(&plan, &ctx));
        match rule.apply(plan, &mut ctx).expect("rewrite must not error") {
            RewriteResult::Changed(plan) => plan,
            other => panic!("expected Changed, got: {other:?}"),
        }
    }

    fn assert_join(plan: LogicalPlan, expected_kind: JoinKind) -> JoinNode {
        assert!(
            !contains_apply(&plan),
            "result must not contain Apply: {plan:?}"
        );
        let LogicalPlan::Join(join) = plan else {
            panic!("expected Join, got: {plan:?}");
        };
        assert_eq!(join.join_type, expected_kind);
        assert!(join.condition.is_some(), "join condition must exist");
        join
    }

    fn assert_correlation_condition(condition: &TypedExpr) {
        let ExprKind::BinaryOp { left, op, right } = &condition.kind else {
            panic!("expected binary condition, got: {condition:?}");
        };
        assert_eq!(*op, BinOp::Eq);
        assert_column_id(left, OUTER_K);
        assert_column_id(right, INNER_K);
    }

    fn assert_true_condition(condition: &TypedExpr) {
        assert!(matches!(
            condition.kind,
            ExprKind::Literal(LiteralValue::Bool(true))
        ));
        assert_eq!(condition.data_type, DataType::Boolean);
        assert!(!condition.nullable);
    }

    fn assert_column_id(expr: &TypedExpr, expected: ColumnId) {
        let ExprKind::ColumnRef { column_id, .. } = &expr.kind else {
            panic!("expected column ref, got: {expr:?}");
        };
        assert_eq!(*column_id, expected);
    }

    fn contains_apply(plan: &LogicalPlan) -> bool {
        match plan {
            LogicalPlan::Apply(_) => true,
            LogicalPlan::Filter(n) => contains_apply(&n.input),
            LogicalPlan::Project(n) => contains_apply(&n.input),
            LogicalPlan::Join(n) => contains_apply(&n.left) || contains_apply(&n.right),
            _ => false,
        }
    }

    fn project_outputs_column(plan: &LogicalPlan, expected: ColumnId) -> bool {
        let LogicalPlan::Project(project) = plan else {
            return false;
        };
        project
            .items
            .iter()
            .any(|item| item.output_column_id == expected)
    }

    #[test]
    fn exists_correlated_emits_left_semi() {
        let plan = rewrite(exists_apply(false, correlated_inner(), true));
        let join = assert_join(plan, JoinKind::LeftSemi);

        assert_correlation_condition(join.condition.as_ref().unwrap());
        let LogicalPlan::Project(project) = join.right.as_ref() else {
            panic!("expected Project right, got: {:?}", join.right);
        };
        assert!(matches!(project.input.as_ref(), LogicalPlan::Scan(_)));
    }

    #[test]
    fn exists_correlated_select_one_exposes_inner_key_for_join_on() {
        let plan = rewrite(exists_apply(false, correlated_select_one_inner(), true));
        let join = assert_join(plan, JoinKind::LeftSemi);

        assert_correlation_condition(join.condition.as_ref().unwrap());
        assert!(
            project_outputs_column(join.right.as_ref(), INNER_K),
            "right child must expose INNER_K referenced by the join ON"
        );
    }

    #[test]
    fn exists_correlated_project_scan_returns_unchanged() {
        let rule = ExistentialApplyToJoin;
        let mut ctx = ctx();
        let plan = exists_apply(false, correlated_project_scan_inner(), true);

        let result = rule.apply(plan, &mut ctx).expect("rewrite must not error");

        assert!(matches!(result, RewriteResult::Unchanged));
    }

    #[test]
    fn not_exists_correlated_emits_left_anti() {
        let plan = rewrite(exists_apply(true, correlated_inner(), true));
        let join = assert_join(plan, JoinKind::LeftAnti);

        assert_correlation_condition(join.condition.as_ref().unwrap());
        let LogicalPlan::Project(project) = join.right.as_ref() else {
            panic!("expected Project right, got: {:?}", join.right);
        };
        assert!(matches!(project.input.as_ref(), LogicalPlan::Scan(_)));
    }

    #[test]
    fn exists_uncorrelated_emits_left_semi_on_true() {
        let plan = rewrite(exists_apply(false, scan("inner", INNER_K), false));
        let join = assert_join(plan, JoinKind::LeftSemi);

        assert_true_condition(join.condition.as_ref().unwrap());
        assert!(matches!(join.right.as_ref(), LogicalPlan::Scan(_)));
    }

    #[test]
    fn not_exists_uncorrelated_emits_left_anti_on_true() {
        let plan = rewrite(exists_apply(true, scan("inner", INNER_K), false));
        let join = assert_join(plan, JoinKind::LeftAnti);

        assert_true_condition(join.condition.as_ref().unwrap());
        assert!(matches!(join.right.as_ref(), LogicalPlan::Scan(_)));
    }
}
